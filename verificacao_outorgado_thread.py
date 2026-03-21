# -*- coding: utf-8 -*-
"""
Módulo: verificacao_outorgado_thread.py
=========================================
Thread assíncrona de verificação de excedência de consumo mensal
em relação ao volume outorgado.

Processo em três etapas sequenciais:
  1. Agrega consumo_diario por interferência para o mês/ano solicitado,
     excluindo registros de teste (rótulo terminando em '_teste');
  2. Busca volumes outorgados mensais de view_volume_outorgado
     via CASE por coluna de mês;
  3. Compara e retorna interferências com consumo > outorgado,
     ordenadas pelo maior excesso absoluto.

Suporta cancelamento cooperativo: cancelar() envia conn.cancel()
ao PostgreSQL para interromper a query em curso.

Sinais:
  resultado_signal(list, str, int)  — lista de alertas, nome do mês, ano
  erro_signal(str)                  — mensagem de exceção com traceback
  progresso_signal(str)             — etapa em andamento

Autor : SFI/ANA
Versão: 2.0 – Março/2026
"""

# ---------------------------------------------------------------------------
# Imports Qt – threading e sinais
# ---------------------------------------------------------------------------
from qgis.PyQt.QtCore import QThread, pyqtSignal

# ---------------------------------------------------------------------------
# Imports Python padrão
# ---------------------------------------------------------------------------
import psycopg2
import traceback
from datetime import date, timedelta
import calendar


class VerificacaoOutorgadoThread(QThread):
    """Thread assíncrona para verificação de consumo versus volume outorgado.
 
    Suporta dois modos de operação (controlado pelo parâmetro ``modo``):
 
    **Modo 'mensal'** (padrão — comportamento original):
        Analisa um único mês/ano. O outorgado utilizado é o volume mensal
        da ``view_volume_outorgado`` para o mês especificado.
 
    **Modo 'por_periodo'** (novo):
        Analisa um intervalo de datas ``[data_inicio, data_fim]``. O outorgado
        é calculado proporcionalmente: para cada mês parcialmente coberto pelo
        intervalo, multiplica-se o volume mensal outorgado pela fração de dias
        do mês que está dentro do período. A soma dessas parcelas representa
        o outorgado total pro-rata para o intervalo.
 
    Em ambos os modos, o limiar de alerta é ``consumo > outorgado`` (100%).
    Os resultados incluem o percentual de excesso calculado como
    ``(consumo / outorgado − 1) × 100``.
 
    Signals:
        resultado_signal (list, str, int):
            Lista de tuplas ``(cod_interf, cnarh, usuario, operador, rotulos,
            consumo, outorgado, percentual_excesso)``, rótulo do período e
            ano (0 no modo por_periodo).
        erro_signal (str): Mensagem de exceção com traceback.
        progresso_signal (str): Etapa em andamento.
 
    Attributes:
        conn (psycopg2.connection): Conexão principal; usada só para DSN.
        mes (int): Mês de referência — usado apenas no modo 'mensal'.
        ano (int): Ano de referência — usado apenas no modo 'mensal'.
        nome_mes (str): Rótulo do período; repassado ao signal de resultado.
        senha (str): Credencial para abertura da conexão dedicada.
        modo (str): ``'mensal'`` (padrão) ou ``'por_periodo'``.
        data_inicio (date | None): Data de início — obrigatório em por_periodo.
        data_fim (date | None): Data de fim — obrigatório em por_periodo.
        thread_conn (psycopg2.connection | None): Conexão exclusiva da thread.
        _cancelado (bool): Flag de cancelamento cooperativo.
    """
 
    resultado_signal = pyqtSignal(list, str, int)
    erro_signal      = pyqtSignal(str)
    progresso_signal = pyqtSignal(str)
 
    def __init__(self, conn, mes, ano, nome_mes, senha,
                 modo='mensal', data_inicio=None, data_fim=None):
        super().__init__()
        self.conn        = conn
        self.mes         = mes
        self.ano         = ano
        self.nome_mes    = nome_mes
        self.senha       = senha
        self.modo        = modo          # 'mensal' | 'por_periodo'
        self.data_inicio = data_inicio   # date object — modo por_periodo
        self.data_fim    = data_fim      # date object — modo por_periodo
        self.thread_conn = None
        self._cancelado  = False
  
    def cancelar(self):
        self._cancelado = True
        print("[THREAD] Cancelamento solicitado pelo usuário")
        if self.thread_conn:
            try:
                self.thread_conn.cancel()
                print("[THREAD] Comando CANCEL enviado ao PostgreSQL")
            except Exception as e:
                print(f"[THREAD] Erro ao cancelar: {e}")
  
    def run(self):
        import psycopg2
        try:
            if self._cancelado:
                return
 
            dsn = self.conn.get_dsn_parameters()
            self.thread_conn = psycopg2.connect(
                host=dsn.get('host'),
                database=dsn.get('dbname'),
                user=dsn.get('user'),
                password=self.senha,
                port=dsn.get('port', '5432'),
            )
            self.thread_conn.set_session(autocommit=False)
            cursor = self.thread_conn.cursor()
 
            if self.modo == 'por_periodo':
                self._run_por_periodo(cursor)
            else:
                self._run_mensal(cursor)
 
        except Exception as e:
            if not self._cancelado:
                import traceback
                self.erro_signal.emit(f"{str(e)}\n\n{traceback.format_exc()}")
        finally:
            if self.thread_conn:
                try:
                    self.thread_conn.close()
                except Exception:
                    pass
  
    @staticmethod
    def _meses_no_periodo(data_inicio, data_fim):
        """Retorna lista de (ano, mes, frac_dias) que o período cobre.
 
        ``frac_dias`` é a proporção de dias do mês que está dentro do
        intervalo [data_inicio, data_fim] em relação ao total de dias do mês.
        Usada para calcular o outorgado pro-rata no modo por_periodo.
        """
        resultado = []
        cur = date(data_inicio.year, data_inicio.month, 1)
        while cur <= data_fim:
            total_dias = calendar.monthrange(cur.year, cur.month)[1]
            fim_mes    = date(cur.year, cur.month, total_dias)
            inicio_ef  = max(data_inicio, cur)
            fim_ef     = min(data_fim, fim_mes)
            dias_ef    = (fim_ef - inicio_ef).days + 1
            resultado.append((cur.year, cur.month, dias_ef / total_dias))
            # Avança para o 1º dia do próximo mês
            cur = date(cur.year + 1, 1, 1) if cur.month == 12 \
                  else date(cur.year, cur.month + 1, 1)
        return resultado
  
    def _run_mensal(self, cursor):
        """Verifica consumo de um único mês versus outorgado mensal."""
 
        # ── ETAPA 1: Consumo mensal ───────────────────────────────────────
        self.progresso_signal.emit(
            "Calculando consumo mensal com validação de anomalias..."
        )
        if self._cancelado:
            cursor.close(); return
 
        query_consumo = """
            SELECT
                i.nu_interferencia_cnarh::integer,
                i.nu_cnarh,
                sfi.nome_empreendimento,
                i.usuario,
                i.operador,
                STRING_AGG(DISTINCT i.rotulo_intervencao_medidor, ', '
                    ORDER BY i.rotulo_intervencao_medidor) AS rotulos_medidores,
                COALESCE(SUM(t.consumo_diario), 0) AS consumo_mes
            FROM view_usuario_operador_id_rotulo i
            LEFT JOIN tb_telemetria_intervencao_diaria t
                ON i.intervencao_id = t.intervencao_id
                AND EXTRACT(MONTH FROM t.data) = %s
                AND EXTRACT(YEAR  FROM t.data) = %s
            LEFT JOIN tb_intervencao tb ON tb.id = i.intervencao_id
            LEFT JOIN public.tb_mv_sfi_cnarh40 sfi
                ON sfi.codigo_interferencia = i.nu_interferencia_cnarh::integer            
            WHERE i.nu_interferencia_cnarh IS NOT NULL
              AND i.nu_interferencia_cnarh <> 'TESTE'
              AND i.rotulo_intervencao_medidor !~~ '%%999%%'
              AND i.rotulo_intervencao_medidor !~~ '%%VERDE GRANDE%%'
              AND i.rotulo_intervencao_medidor !~~ '%%#'
            GROUP BY i.nu_interferencia_cnarh, i.nu_cnarh, i.usuario, i.operador, sfi.nome_empreendimento
            HAVING COALESCE(SUM(t.consumo_diario), 0) > 0;
        """
        cursor.execute(query_consumo, (self.mes, self.ano))
        consumo_resultados = cursor.fetchall()
 
        if self._cancelado:
            cursor.close(); return
 
        # ── ETAPA 2: Volumes outorgados ───────────────────────────────────
        self.progresso_signal.emit("Buscando volumes outorgados...")
 
        query_outorgado = """
            SELECT
                codigo_interferencia,
                CASE %s
                    WHEN 1  THEN vol_jan  WHEN 2  THEN vol_fev
                    WHEN 3  THEN vol_mar  WHEN 4  THEN vol_abr
                    WHEN 5  THEN vol_mai  WHEN 6  THEN vol_jun
                    WHEN 7  THEN vol_jul  WHEN 8  THEN vol_ago
                    WHEN 9  THEN vol_set  WHEN 10 THEN vol_out
                    WHEN 11 THEN vol_nov  WHEN 12 THEN vol_dez
                END AS volume_outorgado_mes
            FROM view_volume_outorgado
            WHERE CASE %s
                WHEN 1  THEN vol_jan  WHEN 2  THEN vol_fev
                WHEN 3  THEN vol_mar  WHEN 4  THEN vol_abr
                WHEN 5  THEN vol_mai  WHEN 6  THEN vol_jun
                WHEN 7  THEN vol_jul  WHEN 8  THEN vol_ago
                WHEN 9  THEN vol_set  WHEN 10 THEN vol_out
                WHEN 11 THEN vol_nov  WHEN 12 THEN vol_dez
            END > 0;
        """
        cursor.execute(query_outorgado, (self.mes, self.mes))
        outorgado_map = {row[0]: float(row[1]) for row in cursor.fetchall() if row[1]}
 
        if self._cancelado:
            cursor.close(); return
 
        # ── ETAPA 3: Combinar e filtrar ───────────────────────────────────
        self.progresso_signal.emit("Comparando consumo vs. outorgado...")
        resultados_finais = self._combinar_e_filtrar(consumo_resultados, outorgado_map)
        cursor.close()
 
        if self._cancelado:
            return
 
        self.resultado_signal.emit(resultados_finais, self.nome_mes, self.ano)
        print(f"[THREAD] Mensal concluído: {len(resultados_finais)} alertas")
 
    def _run_por_periodo(self, cursor):
        """Verifica consumo de um intervalo de datas versus outorgado mensal cheio.

        Cada chamada cobre exatamente um mês (a expansão por mês é feita em
        JanelaMonitoramento._iniciar_verificacao_selecionados). O outorgado
        utilizado é o volume mensal integral da view_volume_outorgado, sem
        ponderação pro-rata.
        """
 
        # ── ETAPA 1: Consumo no intervalo ─────────────────────────────────
        self.progresso_signal.emit(
            f"Calculando consumo do período "
            f"{self.data_inicio.strftime('%d/%m/%Y')} a "
            f"{self.data_fim.strftime('%d/%m/%Y')}..."
        )
        if self._cancelado:
            cursor.close(); return
 
        query_consumo = """
            SELECT
                i.nu_interferencia_cnarh::integer,
                i.nu_cnarh,
                sfi.nome_empreendimento,
                i.usuario,
                i.operador,
                STRING_AGG(DISTINCT i.rotulo_intervencao_medidor, ', '
                    ORDER BY i.rotulo_intervencao_medidor) AS rotulos_medidores,
                COALESCE(SUM(t.consumo_diario), 0) AS consumo_mes
            FROM view_usuario_operador_id_rotulo i
            LEFT JOIN tb_telemetria_intervencao_diaria t
                ON i.intervencao_id = t.intervencao_id
                AND t.data BETWEEN %s AND %s
            LEFT JOIN tb_intervencao tb ON tb.id = i.intervencao_id
            LEFT JOIN public.tb_mv_sfi_cnarh40 sfi
                ON sfi.codigo_interferencia = i.nu_interferencia_cnarh::integer            
            WHERE i.nu_interferencia_cnarh IS NOT NULL
              AND i.nu_interferencia_cnarh <> 'TESTE'
              AND i.rotulo_intervencao_medidor !~~ '%%999%%'
              AND i.rotulo_intervencao_medidor !~~ '%%VERDE GRANDE%%'
              AND i.rotulo_intervencao_medidor !~~ '%%#'
            GROUP BY i.nu_interferencia_cnarh, i.nu_cnarh, i.usuario, i.operador, sfi.nome_empreendimento
            HAVING COALESCE(SUM(t.consumo_diario), 0) > 0;
        """
        cursor.execute(query_consumo, (self.data_inicio, self.data_fim))
        consumo_resultados = cursor.fetchall()
 
        if self._cancelado:
            cursor.close(); return
 
        # ── ETAPA 2: Outorgado mensal cheio (sem pro-rata) ────────────────
        # data_inicio e data_fim pertencem ao mesmo mês (a expansão por mês
        # é feita em JanelaMonitoramento._iniciar_verificacao_selecionados).
        self.progresso_signal.emit("Buscando volume outorgado mensal...")

        col_map = {
            1: 'vol_jan', 2: 'vol_fev', 3: 'vol_mar', 4: 'vol_abr',
            5: 'vol_mai', 6: 'vol_jun', 7: 'vol_jul', 8: 'vol_ago',
            9: 'vol_set', 10: 'vol_out', 11: 'vol_nov', 12: 'vol_dez',
        }
        mes_ref = self.data_inicio.month
        col     = col_map[mes_ref]
        cursor.execute(f"""
            SELECT codigo_interferencia, {col}
            FROM view_volume_outorgado
            WHERE {col} > 0
        """)
        outorgado_map = {
            row[0]: float(row[1])
            for row in cursor.fetchall() if row[1]
        }
 
        if self._cancelado:
            cursor.close(); return
 
        # ── ETAPA 3: Combinar e filtrar ───────────────────────────────────
        self.progresso_signal.emit("Comparando consumo vs. outorgado...")
        resultados_finais = self._combinar_e_filtrar(consumo_resultados, outorgado_map)
        cursor.close()
 
        if self._cancelado:
            return
 
        # ano=0 sinaliza modo por_periodo ao receptor do signal
        self.resultado_signal.emit(resultados_finais, self.nome_mes, 0)
        print(f"[THREAD] Por período concluído: {len(resultados_finais)} alertas")
  
    def _combinar_e_filtrar(self, consumo_resultados, outorgado_map):
        """Cruza consumo com outorgado e calcula percentual de uso.

        Retorna TODOS os registros com consumo > 0:
          - consumo > outorgado  → percentual positivo  (alerta)
          - consumo <= outorgado → percentual negativo ou zero (dentro do limite)

        O campo índice 9 (eh_alerta: bool) permite que o receptor
        distinga alertas reais de registros informativos.

        Returns:
            list[tuple]: Cada tupla tem 10 elementos:
                (cod_interf, cnarh, empreendimento, usuario, operador,
                 rotulos, consumo, outorgado, percentual, eh_alerta)
                Alertas primeiro (desc. por percentual), depois informativos.
        """
        alertas    = []
        informativos = []

        for row in consumo_resultados:
            cod_interf    = row[0]
            consumo_bruto = float(row[6]) if row[6] else 0.0
            outorgado     = outorgado_map.get(cod_interf, 0.0)

            if outorgado > 0:
                percentual = round((consumo_bruto / outorgado - 1.0) * 100.0, 1)
            else:
                percentual = 0.0

            eh_alerta = outorgado > 0 and consumo_bruto > outorgado

            registro = (
                row[0], row[1], row[2], row[3], row[4], row[5],
                consumo_bruto, outorgado, percentual, eh_alerta,
            )

            if eh_alerta:
                alertas.append(registro)
            else:
                informativos.append(registro)

        alertas.sort(key=lambda x: x[8], reverse=True)
        informativos.sort(key=lambda x: x[8], reverse=True)
        return alertas + informativos