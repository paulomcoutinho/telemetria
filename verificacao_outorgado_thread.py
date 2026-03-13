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


class VerificacaoOutorgadoThread(QThread):
    """Thread assíncrona para verificação de consumo mensal versus volume outorgado.

    Executa a comparação de consumo de telemetria com os volumes outorgados
    pelo CNARH em segundo plano, sem bloquear a interface gráfica do QGIS.
    Abre uma conexão PostgreSQL dedicada (independente da conexão principal
    do plugin) e realiza três etapas sequenciais:

        1. **Consumo mensal**: agrega o ``consumo_diario`` de
           ``tb_telemetria_intervencao_diaria`` por interferência para o
           mês/ano solicitado, filtrando registros de teste e o operador
           interno (RHODIA), retornando apenas interferências com consumo
           registrado.
        2. **Volumes outorgados**: consulta ``view_volume_outorgado`` para
           obter o volume mensal outorgado de cada interferência, selecionando
           a coluna do mês correspondente via CASE.
        3. **Comparação e ordenação**: cruza os dois conjuntos e filtra apenas
           os casos em que ``consumo > outorgado``, ordenando pelo maior
           excesso absoluto.

    Suporta cancelamento cooperativo via ``cancelar()``: o flag ``_cancelado``
    é verificado entre etapas e, quando ativo, envia um comando ``CANCEL``
    ao PostgreSQL para interromper a query em execução.

    Signals:
        resultado_signal (list, str, int): Emitido ao concluir com sucesso;
            transporta a lista de alertas ``(cod_interf, cnarh, usuario,
            operador, rotulos, consumo_bruto, outorgado)``, o nome do mês
            e o ano analisados.
        erro_signal (str): Emitido em caso de exceção não tratada; inclui
            o traceback completo para diagnóstico.
        progresso_signal (str): Emitido entre etapas com mensagem descritiva
            do passo em andamento; pode ser conectado a um ``QLabel`` de status.

    Attributes:
        conn (psycopg2.connection): Conexão principal do plugin; usada apenas
            para extrair os parâmetros DSN (host, db, porta) e abrir uma
            nova conexão exclusiva da thread.
        mes (int): Mês de referência (1–12).
        ano (int): Ano de referência (ex.: 2025).
        nome_mes (str): Nome por extenso do mês (ex.: ``"Janeiro"``);
            repassado intacto ao signal de resultado.
        senha (str): Credencial para abertura da conexão dedicada.
        thread_conn (psycopg2.connection | None): Conexão exclusiva da thread;
            fechada no bloco ``finally`` do ``run()``.
        _cancelado (bool): Flag de cancelamento cooperativo; definido por
            ``cancelar()`` e verificado entre etapas do ``run()``.
    """
    
    resultado_signal = pyqtSignal(list, str, int)
    erro_signal      = pyqtSignal(str)
    progresso_signal = pyqtSignal(str)

    def __init__(self, conn, mes, ano, nome_mes, senha):
        super().__init__()
        self.conn        = conn
        self.mes         = mes
        self.ano         = ano
        self.nome_mes    = nome_mes
        self.senha       = senha
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
                WHERE i.nu_interferencia_cnarh IS NOT NULL
                  AND i.nu_interferencia_cnarh <> 'TESTE'
                  AND i.rotulo_intervencao_medidor !~~ '%%999%%'
                  AND i.rotulo_intervencao_medidor !~~ '%%VERDE GRANDE%%'
                  AND i.rotulo_intervencao_medidor !~~ '%%#'
                GROUP BY i.nu_interferencia_cnarh, i.nu_cnarh, i.usuario, i.operador
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
            outorgado_resultados = {row[0]: row[1] for row in cursor.fetchall()}

            if self._cancelado:
                cursor.close(); return

            # ── ETAPA 3: Combinar e filtrar ───────────────────────────────────
            self.progresso_signal.emit("Comparando consumo vs. outorgado...")

            resultados_finais = []
            for row in consumo_resultados:
                cod_interf    = row[0]
                consumo_bruto = float(row[5]) if row[5] else 0.0
                outorgado     = float(outorgado_resultados.get(cod_interf, 0))

                if consumo_bruto > outorgado:
                    resultados_finais.append((
                        row[0],   # cod_interf
                        row[1],   # cnarh
                        row[2],   # usuario
                        row[3],   # operador
                        row[4],   # rotulos_medidores
                        consumo_bruto,
                        outorgado,
                    ))

            # Ordena por maior excesso
            resultados_finais.sort(
                key=lambda x: float(x[5]) - float(x[6]), reverse=True
            )

            cursor.close()

            if self._cancelado:
                return

            self.resultado_signal.emit(resultados_finais, self.nome_mes, self.ano)
            print(f"[THREAD] Verificação concluída: {len(resultados_finais)} alertas")

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
    
