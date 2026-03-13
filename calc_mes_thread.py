# -*- coding: utf-8 -*-
"""
Módulo: calc_mes_thread.py
============================
Thread assíncrona para cálculo do consumo diário corrigido de um mês completo.

Reproduz o algoritmo de detecção e correção de anomalias de
JanelaMonitoramentoDetalhes.preencher_grid_15min() sem renderizar widgets,
tornando-o seguro para execução em background.

Correções aplicadas por leitura (para cada medidor, dia a dia):
  - Wrap-around (salto negativo)       : delta < 0
      → incremento = vazao × duracao; acumula correcao_acumulada
  - Injeção espúria (salto absurdo)    : delta > vn × dur × FATOR_SEGURANCA
      → subtrai excesso; preserva incremento físico esperado
  - Continuação pós-overflow           : overflow_detectado == True
      → aplica correcao_acumulada antes de calcular delta

Constante padrão: FATOR_SEGURANCA = 5.0 (500 % da capacidade nominal).

Sinais:
  progresso(int, int, str)       — dia_atual, total_dias, mensagem
  dia_concluido(str, float, bool) — data 'AAAA-MM-DD', total m³, flag anomalia
  finalizado()                   — emitido ao concluir todos os dias
  erro(str)                      — mensagem de exceção

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
import calendar
import traceback


class CalcMesThread(QThread):
    """Thread assíncrona para cálculo corrigido do consumo diário de todo um mês.

    Reproduz o mesmo algoritmo de detecção e correção de anomalias
    implementado em ``preencher_grid_15min()`` da ``JanelaMonitoramentoDetalhes``,
    porém sem instanciar widgets, tornando-o seguro para execução em background
    sem bloquear o loop de eventos do Qt.

    O processamento percorre todos os dias do mês solicitado (via
    ``calendar.monthrange``) e, para cada dia e cada medidor da lista:
        1. Consulta ``tb_telemetria_intervencao`` para obter as leituras
           ordenadas cronologicamente (vazão, consumo acumulado, duração).
        2. Calcula o incremento real entre leituras consecutivas, corrigindo:
           - **Saltos negativos** (wrap-around do contador): substituídos por
             ``vazao × duracao``;
           - **Saltos positivos absurdos** (acima de ``vazao_nominal ×
             duracao × FATOR_SEGURANCA``): excesso subtraído, preservando o
             incremento físico esperado;
           - **Continuação pós-overflow**: ajustada pela acumulação de
             correções anteriores.
        3. Emite ``dia_concluido`` com o total corrigido do dia e o flag
           de anomalia detectada.

    Ao término de todos os dias, emite ``finalizado``. Em caso de exceção,
    emite ``erro`` com a mensagem e interrompe o processamento.

    Signals:
        progresso (int, int, str): Dia atual, total de dias no mês e mensagem
            descritiva (ex.: ``"Calculando 03/04/2025…"``).
        dia_concluido (str, float, bool): Data no formato ``"AAAA-MM-DD"``,
            total corrigido do dia em m³ e flag indicando se houve anomalia.
        finalizado (): Emitido sem parâmetros ao concluir todos os dias.
        erro (str): Mensagem de exceção em caso de falha.

    Attributes:
        conn (psycopg2.connection): Conexão ao PostgreSQL compartilhada com
            a thread principal (apenas leitura; sem commits).
        ids_medidores (list[int]): Lista de IDs de medidores a calcular;
            os volumes diários são somados quando há mais de um.
        ano (int): Ano de referência.
        mes (int): Mês de referência (1–12).
        FATOR_SEGURANCA (float): Multiplicador sobre ``vazao_nominal ×
            duracao`` para definir o limiar de salto absurdo; padrão 5,0
            (500 % da capacidade nominal).
    """
    
    progresso    = pyqtSignal(int, int, str)
    dia_concluido = pyqtSignal(str, float, bool)
    finalizado   = pyqtSignal()
    erro         = pyqtSignal(str)

    def __init__(self, conn, ids_medidores, ano, mes,
                 fator_seguranca=5.0, parent=None):
        super().__init__(parent)
        self.conn            = conn
        self.ids_medidores   = ids_medidores
        self.ano             = ano
        self.mes             = mes
        self.FATOR_SEGURANCA = fator_seguranca

    def run(self):
        try:
            import calendar as cal_mod
            dias_no_mes = cal_mod.monthrange(self.ano, self.mes)[1]

            cursor = self.conn.cursor()

            # Busca vazão nominal de todos os medidores de uma vez
            cursor.execute(
                "SELECT id, vazao_nominal FROM tb_intervencao WHERE id = ANY(%s)",
                (list(self.ids_medidores),)
            )
            vazoes = {row[0]: float(row[1]) if row[1] else 0.0
                      for row in cursor.fetchall()}

            for dia_num in range(1, dias_no_mes + 1):
                data_str = f"{self.ano}-{self.mes:02d}-{dia_num:02d}"
                self.progresso.emit(dia_num, dias_no_mes,
                                    f"Calculando {dia_num:02d}/{self.mes:02d}/{self.ano}…")

                total_dia   = 0.0
                is_anom_dia = False

                for id_med in self.ids_medidores:
                    vn = vazoes.get(id_med, 0.0)
                    total_med, is_anom_med = self._calc_dia(cursor, id_med, vn, data_str)
                    total_dia   += total_med
                    is_anom_dia  = is_anom_dia or is_anom_med

                self.dia_concluido.emit(data_str, total_dia, is_anom_dia)

            cursor.close()
            self.finalizado.emit()

        except Exception as e:
            self.erro.emit(str(e))

    def _calc_dia(self, cursor, id_medidor, vazao_nominal, data_str):
        """
        Mesma lógica de preencher_grid_15min() — sem renderização.
        Retorna (total_corrigido_dia, is_anomalia_dia).
        """
        cursor.execute("""
            SELECT vazao, consumo, duracao
            FROM tb_telemetria_intervencao
            WHERE intervencao_id = %s AND DATE(data) = %s
            ORDER BY data
        """, (id_medidor, data_str))
        rows = cursor.fetchall()

        consumo_anterior      = None
        correcao_acumulada    = 0.0
        overflow_detectado    = False
        ultimo_consumo_valido = 0.0
        total_dia             = 0.0
        is_anom_dia           = False

        for vazao, consumo, duracao in rows:
            if consumo is None:
                continue

            duracao_s      = float(duracao) if duracao else 900.0
            limite         = (vazao_nominal * duracao_s * self.FATOR_SEGURANCA
                              if vazao_nominal else float('inf'))
            vazao_usar     = (float(vazao) if vazao and float(vazao) > 0
                              else (vazao_nominal or 0.0))
            delta_corrigido = 0.0

            if consumo_anterior is not None:
                consumo_f  = float(consumo)      - correcao_acumulada
                anterior_f = float(consumo_anterior) - correcao_acumulada
                delta      = consumo_f - anterior_f

                if delta < 0:
                    # Salto negativo — wrap-around
                    is_anom_dia        = True
                    overflow_detectado = True
                    salto              = float(consumo) - float(consumo_anterior)
                    correcao_acumulada += salto
                    delta_corrigido    = vazao_usar * duracao_s
                    ultimo_consumo_valido += delta_corrigido

                elif delta > limite:
                    # Salto positivo absurdo — injeção espúria
                    is_anom_dia        = True
                    overflow_detectado = True
                    incremento_esp     = vazao_usar * duracao_s
                    excesso            = (float(consumo) - float(consumo_anterior)) - incremento_esp
                    correcao_acumulada += excesso
                    delta_corrigido    = incremento_esp
                    ultimo_consumo_valido += delta_corrigido

                elif overflow_detectado:
                    # Continuação pós-overflow
                    is_anom_dia       = True
                    consumo_corr      = float(consumo) - correcao_acumulada
                    delta_corr        = consumo_corr - ultimo_consumo_valido
                    if 0 <= delta_corr <= limite:
                        delta_corrigido = delta_corr
                    else:
                        delta_corrigido = vazao_usar * duracao_s
                    ultimo_consumo_valido += delta_corrigido

                else:
                    # Normal
                    delta_corrigido       = delta
                    ultimo_consumo_valido = float(consumo) - correcao_acumulada

            else:
                # Primeiro registro do dia
                ultimo_consumo_valido = float(consumo)
                delta_corrigido       = 0.0

            total_dia        += delta_corrigido
            consumo_anterior  = consumo

        return total_dia, is_anom_dia

