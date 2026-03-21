# -*- coding: utf-8 -*-
"""
Módulo: janela_monitoramento_detalhes.py
==========================================
Janela de monitoramento detalhado com três níveis de granularidade temporal.

  [1] Calendário mensal
      Grid com consumo diário e indicadores visuais de anomalia (célula laranja).
      Totais pré-calculados por CalcMesThread e cacheados em _totais_15min_por_dia.

  [2] Tabela de dados diários
      Visualização e edição inline com commit em lote via UPDATE.
      Células editadas destacadas em amarelo.
      Proteção contra saída acidental com dados pendentes não salvos.

  [3] Abas de 15 minutos (dinâmicas)
      Criadas sob demanda ao clicar em uma data do calendário.
      Leituras completas de tb_telemetria_intervencao com marcação de anomalias
      e tooltip explicativo por célula anômala.

Constantes de cálculo:
  FATOR_SEGURANCA  = 5.0    (500 % da capacidade nominal)
  SEGUNDOS_DIA     = 86400
  SEGUNDOS_HORA    = 3600
  INTERVALO_PADRAO = 900    (15 minutos em segundos)

Cache de sessão:
  cache_calendario  : totais mensais por data, evita recalcular ao trocar mês
  cache_15min       : leituras intraday por data, evita roundtrip repetido ao banco

Autor : SFI/ANA
Versão: 2.0 – Março/2026
"""

# ---------------------------------------------------------------------------
# Imports Qt – widgets, layout e utilitários de UI
# ---------------------------------------------------------------------------
from qgis.PyQt.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QPushButton, QComboBox, QFrame,
    QMessageBox, QSizePolicy, QScrollArea,
    QTableWidget, QTableWidgetItem, QHeaderView,
    QTabWidget, QDateEdit, QAbstractItemView,
    QProgressBar, QApplication, QMenu, QAction,
    QDialog, QLineEdit, QDesktopWidget,   
)
from qgis.PyQt.QtCore import Qt, QDate, QTimer, QThread, pyqtSignal, QDateTime
from qgis.PyQt.QtGui import QColor, QFont, QDoubleValidator, QIntValidator

# ---------------------------------------------------------------------------
# Imports Python padrão
# ---------------------------------------------------------------------------
import os
import psycopg2
import math
import calendar
import traceback
from datetime import datetime, date

# ---------------------------------------------------------------------------
# Sistema de design centralizado
# ---------------------------------------------------------------------------
try:
    from . import ui_tema
except ImportError:
    import ui_tema

# ---------------------------------------------------------------------------
# Thread de cálculo de totais mensais
# ---------------------------------------------------------------------------
from .calc_mes_thread import CalcMesThread

# ---------------------------------------------------------------------------
# Constantes de cálculo (compartilhadas com CalcMesThread)
# ---------------------------------------------------------------------------
FATOR_SEGURANCA  = 5.0
SEGUNDOS_DIA     = 86400
SEGUNDOS_HORA    = 3600
INTERVALO_PADRAO = 900


class JanelaMonitoramentoDetalhes(QWidget):
    """Janela de monitoramento detalhado de leituras de telemetria por medidor e data.

    Apresenta os dados brutos e processados de ``tb_telemetria_intervencao``
    e ``tb_telemetria_intervencao_diaria`` em múltiplas abas temporais,
    com suporte a edição inline, exportação de relatórios e visualização
    georreferenciada no canvas do QGIS.

    A janela é organizada em três níveis de granularidade:

        **Calendário (visão mensal)**: grid com os dias do mês; cada célula
        exibe o consumo diário registrado em ``tb_telemetria_intervencao_diaria``
        ou o total corrigido calculado pela thread ``CalcMesThread`` a partir
        das leituras de 15 minutos. Células com anomalias são destacadas em
        laranja; dias sem dado ficam em cinza claro.

        **Dados diários (visão mensal contínua)**: tabela com todos os dias
        do intervalo configurado pelo usuário, permitindo edição direta dos
        valores de consumo diário — os campos editados ficam em amarelo até
        serem salvos.

        **Dados a cada 15 minutos**: para cada dia clicado no calendário,
        uma aba dinâmica é criada em ``tabs_15min_internas`` exibindo todas
        as leituras intraday com colunas de hora, vazão (m³/s), consumo
        acumulado (m³) e duração (s). Leituras anômalas são marcadas com
        ícone de alerta e tooltip explicativo.

    Recursos adicionais:
        - **Modo edição**: habilitado por botão de alternância; permite alterar
          valores diários diretamente na tabela e salva via UPDATE em lote,
          verificando integridade antes de confirmar. Ao sair do modo edição
          com alterações pendentes, um diálogo de confirmação impede perda
          acidental.
        - **Ver no Mapa**: minimiza a janela e aciona o pan do QGIS Canvas
          para as coordenadas do medidor selecionado.
        - **Exportação TXT**: gera relatório de leituras de 15 minutos
          para uma data específica, com estatísticas diárias consolidadas,
          salvo em Downloads.
        - **Cache de dados**: ``cache_calendario`` e ``cache_15min``
          armazenam resultados de consultas já realizadas na sessão,
          reduzindo o número de roundtrips ao banco ao navegar entre datas.

    Constantes de cálculo:
        FATOR_SEGURANCA (float): Multiplicador sobre ``vazao_nominal ×
            duracao`` para classificar saltos como anômalos (padrão 5,0).
        SEGUNDOS_DIA (int): 86400 s — utilizado no cálculo do limite físico
            diário máximo.
        SEGUNDOS_HORA (int): 3600 s — fator de conversão m³/s → m³/h.
        INTERVALO_PADRAO (int): 900 s (15 min) — duração padrão assumida
            quando a coluna ``duracao`` é nula.

    Attributes:
        janela_anterior (JanelaMonitoramento): Referência à janela de busca;
            restaurada ao voltar.
        conn (psycopg2.connection): Conexão ativa ao PostgreSQL.
        lista_ids_selecionados (list[int]): IDs dos medidores incluídos
            na visualização.
        lista_dados_selecionados (list[tuple]): Metadados completos de cada
            medidor selecionado.
        usuario_logado (str | None): Usuário autenticado na sessão.
        criterio_busca (str | None): Critério da busca de origem.
        nome_completo (str | None): Nome completo do usuário/sistema buscado.
        eh_multipla_interferencia (bool): ``True`` quando a seleção abrange
            mais de uma interferência CNARH.
        codigos_interf (set[str]): Códigos de interferência distintos
            presentes na seleção atual.
        is_selecao_total (bool): ``True`` quando todos os medidores do
            resultado de busca foram selecionados.
        tabs_15min_internas (dict[str, QWidget]): Mapeamento
            ``data_str → aba`` das abas de 15 minutos criadas dinamicamente.
        cache_calendario (dict): Cache de resultados mensais indexados por
            ``(id_medidor, ano, mes)``.
        cache_15min (dict): Cache de leituras de 15 minutos indexadas por
            ``(id_medidor, data_str)``.
        _totais_15min_por_dia (dict[str, dict]): Acumulador de totais
            corrigidos calculados pela ``CalcMesThread``, no formato
            ``{data_str: {'total': float, 'is_anomalia': bool}}``.
        modo_edicao (bool): Indica se o modo de edição inline está ativo.
        celulas_selecionadas (list): Células atualmente marcadas para edição.
        dados_editados (dict): Dicionário de alterações pendentes não salvas.
        id_medidor_atual (int | None): ID do medidor cujos dados estão
            sendo exibidos no momento.
        widgets_celulas (dict): Mapeamento de coordenadas de célula para
            o widget ou item correspondente na grade de edição.
    """
    
    def __init__(self, janela_anterior, conexao, lista_ids_medidores, lista_dados_medidores, usuario=None, criterio_busca=None, nome_completo=None, eh_multipla_interferencia=False, codigos_interf=None, is_selecao_total=False):
        super().__init__()
        self.janela_anterior = janela_anterior
        self.conn = conexao
        self.lista_ids_selecionados = lista_ids_medidores
        self.lista_dados_selecionados = lista_dados_medidores
        self.usuario_logado = usuario
        self.criterio_busca = criterio_busca
        self.nome_completo = nome_completo
        self.eh_multipla_interferencia = eh_multipla_interferencia
        self.codigos_interf = codigos_interf if codigos_interf else set()
        self.is_selecao_total = is_selecao_total
        
        self.FATOR_SEGURANCA = 5.0       # 500% de tolerância - captura apenas valores > 5x capacidade
        #self.FATOR_SEGURANCA = 10.0  # 1000% - captura > 10x
        self.SEGUNDOS_DIA = 86400        # 24 horas * 3600 segundos
        self.SEGUNDOS_HORA = 3600        # 3600 segundos
        self.INTERVALO_PADRAO = 900      # 15 minutos em segundos
        
        self.setWindowTitle("Telemetria - DURH Diária por Telemetria")
        self.setGeometry(100, 100, 1200, 800)
        self.center()
        
        # Referência para abas de 15 minutos criadas dinamicamente
        self.tabs_15min_internas = {} 
        
        # Cache para otimização
        self.cache_calendario = {}
        self.cache_15min = {}
        # Cache de totais corrigidos por dia vindos da soma dos deltas de 15 min.
        # Populado por atualizar_dados_15min() a cada vez que o usuário abre um
        # dia na aba de 15 min. Persiste durante a sessão da janela.
        # Formato: {data_str: {'total': float, 'is_anomalia': bool}}
        self._totais_15min_por_dia = {}        
        
        # Variáveis para controle de edição
        self.modo_edicao = False
        self.celulas_selecionadas = []
        self.dados_editados = {}
        self.id_medidor_atual = None
        self.widgets_celulas = {}

        self.initUI()
    
    def calcular_limite_maximo_diario(self, vazao_nominal):
        """
        Calcula o limite máximo físico diário baseado na vazão nominal.
        
        Fórmula do relatório: limite = vazao_nominal × 86400 × 1.10
        
        Args:
            vazao_nominal: Vazão nominal da interferência em m³/s
            
        Returns:
            float: Limite máximo de consumo diário em m³
        """
        if not vazao_nominal or vazao_nominal <= 0:
            return float('inf')  # Sem limite se não houver vazão nominal
        return vazao_nominal * self.SEGUNDOS_DIA * self.FATOR_SEGURANCA
    
    def calcular_limite_maximo_intervalo(self, vazao_nominal, duracao_segundos=None):
        """
        Calcula o limite máximo para um intervalo de leitura (15 min padrão).
        
        Fórmula do relatório: limite = vazao_nominal × duracao × 1.10
        
        Args:
            vazao_nominal: Vazão nominal em m³/s
            duracao_segundos: Duração do intervalo (padrão: 900s = 15min)
            
        Returns:
            float: Limite máximo de consumo para o intervalo em m³
        """
        if not vazao_nominal or vazao_nominal <= 0:
            return float('inf')
        duracao = duracao_segundos if duracao_segundos else self.INTERVALO_PADRAO
        return vazao_nominal * duracao * self.FATOR_SEGURANCA
    
    def validar_consumo_diario(self, consumo_diario, vazao_nominal, 
                               vazao_media=None, duracao_hr=None):
        """
        Valida consumo diário contra overflow do contador cumulativo.
               
        Args:
            consumo_diario: Valor persistido no banco (m³)
            vazao_nominal: Vazão nominal da interferência (m³/s)
            vazao_media: Vazão média operacional do dia (m³/s) - opcional
            duracao_hr: Horas de operação no dia - opcional
            
        Returns:
            dict: {
                'valor_exibicao': float,      # Valor corrigido ou original
                'is_anomalia': bool,          # True se detectado overflow
                'valor_original': float,      # Valor bruto do banco
                'valor_corrigido': float,     # Valor estimado via vazão
                'metodo_calculo': str,        # 'original', 'vazao_media', 'vazao_nominal'
                'fator_erro': float           # Razão consumo/limite (para debug)
            }
        """
        resultado = {
            'valor_original': float(consumo_diario) if consumo_diario else 0.0,
            'is_anomalia': False,
            'metodo_calculo': 'original',
            'fator_erro': 1.0
        }
        
        # Sem vazão nominal, não é possível validar
        if not vazao_nominal or vazao_nominal <= 0:
            resultado['valor_exibicao'] = resultado['valor_original']
            resultado['valor_corrigido'] = resultado['valor_original']
            return resultado
        
        # Cálculo do limite máximo físico
        limite_maximo = self.calcular_limite_maximo_diario(vazao_nominal)
        consumo = resultado['valor_original']
        
        # Detecção de overflow (Seção 4.4 do relatório)
        if consumo > limite_maximo:
            resultado['is_anomalia'] = True
            resultado['fator_erro'] = consumo / limite_maximo if limite_maximo > 0 else 0
            
            # Fallback hierárquico conforme relatório:
            # 1. Preferencialmente: vazao_media × duracao_hr × 3600
            # 2. Conservador: vazao_nominal × duracao_hr × 3600 (ou 24h se não informado)
            
            horas_operacao = duracao_hr if duracao_hr and duracao_hr > 0 else 24
            
            if vazao_media and vazao_media > 0:
                # Usa vazão média real do dia (mais precisa)
                valor_corrigido = vazao_media * horas_operacao * self.SEGUNDOS_HORA
                resultado['metodo_calculo'] = 'vazao_media'
            else:
                # Usa vazão nominal como estimativa conservadora
                valor_corrigido = vazao_nominal * horas_operacao * self.SEGUNDOS_HORA
                resultado['metodo_calculo'] = 'vazao_nominal'
            
            resultado['valor_corrigido'] = valor_corrigido
            resultado['valor_exibicao'] = valor_corrigido
            
        else:
            resultado['valor_corrigido'] = consumo
            resultado['valor_exibicao'] = consumo
            
        return resultado
    
    def validar_delta_consumo_15min(self, consumo_atual, consumo_anterior, 
                                    vazao_nominal, vazao_intervalo=None, 
                                    duracao_segundos=None):
        """
        Valida delta de consumo entre leituras consecutivas de 15 min.
        
        Args:
            consumo_atual: Valor do contador cumulativo atual (m³)
            consumo_anterior: Valor do contador cumulativo anterior (m³)
            vazao_nominal: Vazão nominal (m³/s)
            vazao_intervalo: Vazão registrada no intervalo (m³/s) - opcional
            duracao_segundos: Duração do intervalo (padrão: 900s)
            
        Returns:
            dict: Informações de validação similar ao método diário
        """
        resultado = {
            'delta_original': 0.0,
            'is_anomalia': False,
            'metodo_calculo': 'original',
            'delta_corrigido': 0.0
        }
        
        # Calcular delta bruto (pode ser negativo em reinicializações)
        if consumo_atual is not None and consumo_anterior is not None:
            delta = float(consumo_atual) - float(consumo_anterior)
        else:
            delta = 0.0
            
        resultado['delta_original'] = delta
        
        # Validar apenas se temos vazão nominal
        if not vazao_nominal or vazao_nominal <= 0:
            resultado['delta_corrigido'] = delta if delta >= 0 else 0
            resultado['delta_exibicao'] = resultado['delta_corrigido']
            return resultado
        
        # Limite para o intervalo (Seção 4.2)
        limite_intervalo = self.calcular_limite_maximo_intervalo(
            vazao_nominal, duracao_segundos)
        
        # Regras de validação:
        # 1. Delta negativo = reinicialização de contador (usar vazão)
        # 2. Delta > limite = overflow (usar vazão)
        # 3. 0 <= delta <= limite = leitura válida
        if delta < 0 or delta > limite_intervalo:
            resultado['is_anomalia'] = True
            
            duracao = duracao_segundos if duracao_segundos else self.INTERVALO_PADRAO
            
            if vazao_intervalo and vazao_intervalo > 0:
                # Usa vazão do intervalo
                delta_corrigido = vazao_intervalo * duracao
                resultado['metodo_calculo'] = 'vazao_intervalo'
            else:
                # Usa vazão nominal
                delta_corrigido = vazao_nominal * duracao
                resultado['metodo_calculo'] = 'vazao_nominal'
                
            resultado['delta_corrigido'] = delta_corrigido
            resultado['delta_exibicao'] = delta_corrigido
            
        else:
            resultado['delta_corrigido'] = delta
            resultado['delta_exibicao'] = delta
            
        return resultado
    
    def obter_estilo_celula_anomalia(self, is_anomalia, is_critico=False):
        """
        Retorna configurações de estilo para células com anomalia.
        
        Args:
            is_anomalia: Boolean - houve overflow detectado?
            is_critico: Boolean - consumo > 120% do outorgado?
            
        Returns:
            dict: Configurações de estilo (cores, tooltip, ícone)
        """
        estilo = {
            'cor_fundo': QColor("#ffffff"),
            'cor_texto': QColor("#000000"),
            'cor_borda': "#dee2e6",
            'tooltip': None,
            'icone': None,
            'fonte_negrito': False
        }
        
        if is_anomalia:
            # AMARELO: Overflow detectado e corrigido (Seção 5.2 do relatório)
            estilo['cor_fundo'] = QColor("#fff3cd")  # Amarelo alerta
            estilo['cor_texto'] = QColor("#856404")   # Texto marrom
            estilo['cor_borda'] = "#ffc107"
            estilo['tooltip'] = (
                "⚠️ ANOMALIA DETECTADA: Overflow do contador cumulativo\n"
                "O valor excede o limite físico baseado na vazão nominal.\n"
                "Valor exibido foi corrigido via cálculo por vazão × duração."
            )
            estilo['icone'] = "⚠️"
            estilo['fonte_negrito'] = True
            
        elif is_critico:
            # VERMELHO CLARO: Consumo > 120% do outorgado
            estilo['cor_fundo'] = QColor("#f8d7da")
            estilo['cor_texto'] = QColor("#721c24")
            estilo['cor_borda'] = "#dc3545"
            estilo['tooltip'] = "Consumo crítico: > 120% do volume outorgado"
            estilo['fonte_negrito'] = True
            
        return estilo

    def _build_header_multipla(self, header_layout):
        """Cabeçalho para seleção com múltiplas interferências (igual ao de gráficos)."""
        # limpar layout
        while header_layout.count():
            item = header_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        # linha 1 – critério de busca
        if self.criterio_busca == "Sistema Hídrico":
            display     = self.nome_completo or self.criterio_busca
            titulo_txt  = f"🌊 Sistema Hídrico: {display}"
        elif self.criterio_busca == "CNARH":
            display     = self.nome_completo or self.criterio_busca
            titulo_txt  = f"📋 CNARH: {display}"
        else:
            display     = self.nome_completo or self.criterio_busca or "—"
            titulo_txt  = f"👤 Usuário: {display}"

        lbl_titulo = QLabel(titulo_txt)
        lbl_titulo.setStyleSheet(
            "font-size: 16px; font-weight: bold; color: #175cc3; margin-bottom: 8px;")
        lbl_titulo.setWordWrap(True)
        header_layout.addWidget(lbl_titulo)

        # linha 2 – interferências
        codigos_txt = ", ".join(sorted(str(c) for c in self.codigos_interf))
        lbl_interf  = QLabel(f"Interferências: {codigos_txt}")
        lbl_interf.setStyleSheet("""
            font-size: 12px; color: #495057; padding: 8px;
            background-color: #f8f9fa; border-radius: 3px;
            border: 1px solid #dee2e6;
        """)
        lbl_interf.setWordWrap(True)
        header_layout.addWidget(lbl_interf)

    def _build_header_padrao(self, header_layout):
        """Cabeçalho padrão (único medidor ou mesma interferência)."""
        primeiro    = self.lista_dados_selecionados[0]
        cnarh       = primeiro[4]
        cod_interf  = primeiro[5]
        rotulos     = [d[1] for d in self.lista_dados_selecionados]

        partes = []
        partes.append(f"CNARH: {cnarh}"         if cnarh      and cnarh      != "Não informado" else "CNARH: Não informado")
        partes.append(f"Interferência: {cod_interf}" if cod_interf and cod_interf != "Não informado" else "Interferência: Não informada")
        partes.append(("Medidor: " if len(rotulos) == 1 else "Medidores: ") + ", ".join(rotulos))

        titulo = QLabel(" | ".join(partes))
        titulo.setStyleSheet("font-size: 16px; font-weight: bold; color: #175cc3;")
        titulo.setWordWrap(True)
        header_layout.addWidget(titulo)
            
    def initUI(self):
        """Configura a interface do monitoramento usando abas."""
        try:
            ui_tema.aplicar_tema_arredondado(self)
        except NameError:
            pass

        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(10)

        # ── CABEÇALHO ────────────────────────────────────────
        header_container = QWidget()
        header_container.setStyleSheet("""
            QWidget {
                background-color: white;
                border: 1px solid #dee2e6;
                border-radius: 5px;
                padding: 15px;
            }
        """)
        header_layout = QVBoxLayout(header_container)

        if self.eh_multipla_interferencia:
            self._build_header_multipla(header_layout)
        else:
            self._build_header_padrao(header_layout)

        main_layout.addWidget(header_container)

        # ── ABAS PRINCIPAIS ──────────────────────────────────
        self.tabs_monitoramento = QTabWidget()
        self.tabs_monitoramento.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #dee2e6;
                border-radius: 5px;
                background-color: white;
                margin-top: 5px;
            }
            QTabBar::tab {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                padding: 8px 16px;
                margin-right: 2px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
            }
            QTabBar::tab:selected {
                background-color: white;
                border-bottom: 2px solid #175cc3;
                font-weight: bold;
            }
            QTabBar::tab:hover { background-color: #e9ecef; }
        """)

        # ── ABA 0: Consumo Diário ─────────────────────────────
        self.tab_diario      = QWidget()
        tab_diario_layout    = QVBoxLayout(self.tab_diario)

        data_diaria_container= QWidget()
        data_diaria_layout   = QHBoxLayout(data_diaria_container)
        self.lbl_data_diaria = QLabel("Selecione o mês/ano:")

        self.combo_mes = QComboBox()
        self.combo_mes.addItems(["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                                 "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"])
        self.combo_mes.setCurrentIndex(QDate.currentDate().month() - 1)
        self.combo_mes.currentIndexChanged.connect(self.atualizar_calendario)

        self.combo_ano = QComboBox()
        for ano in range(2020, QDate.currentDate().year() + 2):
            self.combo_ano.addItem(str(ano))
        self.combo_ano.setCurrentText(str(QDate.currentDate().year()))
        self.combo_ano.currentIndexChanged.connect(self.atualizar_calendario)

        data_diaria_layout.addWidget(self.lbl_data_diaria)
        data_diaria_layout.addWidget(self.combo_mes)
        data_diaria_layout.addWidget(self.combo_ano)
        data_diaria_layout.addStretch()

        self.lbl_titulo_calendario = QLabel()
        self.lbl_titulo_calendario.setStyleSheet(
            "font-size: 14px; font-weight: bold; color: #175cc3;"
            "text-align: center; margin: 10px 0;")

        self.tabela_calendario = QTableWidget()
        self.tabela_calendario.setColumnCount(7)
        self.tabela_calendario.setHorizontalHeaderLabels(
            ["Dom","Seg","Ter","Qua","Qui","Sex","Sáb"])
        self.tabela_calendario.verticalHeader().setVisible(False)
        self.tabela_calendario.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tabela_calendario.verticalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tabela_calendario.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.tabela_calendario.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabela_calendario.setSelectionMode(QAbstractItemView.NoSelection)
        self.tabela_calendario.setStyleSheet("""
            QTableWidget {
                border: 1px solid #dee2e6; border-radius: 8px;
                gridline-color: #F0F0F0; background-color: #ffffff;
            }
            QHeaderView::section {
                background-color: #F8F9FA; padding: 10px;
                border: none; border-bottom: 1px solid #dee2e6; font-weight: bold;
            }
        """)

        tab_diario_layout.addWidget(data_diaria_container)
        tab_diario_layout.addWidget(self.lbl_titulo_calendario)
        tab_diario_layout.addWidget(self.tabela_calendario)

        # ── ABA 1: Dados 15 min ───────────────────────────────
        self.tab_15min        = QWidget()
        tab_15min_layout      = QVBoxLayout(self.tab_15min)

        self.container_15min_topo = QWidget()
        layout_15min_topo         = QHBoxLayout(self.container_15min_topo)
        self.lbl_data_15min       = QLabel("Selecione a data:")
        self.date_edit            = QDateEdit(QDate.currentDate())
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDisplayFormat("dd/MM/yyyy")
        self.date_edit.dateChanged.connect(self.atualizar_dados_15min)

        try:
            primary_color = ui_tema.StyleConfig.PRIMARY_COLOR
        except NameError:
            primary_color = "#175cc3"

        self.btn_modo_edicao = QPushButton("Habilitar Edição")
        self.btn_modo_edicao.setCheckable(True)
        self.btn_modo_edicao.setStyleSheet(f"""
            QPushButton {{
                background-color: #ffc107; color: #212529;
                border-radius: 8px; padding: 8px 16px;
                font-weight: bold; border: 1px solid #ffc107;
            }}
            QPushButton:checked {{
                background-color: white; color: {primary_color};
                border: 1px solid {primary_color};
            }}
            QPushButton:hover {{ background-color: #e0a800; border: 1px solid #e0a800; }}
            QPushButton:checked:hover {{ background-color: #f8f9fa; border: 1px solid {primary_color}; }}
        """)
        self.btn_modo_edicao.toggled.connect(self.alternar_modo_edicao)
        if self.usuario_logado == "telemetria_ro":
            self.btn_modo_edicao.hide()

        self.btn_selecionar_tudo = QPushButton("Selecionar Tudo")
        self.btn_selecionar_tudo.setStyleSheet("""
            QPushButton {
                background-color: #6c757d; color: white;
                border-radius: 8px; padding: 8px 16px;
                font-weight: bold; border: 1px solid #6c757d;
            }
            QPushButton:hover { background-color: #5a6268; }
        """)
        self.btn_selecionar_tudo.clicked.connect(self.selecionar_todas_celulas)
        self.btn_selecionar_tudo.setVisible(False)

        self.btn_limpar_selecao = QPushButton("Limpar Seleção")
        self.btn_limpar_selecao.setStyleSheet("""
            QPushButton {
                background-color: #17a2b8; color: white;
                border-radius: 8px; padding: 8px 16px;
                font-weight: bold; border: 1px solid #17a2b8;
            }
            QPushButton:hover { background-color: #138496; }
            QPushButton:disabled { background-color: #6c757d; color: #dee2e6; }
        """)
        self.btn_limpar_selecao.clicked.connect(self.limpar_selecao)
        self.btn_limpar_selecao.setVisible(False)
        self.btn_limpar_selecao.setEnabled(False)

        self.btn_cancelar_edicao = QPushButton("Parar Edição")
        self.btn_cancelar_edicao.setStyleSheet("""
            QPushButton {
                background-color: #dc3545; color: white;
                border-radius: 8px; padding: 8px 16px;
                font-weight: bold; border: 1px solid #dc3545;
            }
            QPushButton:hover { background-color: #c82333; }
        """)
        self.btn_cancelar_edicao.clicked.connect(self.cancelar_edicao)
        self.btn_cancelar_edicao.setVisible(False)

        layout_15min_topo.addWidget(self.lbl_data_15min)
        layout_15min_topo.addWidget(self.date_edit)
        layout_15min_topo.addStretch()
        layout_15min_topo.addWidget(self.btn_modo_edicao)
        layout_15min_topo.addWidget(self.btn_selecionar_tudo)
        layout_15min_topo.addWidget(self.btn_limpar_selecao)
        layout_15min_topo.addWidget(self.btn_cancelar_edicao)

        tab_15min_layout.addWidget(self.container_15min_topo)

        self.conteudo_15min_widget  = QWidget()
        self.layout_conteudo_15min  = QVBoxLayout(self.conteudo_15min_widget)
        self.layout_conteudo_15min.setContentsMargins(0, 0, 0, 0)
        tab_15min_layout.addWidget(self.conteudo_15min_widget)

        self.tabs_monitoramento.addTab(self.tab_diario, "Consumo Diário")
        self.tabs_monitoramento.addTab(self.tab_15min,  "Dados a cada 15 minutos")
        self.tabs_monitoramento.currentChanged.connect(self.mudar_aba_monitoramento)

        main_layout.addWidget(self.tabs_monitoramento)

        # ── ESTATÍSTICAS ──────────────────────────────────────
        self.estatisticas_container = QWidget()
        self.estatisticas_container.setStyleSheet("""
            QWidget {
                background-color: white; border: 1px solid #dee2e6;
                border-radius: 5px; padding: 10px;
            }
        """)
        self.estatisticas_layout = QHBoxLayout(self.estatisticas_container)

        self.lbl_dias_dados      = QLabel("Dias: --")
        self.lbl_dias_dados.setStyleSheet("color: #6c757d;")
        self.lbl_periodo         = QLabel("Período: --")
        self.lbl_periodo.setStyleSheet("color: #6c757d;")
        self.lbl_leituras_totais = QLabel("Leituras Totais: --")
        self.lbl_leituras_totais.setStyleSheet("color: #6c757d;")
        self.lbl_consumo_total   = QLabel("Volume total no mês: -- m³")
        self.lbl_consumo_total.setStyleSheet("color: #6c757d; font-weight: bold;")

        self.estatisticas_layout.addWidget(self.lbl_dias_dados)
        self.estatisticas_layout.addWidget(self.lbl_periodo)
        self.estatisticas_layout.addWidget(self.lbl_leituras_totais)
        self.estatisticas_layout.addWidget(self.lbl_consumo_total)
        self.lbl_outorgado = QLabel("Outorgado: -- m³")
        self.lbl_outorgado.setStyleSheet("color: #6c757d; font-weight: bold;")
        self.lbl_outorgado.setToolTip("Volume outorgado para o mês selecionado")
        self.estatisticas_layout.addWidget(self.lbl_outorgado)        
        self.estatisticas_layout.addStretch()

        # ── Botão de cálculo real do mês ──────────────────────
        self.btn_calc_mes = QPushButton("⚙ Calcular total real do mês")
        self.btn_calc_mes.setToolTip(
            "Calcula o total corrigido de cada dia do mês somando os\n"
            "intervalos de 15 min, sem precisar abrir cada dia.\n"
            "O cálculo pode levar alguns segundos dependendo do volume de dados."
        )
        self.btn_calc_mes.setStyleSheet("""
            QPushButton {
                background-color: #1a6b2e;
                color: white;
                border-radius: 6px;
                padding: 6px 14px;
                font-weight: bold;
                font-size: 11px;
            }
            QPushButton:hover  { background-color: #145722; }
            QPushButton:disabled {
                background-color: #a5d6a7;
                color: #ffffff;
            }
        """)
        self.btn_calc_mes.clicked.connect(self.calcular_total_mes_real)
        self.btn_calc_mes.setVisible(True)   # visível só na aba diária (ajuste no Trecho 3)

        # Barra de progresso (oculta até o cálculo iniciar)
        self.progress_calc_mes = QProgressBar()
        self.progress_calc_mes.setRange(0, 100)
        self.progress_calc_mes.setFixedWidth(160)
        self.progress_calc_mes.setFixedHeight(18)
        self.progress_calc_mes.setVisible(False)
        self.progress_calc_mes.setStyleSheet("""
            QProgressBar {
                border: 1px solid #a5d6a7;
                border-radius: 4px;
                background: #f1f8e9;
                text-align: center;
                font-size: 10px;
            }
            QProgressBar::chunk { background-color: #1a6b2e; border-radius: 3px; }
        """)

        self.estatisticas_layout.addWidget(self.btn_calc_mes)
        self.estatisticas_layout.addWidget(self.progress_calc_mes)

        main_layout.addWidget(self.estatisticas_container)

        # ── BOTÕES DE AÇÃO ────────────────────────────────────
        botoes_container = QWidget()
        botoes_container.setStyleSheet("""
            QWidget {
                background-color: white; border: 1px solid #dee2e6;
                border-radius: 5px; padding: 15px;
            }
        """)
        botoes_layout = QHBoxLayout(botoes_container)

        btn_atualizar = QPushButton("Atualizar Dados")
        btn_atualizar.setStyleSheet(f"""
            QPushButton {{
                background-color: {primary_color};
                color: white;
                border-radius: 8px;
                padding: 10px 20px;
                font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: #5474b8;
            }}
        """)
        btn_atualizar.clicked.connect(self.atualizar_dados)

        btn_exportar = QPushButton("Exportar TXT")
        btn_exportar.setStyleSheet(f"""
            QPushButton {{
                background-color: {primary_color};
                color: white;
                border-radius: 8px;
                padding: 10px 20px;
                font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: #5474b8;
            }}
        """)
        btn_exportar.clicked.connect(self.exportar_relatorio)

        botoes_layout.addWidget(btn_atualizar)
        botoes_layout.addWidget(btn_exportar)
        botoes_layout.addStretch()

        btn_voltar = QPushButton("Voltar")
        btn_voltar.setStyleSheet(f"""
            QPushButton {{
                background-color: white; color: {primary_color};
                border: 1px solid {primary_color};
                border-radius: 8px; padding: 10px 20px; font-weight: bold;
            }}
            QPushButton:hover {{ background-color: #f0f0f0; }}
        """)
        btn_voltar.clicked.connect(self.voltar)
        botoes_layout.addWidget(btn_voltar)

        main_layout.addWidget(botoes_container)
        self.setLayout(main_layout)

        self.atualizar_calendario()
      
    def mudar_aba_monitoramento(self, index):
        # Botão visível apenas na aba de consumo diário
        if hasattr(self, 'btn_calc_mes'):
            self.btn_calc_mes.setVisible(index == 0)
        if hasattr(self, 'progress_calc_mes'):
            self.progress_calc_mes.setVisible(False)

        if index == 0:
            self.atualizar_calendario()
            if self.modo_edicao:
                self.btn_modo_edicao.setChecked(False)
                self.alternar_modo_edicao(False)
        else:
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                mes         = self.combo_mes.currentIndex() + 1
                ano         = int(self.combo_ano.currentText())
                primeiro_dia = QDate(ano, mes, 1)
                data_atual   = self.date_edit.date()

                if data_atual.month() != mes or data_atual.year() != ano:
                    self.date_edit.setDate(primeiro_dia)
                elif data_atual == QDate.currentDate() and data_atual.day() != 1:
                    self.date_edit.setDate(primeiro_dia)

                self.atualizar_dados_15min()
            finally:
                QApplication.restoreOverrideCursor()

    def calcular_total_mes_real(self):
        """
        Dispara CalcMesThread para calcular o total corrigido de todos os dias
        do mês sem precisar abrir cada dia na aba de 15 min.
        """
        mes = self.combo_mes.currentIndex() + 1
        ano = int(self.combo_ano.currentText())

        # Desabilita botão durante o cálculo
        self.btn_calc_mes.setEnabled(False)
        self.btn_calc_mes.setText("⚙ Calculando…")
        self.progress_calc_mes.setValue(0)
        self.progress_calc_mes.setVisible(True)

        self._calc_mes_thread = CalcMesThread(
            conn           = self.conn,
            ids_medidores  = self.lista_ids_selecionados,
            ano            = ano,
            mes            = mes,
            fator_seguranca= self.FATOR_SEGURANCA,
        )
        self._calc_mes_thread.progresso.connect(self._on_calc_mes_progresso)
        self._calc_mes_thread.dia_concluido.connect(self._on_calc_mes_dia)
        self._calc_mes_thread.finalizado.connect(self._on_calc_mes_finalizado)
        self._calc_mes_thread.erro.connect(self._on_calc_mes_erro)
        self._calc_mes_thread.start()

    def _on_calc_mes_progresso(self, dia_atual, total_dias, mensagem):
        """Atualiza barra de progresso."""
        pct = int(dia_atual / total_dias * 100)
        self.progress_calc_mes.setValue(pct)
        self.progress_calc_mes.setFormat(f"{dia_atual}/{total_dias}")

    def _on_calc_mes_dia(self, data_str, total_dia, is_anomalia):
        """Recebe o resultado de cada dia e salva em _totais_15min_por_dia."""
        if not hasattr(self, '_totais_15min_por_dia'):
            self._totais_15min_por_dia = {}
        self._totais_15min_por_dia[data_str] = {
            'total':       total_dia,
            'is_anomalia': is_anomalia,
        }

    def _on_calc_mes_finalizado(self):
        """Cálculo concluído: reabilita botão e atualiza o calendário."""
        self.btn_calc_mes.setEnabled(True)
        self.btn_calc_mes.setText("Calcular total real do mês")
        self.progress_calc_mes.setValue(100)
        self.progress_calc_mes.setVisible(False)

        # Atualiza calendário para refletir os novos totais no tooltip e rodapé
        self.atualizar_calendario(force=True)

    def _on_calc_mes_erro(self, mensagem):
        """Trata erro no thread de cálculo."""
        self.btn_calc_mes.setEnabled(True)
        self.btn_calc_mes.setText("Calcular total real do mês")
        self.progress_calc_mes.setVisible(False)
        QMessageBox.warning(
            self, "Erro no cálculo",
            f"Erro ao calcular total real do mês:\n{mensagem}"
        )
                
    def atualizar_calendario(self, force=False):
        """
        Atualiza o calendário mensal com os dados diários e validação de overflow.
        Inclui comparação com volume outorgado do mês.

        Parâmetro force=True: ignora o guard de aba ativa (usado por
        _on_calc_mes_finalizado para atualizar o rodapé mesmo quando o usuário
        está na aba de 15 min durante o cálculo).
        """
        # Guard: só bloqueia quando chamado pela troca de aba (não forçado)
        if not force and self.tabs_monitoramento.currentIndex() != 0:
            return

        if not self.lista_ids_selecionados:
            QMessageBox.warning(self, "Aviso", "Nenhum medidor selecionado.")
            return

        mes      = self.combo_mes.currentIndex() + 1
        ano      = int(self.combo_ano.currentText())
        mes_nome = self.combo_mes.currentText()
        self.lbl_titulo_calendario.setText(f"{mes_nome} {ano}")

        primeiro_dia             = QDate(ano, mes, 1)
        dias_no_mes              = primeiro_dia.daysInMonth()
        coluna_inicio            = primeiro_dia.dayOfWeek() - 1
        total_celulas_necessarias = coluna_inicio + dias_no_mes
        linhas_necessarias       = math.ceil(total_celulas_necessarias / 7)

        self.tabela_calendario.setRowCount(0)
        self.tabela_calendario.setRowCount(linhas_necessarias)

        cursor = None
        try:
            cursor = self.conn.cursor()

            # =================================================================
            # QUERY: dados brutos para processamento diário
            # =================================================================
            query = """
            SELECT
                DATE(t.data)          AS data_dia,
                t.consumo_diario      AS consumo_diario_bruto,
                t.leituras_perc,
                t.leituras_qtd,
                t.vazao_media,
                t.vazao_max,
                tb.vazao_nominal,
                t.duracao_hr,
                t.data                AS data_hora_completa
            FROM tb_telemetria_intervencao_diaria t
            JOIN tb_intervencao tb ON tb.id = t.intervencao_id
            WHERE t.intervencao_id IN %s
              AND DATE(t.data) BETWEEN %s AND %s
            ORDER BY DATE(t.data), t.intervencao_id;
            """
            data_inicio = f"{ano}-{mes:02d}-01"
            data_fim    = f"{ano}-{mes:02d}-{dias_no_mes:02d}"
            tuple_ids   = tuple(self.lista_ids_selecionados)

            cursor.execute(query, (tuple_ids, data_inicio, data_fim))
            dados_mes = cursor.fetchall()

            # =================================================================
            # ACUMULADORES
            # =================================================================
            dados_por_dia            = {}
            total_consumo_bruto      = 0.0
            total_consumo_corrigido  = 0.0   # estimativa via tabela diária

            for row in dados_mes:
                (data_dia, consumo_bruto, leituras_perc, leituras_qtd,
                 vazao_media, vazao_max, vazao_nominal, duracao_hr,
                 data_hora_completa) = row

                dia = (data_dia.day if hasattr(data_dia, 'day')
                       else int(str(data_dia).split('-')[2]))

                if dia not in dados_por_dia:
                    dados_por_dia[dia] = {
                        'consumo_bruto':      0.0,
                        'consumo_corrigido':  0.0,
                        'is_anomalia':        False,
                        'leituras_perc':      leituras_perc,
                        'leituras_qtd':       0,
                        'vazao_media':        vazao_media,
                        'vazao_max':          vazao_max,
                        'duracao_hr':         0.0,
                        'detalhes_medidores': [],
                    }

                validacao = self.validar_consumo_diario(
                    consumo_diario=consumo_bruto,
                    vazao_nominal=vazao_nominal,
                    vazao_media=vazao_media,
                    duracao_hr=duracao_hr,
                )

                dados_por_dia[dia]['consumo_bruto']     += validacao['valor_original']
                dados_por_dia[dia]['consumo_corrigido'] += validacao['valor_exibicao']
                dados_por_dia[dia]['leituras_qtd']      += int(leituras_qtd or 0)
                dados_por_dia[dia]['duracao_hr']        += float(duracao_hr or 0)

                if validacao['is_anomalia']:
                    dados_por_dia[dia]['is_anomalia'] = True
                    dados_por_dia[dia]['detalhes_medidores'].append({
                        'vazao_nominal':  vazao_nominal,
                        'valor_original': validacao['valor_original'],
                        'valor_corrigido': validacao['valor_exibicao'],
                        'metodo':         validacao['metodo_calculo'],
                    })

                total_consumo_bruto     += validacao['valor_original']
                total_consumo_corrigido += validacao['valor_exibicao']

            total_dias_anomalos_estimado = sum(
                1 for d in dados_por_dia.values() if d['is_anomalia']
            )

            # =================================================================
            # BUG 1 — CORREÇÃO: incorporar totais reais do CalcMesThread
            # Se _totais_15min_por_dia já cobre TODOS os dias com dados, usa
            # a soma real como valor do rodapé; caso contrário mantém estimativa.
            # =================================================================
            cache_15min = getattr(self, '_totais_15min_por_dia', {})

            total_15min_real      = 0.0
            total_15min_anomalos  = 0
            dias_com_15min        = 0

            for dia, d_dia in dados_por_dia.items():
                data_str_dia = f"{ano}-{mes:02d}-{dia:02d}"
                entrada_15min = cache_15min.get(data_str_dia)
                if entrada_15min is not None:
                    total_15min_real    += entrada_15min['total']
                    dias_com_15min      += 1
                    if entrada_15min['is_anomalia'] or d_dia['is_anomalia']:
                        total_15min_anomalos += 1

            # Considera "cobertura total" quando todos os dias com dados têm
            # entrada no cache — ou seja, o CalcMesThread completou o mês.
            cobertura_total = (dias_com_dados := len(dados_por_dia)) > 0 and \
                              dias_com_15min == dias_com_dados

            # Valores que o rodapé vai usar
            if cobertura_total:
                valor_rodape       = total_15min_real
                anomalos_rodape    = total_15min_anomalos
                sufixo_fonte       = ""            # sem sufixo: é o valor real
            else:
                valor_rodape       = total_consumo_corrigido
                anomalos_rodape    = total_dias_anomalos_estimado
                sufixo_fonte       = (
                    f" ({dias_com_15min}/{dias_com_dados} dias reais)"
                    if dias_com_15min > 0 else ""
                )

            # =================================================================
            # VOLUME OUTORGADO
            # =================================================================
            volume_outorgado = 0.0
            try:
                col_mes = [
                    'vol_jan','vol_fev','vol_mar','vol_abr','vol_mai','vol_jun',
                    'vol_jul','vol_ago','vol_set','vol_out','vol_nov','vol_dez',
                ][mes - 1]

                codigos = []
                for d in self.lista_dados_selecionados:
                    if d and len(d) > 5 and d[5]:
                        try:
                            cod_int = int(d[5])
                            if cod_int not in codigos:
                                codigos.append(cod_int)
                        except (ValueError, TypeError):
                            pass

                if codigos:
                    placeholders = ','.join(['%s'] * len(codigos))
                    cursor.execute(
                        f"SELECT COALESCE(SUM({col_mes}), 0) "
                        f"FROM view_volume_outorgado "
                        f"WHERE codigo_interferencia IN ({placeholders})",
                        codigos,
                    )
                    row_out = cursor.fetchone()
                    if row_out and row_out[0] is not None:
                        volume_outorgado = float(row_out[0])
            except Exception as e_out:
                print(f"[AVISO] Volume outorgado: {e_out}")

            self._volume_outorgado_mes = volume_outorgado

            # =================================================================
            # ESTATÍSTICAS
            # =================================================================
            total_leituras = sum(d['leituras_qtd'] for d in dados_por_dia.values())

            self.lbl_dias_dados.setText(f"Dias: {dias_com_dados}")
            self.lbl_periodo.setText(
                f"Período: {data_inicio.replace('-','/')} a {data_fim.replace('-','/')}"
            )
            self.lbl_leituras_totais.setText(f"Leituras Totais: {total_leituras}")

            # =================================================================
            # RODAPÉ — lbl_consumo_total
            # =================================================================
            fmt = lambda v: (
                f"{v:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

            if anomalos_rodape > 0:
                self.lbl_consumo_total.setText(
                    f"Volume: {fmt(total_consumo_bruto)} m³ "
                    f"(corrigido: {fmt(valor_rodape)} m³){sufixo_fonte} | "
                    f"{anomalos_rodape} dia(s) ajustado(s)"
                )
                self.lbl_consumo_total.setStyleSheet("color: #856404; font-weight: bold;")

                tooltip_consumo = (
                    f"Volume original (com anomalias): {fmt(total_consumo_bruto)} m³\n"
                    f"Volume corrigido: {fmt(valor_rodape)} m³\n"
                )
                if cobertura_total:
                    tooltip_consumo += "Fonte: soma real dos intervalos de 15 min\n"
                    tooltip_consumo += (
                        f"Diferença: {fmt(total_consumo_bruto - valor_rodape)} m³\n"
                        f"Redução de "
                        f"{((total_consumo_bruto - valor_rodape) / total_consumo_bruto * 100):.1f}%"
                        if total_consumo_bruto > 0 else ""
                    )
                else:
                    tooltip_consumo += (
                        "Fonte: estimativa por validação da tabela diária\n"
                        f"Diferença: {fmt(total_consumo_bruto - valor_rodape)} m³\n"
                        f"Redução de "
                        f"{((total_consumo_bruto - valor_rodape) / total_consumo_bruto * 100):.1f}%"
                        if total_consumo_bruto > 0 else ""
                    )

                if volume_outorgado > 0 and valor_rodape > volume_outorgado:
                    tooltip_consumo += (
                        f"\n\n⚠️ Consumo corrigido supera o outorgado em "
                        f"{fmt(valor_rodape - volume_outorgado)} m³"
                    )
                self.lbl_consumo_total.setToolTip(tooltip_consumo)
            else:
                label_fonte = "✅ TOTAL REAL" if cobertura_total else "Volume total no mês"
                self.lbl_consumo_total.setText(
                    f"{label_fonte}: {fmt(valor_rodape)} m³{sufixo_fonte}"
                )
                self.lbl_consumo_total.setStyleSheet("color: #28a745; font-weight: bold;")
                tip = ("Total calculado a partir da soma dos intervalos de 15 min"
                       if cobertura_total
                       else "Nenhuma anomalia detectada no período")
                self.lbl_consumo_total.setToolTip(tip)

            # =================================================================
            # RODAPÉ — lbl_outorgado
            # =================================================================
            if not hasattr(self, 'lbl_outorgado'):
                self.lbl_outorgado = QLabel("Outorgado: -- m³")
                self.lbl_outorgado.setStyleSheet("color: #6c757d; font-weight: bold;")
                self.lbl_outorgado.setToolTip("Volume outorgado para o mês selecionado")
                self.estatisticas_layout.addWidget(self.lbl_outorgado)

            if volume_outorgado > 0:
                self.lbl_outorgado.setText(f"Outorgado: {fmt(volume_outorgado)} m³/mês")

                if valor_rodape > volume_outorgado:
                    self.lbl_outorgado.setStyleSheet("color: #dc3545; font-weight: bold;")
                    self.lbl_outorgado.setToolTip(
                        f"⚠️ Consumo corrigido ({fmt(valor_rodape)} m³) "
                        f"supera o outorgado ({fmt(volume_outorgado)} m³)\n"
                        f"Excesso: {fmt(valor_rodape - volume_outorgado)} m³ "
                        f"({((valor_rodape - volume_outorgado) / volume_outorgado * 100):.1f}% do outorgado)"
                    )
                else:
                    self.lbl_outorgado.setStyleSheet("color: #28a745; font-weight: bold;")
                    self.lbl_outorgado.setToolTip(
                        f"✅ Consumo corrigido ({fmt(valor_rodape)} m³) "
                        f"dentro do outorgado ({fmt(volume_outorgado)} m³)\n"
                        f"Margem disponível: {fmt(volume_outorgado - valor_rodape)} m³ "
                        f"({((1 - valor_rodape / volume_outorgado) * 100):.1f}%)"
                    )
            else:
                self.lbl_outorgado.setText("Outorgado: não cadastrado")
                self.lbl_outorgado.setStyleSheet("color: #adb5bd; font-weight: bold;")
                self.lbl_outorgado.setToolTip(
                    "Nenhum volume outorgado encontrado para esta interferência/mês."
                )

            # =================================================================
            # PREENCHER CALENDÁRIO
            # =================================================================
            dia_atual    = 1
            linha_atual  = 0
            coluna_atual = coluna_inicio

            while dia_atual <= dias_no_mes:
                item = QTableWidgetItem()
                item.setTextAlignment(Qt.AlignCenter)
                self.tabela_calendario.setItem(linha_atual, coluna_atual, item)

                dados_dia  = dados_por_dia.get(dia_atual)
                cell_widget = self.criar_widget_celula_calendario(dia_atual, dados_dia)
                self.tabela_calendario.setCellWidget(linha_atual, coluna_atual, cell_widget)

                item.setText(" ")
                if dados_dia and dados_dia.get('is_anomalia'):
                    item.setBackground(QColor("#fff3cd"))
                else:
                    item.setBackground(QColor("#f8f9fa"))

                dia_atual    += 1
                coluna_atual += 1
                if coluna_atual >= 7:
                    coluna_atual = 0
                    linha_atual += 1

        except Exception as e:
            print(f"Erro ao atualizar calendário: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.warning(self, "Erro", f"Erro ao carregar dados: {e}")
        finally:
            if cursor:
                cursor.close()
                
    def criar_widget_celula_calendario(self, dia, dados):
        """
        Cria widget personalizado para célula do calendário.

        Tooltip atualizado: usa o total real de 15 min (self._totais_15min_por_dia)
        quando disponível; caso contrário, usa estimativa da tabela diária.
        """
        widget = QFrame()
        widget.setObjectName("CelulaCalendario")
        widget.setCursor(Qt.PointingHandCursor)

        layout = QVBoxLayout(widget)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(2)

        lbl_dia = QLabel(str(dia))
        lbl_dia.setAlignment(Qt.AlignRight | Qt.AlignTop)
        lbl_dia.setFont(QFont('Segoe UI', 10, QFont.Bold))

        try:
            text_color    = ui_tema.StyleConfig.TEXT_DARK
            border_color  = ui_tema.StyleConfig.BORDER_COLOR
            bg_color      = ui_tema.StyleConfig.BACKGROUND_WHITE
            primary_color = ui_tema.StyleConfig.PRIMARY_COLOR
            hover_color   = ui_tema.StyleConfig.HOVER_COLOR
        except NameError:
            text_color    = "#343a40"
            border_color  = "#dee2e6"
            bg_color      = "#ffffff"
            primary_color = "#175cc3"
            hover_color   = "#f1f3f5"

        lbl_dia.setStyleSheet(
            f"color: {text_color}; background: transparent; border: none;"
        )
        layout.addWidget(lbl_dia)

        container_dados = QWidget()
        container_dados.setStyleSheet("background: transparent; border: none;")
        layout_dados = QVBoxLayout(container_dados)
        layout_dados.setContentsMargins(0, 0, 0, 0)
        layout_dados.setSpacing(2)

        # Descobre se o dia já tem total calculado pelos 15 min
        mes = self.combo_mes.currentIndex() + 1
        ano = int(self.combo_ano.currentText())
        data_str_dia = f"{ano}-{mes:02d}-{dia:02d}"
        total_15min = getattr(self, '_totais_15min_por_dia', {}).get(data_str_dia)

        if dados:
            consumo_exibicao = dados.get('consumo_corrigido', 0)
            consumo_bruto    = dados.get('consumo_bruto', 0)
            is_anomalia      = dados.get('is_anomalia', False)

            # Se já temos o total real dos 15 min, usa ele como exibição principal
            # (substitui a estimativa da tabela diária)
            if total_15min is not None:
                consumo_exibicao_label = total_15min['total']
                is_anomalia_label      = total_15min['is_anomalia'] or is_anomalia
            else:
                consumo_exibicao_label = consumo_exibicao
                is_anomalia_label      = is_anomalia

            if is_anomalia_label:
                cor_consumo   = "#856404"
                fonte_consumo = "font-weight: bold;"
                icone         = "⚠️ "
            else:
                cor_consumo   = text_color
                fonte_consumo = "font-weight: 500;"
                icone         = ""

            lbl_consumo = QLabel(f"{icone}{consumo_exibicao_label:.1f} m³")
            lbl_consumo.setAlignment(Qt.AlignCenter)
            lbl_consumo.setFont(QFont('Segoe UI', 10))
            lbl_consumo.setStyleSheet(
                f"color: {cor_consumo}; {fonte_consumo} background: transparent; border: none;"
            )
            layout_dados.addWidget(lbl_consumo)

            if is_anomalia_label:
                lbl_alerta = QLabel("●")
                lbl_alerta.setAlignment(Qt.AlignCenter)
                lbl_alerta.setStyleSheet(
                    "color: #ffc107; font-size: 8px; background: transparent;"
                )
                layout_dados.addWidget(lbl_alerta)
        else:
            lbl_sem_dados = QLabel("-")
            lbl_sem_dados.setAlignment(Qt.AlignCenter)
            lbl_sem_dados.setStyleSheet(
                f"color: #adb5bd; background: transparent; border: none;"
            )
            layout_dados.addWidget(lbl_sem_dados)

        layout.addWidget(container_dados)
        layout.addStretch()

        # Borda/fundo baseado em anomalia (usa total_15min se disponível)
        is_anom_visual = (
            (total_15min['is_anomalia'] if total_15min else False)
            or (dados.get('is_anomalia', False) if dados else False)
        )
        cor_borda = "#ffc107" if is_anom_visual else border_color
        cor_fundo = "#fff3cd" if is_anom_visual else bg_color

        widget.setStyleSheet(f"""
            QFrame#CelulaCalendario {{
                background-color: {cor_fundo};
                border: 1px solid {cor_borda};
                border-radius: 4px;
                margin: 1px;
            }}
            QFrame#CelulaCalendario:hover {{
                border: 2px solid {primary_color};
                background-color: {hover_color};
            }}
        """)

        # ── TOOLTIP ──────────────────────────────────────────────────────────
        if dados:
            consumo_bruto    = dados.get('consumo_bruto', 0)
            consumo_estimado = dados.get('consumo_corrigido', 0)  # estimativa tabela diária
            is_anomalia      = dados.get('is_anomalia', False)

            tooltip = f"Dia: {dia:02d}/{mes:02d}/{ano}"

            if total_15min is not None:
                # ── Bloco principal: total REAL dos 15 min ──────────────────
                tooltip += f"\n\n✅ TOTAL REAL (soma dos intervalos de 15 min):"
                tooltip += f"\n   {total_15min['total']:,.1f} m³"

                if total_15min['is_anomalia']:
                    tooltip += (
                        f"\n   ⚠️ Inclui intervalos com overflow corrigido "
                        f"por vazão × duração."
                    )

                # ── Bloco secundário: estimativa da tabela diária ───────────
                tooltip += f"\n\n📋 Estimativa pela tabela diária:"
                if is_anomalia:
                    tooltip += f"\n   Bruto (com erro):  {consumo_bruto:,.1f} m³"
                    tooltip += f"\n   Corrigido (vazão×dur): {consumo_estimado:,.1f} m³"
                else:
                    tooltip += f"\n   {consumo_estimado:,.1f} m³"

            else:
                # ── Sem total de 15 min ainda ───────────────────────────────
                tooltip += f"\n\n📋 Consumo total (estimativa tabela diária):"
                tooltip += f"\n   {consumo_estimado:,.1f} m³"

                if is_anomalia:
                    tooltip += f"\n   ⚠️ Valor bruto com erro: {consumo_bruto:,.1f} m³"
                    tooltip += (
                        f"\n   Estimativa corrigida por vazão × duração:"
                        f" {consumo_estimado:,.1f} m³"
                    )
                tooltip += (
                    f"\n\n💡 Abra este dia na aba de 15 min para calcular"
                    f"\n   o total exato a partir dos intervalos individuais."
                )

            if dados.get('leituras_qtd'):
                tooltip += f"\n\nLeituras: {dados['leituras_qtd']}"
                if dados.get('leituras_perc'):
                    tooltip += f" ({dados['leituras_perc']:.1f}%)"

            # Detalhes por medidor (quando há anomalia na tabela diária)
            if is_anomalia and dados.get('detalhes_medidores'):
                tooltip += "\n\nDetalhes por medidor (tabela diária):"
                for i, det in enumerate(dados['detalhes_medidores'][:3], 1):
                    tooltip += (
                        f"\n  {i}. Vazão nominal: {det['vazao_nominal']:.3f} m³/s | "
                        f"Erro: {det['valor_original']:,.1f} m³ → "
                        f"Corrigido: {det['valor_corrigido']:,.1f} m³"
                    )
                if len(dados['detalhes_medidores']) > 3:
                    tooltip += (
                        f"\n  ... e mais "
                        f"{len(dados['detalhes_medidores']) - 3} medidor(es)"
                    )

            widget.setToolTip(tooltip)
        else:
            widget.setToolTip(f"Dia: {dia:02d}/{mes:02d}/{ano}\nSem dados disponíveis")

        widget.mousePressEvent = lambda event: self.ir_para_dia_15min(dia)
        return widget
        
    def ir_para_dia_15min(self, dia):
        """Muda para a aba de 15 minutos configurando a data do dia clicado."""
        # AJUSTE: Ativar cursor de espera pois o carregamento leva ~2s
        QApplication.setOverrideCursor(Qt.WaitCursor)
        
        try:
            mes = self.combo_mes.currentIndex() + 1
            ano = int(self.combo_ano.currentText())
            data_clicada = QDate(ano, mes, dia)
            if data_clicada.isValid():
                self.date_edit.setDate(data_clicada)
                self.tabs_monitoramento.setCurrentIndex(1)  # Isso dispara atualizar_dados_15min
        finally:
            # Restaura o cursor em caso de sucesso ou erro
            QApplication.restoreOverrideCursor()

    def atualizar_dados_15min(self):
        """
        Atualiza a grade de 15 min.

        Armazena em self._totais_15min_por_dia o total corrigido do dia
        (soma de todos os medidores) para uso no tooltip e rodapé do calendário.

        Casos:
          A) eh_multipla_interferencia=True  →  sub1=interferência, sub2=medidores
          B) len(lista) > 1 (mesma interf.)  →  sub-abas por rótulo
          C) único medidor                   →  grade direta
        """
        if self.tabs_monitoramento.currentIndex() != 1:
            return
        if not self.lista_ids_selecionados:
            QMessageBox.warning(self, "Aviso", "Nenhum medidor selecionado.")
            return

        # Limpa conteúdo anterior
        for i in reversed(range(self.layout_conteudo_15min.count())):
            w = self.layout_conteudo_15min.itemAt(i).widget()
            if w:
                w.deleteLater()

        self.tabs_15min_internas     = {}
        self.widgets_celulas         = {}
        self._totais_dia_por_medidor = {}   # {id_med: (total, is_anomalia)} — usado por atualizar_stats_15min

        data_selecionada = self.date_edit.date()
        data_str         = data_selecionada.toString("yyyy-MM-dd")

        # ── CASO A ───────────────────────────────────────────────────────────
        if self.eh_multipla_interferencia:
            self._build_15min_multipla_interferencia(data_str)

        # ── CASO B ───────────────────────────────────────────────────────────
        elif len(self.lista_ids_selecionados) > 1:
            sub_tabs = QTabWidget()
            self.layout_conteudo_15min.addWidget(sub_tabs)

            for dados in self.lista_dados_selecionados:
                id_med = dados[0]
                rotulo = dados[1]
                tab_c  = QWidget()
                tl     = QVBoxLayout(tab_c)
                tl.setContentsMargins(5, 5, 5, 5)
                scroll = QScrollArea()
                scroll.setWidgetResizable(True)
                grid_w = QWidget()
                grid_l = QGridLayout(grid_w)
                scroll.setWidget(grid_w)
                tl.addWidget(scroll)
                sub_tabs.addTab(tab_c, rotulo)

                total, is_anom = self.preencher_grid_15min(grid_l, id_med, data_str)
                self._totais_dia_por_medidor[id_med] = (total, is_anom)
                self.tabs_15min_internas[id_med] = grid_l

        # ── CASO C ───────────────────────────────────────────────────────────
        else:
            id_med = self.lista_ids_selecionados[0]
            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            grid_w = QWidget()
            grid_l = QGridLayout(grid_w)
            scroll.setWidget(grid_w)
            self.layout_conteudo_15min.addWidget(scroll)

            total, is_anom = self.preencher_grid_15min(grid_l, id_med, data_str)
            self._totais_dia_por_medidor[id_med] = (total, is_anom)

        # ── Grava total do DIA em _totais_15min_por_dia ───────────────────────
        # Soma todos os medidores processados nesta chamada e persiste no dict
        # de dias — isso é o que o calendário vai ler no tooltip e no rodapé.
        total_dia   = sum(t for t, _ in self._totais_dia_por_medidor.values())
        is_anom_dia = any(a for _, a in self._totais_dia_por_medidor.values())
        self._totais_15min_por_dia[data_str] = {
            'total':       total_dia,
            'is_anomalia': is_anom_dia,
        }

        self.atualizar_stats_15min(data_selecionada)
        
    def _build_15min_multipla_interferencia(self, data_str):
        """
        Monta sub-abas para múltiplas interferências.
        Captura retorno de preencher_grid_15min e popula
        self._totais_dia_por_medidor (consolidado em _totais_15min_por_dia
        pelo chamador atualizar_dados_15min).
        """
        grupos = {}
        for d in self.lista_dados_selecionados:
            cod = d[5]
            grupos.setdefault(cod, []).append(d)

        sub1 = QTabWidget()
        sub1.setStyleSheet("""
            QTabBar::tab { background-color: #e9ecef; padding: 6px 14px; font-size: 11px; }
            QTabBar::tab:selected {
                background-color: white; border-bottom: 2px solid #175cc3; font-weight: bold;
            }
        """)
        self.layout_conteudo_15min.addWidget(sub1)

        for cod_interf, medidores in sorted(grupos.items()):
            tab_interf = QWidget()
            tl_interf  = QVBoxLayout(tab_interf)
            tl_interf.setContentsMargins(4, 4, 4, 4)

            if len(medidores) == 1:
                d      = medidores[0]
                id_med = d[0]
                scroll = QScrollArea()
                scroll.setWidgetResizable(True)
                grid_w = QWidget()
                grid_l = QGridLayout(grid_w)
                scroll.setWidget(grid_w)
                tl_interf.addWidget(scroll)

                total, is_anom = self.preencher_grid_15min(grid_l, id_med, data_str)
                self._totais_dia_por_medidor[id_med] = (total, is_anom)
                self.tabs_15min_internas[id_med] = grid_l
            else:
                sub2 = QTabWidget()
                sub2.setStyleSheet("""
                    QTabBar::tab {
                        background-color: #f8f9fa; padding: 5px 12px; font-size: 10px;
                    }
                    QTabBar::tab:selected {
                        background-color: white; border-bottom: 2px solid #28a745;
                    }
                """)
                tl_interf.addWidget(sub2)

                for d in medidores:
                    id_med = d[0]
                    rotulo = d[1]
                    tab_med = QWidget()
                    tl_med  = QVBoxLayout(tab_med)
                    tl_med.setContentsMargins(4, 4, 4, 4)
                    scroll  = QScrollArea()
                    scroll.setWidgetResizable(True)
                    grid_w  = QWidget()
                    grid_l  = QGridLayout(grid_w)
                    scroll.setWidget(grid_w)
                    tl_med.addWidget(scroll)
                    sub2.addTab(tab_med, rotulo)

                    total, is_anom = self.preencher_grid_15min(grid_l, id_med, data_str)
                    self._totais_dia_por_medidor[id_med] = (total, is_anom)
                    self.tabs_15min_internas[id_med] = grid_l

            sub1.addTab(tab_interf, f"Interf. {cod_interf}")

        if sub1.count() > 0:
            sub1.setCurrentIndex(0)
            
    def preencher_grid_15min(self, layout, id_medidor, data_str):
        """
        Preenche o grid de 15 min com validação de overflow por intervalo.

        Correções aplicadas (análise 28/02/2026):
        - Bug A: branch separado para salto POSITIVO; seta correcao_acumulada
                 corretamente; atualiza ultimo_consumo_valido em todos os
                 branches de anomalia.
        - delta_corrigido preenchido para TODOS os registros (normal e anômalo).
        - Acumula total_dia_corrigido e retorna (total, is_anomalia_dia) para
          que atualizar_stats_15min() use sem depender da tabela diária.
        """
        try:
            cursor = self.conn.cursor()

            cursor.execute("""
                SELECT data, vazao, consumo, duracao
                FROM tb_telemetria_intervencao
                WHERE intervencao_id = %s AND DATE(data) = %s
                ORDER BY data;
            """, (id_medidor, data_str))
            dados_brutos = cursor.fetchall()

            cursor.execute(
                "SELECT vazao_nominal FROM tb_intervencao WHERE id = %s",
                (id_medidor,)
            )
            res_vaz = cursor.fetchone()
            vazao_nominal = float(res_vaz[0]) if res_vaz and res_vaz[0] else 0.0

            layout.setSpacing(1)
            layout.setContentsMargins(2, 2, 2, 2)

            # Cabeçalhos (apenas quando o layout está vazio)
            if layout.count() == 0:
                estilo_hdr = """
                    QLabel {
                        background-color: #F8F9FA;
                        border: 1px solid #dee2e6;
                        padding: 8px;
                        font-weight: bold;
                        color: #343a40;
                    }
                """
                lbl_header = QLabel("Hora")
                lbl_header.setAlignment(Qt.AlignCenter)
                lbl_header.setStyleSheet(estilo_hdr)
                layout.addWidget(lbl_header, 0, 0)

                for idx, p in enumerate(["00", "15", "30", "45"], 1):
                    lbl = QLabel(f"{p} min")
                    lbl.setAlignment(Qt.AlignCenter)
                    lbl.setStyleSheet(estilo_hdr)
                    layout.addWidget(lbl, 0, idx)

            horas   = list(range(24))
            periodos = [0, 15, 30, 45]

            # =================================================================
            # PASSO 1 — Processar leituras com correção acumulativa (Bug A fix)
            # =================================================================
            registros_processados  = []
            consumo_anterior       = None
            correcao_acumulada     = 0.0   # offset subtraído de todos os valores futuros
            overflow_detectado     = False
            ultimo_consumo_valido  = 0.0

            # Acumuladores para o total do dia (Bug C fix)
            total_dia_corrigido    = 0.0
            is_anomalia_dia        = False

            for row in dados_brutos:
                if len(row) < 4:
                    continue

                data_hora, vazao, consumo, duracao = row

                is_anomalia      = False
                delta_corrigido  = None
                consumo_corrigido = None

                duracao_intervalo = float(duracao) if duracao else 900.0
                limite_intervalo  = (
                    vazao_nominal * duracao_intervalo * self.FATOR_SEGURANCA
                    if vazao_nominal else float('inf')
                )

                if consumo is not None and consumo_anterior is not None:
                    try:
                        consumo_atual_float = float(consumo)       - correcao_acumulada
                        consumo_ant_float   = float(consumo_anterior) - correcao_acumulada
                        delta               = consumo_atual_float   - consumo_ant_float

                        vazao_usar = (
                            float(vazao) if vazao and float(vazao) > 0
                            else (vazao_nominal or 0.0)
                        )

                        # ----------------------------------------------------------
                        # Branch 1: salto NEGATIVO — reinicialização / wrap-around
                        # ----------------------------------------------------------
                        if delta < 0:
                            is_anomalia       = True
                            overflow_detectado = True

                            salto              = float(consumo) - float(consumo_anterior)
                            correcao_acumulada += salto   # de-biasa os próximos

                            delta_corrigido    = vazao_usar * duracao_intervalo
                            consumo_corrigido  = ultimo_consumo_valido + delta_corrigido
                            ultimo_consumo_valido = consumo_corrigido   # Bug A fix

                        # ----------------------------------------------------------
                        # Branch 2: salto POSITIVO absurdo — injeção de valor espúrio
                        #           *** BRANCH NOVO — Bug A fix ***
                        # ----------------------------------------------------------
                        elif delta > limite_intervalo:
                            is_anomalia       = True
                            overflow_detectado = True

                            # Desconta o excesso para que leituras futuras sejam
                            # de-biasadas pelo mesmo offset.
                            incremento_esperado = vazao_usar * duracao_intervalo
                            excesso             = (
                                float(consumo) - float(consumo_anterior)
                            ) - incremento_esperado
                            correcao_acumulada += excesso   # ← Bug A: setava 0 antes

                            delta_corrigido    = incremento_esperado
                            consumo_corrigido  = ultimo_consumo_valido + delta_corrigido
                            ultimo_consumo_valido = consumo_corrigido   # Bug A fix

                        # ----------------------------------------------------------
                        # Branch 3: continuação pós-overflow
                        #           (correcao_acumulada já está correta)
                        # ----------------------------------------------------------
                        elif overflow_detectado:
                            is_anomalia = True

                            consumo_atual_corr = float(consumo) - correcao_acumulada
                            delta_corr         = consumo_atual_corr - ultimo_consumo_valido

                            if 0 <= delta_corr <= limite_intervalo:
                                delta_corrigido = delta_corr
                            else:
                                # Segurança: usa vazão × duração se ainda fora do range
                                delta_corrigido = vazao_usar * duracao_intervalo

                            consumo_corrigido  = ultimo_consumo_valido + delta_corrigido
                            ultimo_consumo_valido = consumo_corrigido   # Bug A fix

                        # ----------------------------------------------------------
                        # Branch 4: normal — sem anomalia
                        # ----------------------------------------------------------
                        else:
                            delta_corrigido    = delta
                            consumo_corrigido  = consumo_atual_float
                            ultimo_consumo_valido = consumo_corrigido

                    except (ValueError, TypeError) as e:
                        print(f"[AVISO] preencher_grid_15min — erro no delta: {e}")
                        delta_corrigido   = 0.0
                        consumo_corrigido = float(consumo) if consumo else 0.0

                else:
                    # Primeiro registro do dia
                    consumo_corrigido = float(consumo) if consumo else 0.0
                    delta_corrigido   = 0.0
                    if consumo_corrigido > 0:
                        ultimo_consumo_valido = consumo_corrigido

                # Acumula total do dia (Bug C fix)
                total_dia_corrigido += (delta_corrigido or 0.0)
                if is_anomalia:
                    is_anomalia_dia = True

                registros_processados.append({
                    'data_hora':          data_hora,
                    'vazao':              vazao,
                    'consumo_bruto':      consumo,
                    'consumo_corrigido':  consumo_corrigido,
                    'duracao':            duracao,
                    'is_anomalia':        is_anomalia,
                    'delta_corrigido':    delta_corrigido,   # ← presente para TODOS
                    'correcao_acumulada': correcao_acumulada,
                })

                consumo_anterior = consumo

            # =================================================================
            # PASSO 2 — Indexar por (hora, minuto)
            # =================================================================
            dados_por_horario = {}
            for reg in registros_processados:
                dh  = reg['data_hora']
                h_d = dh.hour   if hasattr(dh, 'hour')   else int(str(dh)[11:13])
                m_d = dh.minute if hasattr(dh, 'minute') else int(str(dh)[14:16])
                chave = (h_d, (m_d // 15) * 15)
                dados_por_horario[chave] = reg

            # =================================================================
            # PASSO 3 — Renderizar grid
            # =================================================================
            estilo_hora = """
                QLabel {
                    background-color: #F8F9FA;
                    border: 1px solid #dee2e6;
                    padding: 5px;
                    font-weight: bold;
                    color: #343a40;
                }
            """
            for linha, hora in enumerate(horas, 1):
                lbl_hora = QLabel(f"{hora:02d}:00")
                lbl_hora.setAlignment(Qt.AlignCenter)
                lbl_hora.setStyleSheet(estilo_hora)
                layout.addWidget(lbl_hora, linha, 0)

                for coluna, minuto_int in enumerate(periodos, 1):
                    reg = dados_por_horario.get((hora, minuto_int))

                    if reg:
                        dados_totais = (
                            reg['data_hora'],
                            reg['vazao'],
                            reg['consumo_bruto'],
                            reg['duracao'],
                        )
                        widget = self.criar_widget_15min_compacto_editavel(
                            hora, minuto_int, dados_totais, vazao_nominal, id_medidor,
                            is_anomalia=reg['is_anomalia'],
                            consumo_corrigido=reg['consumo_corrigido'],
                            delta_corrigido=reg['delta_corrigido'],
                        )
                    else:
                        widget = self.criar_widget_15min_compacto_editavel(
                            hora, minuto_int, None, vazao_nominal, id_medidor,
                            is_anomalia=False,
                            consumo_corrigido=None,
                            delta_corrigido=None,
                        )

                    layout.addWidget(widget, linha, coluna)
                    self.widgets_celulas[(hora, minuto_int, id_medidor)] = widget

            # Retorna o total corrigido do dia para Bug C fix
            return total_dia_corrigido, is_anomalia_dia

        except Exception as e:
            print(f"Erro ao preencher grid 15min: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.warning(self, "Erro", f"Erro ao carregar dados de 15 minutos: {e}")
            return 0.0, False   # retorno seguro em caso de erro
        finally:
            cursor.close()
            
    def criar_widget_15min_compacto_editavel(self, hora, minuto, dados_totais,
                                             vazao_nominal, id_medidor,
                                             is_anomalia=False,
                                             consumo_corrigido=None,
                                             delta_corrigido=None):
        """
        Cria widget de célula 15 min com indicador visual de overflow.

        Bug B fix: exibe delta_corrigido (incremento do intervalo, ≈ 1.000 m³)
        como valor principal. O contador bruto e o valor de-biasado ficam no
        tooltip, onde têm contexto adequado.
        """
        periodo_widget = QFrame()
        periodo_widget.setObjectName(f"Celula15Min_{hora:02d}_{minuto:02d}")
        periodo_widget.setMinimumHeight(80)
        periodo_widget.setProperty("hora", hora)
        periodo_widget.setProperty("minuto", int(minuto))
        periodo_widget.setProperty("id_medidor", id_medidor)

        layout_interno = QVBoxLayout(periodo_widget)
        layout_interno.setContentsMargins(5, 5, 5, 5)
        layout_interno.setSpacing(1)

        try:
            border_color  = ui_tema.StyleConfig.BORDER_COLOR
            text_color    = ui_tema.StyleConfig.TEXT_DARK
            primary_color = ui_tema.StyleConfig.PRIMARY_COLOR
        except NameError:
            border_color  = "#dee2e6"
            text_color    = "#343a40"
            primary_color = "#175cc3"

        # Cores baseadas em anomalia
        if is_anomalia:
            cor_fundo      = "#fff3cd"
            cor_borda      = "#ffc107"
            cor_consumo    = "#856404"
            fonte_consumo  = "font-weight: bold;"
            icone_consumo  = "⚠️ "
        else:
            cor_fundo      = "#ffffff"
            cor_borda      = border_color
            cor_consumo    = "#666666"
            fonte_consumo  = ""
            icone_consumo  = ""

        periodo_widget.setStyleSheet(f"""
            QFrame#Celula15Min_{hora:02d}_{minuto:02d} {{
                background-color: {cor_fundo};
                border: 1px solid {cor_borda};
                border-radius: 0px;
            }}
            QFrame#Celula15Min_{hora:02d}_{minuto:02d}:hover {{
                border: 2px solid {primary_color};
                background-color: #f8f9fa;
            }}
        """)

        if dados_totais:
            if len(dados_totais) >= 4:
                data_hora, vazao, consumo, duracao = dados_totais[0:4]
            else:
                data_hora, vazao, consumo, duracao = None, None, None, None

            consumo_bruto = float(consumo) if consumo is not None else 0.0

            # =================================================================
            # Bug B fix: exibir delta_corrigido na célula (não o contador bruto)
            # =================================================================
            if delta_corrigido is not None:
                valor_exibir = delta_corrigido
            else:
                # Fallback para registros sem delta calculado (não deveria ocorrer
                # após Bug A fix, mas mantém compatibilidade)
                valor_exibir = consumo_bruto

            lbl_consumo = QLabel(f"{icone_consumo}{valor_exibir:.1f} m³")
            lbl_consumo.setAlignment(Qt.AlignCenter)
            lbl_consumo.setStyleSheet(
                f"font-size: 9px; color: {cor_consumo}; {fonte_consumo} background: transparent;"
            )
            lbl_consumo.setProperty("consumo_original",  consumo_bruto)
            lbl_consumo.setProperty("consumo_exibicao",  valor_exibir)
            lbl_consumo.setProperty("delta_corrigido",   delta_corrigido if delta_corrigido is not None else consumo_bruto)
            lbl_consumo.setProperty("is_anomalia",       is_anomalia)
            layout_interno.addWidget(lbl_consumo)

            # Vazão
            lbl_vazao = QLabel(f"{vazao} m³/s" if vazao else "-- m³/s")
            lbl_vazao.setAlignment(Qt.AlignCenter)
            lbl_vazao.setStyleSheet(
                f"font-weight: bold; font-size: 10px; color: {text_color}; background: transparent;"
            )
            lbl_vazao.setProperty("valor_original", str(vazao) if vazao else "")
            layout_interno.addWidget(lbl_vazao)

            # Duração
            lbl_duracao = QLabel(f"{duracao} s" if duracao else "-- s")
            lbl_duracao.setAlignment(Qt.AlignCenter)
            lbl_duracao.setStyleSheet(
                "font-size: 9px; color: #666666; background: transparent;"
            )
            lbl_duracao.setProperty("duracao_original", duracao if duracao else 0)
            layout_interno.addWidget(lbl_duracao)

            # =================================================================
            # Tooltip — mostra delta (exibido), contador bruto e valor de-biasado
            # =================================================================
            tooltip  = f"Hora: {hora:02d}:{minuto:02d}"
            if vazao:
                tooltip += f"\nVazão registrada: {vazao} m³/s"

            tooltip += f"\n\n📊 CONSUMO DO INTERVALO (exibido):"
            tooltip += f"\n   {valor_exibir:.1f} m³"

            tooltip += f"\n\n📟 CONTADOR DO HIDRÔMETRO (bruto):"
            tooltip += f"\n   {consumo_bruto:,.1f} m³"

            if is_anomalia:
                tooltip += f"\n\n⚠️ OVERFLOW DETECTADO neste intervalo."
                if delta_corrigido is not None:
                    tooltip += f"\n   Incremento estimado por vazão × duração: {delta_corrigido:.1f} m³"
                if consumo_corrigido is not None:
                    tooltip += f"\n   Contador de-biasado: {consumo_corrigido:,.1f} m³"
                tooltip += (
                    "\n\n💡 O valor exibido na célula é uma estimativa do consumo "
                    "real deste intervalo de 15 min, calculada a partir da vazão "
                    "registrada × duração. O contador do hidrômetro apresentou "
                    "salto espúrio e não deve ser usado diretamente."
                )

            periodo_widget.setToolTip(tooltip)

            # Propriedades para uso interno (edição, seleção etc.)
            periodo_widget.setProperty("tem_dados",          True)
            periodo_widget.setProperty("vazao_original",     vazao if vazao else 0)
            periodo_widget.setProperty("consumo_original",   consumo_bruto)
            periodo_widget.setProperty("consumo_corrigido",  consumo_corrigido if consumo_corrigido is not None else consumo_bruto)
            periodo_widget.setProperty("delta_corrigido",    delta_corrigido   if delta_corrigido   is not None else consumo_bruto)
            periodo_widget.setProperty("duracao_original",   duracao if duracao else 0)
            periodo_widget.setProperty("data_hora_original", data_hora)
            periodo_widget.setProperty("is_anomalia",        is_anomalia)

        else:
            label_vazio = QLabel("--")
            label_vazio.setAlignment(Qt.AlignCenter)
            label_vazio.setStyleSheet(
                "color: #ADB5BD; font-size: 10px; background: transparent;"
            )
            layout_interno.addWidget(label_vazio)

            periodo_widget.setProperty("tem_dados", False)
            periodo_widget.setToolTip(f"{hora:02d}:{minuto:02d} - Sem dados")

        periodo_widget.mousePressEvent = lambda event, h=hora, m=minuto, idm=id_medidor, pw=periodo_widget: \
            self.clicar_celula_15min(event, h, m, idm, pw)

        return periodo_widget
        
    def clicar_celula_15min(self, event, hora, minuto, id_medidor, widget):
        """Manipula o clique em uma célula de 15 minutos."""
        if not self.modo_edicao:
            return
        
        modifiers = QApplication.keyboardModifiers()
        
        # Ctrl + Clique: adiciona/remove da seleção (toggle)
        if modifiers == Qt.ControlModifier:
            self.alternar_selecao_celula(hora, minuto, id_medidor, widget)
        
        # Shift + Clique: seleção em intervalo
        elif modifiers == Qt.ShiftModifier and self.celulas_selecionadas:
            # Implementar seleção por intervalo (simplificado)
            pass
        
        # Clique normal: menu de contexto
        elif event.button() == Qt.RightButton:
            self.mostrar_menu_contexto(event, hora, minuto, id_medidor, widget)
        
        # Clique normal sem modificadores: seleciona única célula
        else:
            # Desselecionar todas as células
            self.limpar_selecao()
            
            # Selecionar esta célula
            self.selecionar_celula(hora, minuto, id_medidor, widget)
            
            # Mostrar menu de contexto automaticamente
            self.mostrar_menu_contexto(event, hora, minuto, id_medidor, widget)
        
        # Atualizar estado do botão salvar
        self.atualizar_estado_botao_salvar()

    def selecionar_celula(self, hora, minuto, id_medidor, widget):
        """Seleciona uma célula individual."""
        # AJUSTE: Garante que o minuto seja inteiro para consistência no dicionário
        celula = (hora, int(minuto), id_medidor)
        
        if celula not in self.celulas_selecionadas:
            widget.setProperty("selecionada", True)
            self.celulas_selecionadas.append(celula)
            widget.setStyleSheet(widget.styleSheet() + "background-color: #e3f2fd; border: 2px solid #2196f3;")

    def alternar_selecao_celula(self, hora, minuto, id_medidor, widget):
        """Alterna a seleção de uma célula (seleciona/desseleciona)."""
        # AJUSTE: Garante que o minuto seja inteiro para consistência no dicionário
        celula = (hora, int(minuto), id_medidor)
        
        if celula in self.celulas_selecionadas:
            # Desselecionar
            widget.setProperty("selecionada", False)
            self.celulas_selecionadas.remove(celula)
            
            # Restaurar estilo original
            try:
                border_color = ui_tema.StyleConfig.BORDER_COLOR if hasattr(ui_tema, 'StyleConfig') else "#dee2e6"
                primary_color = ui_tema.StyleConfig.PRIMARY_COLOR if hasattr(ui_tema, 'StyleConfig') else "#175cc3"
                bg_white = ui_tema.StyleConfig.BACKGROUND_WHITE if hasattr(ui_tema, 'StyleConfig') else "#ffffff"
                hover_color = ui_tema.StyleConfig.HOVER_COLOR if hasattr(ui_tema, 'StyleConfig') else "#f8f9fa"
                
                # CORREÇÃO CRÍTICA: Usa o objectName real do widget para garantir match exato (bug do 00 vs 0)
                obj_name = widget.objectName()
                
                # Verificar se é anomalia para restaurar cor correta
                is_anomalia = widget.property("is_anomalia")
                if is_anomalia:
                    cor_fundo = "#fff3cd"
                    cor_borda = "#ffc107"
                else:
                    cor_fundo = "#ffffff"
                    cor_borda = border_color
                
                widget.setStyleSheet(f"""
                    QFrame#{obj_name} {{
                        background-color: {cor_fundo};
                        border: 1px solid {cor_borda};
                        border-radius: 0px;
                    }}
                    QFrame#{obj_name}:hover {{
                        border: 2px solid {primary_color};
                        background-color: {hover_color};
                    }}
                """)
                # Forçar atualização visual
                widget.style().unpolish(widget)
                widget.style().polish(widget)
                
            except Exception as e:
                print(f"Erro ao restaurar estilo: {e}")
                obj_name = widget.objectName()
                widget.setStyleSheet(f"""
                    QFrame#{obj_name} {{
                        background-color: #ffffff;
                        border: 1px solid #dee2e6;
                        border-radius: 0px;
                    }}
                    QFrame#{obj_name}:hover {{
                        border: 2px solid #175cc3;
                        background-color: #f8f9fa;
                    }}
                """)
                widget.style().unpolish(widget)
                widget.style().polish(widget)
        else:
            # Selecionar
            widget.setProperty("selecionada", True)
            self.celulas_selecionadas.append(celula)
            widget.setStyleSheet(widget.styleSheet() + "background-color: #e3f2fd; border: 2px solid #2196f3;")
        
        # Atualizar estado dos botões
        self.atualizar_estado_botao_salvar()

    def limpar_selecao(self):
        """
        Limpa todas as seleções das células, restaurando visualmente o estilo original.
        """
        # 1. Iterar todas as células visíveis (via dicionário de widgets)
        for (hora, minuto, id_medidor), widget in self.widgets_celulas.items():
            
            # Resetar propriedade lógica
            widget.setProperty("selecionada", False)
            
            # Obter o nome real do objeto (garante funcionamento na coluna "00 min")
            nome_objeto = widget.objectName()
            if not nome_objeto:
                # Fallback se não tiver nome (raro)
                nome_objeto = f"Celula15Min_{hora:02d}_{minuto:02d}"
                widget.setObjectName(nome_objeto)

            # Definir cores (tratamento de erro para ui_tema)
            try:
                border_color = ui_tema.StyleConfig.BORDER_COLOR
                primary_color = ui_tema.StyleConfig.PRIMARY_COLOR
                bg_white = ui_tema.StyleConfig.BACKGROUND_WHITE
                hover_color = ui_tema.StyleConfig.HOVER_COLOR
            except NameError:
                border_color = "#dee2e6"
                primary_color = "#175cc3"
                bg_white = "#ffffff"
                hover_color = "#f8f9fa"

            # 3. Restaurar o estilo base correto
            # Verifica se tem dados e se é anomalia para decidir cor de fundo
            tem_dados = widget.property("tem_dados")
            is_anomalia = widget.property("is_anomalia")
            
            if is_anomalia:
                base_bg = "#fff3cd"  # Amarelo para anomalias
                base_border = "#ffc107"
            elif tem_dados:
                base_bg = bg_white
                base_border = border_color
            else:
                base_bg = "#f8f9fa"
                base_border = border_color

            # Constrói stylesheet usando o nome do objeto específico
            estilo_base = f"""
            QFrame#{nome_objeto} {{
                background-color: {base_bg};
                border: 1px solid {base_border};
                border-radius: 0px;
            }}
            QFrame#{nome_objeto}:hover {{
                border: 2px solid {primary_color};
                background-color: {hover_color};
            }}
            """

            # Aplicar o estilo (substitui o anterior)
            widget.setStyleSheet(estilo_base)

            # 4. Forçar repaint visual (Essencial para o PyQt processar imediatamente)
            widget.style().unpolish(widget)
            widget.style().polish(widget)
            widget.update()

        # Limpar lista de seleção lógica
        self.celulas_selecionadas.clear()

        # Atualizar estado dos botões
        self.atualizar_estado_botao_salvar()

    def mostrar_menu_contexto(self, event, hora, minuto, id_medidor, widget):
        """Mostra menu de contexto para a célula."""
        menu = QMenu(self)
        
        # Opção Editar
        acao_editar = QAction("✏️ Editar", self)
        acao_editar.triggered.connect(lambda: self.editar_celula(hora, minuto, id_medidor, widget))
        menu.addAction(acao_editar)
        
        # Separador
        menu.addSeparator()
        
        # Opção para apagar dados
        if widget.property("tem_dados"):
            acao_apagar = QAction("🗑️ Apagar Dados", self)
            
            # AJUSTE: Chama diretamente as novas funções para evitar duplicidade de mensagens
            if len(self.celulas_selecionadas) > 1:
                # Chama apagar_multiplas_celulas() diretamente
                acao_apagar.triggered.connect(lambda: self.apagar_multiplas_celulas())
            else:
                # Chama apagar_dados_celula(hora...) diretamente
                acao_apagar.triggered.connect(lambda: self.apagar_dados_celula(hora, minuto, id_medidor, widget))
                
            menu.addAction(acao_apagar)
        
        # Opção para selecionar todas as células do dia
        acao_selecionar_tudo = QAction("✓ Selecionar Tudo", self)
        acao_selecionar_tudo.triggered.connect(self.selecionar_todas_celulas)
        menu.addAction(acao_selecionar_tudo)
        
        # Opção para limpar seleção
        acao_limpar_selecao = QAction("🗙 Limpar Seleção", self)
        acao_limpar_selecao.triggered.connect(self.limpar_selecao)
        menu.addAction(acao_limpar_selecao)
        
        # Mostrar menu na posição do clique
        menu.exec_(event.globalPos())

    def editar_celula(self, hora, minuto, id_medidor, widget):
        """Abre diálogo para editar Vazão, Consumo e Duração."""
        if not widget.property("tem_dados"):
            QMessageBox.information(self, "Informação", "Não há dados para editar nesta célula.")
            return

        # Valores atuais
        vazao_atual = float(widget.property("vazao_original"))
        consumo_atual = float(widget.property("consumo_original"))
        duracao_atual = float(widget.property("duracao_original"))

        # Criar Diálogo
        dialog = QDialog(self)
        dialog.setWindowTitle("Editar Dados (15 Min)")
        dialog.setModal(True)
        layout_dialog = QVBoxLayout(dialog)
        
        # Input Vazão
        layout_vazao = QHBoxLayout()
        layout_vazao.addWidget(QLabel("Vazão (m³/s):"))
        input_vazao = QLineEdit(f"{vazao_atual}")
        input_vazao.setValidator(QDoubleValidator(0.000, 999.999, 3))
        layout_vazao.addWidget(input_vazao)
        layout_dialog.addLayout(layout_vazao)
        
        # Input Consumo
        layout_consumo = QHBoxLayout()
        layout_consumo.addWidget(QLabel("Consumo (m³):"))
        input_consumo = QLineEdit(f"{consumo_atual:.2f}")
        input_consumo.setValidator(QDoubleValidator(0.000, 999999.999, 2))
        layout_consumo.addWidget(input_consumo)
        layout_dialog.addLayout(layout_consumo)

        # Input Duração
        layout_duracao = QHBoxLayout()
        layout_duracao.addWidget(QLabel("Duração (segundos):"))
        input_duracao = QLineEdit(f"{duracao_atual}")
        input_duracao.setValidator(QIntValidator(0, 3600))
        layout_duracao.addWidget(input_duracao)
        layout_dialog.addLayout(layout_duracao)

        # Botões
        btn_box = QHBoxLayout()
        btn_cancelar = QPushButton("Cancelar")
        btn_salvar = QPushButton("Salvar")
        btn_salvar.setStyleSheet("background-color: #28a745; color: white; font-weight: bold;")
        
        def fechar_dialog(): dialog.reject()
        def tentar_salvar():
            try:
                v = float(input_vazao.text())
                c = float(input_consumo.text())
                d = int(input_duracao.text())
                dialog.accept()
                self.salvar_edicao_tres_campos(v, c, d, hora, minuto, id_medidor, widget)
            except ValueError:
                QMessageBox.warning(dialog, "Erro", "Valores inválidos inseridos.")
        
        btn_cancelar.clicked.connect(fechar_dialog)
        btn_salvar.clicked.connect(tentar_salvar)
        btn_box.addStretch()
        btn_box.addWidget(btn_cancelar)
        btn_box.addWidget(btn_salvar)
        layout_dialog.addLayout(btn_box)

        dialog.exec_()

    def salvar_edicao_tres_campos(self, nova_vazao, novo_consumo, nova_duracao, hora, minuto, id_medidor, widget):
        """Salva os 3 campos sem recalcular cadeia (apenas cadastro)."""
        
        # Confirmação
        resposta = QMessageBox.question(
            self,
            "Confirmar Alteração",
            f"Atualizar valores para:\n"
            f"Vazão: {nova_vazao} m³/s\n"
            f"Consumo: {novo_consumo} m³\n"
            f"Duração: {nova_duracao} s\n\n"
            f"Confirma?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes
        )

        if resposta == QMessageBox.No:
            return

        # --- INÍCIO DA OPERAÇÃO (Com Ampulheta) ---
        QApplication.setOverrideCursor(Qt.WaitCursor)
        
        try:
            # Buscar ID
            cursor_busca_id = self.conn.cursor()
            data_str = self.date_edit.date().toString("yyyy-MM-dd")
            query_id = """
            SELECT id FROM tb_telemetria_intervencao
            WHERE intervencao_id = %s 
            AND DATE(data) = %s 
            AND EXTRACT(HOUR FROM data) = %s 
            AND EXTRACT(MINUTE FROM data) = %s
            """
            cursor_busca_id.execute(query_id, (id_medidor, data_str, hora, int(minuto)))
            resultado_id = cursor_busca_id.fetchone()
            id_alterado = resultado_id[0] if resultado_id else None
            cursor_busca_id.close()

            if not id_alterado:
                 raise Exception("Registro não encontrado.")

            # UPDATE SIMPLES (Sem Ripple/Cadeia)
            cursor = self.conn.cursor()
            query_update = """
            UPDATE tb_telemetria_intervencao
            SET vazao = %s, consumo = %s, duracao = %s
            WHERE id = %s;
            """
            cursor.execute(query_update, (nova_vazao, novo_consumo, nova_duracao, id_alterado))
            self.conn.commit()
            cursor.close()
            
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao salvar: {str(e)}")
            QApplication.restoreOverrideCursor()
            return

        finally:
            QApplication.restoreOverrideCursor()
        # --- FIM DA OPERAÇÃO ---

        # Atualizar Visualmente (Todos os 3 labels)
        # 1. Vazão
        self.label_vazao = widget.findChild(QLabel, "vazao_label") # Se usarmos objectName seria melhor, mas vamos buscar pelo tipo/propriedade se não setarmos nomes.
        # Como não setamos objectNames específicos para labels, vamos refazer a atualização removendo labels e recriando.
        
        # Manter apenas o Frame e limpar os labels antigos
        layout = widget.layout()
        while layout.count():
            item = layout.takeAt(0)
            widget_w = item.widget()
            if widget_w:
                widget_w.deleteLater()

        # Recriar os labels com os novos valores
        try:
            text_color = ui_tema.StyleConfig.TEXT_DARK
        except NameError:
            text_color = "#343a40"

        lbl_vazao = QLabel(f"{nova_vazao} m³/s")
        lbl_vazao.setAlignment(Qt.AlignCenter)
        lbl_vazao.setStyleSheet(f"font-weight: bold; font-size: 10px; color: {text_color}; background: transparent;")
        layout.addWidget(lbl_vazao)

        lbl_consumo = QLabel(f"{novo_consumo:.1f} m³")
        lbl_consumo.setAlignment(Qt.AlignCenter)
        lbl_consumo.setStyleSheet(f"font-size: 9px; color: #666666; background: transparent;")
        layout.addWidget(lbl_consumo)

        lbl_duracao = QLabel(f"{nova_duracao} s")
        lbl_duracao.setAlignment(Qt.AlignCenter)
        lbl_duracao.setStyleSheet(f"font-size: 9px; color: #666666; background: transparent;")
        layout.addWidget(lbl_duracao)

        # Atualizar propriedades
        widget.setProperty("vazao_original", nova_vazao)
        widget.setProperty("consumo_original", novo_consumo)
        widget.setProperty("duracao_original", nova_duracao)
        
        # Indicador visual de edição
        widget.setStyleSheet(widget.styleSheet() + "background-color: #e8f5e8; border: 2px solid #4caf50;")
    
    def confirmar_apagar_dados(self, hora, minuto, id_medidor, widget):
        """Confirma a exclusão dos dados da célula."""
        resposta = QMessageBox.question(
            self,
            "Confirmar Exclusão",
            f"Deseja realmente apagar os dados das {hora:02d}:{minuto}?\n\n"
            f"Esta ação removerá permanentemente o registro do banco de dados.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if resposta == QMessageBox.Yes:
            # Segunda confirmação para exclusão
            resposta2 = QMessageBox.warning(
                self,
                "Atenção - Exclusão Irreversível",
                "⚠️ ATENÇÃO: Esta ação é IRREVERSÍVEL!\n\n"
                "Os dados serão permanentemente excluídos do banco de dados "
                "e não poderão ser recuperados.\n\n"
                "Tem certeza que deseja continuar?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if resposta2 == QMessageBox.Yes:
                self.apagar_dados_celula(hora, minuto, id_medidor, widget)

    def confirmar_apagar_multiplas_celulas(self):
        """Confirma a exclusão de múltiplas células selecionadas."""
        qtd_celulas = len(self.celulas_selecionadas)
        
        resposta = QMessageBox.question(
            self,
            "Confirmar Exclusão Múltipla",
            f"Deseja realmente apagar os dados de {qtd_celulas} célula(s) selecionada(s)?\n\n"
            f"Esta ação removerá permanentemente os registros do banco de dados.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if resposta == QMessageBox.Yes:
            # Segunda confirmação para exclusão
            resposta2 = QMessageBox.warning(
                self,
                "Atenção - Exclusão Irreversível",
                f"⚠️ ATENÇÃO: Esta ação é IRREVERSÍVEL!\n\n"
                f"{qtd_celulas} célula(s) serão permanentemente excluídas do banco de dados "
                f"e não poderão ser recuperadas.\n\n"
                f"Tem certeza que deseja continuar?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if resposta2 == QMessageBox.Yes:
                self.apagar_multiplas_celulas()

    def apagar_multiplas_celulas(self):
        """Elimina múltiplos registros SEM recalcular cadeia."""
        try:
            qtd_celulas = len(self.celulas_selecionadas)
            
            resposta = QMessageBox.question(
                self,
                "Confirmar Exclusão Múltipla",
                f"Deseja realmente apagar {qtd_celulas} registro(s)?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if resposta == QMessageBox.Yes:
                resposta2 = QMessageBox.warning(
                    self,
                    "Atenção - Exclusão Irreversível",
                    f"⚠️ ATENÇÃO: {qtd_celulas} registro(s) serão ELIMINADOS PERMANENTEMENTE!\n\n"
                    "Tem certeza que deseja continuar?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                
                if resposta2 == QMessageBox.Yes:
                    # --- INÍCIO DA OPERAÇÃO ---
                    QApplication.setOverrideCursor(Qt.WaitCursor)
                    
                    try:
                        cursor = self.conn.cursor()
                        data_str = self.date_edit.date().toString("yyyy-MM-dd")
                        ids_apagados = []
                        
                        # 1. Apagar registros
                        for hora, minuto, id_medidor in self.celulas_selecionadas:
                            try:
                                minuto_int = int(minuto)
                                
                                query_id = """
                                SELECT id FROM tb_telemetria_intervencao
                                WHERE intervencao_id = %s 
                                AND DATE(data) = %s 
                                AND EXTRACT(HOUR FROM data) = %s 
                                AND EXTRACT(MINUTE FROM data) = %s
                                """
                                cursor.execute(query_id, (id_medidor, data_str, hora, minuto_int))
                                res = cursor.fetchone()
                                if res:
                                    id_reg = res[0]
                                    ids_apagados.append(id_reg)
                                    
                                    cursor.execute("DELETE FROM tb_telemetria_intervencao WHERE id = %s;", (id_reg,))
                                    
                                # Atualizar widget visualmente
                                chave = (hora, minuto_int, id_medidor)
                                if chave in self.widgets_celulas:
                                    w = self.widgets_celulas[chave]
                                    w.setProperty("tem_dados", False)
                                    w.setStyleSheet(w.styleSheet() + "background-color: #ffebee; border:2px solid #f44336;")
                                    
                                    ly = w.layout()
                                    if ly:
                                        for i in reversed(range(ly.count())):
                                            it = ly.itemAt(i)
                                            if it.widget():
                                                it.widget().deleteLater()
                                        
                                        lbl = QLabel("🗑️ Apagado")
                                        lbl.setAlignment(Qt.AlignCenter)
                                        lbl.setStyleSheet("color: #f44336; font-size: 9px; font-weight: bold;")
                                        ly.addWidget(lbl)
                            except Exception as e:
                                print(f"Erro ao apagar célula {hora}:{minuto}: {e}")
                        
                        self.conn.commit()
                        # REMOVIDO: recalcular cadeia
                        
                        cursor.close()
                    finally:
                        QApplication.restoreOverrideCursor()
                    # --- FIM DA OPERAÇÃO ---
                    
                    self.celulas_selecionadas = []
                    
                    msg = f"{len(ids_apagados)} registro(s) apagado(s)."
                    QMessageBox.information(self, "Resultado", msg)
            
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao apagar dados: {str(e)}")

    def apagar_dados_celula(self, hora, minuto, id_medidor, widget):
        """Elimina o registro fisicamente SEM recalcular cadeia."""
        try:
            resposta = QMessageBox.question(
                self,
                "Confirmar Exclusão",
                f"Deseja realmente apagar os dados das {hora:02d}:{minuto}?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if resposta == QMessageBox.Yes:
                resposta2 = QMessageBox.warning(
                    self,
                    "Atenção - Exclusão Irreversível",
                    "⚠️ ATENÇÃO: Esta ação é IRREVERSÍVEL!\n\n"
                    "O registro será eliminado fisicamente do banco de dados.\n\n"
                    "Tem certeza que deseja continuar?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                
                if resposta2 == QMessageBox.Yes:
                    # --- INÍCIO DA OPERAÇÃO ---
                    QApplication.setOverrideCursor(Qt.WaitCursor)
                    
                    try:
                        cursor = self.conn.cursor()
                        data_str = self.date_edit.date().toString("yyyy-MM-dd")
                        
                        query_id = """
                        SELECT id FROM tb_telemetria_intervencao
                        WHERE intervencao_id = %s 
                        AND DATE(data) = %s 
                        AND EXTRACT(HOUR FROM data) = %s 
                        AND EXTRACT(MINUTE FROM data) = %s
                        """
                        cursor.execute(query_id, (id_medidor, data_str, hora, int(minuto)))
                        resultado_id = cursor.fetchone()
                        id_apagado = resultado_id[0] if resultado_id else None
                        
                        if id_apagado:
                            # DELETE SIMPLES
                            query_delete = "DELETE FROM tb_telemetria_intervencao WHERE id = %s;"
                            cursor.execute(query_delete, (id_apagado,))
                            self.conn.commit()
                            # REMOVIDO: executar_recalculo_cadeia
                            cursor.close()
                    finally:
                        QApplication.restoreOverrideCursor()
                    # --- FIM DA OPERAÇÃO ---
                    
                    widget.setProperty("tem_dados", False)
                    widget.setStyleSheet(widget.styleSheet() + "background-color: #ffebee; border: 2px solid #f44336;")
                    
                    layout = widget.layout()
                    if layout:
                        for i in reversed(range(layout.count())):
                            item = layout.itemAt(i)
                            if item.widget():
                                item.widget().deleteLater()
                        
                        label_apagado = QLabel("🗑️ Apagado")
                        label_apagado.setAlignment(Qt.AlignCenter)
                        label_apagado.setStyleSheet("color: #f44336; font-size: 9px; font-weight: bold;")
                        layout.addWidget(label_apagado)
                    
                    QMessageBox.information(self, "Sucesso", "Dados apagados com sucesso!")
            
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao apagar dados: {str(e)}")

    def alternar_modo_edicao(self, ativado):
        """Ativa ou desativa o modo de edição."""
        self.modo_edicao = ativado
        
        if ativado:
            self.btn_modo_edicao.setText("Modo Edição ATIVO")
            self.btn_modo_edicao.setEnabled(False)
            self.btn_selecionar_tudo.setVisible(True)
            self.btn_limpar_selecao.setVisible(True)
            self.btn_cancelar_edicao.setVisible(True)
            
            # Limpar seleções anteriores
            self.celulas_selecionadas = []
            
            QMessageBox.information(
                self,
                "Modo Edição Ativado",
                "Modo de edição ativado!\n\n"
                "• Clique em uma célula para selecionar\n"
                "• Ctrl+Clique para seleção múltipla\n"
                "• Clique direito para menu de contexto\n"
                "• Use 'Selecionar Tudo' para todas as células\n\n"
                "Ao editar um valor, ele será salvo imediatamente após confirmação."
            )
        else:
            self.btn_modo_edicao.setText("Habilitar Edição")
            self.btn_modo_edicao.setEnabled(True)
            self.btn_selecionar_tudo.setVisible(False)
            self.btn_limpar_selecao.setVisible(False)
            self.btn_cancelar_edicao.setVisible(False)
            
            # Limpar seleções
            self.celulas_selecionadas = []
            
            # --- INÍCIO DO RECARREGAMENTO (Com Ampulheta) ---
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                # Recarregar dados para remover visualizações de edição
                self.atualizar_dados_15min()
            finally:
                QApplication.restoreOverrideCursor()

    def selecionar_todas_celulas(self):
        """Seleciona todas as células do grid atual."""
        if not self.modo_edicao:
            return
        
        # Limpar seleção anterior
        self.celulas_selecionadas = []
        
        # Percorrer todos os widgets no layout_conteudo_15min
        for i in range(self.layout_conteudo_15min.count()):
            widget = self.layout_conteudo_15min.itemAt(i).widget()
            if widget and isinstance(widget, QTabWidget):
                # Se houver tabs, pegar a tab atual
                current_widget = widget.currentWidget()
                if current_widget:
                    scroll_area = current_widget.findChild(QScrollArea)
                    if scroll_area:
                        grid_widget = scroll_area.widget()
                        if grid_widget:
                            self.selecionar_todas_no_grid(grid_widget)
            elif widget and isinstance(widget, QScrollArea):
                grid_widget = widget.widget()
                if grid_widget:
                    self.selecionar_todas_no_grid(grid_widget)
        
        QMessageBox.information(self, "Seleção", f"{len(self.celulas_selecionadas)} células selecionadas.")
        self.atualizar_estado_botao_salvar()

    def selecionar_todas_no_grid(self, grid_widget):
        """Seleciona todas as células em um grid específico."""
        layout = grid_widget.layout()
        if not layout:
            return
        
        for i in range(layout.count()):
            item = layout.itemAt(i)
            if item and item.widget():
                widget = item.widget()
                if widget.objectName().startswith("Celula15Min_"):
                    # Extrair hora e minuto do objectName
                    parts = widget.objectName().split("_")
                    if len(parts) >= 3:
                        hora = int(parts[1])
                        minuto = parts[2]
                        id_medidor = widget.property("id_medidor")
                        
                        # Selecionar visualmente
                        widget.setProperty("selecionada", True)
                        widget.setStyleSheet(widget.styleSheet() + "background-color: #e3f2fd; border: 2px solid #2196f3;")
                        
                        # Adicionar à lista
                        celula = (hora, int(minuto), id_medidor)
                        if celula not in self.celulas_selecionadas:
                            self.celulas_selecionadas.append(celula)

    def atualizar_estado_botao_salvar(self):
        """Atualiza o estado dos botões de controle (Apenas Limpar Seleção)."""

        self.btn_limpar_selecao.setEnabled(len(self.celulas_selecionadas) > 0)

    def salvar_alteracoes(self):
        """Salva todas as alterações no banco de dados."""
        if not self.dados_editados:
            QMessageBox.information(self, "Informação", "Não há alterações para salvar.")
            return
        
        # Contar tipos de alterações
        atualizacoes = sum(1 for v in self.dados_editados.values() if v is not None)
        exclusoes = sum(1 for v in self.dados_editados.values() if v is None)
        
        # Primeira confirmação
        msg = f"Deseja salvar as seguintes alterações?\n\n"
        if atualizacoes > 0:
            msg += f"• {atualizacoes} célula(s) com valores atualizados\n"
        if exclusoes > 0:
            msg += f"• {exclusoes} célula(s) com dados a serem excluídos\n"
        
        resposta = QMessageBox.question(
            self,
            "Confirmar Alterações",
            msg,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if resposta == QMessageBox.No:
            return
        
        # Segunda confirmação para exclusões
        if exclusoes > 0:
            resposta2 = QMessageBox.warning(
                self,
                "Atenção - Exclusões Irreversíveis",
                f"⚠️ ATENÇÃO: {exclusoes} célula(s) serão EXCLUÍDAS PERMANENTEMENTE!\n\n"
                "Esta ação é IRREVERSÍVEL e os dados não poderão ser recuperados.\n\n"
                "Deseja realmente continuar com as exclusões?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if resposta2 == QMessageBox.No:
                # Remover apenas as exclusões da lista
                self.dados_editados = {k: v for k, v in self.dados_editados.items() if v is not None}
                if not self.dados_editados:
                    QMessageBox.information(self, "Informação", "Nenhuma alteração restante para salvar.")
                    return
        
        try:
            cursor = self.conn.cursor()
            data_str = self.date_edit.date().toString("yyyy-MM-dd")
            
            # Processar cada alteração
            sucesso_atualizacoes = 0
            sucesso_exclusoes = 0
            erros = []
            
            for (hora, minuto, id_medidor), novo_valor in self.dados_editados.items():
                data_hora_str = f"{data_str} {hora:02d}:{minuto}:00"
                
                try:
                    if novo_valor is None:  # Exclusão
                        query = """
                        DELETE FROM tb_telemetria_intervencao
                        WHERE intervencao_id = %s 
                        AND DATE(data) = %s 
                        AND HOUR(data) = %s 
                        AND MINUTE(data) = %s
                        """
                        cursor.execute(query, (id_medidor, data_str, hora, minuto))
                        sucesso_exclusoes += 1
                        
                    else:  # Atualização
                        query = """
                        UPDATE tb_telemetria_intervencao
                        SET vazao = %s,
                            consumo = %s * duracao / 3600  -- Recalcular consumo baseado na nova vazão
                        WHERE intervencao_id = %s 
                        AND DATE(data) = %s 
                        AND HOUR(data) = %s 
                        AND MINUTE(data) = %s
                        """
                        cursor.execute(query, (novo_valor, novo_valor, id_medidor, data_str, hora, minuto))
                        sucesso_atualizacoes += 1
                        
                except Exception as e:
                    erros.append(f"{hora:02d}:{minuto} - {str(e)}")
            
            # Commit das alterações
            self.conn.commit()
            
            # Mensagem de resultado
            msg_resultado = "Alterações salvas com sucesso!\n\n"
            if sucesso_atualizacoes > 0:
                msg_resultado += f"• {sucesso_atualizacoes} valor(es) atualizado(s)\n"
            if sucesso_exclusoes > 0:
                msg_resultado += f"• {sucesso_exclusoes} registro(s) excluído(s)\n"
            
            if erros:
                msg_resultado += f"\n⚠️ {len(erros)} erro(s) encontrado(s):\n"
                for erro in erros[:5]:  # Mostrar apenas os primeiros 5 erros
                    msg_resultado += f"  - {erro}\n"
                if len(erros) > 5:
                    msg_resultado += f"  ... e mais {len(erros)-5} erro(s)"
            
            QMessageBox.information(self, "Resultado", msg_resultado)
            
            # Limpar alterações e recarregar dados
            self.dados_editados = {}
            self.celulas_selecionadas = []
            self.atualizar_dados_15min()
            self.atualizar_estado_botao_salvar()
            
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao salvar alterações: {str(e)}")
        finally:
            if cursor:
                cursor.close()

    def cancelar_edicao(self):
        """Cancela todas as edições em andamento."""       
        # Desativar modo edição
        self.btn_modo_edicao.setChecked(False)
        self.alternar_modo_edicao(False)

    def atualizar_stats_15min(self, data_selecionada):
        """
        Calcula e exibe as estatísticas totais para o dia selecionado.

        Bug C fix: o total do dia vem de self._totais_dia_por_medidor, que é
        populado por atualizar_dados_15min() a partir do retorno de
        preencher_grid_15min(). A tabela tb_telemetria_intervencao_diaria é
        consultada apenas para leituras_qtd (não afetada pelo overflow).
        """
        data_str_db = data_selecionada.toString("yyyy-MM-dd")
        data_fmt_br = data_selecionada.toString("dd/MM/yyyy")

        try:
            cursor = self.conn.cursor()
            tuple_ids = tuple(self.lista_ids_selecionados)

            # ------------------------------------------------------------------
            # 1. Total corrigido vem do grid (Bug C fix)
            # ------------------------------------------------------------------
            total_consumo_corrigido = 0.0
            total_dias_anomalos     = 0

            totais = getattr(self, '_totais_dia_por_medidor', {})

            if totais:
                for id_med in self.lista_ids_selecionados:
                    total, is_anom = totais.get(id_med, (0.0, False))
                    total_consumo_corrigido += total
                    if is_anom:
                        total_dias_anomalos += 1
            else:
                # Fallback de segurança: não há totais pré-calculados
                # (ocorre se atualizar_stats_15min for chamado sem passar por
                # atualizar_dados_15min, ex.: caminhos de teste)
                self.lbl_consumo_total.setText("Volume total no dia: -- m³")
                self.lbl_consumo_total.setStyleSheet("color: #6c757d; font-weight: bold;")
                return

            # ------------------------------------------------------------------
            # 2. Número de leituras — ainda vem da tabela diária (não é afetado)
            # ------------------------------------------------------------------
            cursor.execute("""
                SELECT COALESCE(SUM(leituras_qtd), 0)
                FROM tb_telemetria_intervencao_diaria
                WHERE intervencao_id IN %s
                  AND DATE(data) = %s
            """, (tuple_ids, data_str_db))
            total_leituras = int(cursor.fetchone()[0] or 0)

            # ------------------------------------------------------------------
            # 3. Atualizar labels
            # ------------------------------------------------------------------
            self.lbl_dias_dados.setText("Dias: 1")
            self.lbl_periodo.setText(f"Período: {data_fmt_br}")
            self.lbl_leituras_totais.setText(f"Leituras Totais: {total_leituras}")

            if total_dias_anomalos > 0:
                self.lbl_consumo_total.setText(
                    f"Volume total no dia: {total_consumo_corrigido:,.1f} m³ "
                    f"(⚠️ {total_dias_anomalos} medidor(es) com overflow corrigido)"
                )
                self.lbl_consumo_total.setStyleSheet("color: #856404; font-weight: bold;")
                self.lbl_consumo_total.setToolTip(
                    "O total exibido é a soma dos incrementos corrigidos de cada "
                    "intervalo de 15 min.\nLeituras com overflow de contador foram "
                    "substituídas por estimativa via vazão × duração."
                )
            else:
                self.lbl_consumo_total.setText(
                    f"Volume total no dia: {total_consumo_corrigido:,.1f} m³"
                )
                self.lbl_consumo_total.setStyleSheet("color: #28a745; font-weight: bold;")
                self.lbl_consumo_total.setToolTip("Nenhuma anomalia detectada no período.")

        except Exception as e:
            print(f"Erro ao atualizar estatísticas 15min: {e}")
            QMessageBox.warning(self, "Erro", f"Erro ao atualizar estatísticas: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
                
    def atualizar_dados(self):
        """Atualiza todos os dados conforme seleção atual com feedback visual."""
        
        # INÍCIO: Ativar Cursor de Espera
        QApplication.setOverrideCursor(Qt.WaitCursor)
        
        try:
            # 1. Limpeza de cache
            self.cache_calendario = {}
            self.cache_15min = {}
            
            # 2. Desativar modo edição se estiver ativo (Resetar estado interno)
            if self.modo_edicao:
                self.btn_modo_edicao.setChecked(False)
                self.modo_edicao = False
                self.celulas_selecionadas = []
                self.dados_editados = {}
                self.btn_modo_edicao.setText("Habilitar Edição")
                self.btn_modo_edicao.setEnabled(True)
                self.btn_selecionar_tudo.setVisible(False)
                self.btn_limpar_selecao.setVisible(False)
                self.btn_cancelar_edicao.setVisible(False)
            
            # 3. Atualizar a aba ATIVA
            if self.tabs_monitoramento.currentIndex() == 0:
                self.atualizar_calendario()
            else:
                self.atualizar_dados_15min()
                
        except Exception as e:
            # Caso ocorra erro, restaurar cursor e exibir erro, não mostrando mensagem de sucesso
            QApplication.restoreOverrideCursor()
            QMessageBox.critical(self, "Erro", f"Erro ao atualizar dados: {e}")
            return # Para a execução aqui para não mostrar a mensagem de sucesso
        
        finally:
            # FIM: Garantir que o cursor volte ao normal
            QApplication.restoreOverrideCursor()
        
        # Mensagem de Sucesso (aparece com cursor normal agora)
        QMessageBox.information(self, "Atualização", "Dados atualizados com sucesso.")
    
    def exportar_relatorio(self):
            """Exporta relatório dos dados visualizados na pasta Downloads.

            Quando há medidores de mais de uma interferência carregados
            (eh_multipla_interferencia=True), a exportação separa cada
            interferência e gera arquivos individuais — exatamente como já
            ocorre para o caso de interferência única.
            """
            if not self.lista_ids_selecionados:
                QMessageBox.warning(self, "Aviso", "Selecione um medidor primeiro.")
                return

            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                import os

                downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
                if not os.path.exists(downloads_path):
                    QMessageBox.warning(self, "Erro", "Pasta Downloads não encontrada!")
                    return

                cursor = self.conn.cursor()

                # ── Agrupar medidores por código de interferência ──────────────────
                # Preserva a ordem em que as interferências aparecem na seleção.
                grupos: dict[str, list] = {}          # {cod_interf: [dados_medidor, ...]}
                for d in self.lista_dados_selecionados:
                    cod = str(d[5]) if d[5] else "SEM_INTERF"
                    grupos.setdefault(cod, []).append(d)

                # ── ABA 0: CONSUMO DIÁRIO ──────────────────────────────────────────
                if self.tabs_monitoramento.currentIndex() == 0:

                    mes_num  = self.combo_mes.currentIndex() + 1
                    ano      = self.combo_ano.currentText()
                    meses_abrev = ["JAN","FEV","MAR","ABR","MAI","JUN",
                                   "JUL","AGO","SET","OUT","NOV","DEZ"]
                    mes_abrev = meses_abrev[mes_num - 1]

                    primeiro_dia = QDate(int(ano), mes_num, 1)
                    ultimo_dia   = QDate(int(ano), mes_num, primeiro_dia.daysInMonth())
                    data_inicio  = primeiro_dia.toString("yyyy-MM-dd")
                    data_fim     = ultimo_dia.toString("yyyy-MM-dd")

                    # Verificar sobreescrita antecipada para todos os grupos
                    arquivos_existentes = []
                    meta_grupos = []   # lista de dicts com info pronta para cada grupo

                    for cod_interf, medidores_grupo in grupos.items():
                        ids_grupo   = [d[0] for d in medidores_grupo]
                        rotulos     = [d[1] for d in medidores_grupo]
                        cnarh_base  = medidores_grupo[0][4] or "SEM_CNARH"
                        nome_arquivo = (
                            f"{cnarh_base}_{cod_interf}"
                            f"_consumo_diario_{ano}{mes_abrev}.txt"
                        )
                        caminho = os.path.join(downloads_path, nome_arquivo)
                        if os.path.exists(caminho):
                            arquivos_existentes.append(nome_arquivo)
                        meta_grupos.append({
                            "cod_interf": cod_interf,
                            "ids":        ids_grupo,
                            "rotulos":    rotulos,
                            "cnarh":      cnarh_base,
                            "nome":       nome_arquivo,
                            "caminho":    caminho,
                        })

                    if arquivos_existentes:
                        msg = (
                            f"Os seguintes arquivos já existem:\n\n  • "
                            + "\n  • ".join(arquivos_existentes)
                            + "\n\nDeseja substituir todos?"
                        ) if len(arquivos_existentes) > 1 else (
                            f"O arquivo '{arquivos_existentes[0]}' já existe.\n\nDeseja substituir?"
                        )
                        if QMessageBox.question(
                                self, "Arquivo(s) Existente(s)", msg,
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.No:
                            return

                    arquivos_gerados = []

                    for meta in meta_grupos:
                        ids_grupo    = meta["ids"]
                        rotulos      = meta["rotulos"]
                        cod_interf   = meta["cod_interf"]
                        caminho      = meta["caminho"]

                        # Soma das vazões nominais deste grupo
                        cursor.execute(
                            "SELECT vazao_nominal FROM tb_intervencao WHERE id IN %s ORDER BY rotulo;",
                            (tuple(ids_grupo),)
                        )
                        vazoes_nominais   = [r[0] for r in cursor.fetchall() if r[0] is not None]
                        vazao_nominal_total = sum(vazoes_nominais) if vazoes_nominais else 0.0

                        # Info complementar (usuário, operador etc.) do primeiro medidor do grupo
                        cursor.execute("""
                            SELECT inf.nome_usuario, inf.numero_cadastro,
                                   ot.nome, ot.email, inf.codigo_interferencia
                            FROM tb_intervencao i
                            LEFT JOIN tb_intervencao_interferencia ii ON i.id = ii.intervencao_id
                            LEFT JOIN tb_interferencia inf ON ii.interferencia_id = inf.id
                            LEFT JOIN tb_operador_telemetria ot ON i.operador_telemetria = ot.id
                            WHERE i.id = %s;
                        """, (ids_grupo[0],))
                        row_info = cursor.fetchone()
                        usuario, cnarh, op_nome, op_email, _ = row_info if row_info else (
                            None, None, None, None, None)

                        # Dados diários totalizados do grupo com validação de overflow
                        cursor.execute("""
                            SELECT 
                                DATE(t.data) as data_dia,
                                t.consumo_diario,
                                t.leituras_perc,
                                t.leituras_qtd,
                                t.vazao_media,
                                t.vazao_max,
                                t.duracao_hr,
                                tb.vazao_nominal
                            FROM tb_telemetria_intervencao_diaria t
                            JOIN tb_intervencao tb ON tb.id = t.intervencao_id
                            WHERE t.intervencao_id IN %s
                              AND DATE(t.data) BETWEEN %s AND %s
                            ORDER BY DATE(t.data);
                        """, (tuple(ids_grupo), data_inicio, data_fim))
                        dados_diarios = cursor.fetchall()

                        # ── Montar texto do relatório com validação ──
                        rel  = "RELATÓRIO DE MONITORAMENTO - TELEMETRIA (DADOS DIÁRIOS)\n"
                        rel += "=" * 60 + "\n\n"
                        rel += f"Medidores: {', '.join(rotulos)}\n"
                        rel += f"Interferência: {cod_interf}\n"

                        vn_conv = vazao_nominal_total * 3600
                        if len(ids_grupo) > 1:
                            det = " + ".join(
                                f"{v:.3f}".replace('.', ',') for v in vazoes_nominais)
                            rel += (f"Vazão nominal total: {vazao_nominal_total:.3f} m³/s "
                                    f"({vn_conv:.1f} m³/h)\n").replace('.', ',')
                            rel += f"  (Detalhe: {det} m³/s)\n"
                        else:
                            rel += (f"Vazão nominal: {vazao_nominal_total:.3f} m³/s "
                                    f"({vn_conv:.1f} m³/h)\n").replace('.', ',')

                        rel += f"Usuário: {usuario or 'Não informado'}\n"
                        rel += f"CNARH: {cnarh or 'Não informado'}\n"
                        rel += f"Operador: {op_nome or 'Não informado'}\n"
                        rel += f"E-mail do operador: {op_email or 'Não informado'}\n\n"
                        rel += f"PERÍODO: {mes_num:02d}/{ano}\n"
                        rel += "TIPO: DADOS DIÁRIOS\n\n"

                        rel += ("DATA      | LEIT | %LEIT | VZ MÉD (m³/s) | VZ MAX (m³/s)"
                                " | VZ OP MÉD (m³/s) | CONS (m³) | DUR (h) | STATUS\n")
                        rel += "-" * 115 + "\n"

                        tot_leit = 0; tot_cons = 0.0; tot_dur = 0.0
                        tot_dias_anomalos = 0
                        vz_medias = []; vz_ops = []

                        for row in dados_diarios:
                            (data_dia, consumo_bruto, lp, lq, vm, vmax, dur, vn) = row
                            
                            # Aplicar validação de overflow
                            validacao = self.validar_consumo_diario(
                                consumo_diario=consumo_bruto,
                                vazao_nominal=vn,
                                vazao_media=vm,
                                duracao_hr=dur
                            )
                            
                            consumo_exibir = validacao['valor_exibicao']
                            is_anomalia = validacao['is_anomalia']
                            
                            tot_leit += int(lq or 0)
                            tot_cons += consumo_exibir
                            tot_dur  += float(dur or 0)
                            if is_anomalia:
                                tot_dias_anomalos += 1
                            if vm  is not None: vz_medias.append(vm)
                            if vm is not None: vz_ops.append(vm)  # Usando vazao_media como proxy

                            ds = (data_dia.strftime('%Y-%m-%d')
                                  if hasattr(data_dia, 'strftime') else str(data_dia)[:10])
                            
                            status = "⚠️ CORRIGIDO" if is_anomalia else "OK"
                            
                            linha = (
                                f"{ds} | {int(lq or 0):4d} | {float(lp or 0):5.1f} |"
                                f" {float(vm or 0):12.3f} | {float(vmax or 0):12.3f} |"
                                f" {float(vm or 0):14.3f} | {consumo_exibir:8.1f} |"
                                f" {float(dur or 0):5.1f} | {status}"
                            ).replace('.', ',')
                            rel += linha + "\n"

                        rel += "-" * 115 + "\n\n"

                        if dados_diarios:
                            rel += "ESTATÍSTICAS DO MÊS:\n"
                            if vz_medias:
                                vm_m = sum(vz_medias) / len(vz_medias)
                                rel += (f"  • Vazão média mensal: {vm_m:.3f} m³/s "
                                        f"({vm_m*3600:.1f} m³/h)\n").replace('.', ',')
                            else:
                                rel += "  • Vazão média mensal: --\n"
                            if vz_ops:
                                vo_m = sum(vz_ops) / len(vz_ops)
                                rel += (f"  • Vazão de operação média: {vo_m:.3f} m³/s "
                                        f"({vo_m*3600:.1f} m³/h)\n").replace('.', ',')
                            else:
                                rel += "  • Vazão de operação média: --\n"
                            rel += f"  • Consumo total: {tot_cons:.1f} m³\n".replace('.', ',')
                            if tot_dias_anomalos > 0:
                                rel += f"  • ⚠️ Dias com anomalia (overflow): {tot_dias_anomalos}\n"
                            rel += f"  • Duração total: {tot_dur:.1f} horas\n".replace('.', ',')
                            rel += f"  • Leituras totais: {tot_leit}\n"

                        rel += (f"\nRelatório gerado em: "
                                f"{QDateTime.currentDateTime().toString('dd/MM/yyyy HH:mm:ss')}")

                        with open(caminho, mode='w', encoding='utf-8') as f:
                            f.write(rel)
                        arquivos_gerados.append(meta["nome"])

                    # Mensagem final
                    if len(arquivos_gerados) == 1:
                        QMessageBox.information(
                            self, "Relatório Exportado",
                            f"Relatório salvo com sucesso!\n\n"
                            f"Arquivo: {arquivos_gerados[0]}\n"
                            f"Local: {downloads_path}"
                        )
                    elif arquivos_gerados:
                        lista = "\n  • ".join(arquivos_gerados)
                        QMessageBox.information(
                            self, "Relatórios Exportados",
                            f"{len(arquivos_gerados)} arquivo(s) gerado(s):\n\n"
                            f"  • {lista}\n\nLocal: {downloads_path}"
                        )
                    else:
                        QMessageBox.warning(self, "Aviso",
                                            "Nenhum dado encontrado para exportação.")

                # ── ABA 1: DADOS A CADA 15 MINUTOS ────────────────────────────────
                else:
                    data_sel     = self.date_edit.date()
                    data_str_db  = data_sel.toString("yyyy-MM-dd")
                    data_str_fmt = data_sel.toString("dd/MM/yyyy")
                    data_file    = data_sel.toString("yyyyMMdd")

                    # Verificar sobreescrita antecipada para todos os grupos/medidores
                    arquivos_existentes = []
                    meta_15min = []   # [{cod_interf, ids, medidores_info: [...]}]

                    for cod_interf, medidores_grupo in grupos.items():
                        ids_grupo = [d[0] for d in medidores_grupo]

                        # Soma das vazões nominais do grupo (para cabeçalho)
                        cursor.execute(
                            "SELECT SUM(vazao_nominal) FROM tb_intervencao WHERE id IN %s;",
                            (tuple(ids_grupo),)
                        )
                        soma_vn = cursor.fetchone()
                        vn_grupo_total = float(soma_vn[0]) if soma_vn and soma_vn[0] else 0.0

                        medidores_info = []
                        for d in medidores_grupo:
                            id_med = d[0]
                            cursor.execute("""
                                SELECT i.rotulo, i.vazao_nominal,
                                       inf.nome_usuario, inf.numero_cadastro,
                                       ot.nome, ot.email, inf.codigo_interferencia
                                FROM tb_intervencao i
                                LEFT JOIN tb_intervencao_interferencia ii
                                       ON i.id = ii.intervencao_id
                                LEFT JOIN tb_interferencia inf
                                       ON ii.interferencia_id = inf.id
                                LEFT JOIN tb_operador_telemetria ot
                                       ON i.operador_telemetria = ot.id
                                WHERE i.id = %s;
                            """, (id_med,))
                            info = cursor.fetchone()
                            if not info:
                                continue
                            rotulo, vn_ind, usuario, cnarh, op_nome, op_email, cod_db = info
                            rot_seguro = "".join(
                                c for c in rotulo if c.isalnum() or c in (' ', '-', '_')
                            ).strip()
                            cnarh_f  = cnarh or "SEM_CNARH"
                            nome_arq = (f"{cnarh_f}_{cod_interf}_{rot_seguro}"
                                        f"_consumo_dia_{data_file}.txt")
                            caminho  = os.path.join(downloads_path, nome_arq)
                            if os.path.exists(caminho):
                                arquivos_existentes.append(nome_arq)
                            medidores_info.append({
                                "id":          id_med,
                                "rotulo":      rotulo,
                                "rot_seguro":  rot_seguro,
                                "vn_ind":      vn_ind or 0.0,
                                "usuario":     usuario,
                                "cnarh":       cnarh,
                                "op_nome":     op_nome,
                                "op_email":    op_email,
                                "nome_arq":    nome_arq,
                                "caminho":     caminho,
                            })

                        meta_15min.append({
                            "cod_interf":   cod_interf,
                            "ids":          ids_grupo,
                            "vn_total":     vn_grupo_total,
                            "medidores":    medidores_info,
                        })

                    if arquivos_existentes:
                        msg = (
                            f"Os seguintes arquivos já existem:\n\n  • "
                            + "\n  • ".join(arquivos_existentes)
                            + "\n\nDeseja substituir todos?"
                        ) if len(arquivos_existentes) > 1 else (
                            f"O arquivo '{arquivos_existentes[0]}' já existe.\n\nDeseja substituir?"
                        )
                        if QMessageBox.question(
                                self, "Arquivo(s) Existente(s)", msg,
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.No:
                            return

                    arquivos_gerados = []

                    for grp in meta_15min:
                        cod_interf  = grp["cod_interf"]
                        vn_total    = grp["vn_total"]

                        for mi in grp["medidores"]:
                            id_med   = mi["id"]
                            rotulo   = mi["rotulo"]
                            vn_ind   = mi["vn_ind"]
                            caminho  = mi["caminho"]
                            nome_arq = mi["nome_arq"]

                            # Estatísticas diárias consolidadas
                            cursor.execute("""
                                SELECT consumo_diario, leituras_qtd, leituras_perc,
                                       vazao_media, vazao_max, duracao_hr
                                FROM tb_telemetria_intervencao_diaria
                                WHERE intervencao_id = %s AND DATE(data) = %s;
                            """, (id_med, data_str_db))
                            ds = cursor.fetchone()
                            cons_tot = float(ds[0] or 0) if ds else 0
                            lq_tot   = int(ds[1]   or 0) if ds else 0
                            lp_med   = float(ds[2]  or 0) if ds else 0
                            vm_med   = float(ds[3]  or 0) if ds else 0
                            vmax_med = float(ds[4]  or 0) if ds else 0
                            dur_tot  = float(ds[5]  or 0) if ds else 0

                            # Dados a cada 15 min
                            cursor.execute("""
                                SELECT data, vazao, consumo, duracao
                                FROM tb_telemetria_intervencao
                                WHERE intervencao_id = %s AND DATE(data) = %s
                                ORDER BY data;
                            """, (id_med, data_str_db))
                            dados_15 = cursor.fetchall()

                            # ── Montar texto (idêntico ao caso simples) ──
                            rel  = ("RELATÓRIO DE MONITORAMENTO - TELEMETRIA"
                                    " (DADOS A CADA 15 MINUTOS)\n")
                            rel += "=" * 60 + "\n\n"
                            rel += f"Medidor: {rotulo}\n"
                            rel += f"Interferência: {cod_interf}\n"

                            vn_ind_conv   = vn_ind   * 3600
                            vn_total_conv = vn_total * 3600
                            # Exibe vazão individual + total do grupo quando há
                            # mais de um medidor na interferência
                            if len(grp["ids"]) > 1:
                                rel += (f"Vazão nominal do medidor: {vn_ind:.3f} m³/s "
                                        f"({vn_ind_conv:.1f} m³/h)\n").replace('.', ',')
                                rel += (f"Vazão nominal total da interferência: "
                                        f"{vn_total:.3f} m³/s "
                                        f"({vn_total_conv:.1f} m³/h)\n").replace('.', ',')
                            else:
                                rel += (f"Vazão nominal: {vn_ind:.3f} m³/s "
                                        f"({vn_ind_conv:.1f} m³/h)\n").replace('.', ',')

                            rel += f"Usuário: {mi['usuario'] or 'Não informado'}\n"
                            rel += f"CNARH: {mi['cnarh'] or 'Não informado'}\n"
                            rel += f"Operador: {mi['op_nome'] or 'Não informado'}\n"
                            rel += f"E-mail do operador: {mi['op_email'] or 'Não informado'}\n\n"
                            rel += f"DATA: {data_str_fmt}\n"
                            rel += "TIPO: DADOS A CADA 15 MINUTOS\n\n"

                            rel += "HORA   | VAZÃO (m³/s) | CONSUMO (m³) | DURAÇÃO (s)\n"
                            rel += "-" * 55 + "\n"

                            for dh, vazao, consumo, duracao in dados_15:
                                if hasattr(dh, 'strftime'):
                                    hora_s = dh.strftime("%H:%M")
                                else:
                                    hora_s = str(dh)[11:16]
                                linha = (
                                    f"{hora_s} | {float(vazao or 0):11.3f} |"
                                    f" {float(consumo or 0):11.1f} | {int(duracao or 0):12d}"
                                ).replace('.', ',')
                                rel += linha + "\n"

                            rel += "-" * 55 + "\n"

                            if ds:
                                rel += "\nESTATÍSTICAS DO DIA:\n"
                                rel += (f"  • Vazão média: {vm_med:.3f} m³/s "
                                        f"({vm_med*3600:.1f} m³/h)\n").replace('.', ',')
                                rel += (f"  • Vazão máxima: {vmax_med:.3f} m³/s "
                                        f"({vmax_med*3600:.1f} m³/h)\n").replace('.', ',')
                                rel += f"  • Consumo total: {cons_tot:.1f} m³\n".replace('.', ',')
                                rel += f"  • Duração total: {dur_tot:.2f} horas\n".replace('.', ',')
                                rel += f"  • Número de leituras: {lq_tot}\n"

                            rel += (f"\nRelatório gerado em: "
                                    f"{QDateTime.currentDateTime().toString('dd/MM/yyyy HH:mm:ss')}")

                            with open(caminho, mode='w', encoding='utf-8') as f:
                                f.write(rel)
                            arquivos_gerados.append(nome_arq)

                    if len(arquivos_gerados) == 1:
                        QMessageBox.information(
                            self, "Relatório Exportado",
                            f"Relatório salvo com sucesso!\n\n"
                            f"Arquivo: {arquivos_gerados[0]}\n"
                            f"Local: {downloads_path}"
                        )
                    elif arquivos_gerados:
                        lista = "\n  • ".join(arquivos_gerados)
                        QMessageBox.information(
                            self, "Relatórios Exportados",
                            f"{len(arquivos_gerados)} arquivo(s) gerado(s):\n\n"
                            f"  • {lista}\n\nLocal: {downloads_path}"
                        )
                    else:
                        QMessageBox.warning(self, "Aviso",
                                            "Nenhum dado encontrado para exportação.")

            except Exception as e:
                QMessageBox.critical(self, "Erro", f"Erro ao exportar relatório: {e}")
                import traceback
                traceback.print_exc()
            finally:
                QApplication.restoreOverrideCursor()
                if 'cursor' in locals():
                    cursor.close()
  
    def center(self):
        screen_geometry = QDesktopWidget().screenGeometry()
        center_point = screen_geometry.center()
        frame_geometry = self.frameGeometry()
        frame_geometry.moveCenter(center_point)
        self.move(frame_geometry.topLeft())
    
    def voltar(self):
        # Desativar modo edição se estiver ativo
        if self.modo_edicao:
            resposta = QMessageBox.question(
                self,
                "Modo Edição Ativo",
                "O modo de edição está ativo. Deseja realmente sair?\n\n"
                "Todas as alterações não salvas serão perdidas.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if resposta == QMessageBox.No:
                return
        
        self.close()
        try:
            self.janela_anterior.show()
        except RuntimeError:
            pass
    
    def closeEvent(self, event):
        """Evento chamado ao fechar a janela."""
        # Verificar se há alterações não salvas
        if self.modo_edicao and self.dados_editados:
            resposta = QMessageBox.question(
                self,
                "Alterações Não Salvas",
                "Existem alterações não salvas. Deseja realmente sair?\n\n"
                "Todas as alterações não salvas serão perdidas.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if resposta == QMessageBox.No:
                event.ignore()
                return
        
        # Limpar cache ao fechar a janela
        self.cache_calendario = {}
        self.cache_15min = {}
        event.accept()
   
