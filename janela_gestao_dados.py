# -*- coding: utf-8 -*-
"""
Módulo: janela_gestao_dados.py
================================
Janela hub de gestão de dados do plugin DURH Diária por Telemetria.

Organiza em QTabWidget as seguintes abas:
  - [0] Dashboard        : KPIs, gauges e gráfico por sistema hídrico;
  - [1] Operadores       : consulta, edição, exclusão e exportação XLSX;
  - [2] Medidores        : busca, edição, desativação, reativação e exportação XLSX;
  - [3] Atualizar base   : ETL ArcGIS MapServer + Oracle DW → PostGIS
                           (visível somente para perfil diferente de telemetria_ro).

O tamanho da janela é ajustado dinamicamente ao trocar de aba
via ajustar_tamanho_aba().

Autor : SFI/ANA
Versão: 2.0 – Março/2026
"""

# ---------------------------------------------------------------------------
# Imports Qt – widgets, layout e utilitários de UI
# ---------------------------------------------------------------------------
from qgis.PyQt.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTabWidget, QDesktopWidget, QMessageBox,
)
from qgis.PyQt.QtCore import Qt

# ---------------------------------------------------------------------------
# Imports Python padrão
# ---------------------------------------------------------------------------
import psycopg2

# ---------------------------------------------------------------------------
# Sistema de design centralizado
# ---------------------------------------------------------------------------
try:
    from . import ui_tema
except ImportError:
    import ui_tema

# ---------------------------------------------------------------------------
# Widgets das abas
# ---------------------------------------------------------------------------
from .widget_dashboard        import WidgetDashboard
from .widget_operadores       import WidgetOperadores
from .widget_medidores        import WidgetMedidores
from .widget_atualizacao_base import WidgetAtualizacaoBase


class JanelaGestaoDados(QWidget):
    """Janela central de gestão de dados cadastrados no sistema DURH Diária por Telemetria.

    Atua como hub de navegação pós-login, organizando em abas todas as
    funcionalidades de consulta, edição e manutenção da base de dados do
    plugin. Ao ser instanciada, reinicia a transação PostgreSQL pendente e
    força autocommit para evitar locks residuais de sessões anteriores.

    A janela é construída em três etapas sequenciais:
        1. Recebe e armazena as credenciais e a conexão já autenticadas.
        2. Reinicia o estado da conexão (rollback + autocommit).
        3. Monta o QTabWidget com quatro abas subordinadas: Dashboard,
           Operadores cadastrados, Medidores cadastrados e — apenas para
           usuários que não sejam ``telemetria_ro`` — Atualizar base de dados.

    O tamanho da janela é ajustado dinamicamente via ``ajustar_tamanho_aba``
    sempre que o usuário troca de aba, garantindo que cada widget filho
    tenha a área de exibição adequada.

    Attributes:
        tela_inicial (QWidget): Referência à ``TelaInicial``; exibida novamente
            ao pressionar "Voltar para Menu".
        conn (psycopg2.connection): Conexão ativa com o banco PostgreSQL,
            operando em modo autocommit.
        usuario_logado (str | None): Nome do usuário autenticado; utilizado
            para controle de visibilidade de abas restritas.
        senha (str | None): Senha do usuário; repassada ao
            ``WidgetAtualizacaoBase`` para abertura de conexão dedicada
            durante o processo ETL.
        tabs (QTabWidget): Contêiner de abas principal da janela.
        widget_dashboard (WidgetDashboard): Aba de resumo executivo com
            indicadores gerais do sistema.
        widget_operadores (WidgetOperadores): Aba de consulta e edição de
            operadores de telemetria cadastrados.
        widget_medidores (WidgetMedidores): Aba de consulta, edição e
            desativação de medidores cadastrados.
        widget_atualizacao (WidgetAtualizacaoBase): Aba de atualização ETL
            das tabelas de obrigatoriedade e CNARH (oculta para leitores).
    """
       
    def __init__(self, tela_inicial, conexao, usuario=None, senha=None):
        super().__init__()
        self.tela_inicial = tela_inicial
        self.setWindowTitle("Cadastros e base de dados - DURH Diária por Telemetria")     
        self.conn = conexao
        self.usuario_logado = usuario
        self.senha = senha
        
        try:
            self.conn.rollback()
            self.conn.autocommit = True
        except Exception as e:
            print(f"Erro ao resetar conexão em JanelaGestaoDados: {e}")        

        self.setFixedSize(690, 720)
        self.center()
        
        # Widgets principais
        self.tabs = QTabWidget()
        
        self.initUI()
        
    def initUI(self):
        """Configura a interface da janela de gestão usando abas."""
        ui_tema.aplicar_tema_arredondado(self)
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)

        # Widgets para cada tipo de dados
        self.widget_dashboard = WidgetDashboard(self.conn, self.usuario_logado)
        self.widget_operadores = WidgetOperadores(self.conn, self.usuario_logado, self)
        self.widget_medidores = WidgetMedidores(self.conn, self.usuario_logado)
        self.widget_atualizacao = WidgetAtualizacaoBase(
            self.conn,
            self.usuario_logado,
            self.senha,
            parent_tabs=self.tabs,
        )

        # Adicionando as abas (Dashboard primeiro, à esquerda)
        self.tabs.addTab(self.widget_dashboard,  "Dashboard")
        self.tabs.addTab(self.widget_operadores, "Operadores cadastrados")
        self.tabs.addTab(self.widget_medidores,  "Medidores cadastrados")
        
        # Aba visível apenas para usuários que não sejam 'telemetria_ro'
        if self.usuario_logado != "telemetria_ro":
            self.tabs.addTab(self.widget_atualizacao, "Atualizar base de dados")        
        
        self.tabs.currentChanged.connect(self.ajustar_tamanho_aba)

        layout.addWidget(self.tabs)

        # Botão Voltar
        btn_voltar = QPushButton("Voltar para Menu")
        btn_voltar.setStyleSheet(f"""
            QPushButton {{
                background-color: transparent;
                color: {ui_tema.StyleConfig.SECONDARY_COLOR};
                border: 1px solid {ui_tema.StyleConfig.SECONDARY_COLOR};
                padding: 8px;
            }}
            QPushButton:hover {{
                background-color: #f0f0f0;
            }}
        """)
        btn_voltar.clicked.connect(self.voltar)
        layout.addWidget(btn_voltar)

        self.setLayout(layout)

    def ajustar_tamanho_aba(self, index):
        """Ajusta dinamicamente o tamanho da janela conforme a aba selecionada."""
        self.setMinimumSize(0, 0)
        self.setMaximumSize(16777215, 16777215)

        if index == 0:    # Dashboard
            nova_largura = 690
            nova_altura  = 720
            self.resize(nova_largura, nova_altura)
            self.setMinimumSize(600, 600)
            self.setMaximumSize(750, 750)

        elif index == 1:  # Operadores
            nova_largura = 690
            nova_altura  = 420
            self.resize(nova_largura, nova_altura)
            self.setFixedSize(nova_largura, nova_altura)

        elif index == 2:  # Medidores
            nova_largura = 850
            nova_altura  = 700
            self.resize(nova_largura, nova_altura)
            self.setMinimumSize(800, 650)
            self.setMaximumSize(1000, 750)

        self.center()
        self.updateGeometry()
        
    def center(self):
        """Centraliza a janela na tela."""
        screen_geometry = QDesktopWidget().screenGeometry()
        center_point = screen_geometry.center()
        frame_geometry = self.frameGeometry()
        frame_geometry.moveCenter(center_point)
        self.move(frame_geometry.topLeft())
        
    def voltar(self):
        """Volta para a tela inicial."""
        self.close()
        self.tela_inicial.show()

