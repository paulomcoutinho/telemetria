# -*- coding: utf-8 -*-
"""
Módulo: main_plugin.py
======================
Orquestrador do plugin DURH Diária por Telemetria.

Contém exclusivamente as duas classes de entrada da interface:
  - JanelaLogin  : diálogo modal de autenticação PostgreSQL.
  - TelaInicial  : menu principal com cards de navegação.

Todas as demais classes residem em módulos independentes (um arquivo
por classe, nomeados em snake_case idêntico ao nome da classe).
Os imports abaixo tornam essas classes disponíveis no escopo deste
módulo, permitindo que TelaInicial as instancie normalmente.

Autor : SFI/ANA
Versão: 2.0 – Março/2026
"""

# ---------------------------------------------------------------------------
# Imports Qt – widgets, layout e utilitários de UI
# ---------------------------------------------------------------------------
from qgis.PyQt.QtWidgets import (
    QWidget, QDialog, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QCheckBox, QToolButton,
    QFrame, QMessageBox, QDesktopWidget, QApplication,
)
from qgis.PyQt.QtCore import Qt, QSettings, QTimer, QSize
from qgis.PyQt.QtGui import QIcon

# ---------------------------------------------------------------------------
# Imports QGIS core / utils
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
# Módulos filhos – cada classe em seu próprio arquivo
# ---------------------------------------------------------------------------
from .tela_cadastro_operadores      import TelaCadastroOperadores
from .tela_cadastro_medidores       import TelaCadastroMedidores
from .janela_gestao_dados           import JanelaGestaoDados
from .janela_monitoramento          import JanelaMonitoramento


class JanelaLogin(QDialog):
    """Janela de autenticação para acesso ao banco de dados DURH Diária.
    
    Solicita credenciais de acesso antes de permitir o uso das funcionalidades
    de cadastro e monitoramento. As credenciais são validadas através de uma tentativa de conexão
    ao banco de dados PostgreSQL.

    Attributes:
        parent (QWidget): Widget pai (opcional).
        conn (psycopg2.connection): Conexão ativa com o banco (se autenticado).
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Autenticação - DURH Diária por Telemetria")
        
        # Ajuste de tamanho para acomodar o checkbox
        self.setFixedSize(380, 280) 
        
        # Se ui_tema estiver disponível no escopo global
        try:
            ui_tema.aplicar_tema_arredondado(self)
        except NameError:
            pass
            
        self.usuario = None
        self.senha = None
        
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        
        # === ÚNICO CONTAINER BRANCO PRINCIPAL ===
        container_principal = QFrame()
        container_principal.setObjectName("ContainerBranco")
        layout_principal = QVBoxLayout(container_principal)
        layout_principal.setContentsMargins(25, 20, 25, 20)
        
        # Título
        titulo = QLabel("Acesso ao Sistema")
        # Usando uma cor fallback caso ui_tema não carregue, mas mantém sua lógica original
        cor_primaria = getattr(ui_tema.StyleConfig, 'PRIMARY_COLOR', '#007bff')
        titulo.setStyleSheet(f"font-size: 16px; font-weight: bold; color: {cor_primaria}; margin-bottom: 5px;")
        titulo.setAlignment(Qt.AlignCenter)
        layout_principal.addWidget(titulo)
        
        # === CAMPO USUÁRIO (Texto + Input lado a lado) ===
        linha_usuario = QWidget()
        layout_usuario = QHBoxLayout(linha_usuario)
        layout_usuario.setContentsMargins(0, 0, 0, 0)
        layout_usuario.setSpacing(10)
        
        lbl_usuario = QLabel("Usuário:")
        lbl_usuario.setStyleSheet("font-weight: bold; color: #495057;")
        lbl_usuario.setFixedWidth(70)
        
        self.usuario_input = QLineEdit()
        self.usuario_input.setPlaceholderText("Digite o usuário")
        self.usuario_input.setFixedHeight(35)
        
        layout_usuario.addWidget(lbl_usuario)
        layout_usuario.addWidget(self.usuario_input)
        layout_principal.addWidget(linha_usuario)
        
        # === CAMPO SENHA (Texto + Input + Botão Olho) ===
        linha_senha = QWidget()
        layout_senha = QHBoxLayout(linha_senha)
        layout_senha.setContentsMargins(0, 0, 0, 0)
        layout_senha.setSpacing(10)
        
        lbl_senha = QLabel("Senha:")
        lbl_senha.setStyleSheet("font-weight: bold; color: #495057;")
        lbl_senha.setFixedWidth(70)
        
        self.senha_input = QLineEdit()
        self.senha_input.setEchoMode(QLineEdit.Password)
        self.senha_input.setPlaceholderText("Digite a senha")
        self.senha_input.setFixedHeight(35)
        
        self.toggle_senha_btn = QToolButton()
        self.toggle_senha_btn.setCursor(Qt.PointingHandCursor)
        self.toggle_senha_btn.setFixedSize(35, 35)
        self.toggle_senha_btn.setIconSize(QSize(18, 18))
        self.toggle_senha_btn.setStyleSheet("""
            QToolButton {
                border: 1px solid #ccc;
                border-radius: 8px;
                background-color: #f8f9fa;
            }
            QToolButton:hover {
                background-color: #e2e6ea;
                border: 1px solid #adb5bd;
            }
        """)
        self.toggle_senha_btn.clicked.connect(self.toggle_visibilidade_senha)
        
        # Define estado inicial (Oculto = False) e atualiza o ícone
        self.senha_visivel = False
        self.atualizar_icone()
        
        layout_senha.addWidget(lbl_senha)
        layout_senha.addWidget(self.senha_input)
        layout_senha.addWidget(self.toggle_senha_btn)
        layout_principal.addWidget(linha_senha)

        # === CHECKBOX "Lembrar Credenciais" ===
        self.lembrar_chk = QCheckBox("Lembrar credenciais neste computador")
        self.lembrar_chk.setStyleSheet("""
            QCheckBox {
                color: #666666;
                font-size: 12px;
                padding: 5px 0;
            }
            QCheckBox::indicator {
                width: 16px;
                height: 16px;
                border: 1px solid #adb5bd;
                border-radius: 4px;
                background-color: white;
            }
            QCheckBox::indicator:checked {
                background-color: #175cc3;
                border: 1px solid #175cc3;
                image: url(':/images/themes/default/mIconCheckboxChecked.svg');
            }
            QCheckBox::indicator:unchecked:hover {
                border: 1px solid #175cc3;
            }
        """)
        layout_principal.addWidget(self.lembrar_chk)
        
        # Carrega credenciais salvas ANTES de definir placeholder/texto padrão
        self.carregar_credenciais_salvas()
        

        # Aumenta o espaço entre a senha/checkbox e os botões
        layout_principal.addStretch(1)
        
        # Botões
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)
        
        # Cores fallback para botões caso ui_tema não exista
        cor_prim = getattr(ui_tema.StyleConfig, 'PRIMARY_COLOR', '#007bff')
        cor_sec = getattr(ui_tema.StyleConfig, 'SECONDARY_COLOR', '#0056b3')
        
        self.login_btn = QPushButton("Conectar")
        self.login_btn.setDefault(True)
        self.login_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {cor_prim};
                color: white;
                border-radius: 8px;
                padding: 10px 20px;
                font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: {cor_sec};
            }}
        """)
        self.login_btn.clicked.connect(self.tentar_login)
        
        self.cancelar_btn = QPushButton("Cancelar")
        self.cancelar_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: transparent;
                color: {cor_sec};
                border: 1px solid {cor_sec};
                border-radius: 8px;
                padding: 10px 20px;
                font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: #f0f0f0;
            }}
        """)
        self.cancelar_btn.clicked.connect(self.reject)
        
        btn_layout.addWidget(self.login_btn)
        btn_layout.addWidget(self.cancelar_btn)
        layout_principal.addLayout(btn_layout)
        
        layout.addWidget(container_principal)
        self.setLayout(layout)
        self.conn = None

        # Foco inicial no campo de senha se usuário já estiver preenchido
        if self.usuario_input.text():
            self.senha_input.setFocus()
        else:
            self.usuario_input.setFocus()

    def carregar_credenciais_salvas(self):
        """Carrega credenciais salvas do QSettings (se existirem)"""
        settings = QSettings("ANA", "DURH_Telemetria")
        usuario_salvo = settings.value("auth/usuario", "", type=str)
        lembrar = settings.value("auth/lembrar", False, type=bool)
        
        if usuario_salvo and lembrar:
            self.usuario_input.setText(usuario_salvo)
            # Senha é carregada SOMENTE quando usuário coincide (evita vazamento cruzado)
            senha_salva = settings.value("auth/senha", "", type=str)
            if senha_salva:
                self.senha_input.setText(senha_salva)
            self.lembrar_chk.setChecked(True)

    def salvar_credenciais(self):
        """Salva credenciais no QSettings SOMENTE se checkbox estiver marcado"""
        settings = QSettings("ANA", "DURH_Telemetria")
        
        if self.lembrar_chk.isChecked():
            settings.setValue("auth/usuario", self.usuario_input.text().strip())
            settings.setValue("auth/senha", self.senha_input.text().strip())
            settings.setValue("auth/lembrar", True)
        else:
            # Limpa credenciais salvas se desmarcado (segurança)
            settings.remove("auth/usuario")
            settings.remove("auth/senha")
            settings.setValue("auth/lembrar", False)

    def atualizar_icone(self):
        """Atualiza o ícone carregando o SVG diretamente dos recursos do QGIS"""
        if self.senha_visivel:
            # Olho Aberto (Usado para mostrar camadas/senha)
            self.toggle_senha_btn.setIcon(QIcon(':/images/themes/default/mActionShowAllLayers.svg'))
        else:
            # Olho Fechado/Riscado (Usado para ocultar camadas/senha)
            self.toggle_senha_btn.setIcon(QIcon(':/images/themes/default/mActionHideAllLayers.svg'))

    def toggle_visibilidade_senha(self):
        """Alterna a visibilidade da senha e atualiza o ícone"""
        self.senha_visivel = not self.senha_visivel
        self.senha_input.setEchoMode(QLineEdit.Normal if self.senha_visivel else QLineEdit.Password)
        self.atualizar_icone()

    def tentar_login(self):
        usuario = self.usuario_input.text().strip()
        senha = self.senha_input.text().strip()
        
        if not usuario or not senha:
            QMessageBox.warning(self, "Campos Vazios", "Preencha usuário e senha.")
            return
            
        try:
            self.conn = psycopg2.connect(
                host="rds-webgis-dev.cjleec14qixz.sa-east-1.rds.amazonaws.com",
                port=5432,
                dbname="telemetria",
                user=usuario,
                password=senha
            )
            self.usuario = usuario
            self.senha = senha
            
            # Salva credenciais APENAS após autenticação bem-sucedida
            self.salvar_credenciais()
            
            self.accept()
        except psycopg2.OperationalError as e:
            # Trata erros específicos de conexão/autenticação
            error_msg = str(e).lower()
            
            if "password authentication failed" in error_msg:
                # Erro específico de senha incorreta
                QMessageBox.critical(
                    self, 
                    "Falha na Autenticação", 
                    "Usuário ou senha incorretos.\n\n"
                    "Verifique suas credenciais e tente novamente."
                )
            elif "connection refused" in error_msg or "timeout" in error_msg:
                # Erro de conexão com o servidor
                QMessageBox.critical(
                    self, 
                    "Falha na Conexão", 
                    "Não foi possível conectar ao servidor.\n\n"
                    "Verifique sua conexão com a internet e tente novamente."
                )
            elif "database" in error_msg and "does not exist" in error_msg:
                # Banco de dados não existe
                QMessageBox.critical(
                    self, 
                    "Banco de Dados Não Encontrado", 
                    "O banco de dados especificado não existe.\n\n"
                    "Contate o administrador do sistema."
                )
            else:
                # Outros erros operacionais
                QMessageBox.critical(
                    self, 
                    "Erro na Conexão", 
                    f"Não foi possível estabelecer conexão:\n\n{str(e)}"
                )
            
            self.conn = None
            # Limpa senha salva em caso de falha (segurança)
            if self.lembrar_chk.isChecked():
                settings = QSettings("ANA", "DURH_Telemetria")
                settings.remove("auth/senha")
        
        except psycopg2.Error as e:
            # Outros erros do psycopg2
            QMessageBox.critical(
                self, 
                "Erro no Banco de Dados", 
                f"Erro ao conectar ao banco de dados:\n\n{str(e)}"
            )
            self.conn = None
        
        except Exception as e:
            # Erros gerais
            QMessageBox.critical(
                self, 
                "Erro", 
                f"Ocorreu um erro inesperado:\n\n{str(e)}"
            )
            self.conn = None


class TelaInicial(QWidget):
    """Tela inicial do plugin com menu de opções principais.
    
    Permite acesso às funcionalidades de cadastro e monitoramento.
    
    Attributes:
        iface: Interface do QGIS.
    """
    def __init__(self, iface, conn, usuario=None, senha=None):
        """Inicializa a tela principal. Recebe a conexão já estabelecida pelo Login.
        
        Args:
            iface: Interface do QGIS.
            conn (psycopg2.connection): conexão com o banco (já autenticada).
        """    
        super().__init__()
        self.iface = iface
        self.setWindowTitle("DURH Diária por Telemetria - Cadastro e Monitoramento")
        self.conn = conn # Recebe a conexão direta do login

        # Se for leitura (ro), reduz a altura, pois terá menos cards.
        if usuario == "telemetria_ro":
            self.setFixedSize(540, 220)
        else:
            self.setFixedSize(540, 350)
        
        self.usuario_logado = usuario
        self.senha = senha
        self._janelas_abertas = []
        self.initUI()
        self.center()

    def initUI(self):
        """Configura a interface com verificação de login usando cards modernos."""     
        self.setStyleSheet(ui_tema.StyleConfig.MAIN_STYLE)
        
        # Layout principal
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)

        # Grid para os cards
        grid_layout = QGridLayout()
        grid_layout.setSpacing(15)

        # Lógica condicional para exibição dos Cards
        if self.usuario_logado == "telemetria_ro":
            # --- USUÁRIO LEITURA (RO) ---
            # Exibe apenas Gestão e Monitoramento na primeira linha
            
            # Card "Gestão de dados"
            self.btn_gestao = ui_tema.CardButton(
                "Dados Cadastrais", 
                "Visualizar situação do cadastro de operadores e medidores.",
                "gestao"
            )
            self.btn_gestao.clicked.connect(self.verificar_login_antes_gestao)
            grid_layout.addWidget(self.btn_gestao, 0, 0)

            # Card "Monitoramento"
            self.btn_monitoramento = ui_tema.CardButton(
                "Monitoramento", 
                "Telemetria em tempo real e estatísticas.",
                "monitoramento"
            )
            self.btn_monitoramento.clicked.connect(self.verificar_login_antes_monitoramento)
            grid_layout.addWidget(self.btn_monitoramento, 0, 1)

        else:
            # --- USUÁRIO ADMIN (RW) ou OUTROS ---
            # Exibe todos os cards (Layout 2x2 original)

            # Card "Gestão de dados"
            self.btn_gestao = ui_tema.CardButton(
                "Cadastros e dados", 
                "Visualizar, editar cadastros e atualizar base de dados.",
                "gestao"
            )
            self.btn_gestao.clicked.connect(self.verificar_login_antes_gestao)
            grid_layout.addWidget(self.btn_gestao, 0, 0)

            # Card "Monitoramento"
            self.btn_monitoramento = ui_tema.CardButton(
                "Monitoramento", 
                "Telemetria em tempo real, estatísticas e gestão de dados.",
                "monitoramento"
            )
            self.btn_monitoramento.clicked.connect(self.verificar_login_antes_monitoramento)
            grid_layout.addWidget(self.btn_monitoramento, 0, 1)
            
            # Card "Cadastrar Operador"
            self.btn_operador = ui_tema.CardButton(
                "Operadores", 
                "Cadastrar operadores de sistema de medição.",
                "operador"
            )
            self.btn_operador.clicked.connect(self.verificar_login_antes_operadores)
            grid_layout.addWidget(self.btn_operador, 1, 0)

            # Card "Cadastrar Medidor"
            self.btn_medidor = ui_tema.CardButton(
                "Medidores", 
                "Cadastrar medidores de vazão de água.",
                "medidor"
            )
            self.btn_medidor.clicked.connect(self.verificar_login_antes_medidores)
            grid_layout.addWidget(self.btn_medidor, 1, 1)

        main_layout.addLayout(grid_layout)
        
        # Botão Fechar
        self.btn_fechar = QPushButton("Sair do plugin")
        self.btn_fechar.setStyleSheet(f"""
            QPushButton {{
                background-color: transparent;
                color: {ui_tema.StyleConfig.SECONDARY_COLOR};
                border: 1px solid {ui_tema.StyleConfig.SECONDARY_COLOR};
                padding: 8px;
                margin-top: 8px;
            }}
            QPushButton:hover {{
                background-color: #f0f0f0;
            }}
        """)
        self.btn_fechar.clicked.connect(self.fechar)
        main_layout.addWidget(self.btn_fechar)

        self.setLayout(main_layout)

    def verificar_login_antes_monitoramento(self):
        """Verifica login antes de abrir monitoramento."""
        if self.verificar_conexao():
            self.abrir_monitoramento()
        else:
            QMessageBox.warning(self, "Erro de Conexão", "Sessão expirada ou inválida. O plugin será fechado.")
            self.fechar()

    def verificar_login_antes_operadores(self):
        """Verifica login antes de abrir cadastro de operadores."""
        if self.verificar_conexao():
            self.abrir_cadastro_operadores()
        else:
            QMessageBox.warning(self, "Erro de Conexão", "Sessão expirada ou inválida. O plugin será fechado.")
            self.fechar()

    def verificar_login_antes_medidores(self):
        """Verifica login antes de abrir cadastro de medidores."""
        if self.verificar_conexao():
            self.abrir_cadastro_medidores()
        else:
            QMessageBox.warning(self, "Erro de Conexão", "Sessão expirada ou inválida. O plugin será fechado.")
            self.fechar()

    def verificar_login_antes_gestao(self):
        """Verifica login antes de abrir gestão de dados."""
        if self.verificar_conexao():
            self.abrir_gestao_dados()
        else:
            QMessageBox.warning(self, "Erro de Conexão", "Sessão expirada ou inválida. O plugin será fechado.")
            self.fechar()

    def verificar_conexao(self):
        """Verifica se a conexão passada no login ainda é válida.
        
        Returns:
            bool: True se há conexão válida, False caso contrário.
        """
        # Como a conexão vem do login inicial, apenas verificamos se ela ainda existe e não foi fechada.
        if self.conn and not self.conn.closed:
            try:
                # Teste rápido para garantir que a conexão está realmente ativa
                cursor = self.conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return True
            except Exception as e:
                print(f"Erro ao verificar conexão: {e}")
                return False
        return False

    def center(self):
        """Centraliza a janela na tela."""        
        screen_geometry = QDesktopWidget().screenGeometry()  # Obtém a geometria da tela
        center_point = screen_geometry.center()  # Obtém o ponto central da tela
        frame_geometry = self.frameGeometry()  # Obtém a geometria da janela
        frame_geometry.moveCenter(center_point)  # Move o centro da janela para o centro da tela
        self.move(frame_geometry.topLeft())  # Move a janela para a posição correta

    def abrir_gestao_dados(self):
        """Abre tela de gestão de dados com conexão estabelecida."""
        self.tela_gestao = JanelaGestaoDados(self, self.conn, self.usuario_logado, self.senha)
        self.tela_gestao.show()
        self._janelas_abertas.append(self.tela_gestao)
        self.hide()

    def abrir_monitoramento(self):
        """Abre tela de monitoramento com conexão estabelecida."""
        self.tela_monitoramento = JanelaMonitoramento(self, self.conn, self.usuario_logado, self.senha)
        self.tela_monitoramento.show()
        self._janelas_abertas.append(self.tela_monitoramento)
        self.hide()
        
    def abrir_cadastro_operadores(self):
        """Abre tela de cadastro de operadores com conexão estabelecida."""
        self.tela_operadores = TelaCadastroOperadores(self, self.conn)
        self.tela_operadores.show()
        self._janelas_abertas.append(self.tela_operadores)
        self.hide()

    def abrir_cadastro_medidores(self):
        """Abre tela de cadastro de medidores com conexão estabelecida."""
        self.tela_medidores = TelaCadastroMedidores(
            self, 
            self.conn, 
            self.usuario_logado,
            self.senha
        )
        self.tela_medidores.show()
        self._janelas_abertas.append(self.tela_medidores)
        self.hide()

    def closeEvent(self, event):
        for janela in list(self._janelas_abertas):
            try:
                if janela and janela.isVisible():
                    janela.close()
            except RuntimeError:
                pass
        self._janelas_abertas.clear()
        event.accept()

    def fechar(self):
        """Fecha a aplicação."""    
        self.close()

