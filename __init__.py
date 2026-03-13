from qgis.PyQt.QtCore import QTranslator, QCoreApplication, QTimer, Qt, QPoint
from qgis.PyQt.QtGui import QIcon, QPixmap, QPainter, QFont
from qgis.PyQt.QtWidgets import QAction, QDialog, QWidget, QVBoxLayout, QLabel, QApplication, QSplashScreen, QFrame, QMessageBox
from qgis.core import QgsApplication
import os

class SplashScreen(QSplashScreen):
    def __init__(self):
        # Cria um QPixmap transparente como base para permitir bordas arredondadas reais
        pixmap = QPixmap(450, 320)
        pixmap.fill(Qt.transparent)
        super().__init__(pixmap)
        
        # Atributos para permitir transparência e bordas arredondadas
        self.setWindowFlags(Qt.WindowStaysOnTopHint | Qt.FramelessWindowHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        
        # Layout principal em um QFrame para aplicar o estilo
        self.main_widget = QFrame()
        self.main_widget.setObjectName("SplashContainer")
        
        # Cores baseadas no ui_tema (Primary: #175cc3, Secondary: #5474b8)
        self.main_widget.setStyleSheet("""
            QFrame#SplashContainer {
                background-color: white;
                border: 1px solid #E0E0E0;
                border-radius: 20px;
            }
        """)
        
        layout = QVBoxLayout(self.main_widget)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(10)
        
        # Carrega a imagem (Logo)
        plugin_dir = os.path.dirname(__file__)
        splash_path = os.path.join(plugin_dir, 'splash.png')
        
        self.image_label = QLabel()
        try:
            logo_pixmap = QPixmap(splash_path)
            if not logo_pixmap.isNull():
                logo_pixmap = logo_pixmap.scaled(280, 180, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                self.image_label.setPixmap(logo_pixmap)
        except:
            pass
        self.image_label.setAlignment(Qt.AlignCenter)
        self.image_label.setStyleSheet("background: transparent; border: none;")
        
        # Título Principal
        self.lbl_titulo = QLabel("DURH Diária por Telemetria")
        self.lbl_titulo.setStyleSheet("color: #175cc3; font-size: 20px; font-weight: bold; background: transparent; border: none;")
        self.lbl_titulo.setAlignment(Qt.AlignCenter)
        
        # Subtítulo
        self.lbl_subtitulo = QLabel("Monitoramento e Cadastro")
        self.lbl_subtitulo.setStyleSheet("color: #5474b8; font-size: 16px; font-weight: 500; background: transparent; border: none;")
        self.lbl_subtitulo.setAlignment(Qt.AlignCenter)
        
        # Informação Legal/Versão
        self.lbl_info = QLabel()
        self.lbl_info.setTextFormat(Qt.RichText)
        self.lbl_info.setText("""
        <div style='text-align: center;'>
            <p style='font-size: 11px; color: #333333; margin-bottom: 5px;'><b>Resolução ANA n. 188, de 20 de março de 2024</b></p>
            <p style='font-size: 10px; color: #666666;'>Versão 2.0 - Março/2026</p>
            <p style='font-size: 10px; color: #666666;'>SFI / ANA</p>
        </div>
        """)
        self.lbl_info.setAlignment(Qt.AlignCenter)
        self.lbl_info.setStyleSheet("background: transparent; border: none;")
             
        # Adiciona ao layout
        layout.addWidget(self.image_label)
        layout.addSpacing(10)
        layout.addWidget(self.lbl_titulo)
        layout.addWidget(self.lbl_subtitulo)
        layout.addStretch()
        #layout.addSpacing(10)        
        layout.addWidget(self.lbl_info)
        
        # Configura o tamanho e centraliza
        self.setFixedSize(400, 280)
        self.main_widget.setFixedSize(self.size())
        self.centerOnScreen()
        
    def centerOnScreen(self):
        """Centraliza a janela na tela"""
        screen = QApplication.primaryScreen().geometry()
        size = self.geometry()
        self.move(
            (screen.width() - size.width()) // 2,
            (screen.height() - size.height()) // 2
        )
        
    def show(self):
        """Mostra com efeito de fade-in"""
        super().show()
        self.setWindowOpacity(0)
        
        # Animação de fade-in
        self.fade_timer = QTimer()
        self.fade_timer.timeout.connect(self.increaseOpacity)
        self.fade_timer.start(50)
        
    def increaseOpacity(self):
        """Aumenta gradualmente a opacidade"""
        if self.windowOpacity() < 1:
            self.setWindowOpacity(self.windowOpacity() + 0.1)
        else:
            self.fade_timer.stop()
            
    def close(self):
        """Fecha com efeito de fade-out"""
        self.fade_timer = QTimer()
        self.fade_timer.timeout.connect(self.decreaseOpacity)
        self.fade_timer.start(50)
        
    def decreaseOpacity(self):
        """Diminui gradualmente a opacidade"""
        if self.windowOpacity() > 0:
            self.setWindowOpacity(self.windowOpacity() - 0.1)
        else:
            self.fade_timer.stop()
            super().close()

    def paintEvent(self, event):
        """Sobrescreve o paintEvent para desenhar o widget"""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        self.main_widget.render(painter, self.rect().topLeft())

class Cadastro:
    def __init__(self, iface):
        self.iface = iface
        self.plugin_dir = os.path.dirname(__file__)
        self.actions = []
        self.menu = "ANA"
        self.splash = None

    def initGui(self):
        icon_path = os.path.join(self.plugin_dir, "icon.png")
        self.action = QAction(QIcon(icon_path), "DURH Diária por Telemetria v2.0", self.iface.mainWindow())
        self.action.triggered.connect(self.run_with_splash)
        self.iface.addToolBarIcon(self.action)
        self.iface.addPluginToMenu(self.menu, self.action)
        self.actions.append(self.action)

    def run_with_splash(self):
        """Mostra a splash screen antes de carregar o plugin"""
        self.splash = SplashScreen()
        self.splash.show()
        
        # Processa eventos para garantir que a splash seja exibida
        QCoreApplication.processEvents()
        
        # Usa QTimer para garantir que a interface não congele
        QTimer.singleShot(2500, self.run_after_splash)

    def run_after_splash(self):
        """
        Fluxo ajustado: Splash Fecha (Timer) -> Login Abre -> Tela Inicial Abre
        """
        from qgis.PyQt.QtWidgets import QDialog, QMessageBox
        from qgis.PyQt.QtCore import QTimer
        
        try:
            # Importa JanelaLogin (mas não cria ainda)
            from .main_plugin import JanelaLogin
            
            # PASSO 1: Fechar a Splash usando um Timer (Para liberar a thread do clique)
            if self.splash:
                self.splash.close() # Fecha visualmente agora para não ficar "fantasma"
                # O "deferred" do fechar visual já é feito pelo close().
                
            # PASSO 2: Abrir Janela de Login após um pequeno delay
            # Isso garante que a Splash já "saio" antes do Login surgir
            QTimer.singleShot(150, self._abrir_janela_login)
            
        except Exception as e:
            if self.splash:
                self.splash.close()
            QMessageBox.critical(None, "Erro", f"Erro ao iniciar plugin: {e}")

    def _abrir_janela_login(self):
        """Abre a janela de login e gerencia o fluxo pós-login."""
        from qgis.PyQt.QtWidgets import QDialog, QMessageBox
        from .main_plugin import JanelaLogin, TelaInicial
        
        dialogo_login = JanelaLogin()
        resultado_login = dialogo_login.exec_()
        
        # Se o Login foi aceito (clicou em Conectar)
        if resultado_login == QDialog.Accepted:
            conexao_ativa = dialogo_login.conn
            
            if conexao_ativa:
                try:
                    # Abre Tela Inicial
                    usuario_logado = dialogo_login.usuario
                    senha_logado = dialogo_login.senha
                    self.widget = TelaInicial(self.iface, conexao_ativa, usuario_logado, senha_logado)
                    self.widget.show()
                except Exception as e:
                    QMessageBox.critical(None, "Erro", f"Erro ao carregar plugin: {e}")
            else:
                QMessageBox.warning(None, "Erro", "Não foi possível estabelecer conexão.")

    def unload(self):
        for action in self.actions:
            self.iface.removePluginMenu(self.menu, action)
            self.iface.removeToolBarIcon(action)
        if hasattr(self, 'splash') and self.splash:
            self.splash.close()
        if hasattr(self, 'widget'):
            self.widget.close()
            del self.widget            

def classFactory(iface):
    return Cadastro(iface)