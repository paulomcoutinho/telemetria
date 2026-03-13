from qgis.PyQt.QtWidgets import (
    QPushButton, QVBoxLayout, QLabel, QFrame, QGraphicsDropShadowEffect, 
    QWidget, QHBoxLayout
)
from qgis.PyQt.QtCore import Qt, QSize
from qgis.PyQt.QtGui import QColor, QIcon, QFont, QPixmap, QPainter
from qgis.PyQt.QtSvg import QSvgRenderer

class StyleConfig:
    """Configurações globais de estilo para o plugin."""
    
    # Cores
    PRIMARY_COLOR = "#175cc3"
    SECONDARY_COLOR = "#5474b8"
    BACKGROUND_WHITE = "#FFFFFF"
    TEXT_DARK = "#333333"
    BORDER_COLOR = "#E0E0E0"
    HOVER_COLOR = "#F5F5F5"
    
    # Estilos CSS
    MAIN_STYLE = f"""
        QWidget {{
            background-color: #F8F9FA;
            font-family: 'Segoe UI', sans-serif;
        }}
        
        QLabel {{
            color: {TEXT_DARK};
        }}
        
        QLineEdit, QComboBox, QDateEdit, QDateTimeEdit {{
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            padding: 8px;
            background-color: {BACKGROUND_WHITE};
        }}
        
        QLineEdit:focus, QComboBox:focus {{
            border: 2px solid {PRIMARY_COLOR};
        }}
        
        QTabWidget::pane {{
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            background-color: {BACKGROUND_WHITE};
        }}
        
        QTabBar::tab {{
            background: #F0F0F0;
            border: 1px solid {BORDER_COLOR};
            border-bottom: none;
            padding: 12px 25px;
            border-top-left-radius: 10px;
            border-top-right-radius: 10px;
            margin-right: 4px;
            color: #666666;
        }}
        
        QTabBar::tab:selected {{
            background: {BACKGROUND_WHITE};
            border-bottom: 2px solid {PRIMARY_COLOR};
            font-weight: bold;
            color: {PRIMARY_COLOR};
        }}
        
        QTabBar::tab:hover:!selected {{
            background: #E8E8E8;
        }}
        
        QPushButton {{
            background-color: {PRIMARY_COLOR};
            color: white;
            border-radius: 8px;
            padding: 10px 20px;
            font-weight: bold;
        }}
        
        QPushButton:hover {{
            background-color: {SECONDARY_COLOR};
        }}

        QPushButton:disabled {{
            background-color: #cccccc;
            color: #666666;
            border-radius: 8px;
            padding: 10px 20px;
            font-weight: bold;
        }}
        
        QFrame#ContainerBranco {{
            background-color: {BACKGROUND_WHITE};
            border-radius: 15px;
            border: 1px solid {BORDER_COLOR};
        }}
    """

class CardButton(QPushButton):
    """Botão em formato de card com ícone e texto."""
    
    def __init__(self, title, description, icon_name, parent=None):
        super().__init__(parent)
        self.setFixedSize(240, 120)
        self.setCursor(Qt.PointingHandCursor)
        
        # Layout interno
        layout = QHBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(15)
        
        # Ícone
        self.icon_label = QLabel()
        self.icon_label.setFixedSize(48, 48)
        self.set_custom_icon(icon_name)
        layout.addWidget(self.icon_label)
        
        # Texto
        text_container = QVBoxLayout()
        
        self.title_label = QLabel(title)
        self.title_label.setStyleSheet(f"font-weight: bold; font-size: 14px; color: {StyleConfig.PRIMARY_COLOR}; background: transparent;")
        
        self.desc_label = QLabel(description)
        self.desc_label.setWordWrap(True)
        self.desc_label.setStyleSheet("font-size: 11px; color: #666666; background: transparent;")
        
        text_container.addWidget(self.title_label)
        text_container.addWidget(self.desc_label)
        text_container.addStretch()
        
        layout.addLayout(text_container)
        
        # Estilo do Card
        self.setStyleSheet(f"""
            CardButton {{
                background-color: white;
                border: 1px solid {StyleConfig.BORDER_COLOR};
                border-radius: 12px;
            }}
            CardButton:hover {{
                background-color: {StyleConfig.HOVER_COLOR};
                border: 1px solid {StyleConfig.PRIMARY_COLOR};
            }}
        """)
        
        # Efeito de sombra
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(15)
        shadow.setXOffset(0)
        shadow.setYOffset(4)
        shadow.setColor(QColor(0, 0, 0, 30))
        self.setGraphicsEffect(shadow)

    def set_custom_icon(self, name):
        """Define um ícone SVG personalizado."""
        svg_icons = {
            "operador": """<svg viewBox="0 0 24 24" fill="none" stroke="#175cc3" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>""",
            "medidor": """<svg viewBox="0 0 24 24" fill="none" stroke="#175cc3" stroke-width="2">
                <circle cx="12" cy="12" r="10"/>
                <path d="M12 6v6l4 2"/>
                <circle cx="12" cy="12" r="3"/>
                <path d="M12 2a10 10 0 0 1 0 20 10 10 0 0 1 0-20z"/>
            </svg>""",
            "gestao": """<svg viewBox="0 0 24 24" fill="none" stroke="#175cc3" stroke-width="2"><path d="M21 12V7a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h7"/><path d="M16 19h6M19 16v6"/></svg>""",
            "monitoramento": """<svg viewBox="0 0 24 24" fill="none" stroke="#175cc3" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>""",
            "fechar": """<svg viewBox="0 0 24 24" fill="none" stroke="#5474b8" stroke-width="2"><path d="M18 6L6 18M6 6l12 12"/></svg>"""
        }
        
        if name in svg_icons:
            renderer = QSvgRenderer(svg_icons[name].encode('utf-8'))
            pixmap = QPixmap(48, 48)
            pixmap.fill(Qt.transparent)
            painter = QPainter(pixmap)
            renderer.render(painter)
            painter.end()
            self.icon_label.setPixmap(pixmap)

def aplicar_tema_arredondado(widget):
    """Aplica o estilo de container branco e arredondado a um widget."""
    widget.setObjectName("ContainerBranco")
    widget.setStyleSheet(StyleConfig.MAIN_STYLE)