# -*- coding: utf-8 -*-
"""
Módulo: dialogo_unidade_vazao.py
==================================
Diálogo modal de confirmação de unidade para o campo de vazão nominal.

Pré-seleciona inteligentemente a unidade com base na magnitude do valor:
  - valor > 10  →  m³/h  (padrão para especificações agrícolas/industriais)
  - demais      →  m³/s

A conversão efetiva (m³/h ÷ 3600) é responsabilidade do módulo chamador.

Retorna "m3s" ou "m3h" via get_unidade().

Autor : SFI/ANA
Versão: 2.0 – Março/2026
"""

# ---------------------------------------------------------------------------
# Imports Qt – widgets, layout e utilitários de UI
# ---------------------------------------------------------------------------
from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QRadioButton, QButtonGroup,
    QDialogButtonBox, QFrame,
)
from qgis.PyQt.QtCore import Qt

# ---------------------------------------------------------------------------
# Sistema de design centralizado
# ---------------------------------------------------------------------------
try:
    from . import ui_tema
except ImportError:
    import ui_tema


class DialogoUnidadeVazao(QDialog):
    """Diálogo modal de confirmação de unidade para o campo de vazão nominal.

    Exibido automaticamente pelo ``WidgetMedidores`` quando o usuário insere
    um valor no campo de vazão nominal, evitando que dados sejam gravados
    com unidade incorreta. O diálogo apresenta o valor digitado e solicita
    que o usuário declare se ele está em **m³/s** (unidade padrão do banco)
    ou em **m³/h** (que será dividido por 3600 antes de ser persistido).

    A seleção padrão é inteligente: se o valor for maior que 10, o diálogo
    pré-marca m³/h (valores de vazão em m³/s raramente ultrapassam 10 em
    captações individuais); caso contrário, pré-marca m³/s.

    O resultado é acessado via ``get_unidade()``, que retorna ``"m3s"`` ou
    ``"m3h"``, ou ``None`` se o diálogo foi cancelado.

    Attributes:
        valor (float): Valor numérico digitado pelo usuário no campo de vazão.
        unidade_selecionada (str | None): ``"m3s"`` ou ``"m3h"`` após
            confirmação; ``None`` se cancelado.
        radio_m3s (QRadioButton): Opção m³/s (sem conversão).
        radio_m3h (QRadioButton): Opção m³/h (converte dividindo por 3600).
        grupo_unidades (QButtonGroup): Agrupa os dois rádios para garantir
            exclusividade de seleção.
    """
    
    def __init__(self, valor, parent=None):
        super().__init__(parent)
        self.valor = valor
        self.unidade_selecionada = None
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle("Unidade da Vazão")
        self.setFixedWidth(350)
        
        layout = QVBoxLayout()
        layout.setSpacing(15)
        
        label_valor = QLabel(f"Você digitou o valor: <b>{self.valor}</b>")
        label_valor.setStyleSheet("font-size: 13px; padding: 10px;")
        layout.addWidget(label_valor)
        
        label_pergunta = QLabel("Em qual unidade está esse valor?")
        label_pergunta.setStyleSheet("font-size: 12px; color: #555;")
        layout.addWidget(label_pergunta)
        
        self.grupo_unidades = QButtonGroup(self)
        
        self.radio_m3s = QRadioButton("m³/s (metros cúbicos por segundo)")
        self.radio_m3s.setStyleSheet("font-size: 11px; padding: 5px;")
        self.grupo_unidades.addButton(self.radio_m3s, 1)
        layout.addWidget(self.radio_m3s)
        
        self.radio_m3h = QRadioButton("m³/h (metros cúbicos por hora) - será convertido")
        self.radio_m3h.setStyleSheet("font-size: 11px; padding: 5px;")
        self.grupo_unidades.addButton(self.radio_m3h, 2)
        layout.addWidget(self.radio_m3h)
        
        if self.valor > 10:
            self.radio_m3h.setChecked(True)
        else:
            self.radio_m3s.setChecked(True)
        
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)
        
        btn_confirmar = QPushButton("Confirmar")
        btn_confirmar.setStyleSheet("""
            QPushButton { background-color: #5474b8; color: white; font-weight: bold; padding: 8px 20px; border-radius: 4px; border: none; }
            QPushButton:hover { background-color: #2050b8; }
        """)
        btn_confirmar.clicked.connect(self.confirmar)
        
        btn_cancelar = QPushButton("Cancelar")
        btn_cancelar.setStyleSheet("""
            QPushButton { background-color: #6c757d; color: white; padding: 8px 20px; border-radius: 4px; border: none; }
            QPushButton:hover { background-color: #5a6268; }
        """)
        btn_cancelar.clicked.connect(self.reject)
        
        btn_layout.addStretch()
        btn_layout.addWidget(btn_confirmar)
        btn_layout.addWidget(btn_cancelar)
        
        layout.addLayout(btn_layout)
        self.setLayout(layout)
        
    def confirmar(self):
        if self.radio_m3s.isChecked():
            self.unidade_selecionada = "m3s"
        elif self.radio_m3h.isChecked():
            self.unidade_selecionada = "m3h"
        self.accept()
        
    def get_unidade(self):
        return self.unidade_selecionada

