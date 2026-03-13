# -*- coding: utf-8 -*-
"""
Módulo: dialogo_unidade_potencia.py
=====================================
Diálogo modal de confirmação de unidade para o campo de potência do motor.

Pré-seleciona inteligentemente a unidade com base na magnitude do valor:
  - valor < 500 ou valor com decimal  →  cv
  - demais                            →  kW

A conversão efetiva (cv × 0,7355) é responsabilidade do módulo chamador.

Retorna "kw" ou "cv" via get_unidade().

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


class DialogoUnidadePotencia(QDialog):
    """Diálogo modal de confirmação de unidade para o campo de potência do medidor.

    Exibido automaticamente pelo ``WidgetMedidores`` quando o usuário insere
    um valor no campo de potência, garantindo que o banco receba sempre
    quilowatts (kW), unidade padrão de ``tb_intervencao``. O diálogo
    informa o valor digitado e solicita confirmação se ele está em **kW**
    ou em **cv** (cavalo-vapor, convertido multiplicando por 0,7355).

    A seleção padrão é heurística: se o valor for menor que 500 ou contiver
    separador decimal (ponto ou vírgula), pré-marca cv — motores agrícolas
    e industriais de pequeno porte costumam ser especificados em cv;
    valores altos e inteiros tendem a já estar em kW.

    O resultado é acessado via ``get_unidade()``, retornando ``"kw"`` ou
    ``"cv"``, ou ``None`` se o diálogo for cancelado.

    Attributes:
        valor (float): Valor numérico digitado pelo usuário no campo de
            potência.
        unidade_selecionada (str | None): ``"kw"`` ou ``"cv"`` após
            confirmação; ``None`` se cancelado.
        radio_kw (QRadioButton): Opção kW (sem conversão).
        radio_cv (QRadioButton): Opção cv (converte multiplicando por 0,7355).
        grupo_unidades (QButtonGroup): Agrupa os dois rádios para garantir
            exclusividade de seleção.
    """
    
    def __init__(self, valor, parent=None):
        super().__init__(parent)
        self.valor = valor
        self.unidade_selecionada = None
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle("Unidade da Potência")
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
        
        self.radio_kw = QRadioButton("kW (quilowatts)")
        self.radio_kw.setStyleSheet("font-size: 11px; padding: 5px;")
        self.grupo_unidades.addButton(self.radio_kw, 1)
        layout.addWidget(self.radio_kw)
        
        self.radio_cv = QRadioButton("cv (cavalos) - será convertido para kW")
        self.radio_cv.setStyleSheet("font-size: 11px; padding: 5px;")
        self.grupo_unidades.addButton(self.radio_cv, 2)
        layout.addWidget(self.radio_cv)
        
        if self.valor < 500 or '.' in str(self.valor) or ',' in str(self.valor):
            self.radio_cv.setChecked(True)
        else:
            self.radio_kw.setChecked(True)
        
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
        if self.radio_kw.isChecked():
            self.unidade_selecionada = "kw"
        elif self.radio_cv.isChecked():
            self.unidade_selecionada = "cv"
        self.accept()
        
    def get_unidade(self):
        return self.unidade_selecionada

