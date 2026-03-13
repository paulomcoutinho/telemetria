---
name: gui-pyqt5-padroes
description: |
  Padrões de GUI PyQt5/QGIS do plugin: design system, sinais/slots,
  imports Qt corretos, widgets, diálogos auxiliares, exportação XLSX.
  Usar quando: editar ui_tema.py, StyleConfig, CardButton, qualquer
  QWidget/QDialog do plugin, adicionar campo em formulário, criar
  novo diálogo, exportar XLSX com openpyxl, usar QComboBox, conectar
  sinais, QSvgRenderer, ícones SVG inline, tema de cores ANA.
---

# GUI — PyQt5 / QGIS

## Imports Qt (sempre via qgis.PyQt)

```python
from qgis.PyQt.QtWidgets import (
    QWidget, QDialog, QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QCheckBox, QToolButton,
    QFrame, QMessageBox, QDesktopWidget, QApplication, QSizePolicy,
    QAbstractItemView, QHeaderView, QTreeWidget, QTreeWidgetItem,
    QListWidget, QListWidgetItem, QScrollArea, QDateEdit, QDateTimeEdit,
    QTabWidget, QTableWidget, QTableWidgetItem, QProgressBar,
    QDialogButtonBox, QPlainTextEdit, QGraphicsDropShadowEffect,
)
from qgis.PyQt.QtCore import Qt, QSettings, QTimer, QSize, QDate, QThread, pyqtSignal, QObject
from qgis.PyQt.QtGui import QIcon, QFont, QColor, QPixmap, QPainter, QRegExpValidator
from qgis.PyQt.QtSvg import QSvgRenderer
from qgis.core import Qgis, QgsApplication, QgsProject, QgsMessageLog, NULL as QNULL
from qgis.utils import iface
from qgis.gui import QgsMapCanvas
```

Nunca usar `PyQt5.*` diretamente — quebra compatibilidade entre versões do QGIS.
Nunca usar `osgeo.gdal` / `osgeo.ogr` — PostGIS acessado via `psycopg2`.

## Design system — ui_tema.py

```python
StyleConfig.PRIMARY_COLOR    = "#175cc3"   # azul ANA — títulos, bordas foco
StyleConfig.SECONDARY_COLOR  = "#5474b8"   # hover, bordas outline
StyleConfig.BACKGROUND_WHITE = "#FFFFFF"
StyleConfig.BORDER_COLOR     = "#E0E0E0"
StyleConfig.HOVER_COLOR      = "#F5F5F5"
StyleConfig.TEXT_DARK        = "#333333"
```

`MAIN_STYLE` cobre: `QWidget`, `QLabel`, `QLineEdit`, `QComboBox`, `QDateEdit`,
`QTabWidget`, `QPushButton`, `QFrame#ContainerBranco`.

## CardButton

`QPushButton` 240×120px.
Layout interno: `QHBoxLayout` (ícone 48×48 + `QVBoxLayout` com texto).
Ícones via `QSvgRenderer` + `QPainter` sobre `QPixmap` transparente.
Nunca usar arquivos externos de ícone — SVG embutido como string.

## Sinais e slots mapeados

| Sinal | Emitido por | Conectado a |
|---|---|---|
| `log_emitido(str)` | `ETLWorker` | `WidgetAtualizacaoBase._append_log` + `QgsMessageLog` |
| `concluido(bool, bool)` | `ETLWorker` | `WidgetAtualizacaoBase._on_etl_concluido` |
| `erro_fatal(str)` | `ETLWorker` | `WidgetAtualizacaoBase._on_etl_erro` |
| `progresso(int, int, str)` | `CalcMesThread` | `JanelaMonitoramentoDetalhes` |
| `dia_concluido(str, float, bool)` | `CalcMesThread` | slot de atualização de grid |
| `resultado_signal(list, str, int)` | `VerificacaoOutorgadoThread` | `JanelaMonitoramento` |
| `currentChanged(int)` | `QTabWidget` pai | `WidgetAtualizacaoBase._on_tab_changed` |
| `editingFinished` | `input_vazao`, `input_potencia` | `WidgetMedidores.processar_vazao/potencia` |

## Regras críticas de GUI

1. `currentChanged` do QTabWidget em `JanelaGestaoDados` **nunca remover** — gatilho do ETL.
2. Não adicionar `setWindowFlags` em janelas filhas sem necessidade — o código atual é intencional.
3. `QComboBox`: usar `itemText(currentIndex())` para valor confirmado; `currentText()` retorna só o fragmento digitado — crítico em `WidgetMedidores` e `WidgetOperadores`.
4. `deleteLater()` com referências Python ativas → `RuntimeError`. Usar `try/except RuntimeError`.
5. Helper functions em loops → closure com valor errado. Definir **fora** do loop.
6. Filenames com acentos: `unicodedata.normalize('NFKD', nome).encode('ascii','ignore').decode()`.
7. `autocommit = True` em `WidgetMedidores.__init__` e `WidgetOperadores.__init__` — não usar transações explícitas nesses widgets.
8. `QgsMapCanvas` só em `JanelaMonitoramento` — nunca em widgets de gestão de dados.

## Diálogos auxiliares

- `DialogoUnidadeVazao` — confirma m³/s ou m³/h; converte para m³/s.
- `DialogoUnidadePotencia` — confirma kW ou cv; converte para kW.
- `DialogReativacao` — reativação em lote (remove sufixo `#` do rótulo).
- Todos `QDialog` modais, instanciados sob `WidgetMedidores`.

## Exportação XLSX — padrão openpyxl

```python
# Guarda de import
try:
    import openpyxl
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    OPENPYXL_DISPONIVEL = True
except ImportError:
    OPENPYXL_DISPONIVEL = False

# Template padrão
wb = openpyxl.Workbook()
ws = wb.active
ws.merge_cells('A1:Z1')
ws['A1'].value = "TÍTULO"
ws['A1'].font  = Font(bold=True, size=13, color="FFFFFF")
ws['A1'].fill  = PatternFill("solid", fgColor="175cc3")   # azul ANA
# Linha 4: cabeçalhos branco sobre azul
ws.freeze_panes = 'A5'
# Dados: fill alternado #eaf2ff / branco; bordas Side(style='thin')
# Filename: sanitizar com unicodedata antes de salvar
path = os.path.join(os.path.expanduser("~"), "Downloads", filename)
wb.save(path)
```
