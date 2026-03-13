# Telemetria Plugin SFI/ANA — Convenções Globais do Projeto

Projeto: plugin QGIS "DURH Diária por Telemetria" v2.0 — SFI/Agência Nacional de Águas.
Base legal: Resolução ANA n. 188/2024.

## Stack obrigatória

- Python 3.x no ambiente OSGeo4W (QGIS)
- PyQt5 via `qgis.PyQt` — nunca `PyQt5.*` diretamente
- PostgreSQL/PostGIS via `psycopg2` — nunca `sqlalchemy`, `asyncpg` ou ORM
- Exportação XLSX via `openpyxl` — nunca `pandas`, `xlsxwriter`
- Sem `pandas`, `numpy`, `geopandas` — indisponíveis no OSGeo4W

## Banco de dados

- Host: `rds-webgis-dev.cjleec14qixz.sa-east-1.rds.amazonaws.com`
- Porta: `5432` — Banco: `telemetria` — AWS RDS sa-east-1
- Queries sempre com parâmetros psycopg2: `%(param)s` — nunca f-string com input do usuário
- `TRUNCATE` sempre com fallback `DELETE FROM` — role `telemetria_rw` pode não ser owner
- `autocommit = True` nos widgets de edição (`WidgetMedidores`, `WidgetOperadores`)

## Convenções de código

- Um arquivo `.py` por classe, nomeado em `snake_case` idêntico ao nome da classe
- Imports relativos dentro do pacote: `from .modulo import Classe`
- `ui_tema` sempre com fallback: `try: from . import ui_tema / except ImportError: import ui_tema`
- Helper functions definidas **fora** de loops — closures em loops produzem valores errados
- Filenames com acentos: `unicodedata.normalize('NFKD', nome).encode('ascii','ignore').decode()`
- `deleteLater()` somente com guard `try/except RuntimeError`
- Retornos de workers ETL: sempre tuple `(bool, int)` — nunca `bool` sozinho

## Respostas do agente

- Retornar sempre mudanças **cirúrgicas** — nunca o arquivo inteiro
- Verificar se modificação afeta sinais Qt existentes antes de sugerir
- Ao adicionar widget: verificar se `initUI()` já conecta sinais antes de duplicar
- Ao sugerir nova thread: usar padrão `QObject + moveToThread` — nunca herdar `QThread`
- Idioma: comentários e strings de UI em **português brasileiro**
