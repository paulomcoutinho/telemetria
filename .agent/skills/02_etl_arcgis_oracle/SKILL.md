---
name: etl-arcgis-oracle
description: |
  ETL do plugin: ArcGIS MapServer → PostGIS (Etapa 1) e Oracle DW
  CNARH40 → PostGIS (Etapa 2). Usar quando: editar
  widget_atualizacao_base.py, ETLWorker, _fetch_mapserver_paginado,
  _populate_baf, _populate_uam, FIELD_MAPPING, conexão Oracle,
  charset WE8MSWIN1252, UTL_RAW, cx_Oracle, oracledb modo THIN,
  TRUNCATE vs DELETE, permissões DDL, tb_mapserver_obrigatoriedade,
  tb_mv_sfi_cnarh40, SNIRH MapServer, Oracle DW CNARH40.
---

# ETL — widget_atualizacao_base.py

## Constantes de módulo

```python
PG_BASE = {
    'host':   "rds-webgis-dev.cjleec14qixz.sa-east-1.rds.amazonaws.com",
    'port':   5432,  'dbname': "telemetria",
}
ORACLE_CONFIG = {
    'user': 'DW_RO', 'host': 'exacc-prd-scan.ana.gov.br',
    'port': 1521,    'service_name': 'oradw.ana.gov.br',
}
MAPSERVER_URL = "https://portal1.snirh.gov.br/server/rest/services/SFI/Obrigatoriedade_Automonitoramento_DW_v5/MapServer/0"
TABLE_NAME_ETAPA1 = "tb_mapserver_obrigatoriedade"
TABLE_NAME_ETAPA2 = "tb_mv_sfi_cnarh40"
SCHEMA            = "public"
BATCH_INSERT_SIZE = 500
EXECUTA_ETAPA1    = True   # flag — desabilitar em testes
EXECUTA_ETAPA2    = True
```

## ETLWorker

Herda `QObject` — **não** `QThread`. Movido via `moveToThread()`.

```python
log_emitido  = pyqtSignal(str)        # log em tempo real
concluido    = pyqtSignal(bool, bool) # (ok_etapa1, ok_etapa2)
erro_fatal   = pyqtSignal(str)        # QMessageBox.critical na main thread
```

Credenciais injetadas como atributos **antes** do `moveToThread`:
```python
self._worker.pg_usuario = self.usuario_logado
self._worker.pg_senha   = self.senha
```

Cada etapa abre **conexão própria** via `_get_pg()` — nunca reutiliza `self.conn`.
Todos os métodos retornam `(bool, int)` — nunca `bool` sozinho.

## Etapa 1 — ArcGIS MapServer → tb_mapserver_obrigatoriedade

**Sequência**:
1. `_arcgis_metadata()` — verifica disponibilidade + nome da camada
2. `_create_table_etapa1()` — TRUNCATE/DELETE se existir; CREATE + índices se não existir
3. `_set_permissions_etapa1()` — GRANT SELECT para `telemetria_ro`, `telemetria_rw`, `usr_telemetria`, `iusr_coged_ro`, `postgres`
4. `_fetch_mapserver_paginado()` — paginação 2000 features/req; retry exponencial 2s→4s→8s, máx 3 tentativas
5. INSERT em lotes de 500, commit parcial por lote
6. Validação: `total_inserido < 10.000` → aborta ETL
7. `_populate_baf()` — `UPDATE ... SET bafcd, bafnm` via `ST_Intersects` com `ft_sishidrico_buffer` (EPSG:4674)
8. `_populate_uam()` — `UPDATE ... SET cdautomonit, nmautomonit, cdugrh, nmugrh` via `ST_Intersects` com `ft_uam_buffer`

**FIELD_MAPPING** (ArcGIS → PostGIS):

| Campo ArcGIS | Coluna PG | Tipo |
|---|---|---|
| `CÓDIGO_INTERFERENCIA` | `codigo_interferencia` | integer PK |
| `NÚMERO_CNARH` | `numero_cadastro` | text |
| `EMPREENDIMENTO` | `nome_empreendimento` | text |
| `PRAZO_MÁXIMO_INÍCIO_AUTOMONITORAMENTO` | `dr_max_telem` | date |
| `FIM_OBRIGATORIEDADE_AUTOMONITORAMENTO` | `dr_fim_telem` | date |
| `DATA_VENCIMENTO_OUTORGA` | `dr_vencimento_outorga` | date |
| `VAZAO_MEDIA_M3_H` | `vazao_media_m3_h` | double precision |
| `geometry.x` | `longitude` | double precision |
| `geometry.y` | `latitude` | double precision |
| join espacial | `bafcd`, `bafnm` | text |
| join espacial | `cdautomonit`, `nmautomonit`, `cdugrh`, `nmugrh` | text/integer |

Campos `ftype == 'date'`: timestamp ms epoch UTC → `datetime.date` via `_converter_timestamp_ms()`.
Campos integer comuns (OID etc.) **não** passam por essa função.

## Etapa 2 — Oracle DW CNARH40 → tb_mv_sfi_cnarh40

**Driver Oracle**: tenta `cx_Oracle` primeiro; fallback `oracledb` modo **THIN**.
Nunca chamar `init_oracle_client()` — causa erro de encoding no banco WE8MSWIN1252.
Instalação automática via `pip install --user` + `sys.path` dinâmico se ausente.

**Charset**: banco Oracle usa `WE8MSWIN1252`.
Query usa `UTL_RAW.CAST_TO_RAW(RTRIM(...))` → Python recebe `bytes`.
`_decode_oracle_bytes()`: tenta UTF-8, fallback `cp1252`.
Bind variables com Unicode literal (`'\u00e7\u00e3\u00f5'`) para independência do encoding do `.py`.

**Filtro Oracle**: `TIN_DS IN ('Captação', 'Captação em Barramento de Regularização')` na view `CNARH40.MV_SFI_CNARH40`.

**Sequência**:
1. TRUNCATE/DELETE `tb_mv_sfi_cnarh40` ou CREATE se não existir
2. Criar `temp_cnarh` (tabela temporária de sessão PG)
3. Extrair Oracle → lotes de 500 → INSERT em `temp_cnarh`
4. INSERT `temp_cnarh` → `tb_mv_sfi_cnarh40` via JOIN
5. `ANALYZE tb_mv_sfi_cnarh40`

## Tratamento de permissões DDL

```python
try:
    cur.execute(f"TRUNCATE {schema}.{tbl} RESTART IDENTITY")
except psycopg2.errors.InsufficientPrivilege:
    cur.execute(f"DELETE FROM {schema}.{tbl}")
```

`telemetria_rw` pode não ser owner da tabela — sempre usar fallback `DELETE FROM`.

## Ciclo de vida da thread (WidgetAtualizacaoBase)

```python
# Iniciar
self._thread = QThread(self)
self._worker = ETLWorker()
self._worker.pg_usuario = self.usuario_logado
self._worker.pg_senha   = self.senha
self._worker.moveToThread(self._thread)
self._thread.started.connect(self._worker.run)
self._worker.log_emitido.connect(self._append_log)
self._worker.concluido.connect(self._on_etl_concluido)
self._worker.erro_fatal.connect(self._on_etl_erro)
self._thread.start()

# Finalizar
self._thread.quit()
self._thread.wait()
```

Flag `_em_execucao` impede duplo disparo.
Gatilho: sinal `currentChanged` do `QTabWidget` pai — **nunca remover**.
Log widget: dark theme `#1e1e1e` / `#d4d4d4`, fonte Consolas 10px.
