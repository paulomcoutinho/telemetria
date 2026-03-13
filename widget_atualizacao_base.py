# -*- coding: utf-8 -*-
"""
Módulo: widget_atualizacao_base.py
===================================
Implementa a aba "Atualizar base de dados" das classes JanelaGestaoDados e
WidgetAtualizacaoBase do plugin QGIS 'DURH Diária por Telemetria'.

Ao clicar na aba, exibe um diálogo de alerta e, confirmado pelo usuário,
executa o processo ETL completo em thread separada (sem travar o QGIS):

  ETAPA 1 : ArcGIS MapServer  -> PostGIS  (tb_mapserver_obrigatoriedade)
  ETAPA 2 : Oracle DW (CNARH40)   -> PostGIS  (tb_mv_sfi_cnarh40)

Autor: SFI/ANA
Versão: 1.0 – Março/2026
"""

from __future__ import annotations

import json
import ssl
import sys
import traceback
import unicodedata
import urllib.error
import urllib.request
from datetime import datetime
import time

import psycopg2
from qgis.PyQt.QtCore import QObject, QThread, pyqtSignal, Qt
from qgis.PyQt.QtWidgets import (
    QDialog, QDialogButtonBox, QHBoxLayout, QLabel,
    QMessageBox, QPlainTextEdit, QPushButton, QVBoxLayout, QWidget,
)
from qgis.core import (
    Qgis, QgsCoordinateReferenceSystem, QgsCoordinateTransform,
    QgsMessageLog, QgsProject, QgsVectorLayer,
)


def _importar_oracle_driver():
    """
    Tenta importar um driver Oracle em ordem de preferência:
      1. cx_Oracle  – driver clássico, amplamente instalado em ambientes ANA.
      2. oracledb   – driver moderno da Oracle em modo thin (sem cliente nativo).

    Retorna o módulo importado ou levanta ImportError com mensagem clara.
    """
    try:
        import cx_Oracle as _ora          # noqa: N813
        _ora._driver_name = "cx_Oracle"  # marca para uso posterior
        return _ora
    except ImportError:
        pass

    try:
        import oracledb as _ora
        # NUNCA chamar init_oracle_client(): força modo THICK, que nesta versão
        # do driver decodifica VARCHAR internamente como UTF-8 antes de qualquer
        # handler Python, causando erro em bancos com charset WE8MSWIN1252.
        # O modo THIN (padrão do oracledb >= 1) é o correto para este banco.
        _ora._driver_name = "oracledb_thin"
        return _ora
    except ImportError:
        pass

    raise ImportError(
        "Nenhum driver Oracle encontrado no ambiente Python do QGIS.\n"
        "Instale cx_Oracle  OU  oracledb via:\n"
        "  OSGeo4W Shell: pip install oracledb\n"
        "Reinicie o QGIS após a instalação."
    )


PG_BASE = {
    'host':   "rds-webgis-dev.cjleec14qixz.sa-east-1.rds.amazonaws.com",
    'port':   5432,
    'dbname': "telemetria",
}

ORACLE_CONFIG = {
    'user':         'DW_RO',
    'password':     'hVz=aU46u%y[',
    'host':         'exacc-prd-scan.ana.gov.br',
    'port':         1521,
    'service_name': 'oradw.ana.gov.br',
}

MAPSERVER_URL = (
    "https://portal1.snirh.gov.br/server/rest/services/SFI/"
    "Obrigatoriedade_Automonitoramento_DW_v5/MapServer/0"
)
TABLE_NAME_ETAPA1 = "tb_mapserver_obrigatoriedade"
TABLE_NAME_ETAPA2 = "tb_mv_sfi_cnarh40"
SCHEMA            = "public"
BATCH_INSERT_SIZE = 500
EXECUTA_ETAPA1    = True
EXECUTA_ETAPA2    = True


class ETLWorker(QObject):
    """Worker ETL que migra dados de fontes externas para o PostGIS do sistema DURH Diária.

    Executa integralmente em uma ``QThread`` dedicada (gerenciada pelo
    ``WidgetAtualizacaoBase``), sem bloquear o loop de eventos do QGIS.
    Todo o progresso é transmitido em tempo real por meio de sinais Qt,
    alimentando simultaneamente o painel de Log de Mensagens do QGIS e o
    widget de log embutido na interface.

    O processo é dividido em duas etapas independentes e sequenciais,
    controladas pelas flags de módulo ``EXECUTA_ETAPA1`` e ``EXECUTA_ETAPA2``:

    **Etapa 1 — ArcGIS MapServer → PostGIS** (``_execute_etapa1``):
        1. Abre uma conexão PostgreSQL dedicada com as credenciais do usuário
           logado (``pg_usuario`` / ``pg_senha``).
        2. Consulta os metadados do serviço REST do MapServer SFI/ANA
           (``MAPSERVER_URL``) para verificar disponibilidade e obter o
           nome da camada.
        3. Cria ou trunca a tabela ``tb_mapserver_obrigatoriedade`` no schema
           ``public``, recriando índices espaciais e de atributos. Em caso de
           falta de permissão para ``TRUNCATE``, recorre a ``DELETE``.
        4. Configura as permissões de acesso da tabela para os roles
           ``telemetria_ro``, ``telemetria_rw``, ``usr_telemetria``,
           ``iusr_coged_ro`` e ``postgres``.
        5. Baixa todas as feições do serviço de forma paginada
           (``_fetch_mapserver_paginado``), com paginação de 2000 registros
           por requisição, pausa de 2 s entre páginas e retry com backoff
           exponencial (até 3 tentativas por página) para resiliência a
           instabilidades de rede.
        6. Aplica o ``FIELD_MAPPING`` para converter cada atributo ArcGIS
           para a coluna PostGIS correspondente, com tratamento especial de
           campos de data (timestamp em milissegundos epoch UTC →
           ``datetime.date`` via ``_converter_timestamp_ms``).
        7. Insere as feições em lotes de 500 registros (``BATCH_INSERT_SIZE``)
           com commit parcial a cada lote.
        8. Verifica se o total importado é menor que 10.000 registros
           (``MINIMO_ESPERADO``) — se sim, aborta para evitar carga de dados
           parciais.
        9. Executa o join espacial ``_populate_baf`` para preencher os campos
           ``bafcd`` e ``bafnm`` via ``ST_Intersects`` com a camada
           ``ft_sishidrico_buffer`` (sistema hídrico de referência).

    **Etapa 2 — Oracle DW (CNARH40) → PostGIS** (``_execute_etapa2``):
        1. Verifica e, se necessário, instala automaticamente o driver
           ``oracledb`` via ``pip install --user`` sem exigir privilégios de
           administrador (``_garantir_oracledb``). Caso a instalação exija
           reinício do QGIS, emite ``erro_fatal`` com instrução clara.
        2. Abre conexões paralelas ao Oracle DW (``ORACLE_CONFIG``) e ao
           PostgreSQL.
        3. Cria ou trunca a tabela ``tb_mv_sfi_cnarh40``, com 17 colunas de
           volume mensal (``vol_jan`` … ``vol_dez``), coordenadas e chave
           primária em ``codigo_interferencia``.
        4. Cria a tabela temporária de sessão ``temp_cnarh`` no PostgreSQL
           para receber os dados brutos do Oracle antes do join.
        5. Extrai da view materializada ``CNARH40.MV_SFI_CNARH40`` apenas
           interferências do tipo **Captação** cujo vencimento de outorga
           seja posterior à data mínima configurada pelo usuário no diálogo
           de confirmação (padrão: 1º de janeiro do ano anterior).
        6. Popula ``temp_cnarh`` em lotes de 500 linhas com os dados
           extraídos do Oracle.
        7. Executa o JOIN entre ``temp_cnarh`` e
           ``tb_mapserver_obrigatoriedade`` pelo campo ``codigo_interferencia``
           / ``INT_CD``, inserindo o resultado diretamente em
           ``tb_mv_sfi_cnarh40``.
        8. Executa ``ANALYZE`` na tabela final para atualizar as estatísticas
           do planner do PostgreSQL.
        9. Fecha ambas as conexões no bloco ``finally``.

    Ao término das duas etapas, emite o sinal ``concluido`` com os flags
    booleanos de sucesso de cada uma. Em caso de erro fatal (driver
    indisponível, serviço fora do ar, falha de autenticação), emite
    ``erro_fatal`` com mensagem descritiva para exibição em ``QMessageBox``
    na thread principal.

    Signals:
        log_emitido (str): Emitido a cada linha de log gerada; conectado
            simultaneamente ao painel ``QgsMessageLog`` do QGIS e ao
            ``QPlainTextEdit`` do ``WidgetAtualizacaoBase``.
        concluido (bool, bool): Emitido ao finalizar ``run()``; os dois
            booleanos indicam, respectivamente, o sucesso da Etapa 1 e
            da Etapa 2.
        erro_fatal (str): Emitido quando uma condição impede a continuação
            do processo (driver Oracle ausente após tentativa de instalação,
            MapServer inacessível, total de feições abaixo do mínimo esperado).
            A mensagem é exibida como ``QMessageBox.critical`` pelo slot
            ``_on_etl_erro`` do ``WidgetAtualizacaoBase``.

    Class Attributes:
        pg_usuario (str | None): Nome do usuário PostgreSQL; injetado pelo
            ``WidgetAtualizacaoBase`` antes de mover o worker para a thread.
        pg_senha (str | None): Senha PostgreSQL correspondente.
        data_vencimento_minima (datetime.date | None): Data de corte para
            o filtro ``OUT_DT_OUTORGAFINAL`` na query Oracle; definida pelo
            usuário no diálogo de confirmação. Se ``None``, usa 1º de janeiro
            do ano anterior como fallback.
    """
    
    log_emitido    = pyqtSignal(str)
    concluido      = pyqtSignal(bool, bool)
    erro_fatal     = pyqtSignal(str)

    pg_usuario             = None
    pg_senha               = None
    data_vencimento_minima = None

    def _log(self, msg: str) -> None:
        """Emite log para o painel de Log do QGIS e para o widget."""
        QgsMessageLog.logMessage(msg, "Atualizar Base", Qgis.Info)
        self.log_emitido.emit(msg)

    def _ssl_ctx(self):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        return ctx

    def _get_pg(self):
        """Conexão única usando as credenciais do usuário logado."""
        return psycopg2.connect(
            **PG_BASE,
            user=self.pg_usuario,
            password=self.pg_senha,
        )

    def _garantir_oracledb(self) -> bool:
        """
        Verifica se oracledb está instalado. Se não estiver, instala
        automaticamente via pip com flag --user (não requer admin).
        """
        try:
            import oracledb  # noqa
            self._log("   ✓ Driver oracledb já instalado")
            return True
        except ImportError:
            pass

        self._log("   oracledb não encontrado. Instalando via pip --user...")

        import subprocess
        import os
        import sys
        import site
        
        # Encontrar o python.exe real (não o wrapper do QGIS)
        python_exec = sys.executable
        if python_exec.endswith('.bat') or 'qgis' in python_exec.lower():
            base_dir = os.path.dirname(python_exec)
            python_exec = os.path.join(base_dir, 'python.exe')
            if not os.path.exists(python_exec):
                python_exec = sys.executable

        # Configurar para Windows não abrir janela
        creation_flags = 0
        if os.name == 'nt':
            creation_flags = subprocess.CREATE_NO_WINDOW

        # 🔑 INSTALAR COM --user (não requer admin)
        try:
            resultado = subprocess.run(
                [python_exec, "-m", "pip", "install", "oracledb", 
                 "--quiet", "--no-warn-script-location", "--user"],  # ← FLAG --user
                capture_output=True,
                text=True,
                timeout=120,
                creationflags=creation_flags,
                env={**os.environ, 'PYTHONNOUSERSITE': '0'},  # ← Permitir user site
            )

            if resultado.returncode == 0:
                self._log("   ✓ oracledb instalado (modo --user)")
                
                # Adicionar user site-packages ao path
                user_site = site.getusersitepackages()
                if user_site not in sys.path:
                    sys.path.insert(0, user_site)
                    self._log(f"   ✓ Adicionado: {user_site}")
                
                import importlib
                importlib.invalidate_caches()
                
                try:
                    import oracledb  # noqa
                    self._log("   ✓ oracledb carregado")
                    return True
                except ImportError:
                    self._log("   ⚠ Requer reinício do QGIS")
                    self.erro_fatal.emit(
                        "oracledb instalado com sucesso!\n\n"
                        "REINICIE o QGIS para carregar o driver."
                    )
                    return False
            else:
                erro = resultado.stderr.strip() or resultado.stdout.strip()
                self._log(f"   ✗ Falha: {erro}")
                self._mostrar_instrucoes_manuais(erro)
                return False

        except Exception as e:
            self._log(f"   ✗ Erro: {e}")
            self._mostrar_instrucoes_manuais(str(e))
            return False

    def _mostrar_instrucoes_manuais(self, erro: str):
        """Exibe instruções claras de instalação manual."""
        self._log(" " * 60)
        self._log("📋 INSTALAÇÃO MANUAL")
        self._log(" " * 60)
        self._log("1. Feche o QGIS")
        self._log("2. Abra OSGeo4W Shell COMO ADMINISTRADOR")
        self._log("3. Execute: python -m pip install oracledb")
        self._log("4. Reinicie o QGIS")
        self._log(" " * 60)
        
        self.erro_fatal.emit(
            "Não foi possível instalar automaticamente.\n\n"
            f"Erro: {erro}\n\n"
            "SOLUÇÃO:\n"
            "1. Feche o QGIS\n"
            "2. OSGeo4W Shell (Admin) → python -m pip install oracledb\n"
            "3. Reinicie o QGIS"
        )
    
    def _get_oracle(self, ora):
        """Cria conexão Oracle usando o módulo `ora` já importado."""
        dsn = ora.makedsn(
            ORACLE_CONFIG['host'],
            ORACLE_CONFIG['port'],
            service_name=ORACLE_CONFIG['service_name'],
        )
        conn = ora.connect(
            user=ORACLE_CONFIG['user'],
            password=ORACLE_CONFIG['password'],
            dsn=dsn,
        )
        self._log("   ✓ Conexão Oracle estabelecida")
        return conn

    def _table_exists(self, cur, schema, table):
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s)",
            (schema, table),
        )
        return cur.fetchone()[0]

    @staticmethod
    def _converter_valor(val):
        if val is None or val == '' or val == 'NULL':
            return None

        from qgis.PyQt.QtCore import QDateTime, QDate, QTime
        from qgis.core import NULL as QNULL

        try:
            if val == QNULL:
                return None
        except Exception:
            pass

        if isinstance(val, QDateTime):
            return val.toPyDateTime() if val.isValid() else None
        if isinstance(val, QDate):
            return val.toPyDate() if val.isValid() else None
        if isinstance(val, QTime):
            return val.toPyTime() if val.isValid() else None

        return val

    @staticmethod
    def _converter_timestamp_ms(val):
        """
        Converte um valor inteiro retornado pelo MapServer (timestamp em
        milissegundos desde epoch UTC) para um objeto ``datetime.date``.

        Usado **exclusivamente** para campos cujo ftype == 'date' no
        FIELD_MAPPING.  Campos inteiros de outro tipo (ex.: OID) NÃO devem
        passar por esta função.

        Retorna None se val for None/vazio ou se a conversão falhar.
        """
        if val is None or val == '' or val == 'NULL':
            return None
        from datetime import datetime, timezone
        try:
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc).date()
        except Exception:
            return None

    def _arcgis_metadata(self, url):
        req = urllib.request.Request(
            f"{url}?f=json", headers={'User-Agent': 'QGIS Python'}
        )
        with urllib.request.urlopen(req, context=self._ssl_ctx(), timeout=30) as r:
            data = json.loads(r.read().decode('utf-8'))
        if 'error' in data:
            raise Exception(f"Erro na API ArcGIS: {data['error'].get('message','?')}")
        return data

    # ------------------------------------------------------------------
    # ETAPA 1 : ArcGIS MapServer -> PostGIS
    # ------------------------------------------------------------------

    def _create_table_etapa1(self, cur, schema, tbl):
        if self._table_exists(cur, schema, tbl):
            self._log(f"   Tabelas existente '{schema}.{tbl}' encontrada... ")
            try:
                cur.execute(f"TRUNCATE {schema}.{tbl} RESTART IDENTITY")
                self._log(f"   ✓ Tabela truncada com sucesso ")
            except psycopg2.errors.InsufficientPrivilege:
                self._log(f"   ⚠ Sem permissão para TRUNCATE. Tentando DELETE... ")
                try:
                    cur.execute(f"DELETE FROM {schema}.{tbl} ")
                    self._log(f"   ✓ Tabela limpa com DELETE ")
                except Exception as e:
                    self._log(f"   ✗ Falha ao limpar tabela: {e} ")
                    raise
        else:
            self._log(f"   Criando tabela '{schema}.{tbl}'...")
            cur.execute(f"""
                CREATE TABLE {schema}.{tbl} (
                    codigo_interferencia      integer,
                    numero_regla              text,
                    numero_cadastro           text,
                    nome_empreendimento       text,
                    nome_usuario              text,
                    dr_max_telem              date,
                    dr_fim_telem              date,
                    motivo_obrigatoriedade    text,
                    classe_monitoramento      text,
                    tipo_interferencia        text,
                    dr_inicio_outorga         date,
                    dr_vencimento_outorga     date,
                    vazao_media_m3_h          double precision,
                    vazao_maxima_m3_h         double precision,
                    finalidade_outorga        text,
                    dominio                   text,
                    numero_resolucao          text,
                    orgao_origem              text,
                    longitude                 double precision,
                    latitude                  double precision,
                    bafcd                     text,
                    bafnm                     text,
                    cdautomonit               integer,
                    nmautomonit               text,
                    cdugrh                    integer,
                    nmugrh                    text,
                    CONSTRAINT {tbl}_pk PRIMARY KEY (codigo_interferencia)
                )
            """)
        for idx_sql in (
            f"CREATE INDEX IF NOT EXISTS idx_{tbl}_lon  ON {schema}.{tbl}(longitude)",
            f"CREATE INDEX IF NOT EXISTS idx_{tbl}_lat  ON {schema}.{tbl}(latitude)",
            f"CREATE INDEX IF NOT EXISTS idx_{tbl}_xy   ON {schema}.{tbl}(longitude, latitude)",
            f"CREATE INDEX IF NOT EXISTS idx_{tbl}_codi ON {schema}.{tbl}(codigo_interferencia)",
            f"CREATE INDEX IF NOT EXISTS idx_{tbl}_cad  ON {schema}.{tbl}(numero_cadastro)",
        ):
            cur.execute(idx_sql)
        self._log("   ✓ Tabela e índices prontos")

    def _set_permissions_etapa1(self, cur, schema, tbl):
        for role in ("telemetria_ro", "telemetria_rw", "usr_telemetria"):
            cur.execute(f"REVOKE ALL ON TABLE {schema}.{tbl} FROM {role}")
        cur.execute(f"GRANT ALL   ON TABLE {schema}.{tbl} TO iusr_coged_ro")
        cur.execute(f"GRANT ALL   ON TABLE {schema}.{tbl} TO postgres")
        cur.execute(f"GRANT SELECT ON TABLE {schema}.{tbl} TO telemetria_ro")
        cur.execute(f"GRANT ALL ON TABLE {schema}.{tbl} TO telemetria_rw")
        cur.execute(f"GRANT SELECT ON TABLE {schema}.{tbl} TO usr_telemetria")
        self._log("   ✓ Permissões configuradas")

    def _populate_baf(self, cur, schema, tbl):
        """Preenche os campos bafcd e bafnm via spatial join com ft_sishidrico_buffer"""    
        self._log("   Preenchendo bafcd/bafnm...")
        cur.execute(f"""
            UPDATE {schema}.{tbl} AS tgt
            SET bafcd = src.bafcd, bafnm = src.bafnm
            FROM public.ft_sishidrico_buffer AS src
            WHERE tgt.latitude  IS NOT NULL
              AND tgt.longitude IS NOT NULL
              AND ST_Intersects(
                    ST_Transform(
                        ST_SetSRID(ST_MakePoint(tgt.longitude, tgt.latitude), 4326),
                        4674
                    ),
                    src.geom
              )
        """)
        self._log(f"   ✓ {cur.rowcount} registros atualizados com dados de ft_sishidrico_buffer")

    def _populate_uam(self, cur, schema, tbl):
        """Preenche os campos cdautomonit e nmautomonit via spatial join com ft_uam_buffer"""
        self._log("   Preenchendo cdautomonit/nmautomonit...")
        cur.execute(f"""
            UPDATE {schema}.{tbl} AS tgt
            SET cdautomonit = src.cdautomonit, nmautomonit = src.nmautomonit, cdugrh = src.cdugrh, nmugrh = src.nmugrh
            FROM public.ft_uam_buffer AS src
            WHERE tgt.latitude  IS NOT NULL
              AND tgt.longitude IS NOT NULL
              AND ST_Intersects(
                    ST_Transform(
                        ST_SetSRID(ST_MakePoint(tgt.longitude, tgt.latitude), 4326),
                        4674
                    ),
                    src.geom
              )
        """)
        self._log(f"   ✓ {cur.rowcount} registros atualizados com dados de ft_uam_buffer")

    '''
    def _populate_automonit(self, cur, schema, tbl):
        """Preenche os campos cdautomonit e nmautomonit via spatial join com ft_unidade_automonitoramento"""
        log("   Preenchendo campos cdautomonit e nmautomonit via spatial join...")

        cur.execute(f"""
            UPDATE {schema}.{tbl} AS tgt
            SET cdautomonit = src.cdautomonit, nmautomonit = src.nmautomonit
            FROM public.ft_unidade_automonitoramento AS src
            WHERE tgt.latitude IS NOT NULL 
              AND tgt.longitude IS NOT NULL
              AND ST_Intersects(ST_SetSRID(ST_MakePoint(tgt.longitude, tgt.latitude), 4326), src.geom)
        """)

        self._log(f"   ✓ {cur.rowcount} com dados de ft_unidade_automonitoramento")
    '''

    def _fetch_mapserver_paginado(self, url: str, page_size: int = 2000, pausa_seg: float = 2.0) -> list:
        import time
        features  = []
        offset    = 0
        ctx       = self._ssl_ctx()
        MAX_RETRY = 3  # tentativas por página

        while True:
            query_url = (
                f"{url}/query?where=1%3D1"
                f"&outFields=*"
                f"&returnGeometry=true"
                f"&outSR=4326"
                f"&resultOffset={offset}"
                f"&resultRecordCount={page_size}"
                f"&f=json"
            )
            req = urllib.request.Request(query_url, headers={'User-Agent': 'QGIS Python'})

            # --- retry com backoff exponencial ---
            ultima_excecao = None
            for tentativa in range(1, MAX_RETRY + 1):
                try:
                    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
                        data = json.loads(r.read().decode('utf-8'))
                    ultima_excecao = None
                    break  # sucesso — sai do loop de retry
                except Exception as e:
                    ultima_excecao = e
                    espera = pausa_seg * (2 ** (tentativa - 1))  # 2s, 4s, 8s
                    self._log(
                        f"   ⚠ Tentativa {tentativa}/{MAX_RETRY} falhou "
                        f"(offset {offset}): {e}. Aguardando {espera:.0f}s..."
                    )
                    time.sleep(espera)

            if ultima_excecao:
                raise Exception(
                    f"Falha após {MAX_RETRY} tentativas (offset {offset}): {ultima_excecao}"
                )

            if 'error' in data:
                raise Exception(f"Erro do serviço: {data['error'].get('message','?')}")

            page = data.get('features', [])
            features.extend(page)
            self._log(f"   ... {len(features)} feições recebidas")

            if not data.get('exceededTransferLimit', False) or len(page) == 0:
                break

            offset += page_size
            time.sleep(pausa_seg)

        return features

    def _execute_etapa1(self) -> bool:
        self._log("=" * 60)
        self._log("ETAPA 1: ARCGIS MAPSERVER -> POSTGIS")
        self._log(f"Tabela: {TABLE_NAME_ETAPA1}")
        self._log("=" * 60)

        FIELD_MAPPING = {
            'CÓDIGO_INTERFERENCIA':                   ('codigo_interferencia',    'integer'),
            'NÚMERO_REGLA':                           ('numero_regla',            'varchar'),
            'NÚMERO_CNARH':                           ('numero_cadastro',         'varchar'),
            'EMPREENDIMENTO':                         ('nome_empreendimento',     'varchar'),
            'RESPONSÁVEL':                            ('nome_usuario',            'varchar'),
            'PRAZO_MÁXIMO_INÍCIO_AUTOMONITORAMENTO':  ('dr_max_telem',            'date'),
            'FIM_OBRIGATORIEDADE_AUTOMONITORAMENTO':  ('dr_fim_telem',            'date'),
            'MOTIVO_OBRIGATORIEDADE':                 ('motivo_obrigatoriedade',  'varchar'),
            'CLASSE_MONITORAMENTO':                   ('classe_monitoramento',    'varchar'),
            'TIPO_INTERFERÊNCIA':                     ('tipo_interferencia',      'varchar'),
            'DATA_INICIAL_OUTORGA':                   ('dr_inicio_outorga',       'date'),
            'DATA_VENCIMENTO_OUTORGA':                ('dr_vencimento_outorga',   'date'),
            'VAZAO_MEDIA_M3_H':                       ('vazao_media_m3_h',        'double'),
            'VAZAO_MAXIMA_M3_H':                      ('vazao_maxima_m3_h',       'double'),
            'FINALIDADE_OUTORGA':                     ('finalidade_outorga',      'varchar'),
            'DOMINIO':                                ('dominio',                 'varchar'),
            'NÚMERO_ATO_OUTORGA':                     ('numero_resolucao',        'varchar'),
            'ÓRGÃO_ORIGEM':                           ('orgao_origem',            'varchar'),
        }

        MINIMO_ESPERADO = 10000
        
        conn = None
        try:
            # --- DDL (admin) -----------------------------------------------
            self._log("1. Conectando ao PostgreSQL...")
            conn = self._get_pg()
            cur  = conn.cursor()
            self._log("   ✓ Conectado como {self.pg_usuario}")

            self._log("2. Obtendo metadados do MapServer...")
            meta = self._arcgis_metadata(MAPSERVER_URL)
            self._log(f"   Camada   : {meta.get('name','?')}")
            self._log(f"   Registros: {meta.get('maxRecordCount','?')}")

            self._log("3. Preparando tabela...")
            self._create_table_etapa1(cur, SCHEMA, TABLE_NAME_ETAPA1)
            self._set_permissions_etapa1(cur, SCHEMA, TABLE_NAME_ETAPA1)
            conn.commit()

            col_names    = [pg for _, (pg, _) in FIELD_MAPPING.items()]
            col_names   += ['longitude', 'latitude']
            placeholders = ['%s'] * len(col_names)
            insert_sql   = (
                f"INSERT INTO {SCHEMA}.{TABLE_NAME_ETAPA1} "
                f"({', '.join(col_names)}) VALUES ({', '.join(placeholders)})"
            )

            # --- Verificação de disponibilidade do serviço ---
            self._log("4. Verificando disponibilidade do MapServer...")
            try:
                req = urllib.request.Request(
                    f"{MAPSERVER_URL}?f=json",
                    headers={'User-Agent': 'QGIS Python'}
                )
                with urllib.request.urlopen(req, context=self._ssl_ctx(), timeout=15) as r:
                    status = json.loads(r.read().decode('utf-8'))
                if 'error' in status:
                    raise Exception(status['error'].get('message', 'Erro desconhecido'))
                self._log("   ✓ Serviço disponível")
            except Exception as e:
                self._log(f"   ✗ Serviço indisponível: {e}")
                self._log("")
                self._log("⚠ ATENÇÃO: O MapServer da SFI/ANA parece estar fora do ar ou inacessível.")
                self._log("   Possíveis causas:")
                self._log("   • Serviço temporariamente indisponível no portal SNIRH")
                self._log("   • Instabilidade de rede ou proxy corporativo")
                self._log("   • Manutenção programada no servidor ArcGIS")
                self._log("   Tente novamente em alguns minutos.")
                self._log("   URL: " + MAPSERVER_URL)
                # Emite sinal para exibir QMessageBox na thread principal
                self.erro_fatal.emit(
                    "O serviço ArcGIS MapServer está indisponível no momento.\n\n"
                    "Possíveis causas:\n"
                    "• Serviço fora do ar no portal SNIRH\n"
                    "• Instabilidade de rede\n"
                    "• Manutenção no servidor ArcGIS\n\n"
                    "Tente executar a atualização novamente em alguns minutos.\n\n"
                    f"URL: {MAPSERVER_URL}"
                )
                return False

            self._log("5. Buscando feições por partes...")
            all_features = self._fetch_mapserver_paginado(
                MAPSERVER_URL, page_size=2000, pausa_seg=2.0
            )
            total = len(all_features)
            self._log(f"   ✓ {total} feições recebidas")

            # Verificação de mínimo esperado
            if total < MINIMO_ESPERADO:
                self.erro_fatal.emit(
                    f"Serviço retornou apenas {total} feições (mín. esperado: {MINIMO_ESPERADO}).\n"
                    "Importação interrompida para evitar dados parciais."
                )
                return False

            self._log("6. Importando feições...")
            batch, total_imp, erros = [], 0, 0

            for feat_json in all_features:
                try:
                    attrs = feat_json.get('attributes', {})
                    geom  = feat_json.get('geometry', {})

                    row = []
                    for arc_name, (pg_col, ftype) in FIELD_MAPPING.items():
                        val = attrs.get(arc_name)
                        if ftype == 'date' and isinstance(val, int):
                            # MapServer retorna campos de data como timestamp
                            # em milissegundos (epoch UTC). Converter só aqui,
                            # evitando que campos inteiros como OID sejam
                            # tratados como datas.
                            row.append(self._converter_timestamp_ms(val))
                        else:
                            row.append(self._converter_valor(val))

                    lon = geom.get('x')
                    lat = geom.get('y')
                    row += [lon, lat]
                    batch.append(row)

                    if len(batch) >= BATCH_INSERT_SIZE:
                        cur.executemany(insert_sql, batch)
                        conn.commit()
                        total_imp += len(batch)
                        self._log(f"   ... {total_imp} registros inseridos")
                        batch = []

                except Exception as e_feat:
                    erros += 1
                    self._log(f"   ⚠ Erro em feição: {e_feat}")

            if batch:
                cur.executemany(insert_sql, batch)
                conn.commit()
                total_imp += len(batch)

            self._log(f"   ✓ Importação concluída: {total_imp} registros, {erros} erros")

            # Spatial join BAF
            self._populate_baf(cur, SCHEMA, TABLE_NAME_ETAPA1)
            conn.commit()
            
            # Spatial join UAM
            self._populate_uam(cur, SCHEMA, TABLE_NAME_ETAPA1)
            conn.commit()            

            self._log("=" * 60)
            self._log("✓ ETAPA 1 CONCLUÍDA!")
            self._log("=" * 60)
            return True, total_imp

        except Exception as e:
            self._log(f"✗ ERRO ETAPA 1: {e}")
            self._log(traceback.format_exc())
            if conn:
                conn.rollback()
            return False, 0
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # ETAPA 2 : Oracle DW -> PostGIS
    # ------------------------------------------------------------------

    def _create_table_etapa2(self, cur, schema, tbl):
        if self._table_exists(cur, schema, tbl):
            self._log(f"   Tabelas existente '{schema}.{tbl}' encontrada... ")
            # ✅ USAR TRUNCATE AO INVÉS DE DELETE (funciona com TRUNCATE privilege)
            try:
                cur.execute(f"TRUNCATE {schema}.{tbl} RESTART IDENTITY")
                self._log(f"   ✓ Tabela truncada com sucesso ")
            except psycopg2.errors.InsufficientPrivilege:
                self._log(f"   ⚠ Sem permissão para TRUNCATE. Tentando DELETE... ")
                try:
                    cur.execute(f"DELETE FROM {schema}.{tbl} ")
                    self._log(f"   ✓ Tabela limpa com DELETE ")
                except Exception as e:
                    self._log(f"   ✗ Falha ao limpar tabela: {e} ")
                    raise
        else:
            self._log(f"   Criando tabela '{schema}.{tbl}'...")
            cur.execute(f"""
                CREATE TABLE {schema}.{tbl} (
                    codigo_interferencia integer,
                    numero_cadastro      text,
                    nome_empreendimento  text,
                    numero_resolucao     text,
                    tipo_interferencia   text,
                    vol_anual            numeric,
                    vol_jan  numeric, vol_fev numeric, vol_mar numeric,
                    vol_abr  numeric, vol_mai numeric, vol_jun numeric,
                    vol_jul  numeric, vol_ago numeric, vol_set numeric,
                    vol_out  numeric, vol_nov numeric, vol_dez numeric,
                    latitude  numeric,
                    longitude numeric,
                    CONSTRAINT {tbl}_pk PRIMARY KEY (codigo_interferencia)
                )
            """)
        for idx_sql in (
            f"CREATE INDEX IF NOT EXISTS idx_{tbl}_cod  ON {schema}.{tbl}(codigo_interferencia)",
            f"CREATE INDEX IF NOT EXISTS idx_{tbl}_cad  ON {schema}.{tbl}(numero_cadastro)",
            f"CREATE INDEX IF NOT EXISTS idx_{tbl}_xy   ON {schema}.{tbl}(longitude, latitude)",
        ):
            cur.execute(idx_sql)
        cur.execute(
            f"COMMENT ON TABLE {schema}.{tbl} IS %s",
            ('ETL Oracle DW (CNARH40.MV_SFI_CNARH40 - tabela completa) -> PostgreSQL',),
        )
        self._log("   ✓ Tabela e índices prontos")

    def _set_permissions_etapa2(self, cur, schema, tbl):
            for role in ("telemetria_ro", "telemetria_rw", "usr_telemetria"):
                cur.execute(f"REVOKE ALL ON TABLE {schema}.{tbl} FROM {role}")
            cur.execute(f"GRANT ALL    ON TABLE {schema}.{tbl} TO iusr_coged_ro")
            cur.execute(f"GRANT ALL    ON TABLE {schema}.{tbl} TO postgres")
            cur.execute(f"GRANT SELECT ON TABLE {schema}.{tbl} TO telemetria_ro")
            cur.execute(f"GRANT TRUNCATE, INSERT, UPDATE, DELETE, SELECT ON TABLE {schema}.{tbl} TO telemetria_rw")
            cur.execute(f"GRANT SELECT ON TABLE {schema}.{tbl} TO usr_telemetria")
            self._log("   ✓ Permissões configuradas")

    @staticmethod
    def _decode_oracle_bytes(valor):
        """Decodifica bytes vindos do Oracle via UTL_RAW.CAST_TO_RAW + RTRIM.

        O driver oracledb thin entrega os bytes como RAW. Estratégia:
          1. Remove padding NUL (0x00) gerado pelo CAST_TO_RAW em campos CHAR
          2. Tenta UTF-8 (o modo thin converte internamente na maioria dos casos)
          3. Fallback cp1252 (charset real do banco WE8MSWIN1252)
        """
        if valor is None:
            return None
        if isinstance(valor, (bytes, bytearray)):
            limpo = valor.replace(b'\x00', b'')
            try:
                return limpo.decode('utf-8')
            except UnicodeDecodeError:
                return limpo.decode('cp1252', errors='replace')
        return valor

    @staticmethod
    def _decode_oracle_row(row):
        """Aplica _decode_oracle_bytes em todos os campos de uma tupla."""
        return tuple(
            ETLWorker._decode_oracle_bytes(v) if isinstance(v, (bytes, bytearray, str)) else v
            for v in row
        )

    def _extract_oracle(self, ora_conn) -> list:
        # UTL_RAW.CAST_TO_RAW(RTRIM(...)) faz o Oracle entregar bytes crus (RAW),
        # evitando a decodificação UTF-8 automática do oracledb 3.x que falha em
        # bancos com charset WE8MSWIN1252.
        # Filtro TIN_DS via bind variable com escape Unicode para não depender
        # do encoding do arquivo .py.
        query = """
            SELECT DISTINCT
                INT_CD,
                UTL_RAW.CAST_TO_RAW(RTRIM(INT_NU_CNARH))         AS INT_NU_CNARH,
                UTL_RAW.CAST_TO_RAW(RTRIM(EMP_NM_EMPREENDIMENTO)) AS EMP_NM_EMPREENDIMENTO,
                UTL_RAW.CAST_TO_RAW(RTRIM(OUT_NU_ATO))            AS OUT_NU_ATO,
                UTL_RAW.CAST_TO_RAW(RTRIM(TIN_DS))                AS TIN_DS,
                INT_QT_VOLUMEANUAL,
                VOL_JAN, VOL_FEV, VOL_MAR, VOL_ABR, VOL_MAI, VOL_JUN,
                VOL_JUL, VOL_AGO, VOL_SET, VOL_OUT, VOL_NOV, VOL_DEZ,
                INT_NU_LATITUDE, INT_NU_LONGITUDE
            FROM CNARH40.MV_SFI_CNARH40
            WHERE TIN_DS IN (:tin1, :tin2)
              AND INT_NU_CNARH IS NOT NULL
        """
        binds = {
            'tin1': 'Capta\u00e7\u00e3o',
            'tin2': 'Capta\u00e7\u00e3o em Barramento de Regulariza\u00e7\u00e3o',
        }

        cur = ora_conn.cursor()
        try:
            cur.execute(query, binds)
            rows = []
            for raw_row in cur:
                rows.append(self._decode_oracle_row(raw_row))
                if len(rows) % 5000 == 0:
                    self._log(f"   ... {len(rows)} linhas lidas do Oracle.")
            self._log(f"   ✓ Query Oracle concluída: {len(rows)} linhas")
            return rows
        finally:
            cur.close()

    def _create_temp_cnarh(self, pg_conn):
        cur = pg_conn.cursor()
        try:
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_cnarh (
                    INT_CD integer,
                    INT_NU_CNARH text, EMP_NM_EMPREENDIMENTO text, OUT_NU_ATO text, TIN_DS text,
                    INT_QT_VOLUMEANUAL numeric,
                    VOL_JAN numeric, VOL_FEV numeric, VOL_MAR numeric,
                    VOL_ABR numeric, VOL_MAI numeric, VOL_JUN numeric,
                    VOL_JUL numeric, VOL_AGO numeric, VOL_SET numeric,
                    VOL_OUT numeric, VOL_NOV numeric, VOL_DEZ numeric,
                    INT_NU_LATITUDE numeric, INT_NU_LONGITUDE numeric
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_tmp_cnarh_cd ON temp_cnarh (INT_CD)"
            )
            pg_conn.commit()
            self._log("   ✓ Tabela temporária temp_cnarh criada")
        finally:
            cur.close()

    def _insert_temp_cnarh(self, pg_conn, rows: list):
        cur = pg_conn.cursor()
        try:
            ins = """
                INSERT INTO temp_cnarh (
                    INT_CD, INT_NU_CNARH, EMP_NM_EMPREENDIMENTO, OUT_NU_ATO, TIN_DS, INT_QT_VOLUMEANUAL,
                    VOL_JAN, VOL_FEV, VOL_MAR, VOL_ABR, VOL_MAI, VOL_JUN,
                    VOL_JUL, VOL_AGO, VOL_SET, VOL_OUT, VOL_NOV, VOL_DEZ,
                    INT_NU_LATITUDE, INT_NU_LONGITUDE
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            for i in range(0, len(rows), BATCH_INSERT_SIZE):
                cur.executemany(ins, rows[i: i + BATCH_INSERT_SIZE])
                pg_conn.commit()
                self._log(f"   ... {min(i + BATCH_INSERT_SIZE, len(rows))} linhas inseridas")
            self._log(f"   ✓ temp_cnarh preenchida: {len(rows)} linhas")
        finally:
            cur.close()

    def _insert_final(self, pg_conn, schema, tbl2) -> int:
        """INSERT de temp_cnarh → tbl2."""
        cur = pg_conn.cursor()
        try:
            cur.execute(f"""
                INSERT INTO {schema}.{tbl2} (
                    codigo_interferencia, numero_cadastro, nome_empreendimento,
                    numero_resolucao, tipo_interferencia, vol_anual,
                    vol_jan, vol_fev, vol_mar, vol_abr, vol_mai, vol_jun,
                    vol_jul, vol_ago, vol_set, vol_out, vol_nov, vol_dez,
                    latitude, longitude
                )
                SELECT
                    INT_CD, INT_NU_CNARH, EMP_NM_EMPREENDIMENTO,
                    OUT_NU_ATO, TIN_DS,
                    INT_QT_VOLUMEANUAL,
                    VOL_JAN, VOL_FEV, VOL_MAR, VOL_ABR, VOL_MAI, VOL_JUN,
                    VOL_JUL, VOL_AGO, VOL_SET, VOL_OUT, VOL_NOV, VOL_DEZ,
                    INT_NU_LATITUDE, INT_NU_LONGITUDE
                FROM temp_cnarh
                ORDER BY INT_CD
            """)
            n = cur.rowcount
            pg_conn.commit()
            cur.execute(f"ANALYZE {schema}.{tbl2}")
            pg_conn.commit()
            self._log(f"   ✓ Inserção na tabela final concluída: {n} registros")
            return n
        finally:
            cur.close()
 
    def _execute_etapa2(self) -> bool:
        self._log("=" * 60)
        self._log("ETAPA 2: ORACLE DW -> POSTGIS")
        self._log(f"Tabela: {TABLE_NAME_ETAPA2}")
        self._log("=" * 60)
 
        ora_conn = pg = None
        try:
            # Importar driver Oracle
            self._log("0. Verificando driver Oracle...")
            if not self._garantir_oracledb():
                return False
            ora = _importar_oracle_driver()
            self._log(f"   ✓ Driver carregado: {ora._driver_name}")
 
            self._log("1. Conectando ao Oracle DW...")
            ora_conn = self._get_oracle(ora)
 
            self._log("2. Conectando ao PostgreSQL...")
            pg = self._get_pg()
            cur = pg.cursor()
            self._log(f"   ✓ Conectado como {self.pg_usuario}")
 
            self._log("3. Preparando tabela destino...")
            self._create_table_etapa2(cur, SCHEMA, TABLE_NAME_ETAPA2)
            pg.commit()
 
            self._log("4. Criando tabela temporária...")
            self._create_temp_cnarh(pg)
 
            self._log("5. Extraindo dados do Oracle...")
            rows = self._extract_oracle(ora_conn)
 
            self._log("6. Populando tabela temporária...")
            self._insert_temp_cnarh(pg, rows)
 
            self._log("7. Inserindo dados na tabela final...")
            n = self._insert_final(pg, SCHEMA, TABLE_NAME_ETAPA2)
 
            self._log("=" * 60)
            self._log(f"✓ ETAPA 2 CONCLUÍDA! {n} registros inseridos.")
            self._log("=" * 60)
            return True, n
 
        except Exception as e:
            self._log(f"✗ ERRO ETAPA 2: {e}")
            self._log(traceback.format_exc())
            if pg:
                pg.rollback()
            return False, 0
        finally:
            for c in (ora_conn, pg):
                if c:
                    try:
                        c.close()
                    except Exception:
                        pass
                        
    # ------------------------------------------------------------------
    # Ponto de entrada do worker (chamado pela thread)
    # ------------------------------------------------------------------

    def run(self):
        ok1 = ok2 = False
        start = datetime.now()
        self._log("=" * 60)
        self._log("PROCESSO ETL COMPLETO – SFI/ANA  v2.0")
        self._log(f"Início: {start.strftime('%d/%m/%Y %H:%M:%S')}")
        self._log("=" * 60)

        try:
            if EXECUTA_ETAPA1:
                ok1, n1 = self._execute_etapa1()
            else:
                self._log("⚠ ETAPA 1 desabilitada")
                ok1, n1 = False, 0

            if EXECUTA_ETAPA2:
                ok2, n2 = self._execute_etapa2()
            else:
                self._log("⚠ ETAPA 2 desabilitada")
                ok2, n2 = False, 0

        except Exception as e:
            self._log(f"✗ ERRO GERAL: {e}")
            self._log(traceback.format_exc())

        end = datetime.now()
        elapsed = end - start
        self._log("=" * 60)
        self._log("RESUMO FINAL")
        self._log(f"  Etapa 1 (ArcGIS  -> PostGIS): {'✓ SUCESSO' if ok1 else '✗ FALHA'} — {n1} registros em tb_mapserver_obrigatoriedade")
        self._log(f"  Etapa 2 (Oracle  -> PostGIS): {'✓ SUCESSO' if ok2 else '✗ FALHA'} — {n2} registros em tb_mv_sfi_cnarh40")
        self._log(f"  Início : {start.strftime('%d/%m/%Y %H:%M:%S')}")
        self._log(f"  Final  : {end.strftime('%d/%m/%Y %H:%M:%S')}")
        self._log(f"  Tempo total: {elapsed}")
        self._log("=" * 60)

        self.concluido.emit(ok1, ok2)


class WidgetAtualizacaoBase(QWidget):
    """Aba de interface para disparo e monitoramento do processo ETL de atualização da base.

    Integra-se à ``JanelaGestaoDados`` como uma das abas do ``QTabWidget``
    principal, sendo visível apenas para usuários com perfil diferente de
    ``telemetria_ro``. Sua lógica de ativação é intencional e não convencional:
    a aba **não exibe conteúdo permanente útil**; em vez disso, utiliza o
    próprio ato de selecionar a aba como gatilho para iniciar o processo ETL,
    detectado via o sinal ``currentChanged`` do ``QTabWidget`` pai.

    O ciclo completo de operação é:

        1. **Detecção de seleção** (``_on_tab_changed``): ao receber o índice
           da aba selecionada, verifica se é exatamente este widget. Se um
           processo já estiver em andamento, exibe aviso e retorna sem ação.
        2. **Diálogo de confirmação** (``_confirmar_e_executar``): abre um
           ``QDialog`` modal com ícone de alerta, descrição da operação
           (tempo estimado mínimo de 60 minutos), um seletor de data
           ``QDateEdit`` para definir o filtro de vencimento mínimo das
           outorgas (padrão: 1º de janeiro do ano anterior) e botões
           "Sim, iniciar" / "Cancelar". Se cancelado, retorna sem ação.
        3. **Início do ETL** (``_iniciar_etl``): instancia um ``ETLWorker``,
           injeta as credenciais (``pg_usuario``, ``pg_senha``) e a data de
           vencimento mínima escolhida pelo usuário, move o worker para uma
           ``QThread`` dedicada, conecta todos os sinais (``log_emitido``,
           ``concluido``, ``erro_fatal``) aos slots correspondentes e inicia
           a thread.
        4. **Transmissão de log em tempo real** (``_append_log``): cada
           mensagem emitida pelo worker é anexada ao ``QPlainTextEdit``
           estilizado (tema escuro, fonte monoespaçada), com scroll automático
           para a última linha.
        5. **Finalização bem-sucedida** (``_on_etl_concluido``): encerra a
           thread via ``quit()`` + ``wait()``, exibe ``QMessageBox``
           informativo ou de aviso conforme o resultado de cada etapa, atualiza
           o ``lbl_status`` com o resumo e exibe o botão "Limpar log".
        6. **Finalização com erro fatal** (``_on_etl_erro``): encerra a
           thread, exibe ``QMessageBox.critical`` com a mensagem recebida e
           reabilita a aba para nova tentativa.
        7. **Limpeza do log** (``_limpar_log``): oculta o painel de log e o
           botão de limpeza, restaurando a mensagem de instrução inicial para
           que o usuário possa disparar uma nova atualização.

    A UI da aba é intencionalmente minimalista: um ``QLabel`` de status
    centralizado (visível o tempo todo), um ``QPlainTextEdit`` de log de
    tema escuro (visível somente durante e após a execução) e um botão
    "Limpar log" (visível somente após a conclusão).

    Attributes:
        conn (psycopg2.connection): Conexão ativa ao PostgreSQL fornecida
            pela ``JanelaGestaoDados``; usada apenas para extrair parâmetros
            de conexão (DSN) — o ``ETLWorker`` abre sua própria conexão
            dedicada durante a execução.
        usuario_logado (str | None): Nome do usuário autenticado; injetado
            como ``pg_usuario`` no worker antes do início da thread.
        senha (str | None): Credencial do usuário; injetada como ``pg_senha``
            no worker.
        parent_tabs (QTabWidget | None): Referência ao ``QTabWidget`` da
            ``JanelaGestaoDados``; necessária para conectar ``currentChanged``
            e identificar quando esta aba específica é selecionada.
        _em_execucao (bool): Flag de guarda que impede disparos simultâneos
            do ETL caso o usuário navegue entre abas durante o processamento.
        _thread (QThread | None): Thread dedicada ao worker; criada em
            ``_iniciar_etl`` e encerrada em ``_on_etl_concluido`` ou
            ``_on_etl_erro``.
        _worker (ETLWorker | None): Instância do worker ETL movida para
            ``_thread``; referenciada para conexão de sinais e injeção de
            parâmetros.
        _data_vencimento_minima (datetime.date): Data de corte para o filtro
            de vencimento de outorgas, capturada do ``QDateEdit`` no diálogo
            de confirmação e repassada ao ``ETLWorker``.
        lbl_status (QLabel): Rótulo centralizado que exibe o estado atual
            da aba: instrução inicial, progresso em andamento ou resumo
            do resultado.
        txt_log (QPlainTextEdit): Área de log em tema escuro (fundo
            ``#1e1e1e``, texto ``#d4d4d4``, fonte Consolas 10 px); oculta
            por padrão e exibida ao iniciar o ETL.
        btn_limpar (QPushButton): Botão "Limpar log"; oculto por padrão e
            exibido somente após a conclusão (com sucesso ou erro) do ETL.
    """
    
    def __init__(self, conexao, usuario=None, senha=None, parent_tabs=None):
        """
        Parameters
        ----------
        conexao     : conexão psycopg2 já aberta (usada pelo plugin principal)
        usuario     : nome do usuário logado
        parent_tabs : referência ao QTabWidget pai (para conectar currentChanged)
        """
        super().__init__()
        self.conn           = conexao
        self.usuario_logado = usuario
        self.senha          = senha        
        self.parent_tabs    = parent_tabs
        self._em_execucao   = False
        self._thread        = None
        self._worker        = None
        self._initUI()

        # Conecta ao sinal de troca de aba se o QTabWidget pai for passado
        if self.parent_tabs is not None:
            self.parent_tabs.currentChanged.connect(self._on_tab_changed)

    def _initUI(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(10)

        # Rótulo de status (substitui conteúdo "permanente" da aba)
        self.lbl_status = QLabel(
            "Selecione esta aba novamente para iniciar a atualização da base de dados."
        )
        self.lbl_status.setAlignment(Qt.AlignCenter)
        self.lbl_status.setWordWrap(True)
        self.lbl_status.setStyleSheet("color: #555; font-size: 12px;")
        layout.addWidget(self.lbl_status)

        # Área de log (oculta até iniciar o processo)
        self.txt_log = QPlainTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setVisible(False)
        self.txt_log.setStyleSheet(
            "font-family: Consolas, monospace; font-size: 10px; background: #1e1e1e; color: #d4d4d4;"
        )
        layout.addWidget(self.txt_log)

        # Botão para cancelar / fechar log após conclusão
        self.btn_limpar = QPushButton("Limpar log")
        self.btn_limpar.setVisible(False)
        self.btn_limpar.clicked.connect(self._limpar_log)
        layout.addWidget(self.btn_limpar)

    def _on_tab_changed(self, index: int):
        """Disparado sempre que o usuário muda de aba em JanelaGestaoDados."""
        if self.parent_tabs is None:
            return
        # Verifica se a aba selecionada é exatamente este widget
        if self.parent_tabs.widget(index) is not self:
            return
        if self._em_execucao:
            QMessageBox.information(
                self,
                "Processo em andamento",
                "A atualização já está em execução. Aguarde a conclusão.",
            )
            return
        self._confirmar_e_executar()

    def _confirmar_e_executar(self):
            dlg = QDialog(self)
            dlg.setWindowTitle("Atenção – Atualização da base de dados")
            dlg.setMinimumWidth(520)
            dlg.setModal(True)

            v = QVBoxLayout(dlg)
            v.setSpacing(16)
            v.setContentsMargins(24, 24, 24, 24)

            icone = QLabel("⚠")
            icone.setStyleSheet("font-size: 36px;")
            icone.setAlignment(Qt.AlignCenter)
            v.addWidget(icone)

            msg = QLabel(
                "A atualização pode levar pelo menos 60 minutos de processamento, "
                "pois utiliza dados disponíveis em serviços geográficos e banco de dados "
                "da Agência."
                "\n\nDeseja iniciar agora?"
            )
            msg.setWordWrap(True)
            msg.setAlignment(Qt.AlignCenter)
            msg.setStyleSheet("font-size: 12px; color: #333;")
            v.addWidget(msg)

            btns = QDialogButtonBox(QDialogButtonBox.Yes | QDialogButtonBox.No)
            btns.button(QDialogButtonBox.Yes).setText("Sim, iniciar")
            btns.button(QDialogButtonBox.No).setText("Cancelar")
            btns.accepted.connect(dlg.accept)
            btns.rejected.connect(dlg.reject)
            v.addWidget(btns)

            if dlg.exec_() != QDialog.Accepted:
                return

            self._iniciar_etl()
            
    def _iniciar_etl(self):
        self._em_execucao = True
        self.txt_log.clear()
        self.txt_log.setVisible(True)
        self.btn_limpar.setVisible(False)
        self.lbl_status.setText("⏳ Atualização em andamento… consulte o log abaixo.")

        self._thread = QThread(self)
        self._worker = ETLWorker()
        self._worker.pg_usuario = self.usuario_logado
        self._worker.pg_senha = self.senha
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.log_emitido.connect(self._append_log)
        self._worker.concluido.connect(self._on_etl_concluido)
        self._worker.erro_fatal.connect(self._on_etl_erro)

        self._thread.start()

    def _append_log(self, msg: str):
        self.txt_log.appendPlainText(msg)
        self.txt_log.verticalScrollBar().setValue(
            self.txt_log.verticalScrollBar().maximum()
        )

    def _on_etl_concluido(self, ok1: bool, ok2: bool):
        self._em_execucao = False
        self._thread.quit()
        self._thread.wait()

        if ok1 and ok2:
            resumo = "✓ Atualização concluída com sucesso (Etapa 1 e Etapa 2)."
            QMessageBox.information(self, "Atualização concluída", resumo)
        else:
            partes = []
            if not ok1:
                partes.append("Etapa 1 (ArcGIS → PostGIS): FALHA")
            if not ok2:
                partes.append("Etapa 2 (Oracle → PostGIS): FALHA")
            resumo = "Atualização finalizada com erros:\n• " + "\n• ".join(partes)
            QMessageBox.warning(self, "Atualização com erros", resumo)

        self.lbl_status.setText(resumo)
        self.btn_limpar.setVisible(True)

    def _on_etl_erro(self, msg: str):
        self._em_execucao = False
        if self._thread:
            self._thread.quit()
            self._thread.wait()
        QMessageBox.critical(self, "Erro fatal", msg)
        self.lbl_status.setText("✗ Erro fatal durante a atualização.")
        self.btn_limpar.setVisible(True)

    def _limpar_log(self):
        self.txt_log.clear()
        self.txt_log.setVisible(False)
        self.btn_limpar.setVisible(False)
        self.lbl_status.setText(
            "Log limpo. Selecione esta aba novamente para iniciar uma nova atualização."
        )