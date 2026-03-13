# DURH Diária por Telemetria — Documentação Técnica de Manutenção

> **Plugin QGIS | SFI / Agência Nacional de Águas e Saneamento Básico (ANA)**  
> Versão **2.0** · Março/2026 · Resolução ANA n. 188/2024

---

## Sumário

1. [Visão Geral](#1-visão-geral)
2. [Requisitos de Ambiente](#2-requisitos-de-ambiente)
3. [Estrutura de Arquivos](#3-estrutura-de-arquivos)
4. [Arquitetura e Fluxo de Execução](#4-arquitetura-e-fluxo-de-execução)
5. [Catálogo de Módulos e Classes](#5-catálogo-de-módulos-e-classes)
   - 5.1 [`__init__.py` — Entrypoint QGIS](#51-__init__py--entrypoint-qgis)
   - 5.2 [`ui_tema.py` — Sistema de Design](#52-ui_temapy--sistema-de-design)
   - 5.3 [`main_plugin.py` — Orquestrador](#53-main_pluginpy--orquestrador)
   - 5.4 [Módulos de Cadastro](#54-módulos-de-cadastro)
   - 5.5 [Módulos de Gestão de Dados](#55-módulos-de-gestão-de-dados)
   - 5.6 [Módulos de Monitoramento](#56-módulos-de-monitoramento)
   - 5.7 [Módulos de Thread Assíncrona](#57-módulos-de-thread-assíncrona)
   - 5.8 [Módulos de Diálogos Auxiliares](#58-módulos-de-diálogos-auxiliares)
   - 5.9 [`widget_atualizacao_base.py` — Módulo ETL](#59-widget_atualizacao_basepy--módulo-etl)
6. [Banco de Dados PostgreSQL/PostGIS](#6-banco-de-dados-postgresqlpostgis)
7. [Dependências Python](#7-dependências-python)
8. [Perfis de Acesso](#8-perfis-de-acesso)
9. [Processamento Assíncrono (Threads Qt)](#9-processamento-assíncrono-threads-qt)
10. [Integração com QGIS Canvas](#10-integração-com-qgis-canvas)
11. [Integração com Serviços Externos](#11-integração-com-serviços-externos)
12. [Validação de Dados e Detecção de Anomalias](#12-validação-de-dados-e-detecção-de-anomalias)
13. [Exportação de Dados](#13-exportação-de-dados)
14. [Guia de Atualização e Manutenção](#14-guia-de-atualização-e-manutenção)
15. [Troubleshooting](#15-troubleshooting)
16. [Referências](#16-referências)

---

## 1. Visão Geral

O plugin **DURH Diária por Telemetria** é uma aplicação PyQGIS desenvolvida pela Superintendência de Fiscalização (SFI/ANA) para gerenciar o cadastro e o monitoramento de medidores de telemetria hídrica, em conformidade com a Resolução ANA n. 188, de 20 de março de 2024.

O plugin adota uma **arquitetura multi-arquivo**, onde cada classe principal reside em seu próprio módulo `.py` com nome idêntico à classe em `snake_case`. Essa organização favorece a manutenção isolada de cada componente, simplifica a rastreabilidade de alterações em controle de versão e permite que equipes trabalhem em paralelo em módulos distintos sem risco de conflito.

### Funcionalidades principais

| Módulo | Descrição |
|---|---|
| **Autenticação** | Login seguro com persistência de credenciais via `QSettings` e conexão validada ao PostgreSQL |
| **Cadastro de Operadores** | Registro de operadores de telemetria com validação de CPF/CNPJ e consulta ao CNARH |
| **Cadastro de Medidores** | Registro de medidores de vazão com georreferenciamento, vinculação CNARH e conversão de unidades |
| **Gestão de Dados** | Dashboard com KPIs, edição de operadores e medidores, exportação XLSX |
| **Monitoramento** | Busca e seleção de medidores, verificação de outorga vs. consumo, gráficos temporais |
| **Detalhamento 15 min** | Visualização e edição inline de leituras intraday com detecção de anomalias |
| **ETL de Base** | Pipeline de atualização das tabelas de obrigatoriedade a partir de ArcGIS MapServer e Oracle DW (CNARH40) |
| **Acesso CAR Privado** | Integração com serviço REST ArcGIS privado da ANA para dados CAR (LGPD) |

---

## 2. Requisitos de Ambiente

### QGIS

| Parâmetro | Valor |
|---|---|
| Versão mínima declarada | QGIS 3.0 (`qgisMinimumVersion=3.0` no `metadata.txt`) |
| Versões homologadas e testadas | **QGIS 3.38 LTR a QGIS 3.44** |
| Arquitetura | 64 bits (Windows 10/11 ou Linux) |
| Distribuição recomendada | OSGeo4W Network Installer (Windows) |

> **Nota de compatibilidade:** A API `QgsMapToolIdentifyFeature` e os métodos de `QgsVectorLayerUtils` requerem QGIS ≥ 3.16. O uso de `QgsTextBackgroundSettings` e `QgsExpressionContextUtils` está disponível a partir de QGIS 3.10. O intervalo **3.38–3.44** é o recomendado para produção.

### Python

| Parâmetro | Valor |
|---|---|
| Versão | **Python 3.12** (empacotado pelo QGIS via OSGeo4W) |
| Gerenciador de pacotes | `pip` (via OSGeo4W Shell ou Python interno do QGIS) |

### Banco de Dados

| Parâmetro | Valor |
|---|---|
| SGBD | **PostgreSQL 13+** com extensão **PostGIS 3.x** |
| Host de produção | `rds-webgis-dev.cjleec14qixz.sa-east-1.rds.amazonaws.com` (AWS RDS) |
| Database | `telemetria` |
| Porta | `5432` |

---

## 3. Estrutura de Arquivos

```
durh_telemetria/                              ← Diretório raiz do plugin (pacote Python)
│
├── __init__.py                               ← Entrypoint QGIS: SplashScreen + Cadastro (classFactory)
├── metadata.txt                              ← Metadados do plugin (QGIS Plugin Manager)
├── icon.png                                  ← Ícone exibido na barra de ferramentas do QGIS
├── splash.png                                ← Imagem da splash screen de inicialização
│
├── ui_tema.py                                ← Sistema de design: StyleConfig, CardButton, aplicar_tema_arredondado()
│
├── main_plugin.py                            ← Orquestrador: apenas JanelaLogin + TelaInicial
│
│   ── Cadastro ──────────────────────────────────────────────────────────────────────
├── tela_cadastro_operadores.py               ← class TelaCadastroOperadores
├── tela_cadastro_medidores.py                ← class TelaCadastroMedidores
│
│   ── Gestão de Dados ───────────────────────────────────────────────────────────────
├── janela_gestao_dados.py                    ← class JanelaGestaoDados
├── widget_dashboard.py                       ← class WidgetDashboard + GaugeSemicircular + GraficoSisHidrico
├── widget_operadores.py                      ← class WidgetOperadores
├── widget_medidores.py                       ← class WidgetMedidores
│
│   ── Diálogos Auxiliares ───────────────────────────────────────────────────────────
├── dialogo_unidade_vazao.py                  ← class DialogoUnidadeVazao
├── dialogo_unidade_potencia.py               ← class DialogoUnidadePotencia
├── dialogo_reativacao.py                     ← class DialogReativacao
│
│   ── Monitoramento ─────────────────────────────────────────────────────────────────
├── janela_monitoramento.py                   ← class JanelaMonitoramento
├── janela_graficos_medidor.py                ← class JanelaGraficosMedidor
├── janela_monitoramento_detalhes.py          ← class JanelaMonitoramentoDetalhes
│
│   ── Threads Assíncronas ───────────────────────────────────────────────────────────
├── verificacao_outorgado_thread.py           ← class VerificacaoOutorgadoThread
├── calc_mes_thread.py                        ← class CalcMesThread
│
│   ── ETL ────────────────────────────────────────────────────────────────────────────
└── widget_atualizacao_base.py                ← class ETLWorker + class WidgetAtualizacaoBase
```

### Princípios da organização

- Cada classe reside em um arquivo `.py` com **nome idêntico em `snake_case`**, à exceção de `JanelaLogin` e `TelaInicial`, que permanecem em `main_plugin.py` por serem o ponto de entrada da UI e estarem diretamente acopladas ao fluxo de inicialização do plugin;
- Três classes auxiliares de renderização customizada (`GaugeSemicircular`, `GraficoSisHidrico`) são agrupadas com `WidgetDashboard` em `widget_dashboard.py` por não terem uso independente;
- `ETLWorker` e `WidgetAtualizacaoBase` coexistem em `widget_atualizacao_base.py` pela forte coesão operacional entre os dois — o worker não faz sentido fora do contexto da aba que o gerencia;
- Todos os módulos utilizam importações relativas (`from . import ...`) para garantir portabilidade do pacote.

---

## 4. Arquitetura e Fluxo de Execução

### 4.1 Ciclo de vida do plugin no QGIS

```
QGIS inicia
    └─ classFactory(iface)  →  __init__.py
           └─ instancia Cadastro(iface)
                  └─ initGui()
                         └─ registra QAction na toolbar "ANA"
                                └─ clique do usuário → run_with_splash()
                                       ├─ SplashScreen  (2,5 s + fade-in/out)
                                       └─ _abrir_janela_login()  [QTimer 150 ms]
                                              └─ JanelaLogin.exec_()  [bloqueante]
                                                     └─ aceito → TelaInicial(iface, conn, usuario, senha)
```

### 4.2 Fluxo de navegação entre janelas

A navegação segue o padrão de **troca visível** (`hide` / `show`), sem empilhamento modal. Cada janela filha recebe referência à janela-pai (`tela_inicial` ou `janela_anterior`) e, ao fechar ou voltar, chama `self.janela_anterior.show()`. A conexão PostgreSQL (`conn`), o usuário logado e a senha são propagados por todos os níveis da hierarquia.

```
TelaInicial  (main_plugin.py)
│
├─ → TelaCadastroOperadores      (tela_cadastro_operadores.py)
│
├─ → TelaCadastroMedidores       (tela_cadastro_medidores.py)
│
├─ → JanelaGestaoDados           (janela_gestao_dados.py)
│       ├─ [aba 0]  WidgetDashboard          (widget_dashboard.py)
│       │               ├─ GaugeSemicircular  (widget_dashboard.py)  ×2
│       │               └─ GraficoSisHidrico  (widget_dashboard.py)
│       ├─ [aba 1]  WidgetOperadores          (widget_operadores.py)
│       ├─ [aba 2]  WidgetMedidores           (widget_medidores.py)
│       │               ├─ DialogoUnidadeVazao     (dialogo_unidade_vazao.py)    modal
│       │               ├─ DialogoUnidadePotencia  (dialogo_unidade_potencia.py) modal
│       │               └─ DialogReativacao        (dialogo_reativacao.py)       modal
│       └─ [aba 3]  WidgetAtualizacaoBase     (widget_atualizacao_base.py)  ← somente rw
│                       └─ ETLWorker  (QThread)
│
└─ → JanelaMonitoramento         (janela_monitoramento.py)
        ├─ VerificacaoOutorgadoThread  (verificacao_outorgado_thread.py)  QThread
        ├─ → JanelaGraficosMedidor    (janela_graficos_medidor.py)
        └─ → JanelaMonitoramentoDetalhes (janela_monitoramento_detalhes.py)
                └─ CalcMesThread  (calc_mes_thread.py)  QThread
```

### 4.3 Gestão da conexão PostgreSQL

A conexão é criada uma única vez em `JanelaLogin` via `psycopg2.connect()` e propagada por referência a todas as janelas filhas. Janelas que necessitam de estado limpo executam `conn.rollback()` e ativam `conn.autocommit = True` no seu `__init__`. O módulo ETL (`widget_atualizacao_base.py`) abre **conexões próprias e isoladas** durante a execução da thread, não reutilizando a conexão principal.

### 4.4 Padrão de importação entre módulos

Todos os módulos do plugin utilizam importações relativas. Exemplo representativo de `janela_gestao_dados.py`:

```python
# janela_gestao_dados.py
from qgis.PyQt.QtWidgets import QWidget, QVBoxLayout, QPushButton, QTabWidget, QDesktopWidget
from . import ui_tema
from .widget_dashboard        import WidgetDashboard
from .widget_operadores       import WidgetOperadores
from .widget_medidores        import WidgetMedidores
from .widget_atualizacao_base import WidgetAtualizacaoBase
```

Exemplo de `main_plugin.py` (orquestrador):

```python
# main_plugin.py
from . import ui_tema
from .tela_cadastro_operadores      import TelaCadastroOperadores
from .tela_cadastro_medidores       import TelaCadastroMedidores
from .janela_gestao_dados           import JanelaGestaoDados
from .janela_monitoramento          import JanelaMonitoramento
```

---

## 5. Catálogo de Módulos e Classes

### 5.1 `__init__.py` — Entrypoint QGIS

#### `classFactory(iface)`

Função obrigatória exigida pelo QGIS Plugin Manager. Instancia e retorna a classe `Cadastro`.

#### `SplashScreen(QSplashScreen)`

Tela de apresentação exibida durante 2,5 segundos na inicialização. Renderizada via `QPainter` sobre um `QPixmap` transparente, com animações de **fade-in** e **fade-out** controladas por `QTimer` (passo de opacidade 0,1 a cada 50 ms). Exibe o logotipo institucional (`splash.png`), nome do plugin, subtítulo e referência legal (Resolução ANA n. 188/2024). Usa `Qt.WA_TranslucentBackground` e `Qt.FramelessWindowHint` para bordas arredondadas reais sem decoração de janela.

#### `Cadastro`

Classe principal registrada no QGIS. Responsabilidades:

- Registrar a `QAction` na toolbar `"ANA"` e no menu de plugins com ícone (`icon.png`);
- Orquestrar a sequência splash → login → tela inicial via `QTimer.singleShot`;
- Importar `JanelaLogin` e `TelaInicial` de `main_plugin.py` em tempo de execução (lazy import, evitando carregamento desnecessário na inicialização do QGIS);
- Disponibilizar `unload()` para limpeza segura ao desabilitar o plugin (remoção da toolbar, fechamento do widget principal).

---

### 5.2 `ui_tema.py` — Sistema de Design

Módulo de design centralizado. Define a paleta institucional e estilos globais via QSS (Qt Style Sheets). **Não deve ser alterado sem alinhamento com a identidade visual da ANA.**

#### `StyleConfig`

Classe de constantes de estilo (não instanciável na prática). Define a paleta e o stylesheet global.

| Constante | Valor | Uso |
|---|---|---|
| `PRIMARY_COLOR` | `#175cc3` | Botões, títulos, bordas de foco, cabeçalhos XLSX |
| `SECONDARY_COLOR` | `#5474b8` | Hover, subtítulos, bordas secundárias |
| `BACKGROUND_WHITE` | `#FFFFFF` | Fundo de cards e containers |
| `TEXT_DARK` | `#333333` | Texto principal |
| `BORDER_COLOR` | `#E0E0E0` | Bordas de inputs e separadores |
| `HOVER_COLOR` | `#F5F5F5` | Estado hover de elementos interativos |
| `MAIN_STYLE` | String QSS completa | Aplicada via `aplicar_tema_arredondado()` em todas as janelas |

O `MAIN_STYLE` cobre: `QWidget`, `QLabel`, `QLineEdit`, `QComboBox`, `QDateEdit`, `QDateTimeEdit`, `QTabWidget`, `QTabBar`, `QPushButton` e o objeto nomeado `#ContainerBranco`.

#### `CardButton(QPushButton)`

Botão estilizado em formato de card (240×120 px) com ícone SVG renderizado via `QSvgRenderer` e texto em duas linhas (título em negrito + descrição). Efeito de sombra via `QGraphicsDropShadowEffect` (blur 15 px, deslocamento Y 4 px, opacidade 30/255). Ícones disponíveis como SVG inline: `operador`, `medidor`, `gestao`, `monitoramento`, `fechar`. Usado exclusivamente na `TelaInicial`.

#### `aplicar_tema_arredondado(widget)`

Função utilitária que define `objectName = "ContainerBranco"` e aplica o `MAIN_STYLE` ao widget recebido. Chamada no `initUI()` de todas as janelas principais.

---

### 5.3 `main_plugin.py` — Orquestrador

Arquivo enxuto que contém exclusivamente as duas classes de entrada da interface e os imports dos módulos filhos necessários para que `TelaInicial` possa instanciá-los.

#### `JanelaLogin(QDialog)`

Diálogo modal de autenticação exibido antes de qualquer outra janela. Valida as credenciais testando a conexão PostgreSQL via `psycopg2.connect()`. Recursos:

- Campos de usuário e senha com botão de visibilidade (ícone de olho, `QToolButton`);
- Checkbox "Lembrar credenciais" com persistência segura via `QSettings`;
- Tratamento de erros diferenciado por tipo de exceção psycopg2 (autenticação inválida, host inacessível, banco inexistente, timeout).

Ao aceitar, disponibiliza `self.conn` (conexão ativa), `self.usuario` e `self.senha` para o chamador em `__init__.py`.

#### `TelaInicial(QWidget)`

Menu principal do plugin. Exibe `CardButton`s em grade com layout adaptativo por perfil:

- **`telemetria_ro`** — grade 1×2: "Dados Cadastrais" e "Monitoramento";
- **Demais perfis** — grade 2×2: "Operadores", "Medidores", "Cadastros e dados", "Monitoramento".

Verifica a validade da conexão (`SELECT 1`) antes de abrir qualquer janela filha. Em caso de falha, exibe aviso e encerra o plugin. Propaga `conn`, `usuario_logado` e `senha` a todas as janelas filhas instanciadas.

---

### 5.4 Módulos de Cadastro

#### `tela_cadastro_operadores.py` → `TelaCadastroOperadores(QWidget)`

Formulário completo de cadastro de operadores em `tb_operador_telemetria`. Suporta dois modos mutuamente exclusivos via radio buttons:

- **Operador é o próprio usuário de água** — busca dados automaticamente no CNARH via API REST e preenche os campos com o resultado;
- **Operador é terceirizado** — exige preenchimento manual completo.

Valida e-mail com `QRegExpValidator`, CPF/CNPJ com máscara de input, e executa cadastro em lote (múltiplos operadores por sessão). Exibe notificação temporária de confirmação via `QTimer` após cada cadastro bem-sucedido. Flags de estado `_em_consulta` e `_dados_validados` controlam habilitação do botão de cadastrar.

**Importações relevantes:**
```python
from . import ui_tema
```

#### `tela_cadastro_medidores.py` → `TelaCadastroMedidores(QWidget)`

Formulário multi-etapa de cadastro de medidores em `tb_intervencao`. Funcionalidades:

- Vinculação ao operador cadastrado (FK para `tb_operador_telemetria`);
- Conversão automática de unidades de vazão (m³/h → m³/s ÷ 3600) e potência (cv → kW × 0,7355);
- Georreferenciamento por coordenadas inseridas manualmente ou capturadas diretamente no canvas QGIS via `QgsMapToolIdentifyFeature`;
- Exibição do medidor recém-cadastrado como camada vetorial pontual no canvas após confirmação;
- Suporte a cadastro de múltiplos medidores por sessão, com limpeza e reset de flags ao concluir.

**Importações relevantes:**
```python
from . import ui_tema
from .dialogo_unidade_vazao    import DialogoUnidadeVazao
from .dialogo_unidade_potencia import DialogoUnidadePotencia
```

---

### 5.5 Módulos de Gestão de Dados

#### `janela_gestao_dados.py` → `JanelaGestaoDados(QWidget)`

Janela hub pós-login organizada em `QTabWidget` com quatro abas. Reinicia a transação PostgreSQL (`rollback` + `autocommit = True`) ao ser instanciada. Ajusta o tamanho da janela dinamicamente ao trocar de aba via `ajustar_tamanho_aba()`.

| Índice | Aba | Módulo | Dimensões |
|---|---|---|---|
| 0 | Dashboard | `widget_dashboard.py` | 690×720, min 600×600 |
| 1 | Operadores cadastrados | `widget_operadores.py` | Fixo 690×420 |
| 2 | Medidores cadastrados | `widget_medidores.py` | 850×700, min 800×650 |
| 3 | Atualizar base de dados *(somente rw)* | `widget_atualizacao_base.py` | Livre |

**Importações relevantes:**
```python
from . import ui_tema
from .widget_dashboard        import WidgetDashboard
from .widget_operadores       import WidgetOperadores
from .widget_medidores        import WidgetMedidores
from .widget_atualizacao_base import WidgetAtualizacaoBase
```

#### `widget_dashboard.py` → `WidgetDashboard` + `GaugeSemicircular` + `GraficoSisHidrico`

Painel executivo com KPIs em três camadas visuais, alimentadas por queries SQL declaradas como atributos de classe (`SQL_*`):

**`WidgetDashboard(QWidget)`** — orquestra as três camadas e executa as consultas ao banco em `carregar_dados()`:

| Card | Query | Formato exibido |
|---|---|---|
| Operadores | `SQL_OPERADORES` | Inteiro absoluto |
| Usuários | `SQL_USUARIOS_CAD` / `SQL_USUARIOS_OBR` | `cadastrados/obrigados` |
| Interferências | `SQL_INTERF_CAD` / `SQL_INTERF_OBR` | `cadastrados/obrigados` |
| Medidores | `SQL_MEDIDORES` | Inteiro absoluto (excl. testes) |

**`GaugeSemicircular(QWidget)`** — widget de renderização customizada via `QPainter`. Desenha dois arcos concêntricos de 180° com texto de percentual no centro. Escala de cores automática em `_cor_por_percentual()`: verde `#1cc88a` (≥ 70 %), amarelo `#f6c23e` (≥ 40 %), vermelho `#e74a3b` (< 40 %).

**`GraficoSisHidrico(QWidget)`** — widget de barras horizontais por sistema hídrico, renderizado via `QPainter`. Altura calculada dinamicamente (28 px × número de sistemas + 40 px). Compatível com `QScrollArea`. Texto de valores absolutos (`cad/obr`) sobreposto dentro da barra; percentual à direita.

#### `widget_operadores.py` → `WidgetOperadores(QWidget)`

Aba de consulta e edição de operadores em `tb_operador_telemetria`. Funcionalidades:

- Busca por nome via `QComboBox` editável com carregamento da lista completa;
- Edição dos campos Nome, CPF/CNPJ e E-mail com `UPDATE` direto ao clicar em "Salvar alterações";
- Exclusão com confirmação via `QMessageBox` (`DELETE`);
- Navegação para a aba de medidores via referência ao `parent_window` (`JanelaGestaoDados`);
- Exportação XLSX com dois modos: todos os operadores ou somente os com transmissão ativa.

**Importações relevantes:**
```python
from . import ui_tema
```

#### `widget_medidores.py` → `WidgetMedidores(QWidget)`

Aba de consulta, edição e manutenção de medidores em `tb_intervencao`. Suporta busca por 6 critérios:

`Rótulo` · `Nome do usuário` · `CNARH` · `Código UC` · `Operador` · `Sistema Hídrico`

Ao selecionar um medidor na `QTreeWidget`, os campos de detalhe são preenchidos. Ao editar vazão ou potência e o campo perder foco (`editingFinished`), os diálogos de unidade são disparados automaticamente. Campos inválidos são destacados em vermelho. Desativação lógica via sufixo `#` no rótulo. Reativação via `DialogReativacao`. Exportação XLSX com mesma formatação institucional.

**Importações relevantes:**
```python
from . import ui_tema
from .dialogo_unidade_vazao    import DialogoUnidadeVazao
from .dialogo_unidade_potencia import DialogoUnidadePotencia
from .dialogo_reativacao       import DialogReativacao
```

---

### 5.6 Módulos de Monitoramento

#### `janela_monitoramento.py` → `JanelaMonitoramento(QWidget)`

Janela de busca e seleção de medidores para o fluxo de monitoramento. Funcionalidades:

- Busca com autocompletar em tempo real por 3 critérios (CNARH, Usuário, Sistema Hídrico);
- Listagem em `QTreeWidget` com seleção múltipla e detecção automática de múltiplas interferências;
- Verificação assíncrona de consumo vs. outorgado disparada 300 ms após abertura (`QTimer`), via `VerificacaoOutorgadoThread`;
- Minimiza a janela ao navegar para filhas ("Ver no Mapa") e restaura ao retornar;
- Atalho "Selecionar tudo" com flag `is_selecao_total` propagada às janelas filhas.

**Importações relevantes:**
```python
from . import ui_tema
from .verificacao_outorgado_thread  import VerificacaoOutorgadoThread
from .janela_graficos_medidor       import JanelaGraficosMedidor
from .janela_monitoramento_detalhes import JanelaMonitoramentoDetalhes
```

#### `janela_graficos_medidor.py` → `JanelaGraficosMedidor(QWidget)`

Janela de visualização gráfica via Matplotlib embutido (`FigureCanvasQTAgg`). Apresenta dois painéis temporais:

- **Gráfico mensal** — volume consumido por mês no ano corrente, com tooltips interativos ao passar o cursor sobre as barras. Suporte a barras empilhadas ou sobrepostas;
- **Gráfico diário** — consumo dia a dia para o mês selecionado, com linha de volume outorgado e marcação visual de anomalias.

Recursos adicionais: legenda interativa (ocultar/exibir séries por clique), exportação para XLSX com formatação institucional e relatório TXT de 15 minutos para a data selecionada, botão "Ver no Mapa" que minimiza esta janela e a `JanelaMonitoramento` anterior. Tamanho adaptado: 1200×900 para múltiplas interferências, 1000×750 para caso simples.

**Importações relevantes:**
```python
from . import ui_tema
```

#### `janela_monitoramento_detalhes.py` → `JanelaMonitoramentoDetalhes(QWidget)`

Janela de monitoramento detalhado com três níveis de granularidade:

- **Calendário mensal** — grid com consumo diário e indicadores visuais de anomalia (célula laranja). Totais pré-calculados por `CalcMesThread` e cacheados em `_totais_15min_por_dia`;
- **Tabela de dados diários** — visualização e edição inline com commit em lote via `UPDATE`. Células editadas destacadas em amarelo. Proteção contra saída acidental com dados pendentes;
- **Abas de 15 minutos** — criadas dinamicamente por data clicada (`tabs_15min_internas`), com leituras completas de `tb_telemetria_intervencao` e marcação de anomalias com tooltip explicativo.

Constantes de cálculo: `FATOR_SEGURANCA = 5.0`, `SEGUNDOS_DIA = 86400`, `SEGUNDOS_HORA = 3600`, `INTERVALO_PADRAO = 900`. Cache de sessão em `cache_calendario` e `cache_15min` reduz roundtrips ao banco.

**Importações relevantes:**
```python
from . import ui_tema
from .calc_mes_thread import CalcMesThread
```

---

### 5.7 Módulos de Thread Assíncrona

#### `verificacao_outorgado_thread.py` → `VerificacaoOutorgadoThread(QThread)`

Thread assíncrona de verificação de excedência de consumo mensal. Abre conexão PostgreSQL dedicada (a partir dos parâmetros DSN da conexão principal). Três etapas sequenciais:

1. Agrega `consumo_diario` por interferência para o mês/ano solicitado, excluindo registros de teste;
2. Busca volumes outorgados mensais de `view_volume_outorgado` via CASE por coluna de mês;
3. Compara e retorna interferências com `consumo > outorgado`, ordenadas pelo maior excesso absoluto.

Suporta cancelamento cooperativo via `cancelar()` com envio de `conn.cancel()` ao PostgreSQL.

| Signal | Tipo | Descrição |
|---|---|---|
| `resultado_signal` | `(list, str, int)` | Lista de alertas, nome do mês, ano |
| `erro_signal` | `(str)` | Mensagem de exceção com traceback |
| `progresso_signal` | `(str)` | Mensagem descritiva da etapa em andamento |

#### `calc_mes_thread.py` → `CalcMesThread(QThread)`

Thread assíncrona para cálculo do consumo diário corrigido de um mês completo. Reproduz o algoritmo de detecção e correção de anomalias sem renderizar widgets. Para cada dia e cada medidor:

| Anomalia | Critério | Correção |
|---|---|---|
| Wrap-around (salto negativo) | `delta < 0` | Usa `vazao × duracao`; acumula `correcao_acumulada` |
| Injeção espúria (salto positivo absurdo) | `delta > vn × dur × FATOR_SEGURANCA` | Subtrai excesso; preserva incremento físico |
| Continuação pós-overflow | `overflow_detectado == True` | Aplica correção acumulada antes do delta |

| Signal | Tipo | Descrição |
|---|---|---|
| `progresso` | `(int, int, str)` | Dia atual, total de dias, mensagem |
| `dia_concluido` | `(str, float, bool)` | Data `AAAA-MM-DD`, total m³, flag de anomalia |
| `finalizado` | `()` | Emitido ao concluir todos os dias |
| `erro` | `(str)` | Mensagem de exceção |

---

### 5.8 Módulos de Diálogos Auxiliares

#### `dialogo_unidade_vazao.py` → `DialogoUnidadeVazao(QDialog)`

Diálogo modal de confirmação de unidade para o campo de vazão nominal. Pré-seleciona inteligentemente: m³/h para valores > 10 (típico de especificações agrícolas/industriais), m³/s para demais. Retorna `"m3s"` ou `"m3h"` via `get_unidade()`. A conversão de m³/h → m³/s é realizada pelo chamador dividindo por 3600.

#### `dialogo_unidade_potencia.py` → `DialogoUnidadePotencia(QDialog)`

Diálogo modal de confirmação de unidade para o campo de potência do motor. Pré-seleciona cv para valores < 500 ou com separador decimal; kW para demais. Retorna `"kw"` ou `"cv"` via `get_unidade()`. A conversão cv → kW é realizada pelo chamador multiplicando por 0,7355.

#### `dialogo_reativacao.py` → `DialogReativacao(QDialog)`

Diálogo modal para reativação em lote de medidores desativados (rótulo terminando em `#`). Exibe lista com seleção múltipla (`QAbstractItemView.MultiSelection`) e combo de operadores para vinculação. Executa `UPDATE` atômico via `TRIM(TRAILING '#' FROM rotulo)` com `rollback()` automático em caso de erro. Fecha com `QDialog.Accepted` ao concluir, sinalizando ao chamador para recarregar a lista.

---

### 5.9 `widget_atualizacao_base.py` — Módulo ETL

#### `ETLWorker(QObject)`

Worker do processo ETL completo, executado em `QThread` dedicada gerenciada por `WidgetAtualizacaoBase`. Implementa dois pipelines independentes e sequenciais, controlados pelas flags de módulo `EXECUTA_ETAPA1` e `EXECUTA_ETAPA2`.

**Etapa 1 — ArcGIS MapServer → `tb_mapserver_obrigatoriedade`**

| Passo | Ação |
|---|---|
| 1 | Conecta ao PostgreSQL com credenciais do usuário logado |
| 2 | Consulta metadados do serviço REST (`MAPSERVER_URL`) para verificar disponibilidade |
| 3 | Cria ou trunca `tb_mapserver_obrigatoriedade` (22 colunas + 5 índices + permissões para 5 roles) |
| 4 | Verifica disponibilidade do serviço (timeout 15 s); aborta com `erro_fatal` se inacessível |
| 5 | Baixa feições paginadas (2.000/página, pausa 2 s, retry 3× com backoff exponencial) |
| 6 | Valida mínimo de 10.000 feições recebidas antes de inserir |
| 7 | Insere em lotes de 500 com commit parcial; converte timestamps epoch ms → `datetime.date` |
| 8 | Executa join espacial `ST_Intersects` com `ft_sishidrico_buffer` para preencher campos BAF |

**Etapa 2 — Oracle DW (CNARH40) → `tb_mv_sfi_cnarh40`**

| Passo | Ação |
|---|---|
| 0 | Verifica/instala `oracledb` via `pip --user` (sem exigir privilégios de administrador) |
| 1 | Conecta ao Oracle DW via `cx_Oracle` (preferencial) ou `oracledb` modo thin |
| 2 | Conecta ao PostgreSQL |
| 3 | Cria ou trunca `tb_mv_sfi_cnarh40` (18 colunas, PK em `codigo_interferencia`) |
| 4 | Cria tabela temporária de sessão `temp_cnarh` no PostgreSQL |
| 5 | Extrai `CNARH40.MV_SFI_CNARH40` filtrando por tipo `Captação` e vencimento de outorga ≥ data mínima |
| 6 | Popula `temp_cnarh` em lotes de 500 |
| 7 | Executa JOIN `temp_cnarh × tb_mapserver_obrigatoriedade` → insere em `tb_mv_sfi_cnarh40` |
| 8 | Executa `ANALYZE` na tabela final para atualizar estatísticas do planner |

**Sinais:**

| Signal | Tipo | Descrição |
|---|---|---|
| `log_emitido` | `(str)` | Linha de log enviada ao `QgsMessageLog` e ao widget de log |
| `concluido` | `(bool, bool)` | Sucesso da Etapa 1 e da Etapa 2 |
| `erro_fatal` | `(str)` | Condição impeditiva (driver ausente, serviço fora do ar, total abaixo do mínimo) |

**Atributos de classe injetados antes do start da thread:**

```python
ETLWorker.pg_usuario             = "usuario_logado"
ETLWorker.pg_senha               = "senha"
ETLWorker.data_vencimento_minima = datetime.date(ano_anterior, 1, 1)
```

**Constantes de módulo:**

```python
PG_BASE            = { 'host': "rds-...", 'port': 5432, 'dbname': "telemetria" }
ORACLE_CONFIG      = { 'user': 'DW_RO', 'password': '...', 'host': '...', ... }
MAPSERVER_URL      = "https://portal1.snirh.gov.br/server/rest/services/SFI/..."
TABLE_NAME_ETAPA1  = "tb_mapserver_obrigatoriedade"
TABLE_NAME_ETAPA2  = "tb_mv_sfi_cnarh40"
SCHEMA             = "public"
BATCH_INSERT_SIZE  = 500
EXECUTA_ETAPA1     = True
EXECUTA_ETAPA2     = True
```

#### `WidgetAtualizacaoBase(QWidget)`

Aba de interface do ETL dentro de `JanelaGestaoDados`. Visível apenas para usuários com perfil diferente de `telemetria_ro`. Usa `QTabWidget.currentChanged` para disparar automaticamente o diálogo de confirmação ao selecionar a aba.

Ciclo de operação:

1. **Detecção de seleção** — `_on_tab_changed()` verifica se a aba ativa é este widget; aborta se `_em_execucao == True`;
2. **Diálogo de confirmação** — `QDialog` modal com aviso (≥ 60 min), seletor `QDateEdit` de vencimento mínimo (padrão: 1º jan do ano anterior), botões "Sim, iniciar" / "Cancelar";
3. **Início do ETL** — `_iniciar_etl()` instancia `ETLWorker`, injeta credenciais e data, move para `QThread`, conecta sinais;
4. **Log em tempo real** — `_append_log()` anexa cada mensagem ao `QPlainTextEdit` (tema escuro, Consolas 10 px) com auto-scroll;
5. **Finalização** — `_on_etl_concluido()` encerra a thread (`quit()` + `wait()`), exibe `QMessageBox` de resultado, habilita "Limpar log";
6. **Erro fatal** — `_on_etl_erro()` encerra a thread e exibe `QMessageBox.critical`;
7. **Limpeza** — `_limpar_log()` oculta o painel de log e restaura a mensagem de instrução inicial.

---

## 6. Banco de Dados PostgreSQL/PostGIS

### 6.1 Tabelas e views principais consultadas

| Objeto | Tipo | Usado em |
|---|---|---|
| `tb_operador_telemetria` | Tabela | Cadastro, `widget_operadores.py` |
| `tb_intervencao` | Tabela | Cadastro, `widget_medidores.py`, monitoramento |
| `tb_telemetria_intervencao` | Tabela | Leituras de 15 minutos |
| `tb_telemetria_intervencao_diaria` | Tabela | Totais diários |
| `tb_mapserver_obrigatoriedade` | Tabela (ETL Etapa 1) | Dashboard, Etapa 2 do ETL (JOIN) |
| `tb_mv_sfi_cnarh40` | Tabela (ETL Etapa 2) | `view_volume_outorgado` |
| `view_ft_intervencao` | View | Dashboard (`SQL_USUARIOS_CAD`, `SQL_INTERF_CAD`) |
| `view_ft_captacao_obrigatoriedade` | View | Dashboard (`SQL_SISHIDRICO`, `SQL_USUARIOS_OBR`, `SQL_INTERF_OBR`) |
| `view_usuario_operador_id_rotulo` | View | `verificacao_outorgado_thread.py` |
| `view_volume_outorgado` | View | `verificacao_outorgado_thread.py`, gráfico diário |
| `ft_sishidrico_buffer` | Tabela PostGIS | ETL Etapa 1 (join espacial BAF) |

### 6.2 Roles PostgreSQL

| Role | Permissões nas tabelas ETL | Acesso à UI |
|---|---|---|
| `telemetria_ro` | SELECT | Sem aba ETL, sem cadastro |
| `telemetria_rw` | SELECT, INSERT, UPDATE, DELETE, TRUNCATE | Acesso completo |
| `usr_telemetria` | SELECT nas tabelas de referência | — |
| `iusr_coged_ro` | ALL nas tabelas atualizadas pelo ETL | — |
| `postgres` | ALL (superusuário) | — |

---

## 7. Dependências Python

### 7.1 Nativas do QGIS (sempre disponíveis)

| Biblioteca | Versão típica (QGIS 3.38–3.44) | Uso |
|---|---|---|
| `PyQt5` / `qgis.PyQt` | 5.15.x | Toda a interface gráfica |
| `qgis.core` | QGIS 3.38+ | Canvas, camadas, CRS, geometrias |
| `qgis.gui` | QGIS 3.38+ | `QgsMapCanvas`, `QgsMapToolIdentify` |
| `psycopg2` | 2.9.x | Conexão PostgreSQL |

### 7.2 Opcionais (instalar se ausentes)

| Biblioteca | Versão mínima | Instalação | Uso |
|---|---|---|---|
| `matplotlib` | 3.6+ | `pip install matplotlib` | Gráficos em `janela_graficos_medidor.py` |
| `openpyxl` | 3.1+ | `pip install openpyxl` | Exportação XLSX formatada |
| `pandas` | 1.5+ | `pip install pandas` | Exportação alternativa (fallback para CSV se ausente) |
| `oracledb` | 1.3+ | Instalado automaticamente pelo `ETLWorker` | Conexão Oracle DW (Etapa 2) |
| `cx_Oracle` | 8.x | Opcional (alternativa ao `oracledb`) | Conexão Oracle via cliente nativo |

> **Instalação no ambiente QGIS (Windows/OSGeo4W):**
> ```bat
> :: Abrir OSGeo4W Shell
> python -m pip install matplotlib openpyxl pandas
> ```
> O driver `oracledb` é instalado automaticamente quando necessário, sem exigir privilégios de administrador (flag `--user`). Se a instalação automática falhar, ver [Seção 15](#15-troubleshooting).

### 7.3 Flags de disponibilidade em runtime

O módulo `janela_graficos_medidor.py` verifica `matplotlib` e `pandas` com `try/import` no nível do módulo:

```python
# janela_graficos_medidor.py
try:
    from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
    MATPLOTLIB_DISPONIVEL = True
except ImportError:
    MATPLOTLIB_DISPONIVEL = False

try:
    import pandas as pd
    PANDAS_DISPONIVEL = True
except ImportError:
    PANDAS_DISPONIVEL = False
```

Se `matplotlib` estiver ausente, os gráficos não são renderizados. Se `pandas` estiver ausente, a exportação Excel cai para CSV.

---

## 8. Perfis de Acesso

O plugin implementa controle de acesso baseado no **nome do usuário PostgreSQL**, sem tabela de perfis adicional.

| Perfil | Usuário PostgreSQL | Restrições na UI |
|---|---|---|
| **Leitura** | `telemetria_ro` | `TelaInicial` com apenas 2 cards; aba ETL oculta em `JanelaGestaoDados`; botões de edição/exclusão desabilitados |
| **Escrita** | demais (ex.: `telemetria_rw`) | Acesso completo a todas as funcionalidades |

A verificação é feita comparando `self.usuario_logado != "telemetria_ro"` nos pontos de controle de UI de `main_plugin.py` e `janela_gestao_dados.py`.

---

## 9. Processamento Assíncrono (Threads Qt)

O plugin utiliza o padrão **Worker + QThread** do Qt para operações demoradas, evitando congelamento da interface do QGIS.

### Padrão implementado em todos os módulos

```python
self._thread = QThread(self)
self._worker = MeuWorker()
self._worker.moveToThread(self._thread)

self._thread.started.connect(self._worker.run)
self._worker.sinal_resultado.connect(self._on_resultado)
self._worker.sinal_erro.connect(self._on_erro)

self._thread.start()

# Encerramento seguro ao concluir:
self._thread.quit()
self._thread.wait()
```

### Threads existentes no plugin

| Módulo | Classe | Operação | Conexão DB |
|---|---|---|---|
| `verificacao_outorgado_thread.py` | `VerificacaoOutorgadoThread` | Consumo vs. outorgado mensal | Nova conexão (DSN da principal) |
| `calc_mes_thread.py` | `CalcMesThread` | Cálculo corrigido de totais diários | Compartilha conexão principal (SELECT) |
| `widget_atualizacao_base.py` | `ETLWorker` | ETL ArcGIS + Oracle → PostGIS | Conexões novas e isoladas por etapa |

> **Nota:** `CalcMesThread` compartilha a conexão principal da sessão. Em caso de problemas de estado concorrente, considerar abrir conexão dedicada a partir dos parâmetros DSN.

---

## 10. Integração com QGIS Canvas

| Operação | Módulo | Método |
|---|---|---|
| Exibição de medidores como camada vetorial pontual | `tela_cadastro_medidores.py` | `exibir_medidores_no_canvas()` |
| Pan + zoom para coordenadas do medidor | `janela_graficos_medidor.py`, `janela_monitoramento_detalhes.py` | `ver_no_mapa()` |
| Carregamento de camadas base (OSM, ESRI, Google, MapServer) | `janela_monitoramento.py` | métodos de inicialização de canvas |
| Captura de coordenadas por clique no mapa | `tela_cadastro_medidores.py` | `QgsMapToolIdentifyFeature` |
| Reprojeção de coordenadas (WGS84 ↔ SIRGAS2000) | múltiplos módulos | `QgsCoordinateTransform` |

As camadas criadas pelo plugin são adicionadas ao `QgsProject.instance()` e permanecem visíveis no painel de camadas do QGIS durante a sessão.

---

## 11. Integração com Serviços Externos

### 11.1 CNARH — API REST (consultas de cadastro)

Consultado em `tela_cadastro_operadores.py` e `tela_cadastro_medidores.py` para buscar dados de usuários de água pelo número CNARH. Requisição via `urllib.request` com contexto SSL sem verificação de hostname (infraestrutura interna da ANA).

### 11.2 ArcGIS MapServer SFI/ANA — Obrigatoriedade (público)

```
URL: https://portal1.snirh.gov.br/server/rest/services/SFI/
     Obrigatoriedade_Automonitoramento_DW_v5/MapServer/0
```

Utilizado pela Etapa 1 do ETL em `widget_atualizacao_base.py`. Paginação de 2.000 feições por requisição, pausa de 2 s entre páginas, retry com backoff exponencial (2 s, 4 s, 8 s).

### 11.3 ArcGIS REST Service — CAR Privado (autenticado)

Serviço privado da ANA para dados do Cadastro Ambiental Rural (CAR), protegidos por LGPD. Fluxo de token temporário (2 horas) com exibição de termo de responsabilidade ao usuário antes de carregar os dados.

### 11.4 Oracle DW — CNARH40 (ETL Etapa 2)

```
Host:          exacc-prd-scan.ana.gov.br
Porta:         1521
Service Name:  oradw.ana.gov.br
View:          CNARH40.MV_SFI_CNARH40
Usuário:       DW_RO  (somente leitura)
```

Acesso via `cx_Oracle` (preferencial, se instalado) ou `oracledb` modo thin (sem Oracle Instant Client). Instalação automática de `oracledb` via `pip --user` se nenhum driver estiver presente.

---

## 12. Validação de Dados e Detecção de Anomalias

### 12.1 Algoritmo de correção de consumo (leituras de 15 min)

Implementado em `janela_monitoramento_detalhes.py` (`preencher_grid_15min()`) e replicado em `calc_mes_thread.py` (`_calc_dia()`). Corrige o consumo acumulado de medidores de pulso com as seguintes regras:

| Anomalia | Critério de detecção | Correção aplicada |
|---|---|---|
| **Wrap-around** (salto negativo) | `delta < 0` entre leituras consecutivas | `vazao × duracao` como incremento; acumula `correcao_acumulada` |
| **Injeção espúria** (salto positivo absurdo) | `delta > vazao_nominal × duracao × FATOR_SEGURANCA` | Subtrai excesso; preserva incremento físico esperado |
| **Continuação pós-overflow** | `overflow_detectado == True` em leitura seguinte | Aplica correção acumulada antes de calcular delta |

**Constante:** `FATOR_SEGURANCA = 5.0` (500 % da capacidade nominal). Alternativa comentada: `10.0` para medidores de maior variabilidade.

### 12.2 Validação de campos nos formulários de cadastro

| Campo | Módulo | Mecanismo |
|---|---|---|
| E-mail | `tela_cadastro_operadores.py` | `QRegExpValidator` com regex RFC-like |
| CPF/CNPJ | `tela_cadastro_operadores.py` | Máscara de input + verificação de comprimento |
| Vazão nominal | `tela_cadastro_medidores.py`, `widget_medidores.py` | `QDoubleValidator` + `DialogoUnidadeVazao` |
| Potência | `tela_cadastro_medidores.py`, `widget_medidores.py` | `QDoubleValidator` + `DialogoUnidadePotencia` |
| Coordenadas | `tela_cadastro_medidores.py` | Verificação de intervalo válido para território brasileiro |

---

## 13. Exportação de Dados

### 13.1 Padrão de formatação XLSX (institucional)

Todos os relatórios Excel exportados pelo plugin utilizam `openpyxl` com o seguinte padrão visual:

| Elemento | Estilo |
|---|---|
| Linha de título | Merge de colunas, fonte `#175cc3` 13 pt bold |
| Cabeçalho de colunas | Fundo `#175cc3`, fonte branca 10 pt bold, alinhamento centralizado |
| Linhas alternadas | Fundo `#eaf2ff` nas linhas pares |
| Bordas | `thin` em todas as células de dados |
| Alinhamento | Centralizado com `wrap_text=True` |

Arquivos salvos na pasta **Downloads** do usuário do SO. Nome com timestamp: ex. `MEDIDORES_TODOS_20260301_143022.xlsx`.

### 13.2 Exportação de gráficos PNG

Gráficos Matplotlib exportados em 300 DPI via `fig.savefig()` em `janela_graficos_medidor.py`, também para a pasta Downloads.

### 13.3 Relatório TXT de 15 minutos

Relatório textual com dados intraday, estatísticas diárias consolidadas (vazão média, máxima, consumo total, duração, número de leituras) e rodapé de timestamp de geração. Formato compatível com template de e-mail institucional. Salvo em Downloads com nome baseado no rótulo do medidor e data.

---

## 14. Guia de Atualização e Manutenção

### 14.1 Atualizar o plugin (nova versão)

1. Feche o QGIS.
2. Localize o diretório do plugin:
   - **Windows:** `%APPDATA%\QGIS\QGIS3\profiles\default\python\plugins\durh_telemetria\`
   - **Linux:** `~/.local/share/QGIS/QGIS3/profiles/default/python/plugins/durh_telemetria\`
3. Substitua apenas os arquivos `.py` modificados. **Não substitua** `icon.png` e `splash.png` sem verificar as dimensões esperadas (splash: 450×320 px).
4. Atualize `metadata.txt`: campo `version` e novas entradas em `changelog`.
5. Reinicie o QGIS. O Plugin Manager reconhecerá a nova versão automaticamente.

### 14.2 Alterar a string de conexão PostgreSQL (ETL)

A conexão principal do plugin é aberta em `JanelaLogin` com parâmetros digitados pelo usuário. O host, banco e porta fixos do ETL estão em `widget_atualizacao_base.py`:

```python
# widget_atualizacao_base.py
PG_BASE = {
    'host':   "rds-webgis-dev.cjleec14qixz.sa-east-1.rds.amazonaws.com",
    'port':   5432,
    'dbname': "telemetria",
}
```

Editar apenas este dicionário para migrar o ETL para outro host.

### 14.3 Alterar credenciais do Oracle DW

```python
# widget_atualizacao_base.py
ORACLE_CONFIG = {
    'user':         'DW_RO',
    'password':     '...',
    'host':         'exacc-prd-scan.ana.gov.br',
    'port':         1521,
    'service_name': 'oradw.ana.gov.br',
}
```

> **Recomendação de segurança:** Mover estas credenciais para um arquivo de configuração externo (`.env` ou `config.ini`) não versionado, lido em runtime via `configparser` ou `python-dotenv`.

### 14.4 Alterar o URL do MapServer

```python
# widget_atualizacao_base.py
MAPSERVER_URL = (
    "https://portal1.snirh.gov.br/server/rest/services/SFI/"
    "Obrigatoriedade_Automonitoramento_DW_v5/MapServer/0"
)
```

Atualizar apenas esta constante se a camada for movida para nova URL ou versão do serviço.

### 14.5 Alterar o filtro de vencimento padrão da Etapa 2

O padrão (1º de janeiro do ano anterior) está em `WidgetAtualizacaoBase._confirmar_e_executar()` em `widget_atualizacao_base.py`:

```python
ano_anterior = QDate.currentDate().year() - 1
data_edit.setDate(QDate(ano_anterior, 1, 1))
```

### 14.6 Adicionar novo critério de busca em `WidgetMedidores`

1. Em `widget_medidores.py`, `initUI()`, adicionar o item ao `QComboBox`:
   ```python
   self.combo_criterio.addItems([..., "Novo Critério"])
   ```
2. No método `buscar_medidores()`, adicionar `elif` com a query SQL correspondente ao novo critério.

### 14.7 Adicionar nova coluna ao relatório XLSX

1. Adicionar o campo à lista `colunas` no método de exportação do módulo correspondente (`widget_operadores.py` ou `widget_medidores.py`).
2. Adicionar a largura (em caracteres) à lista `larguras`.
3. Atualizar a query SQL de origem se o campo não estiver sendo buscado.
4. O valor `n_cols` é calculado automaticamente como `len(colunas)`.

### 14.8 Adicionar nova aba à `JanelaGestaoDados`

1. Criar o novo widget como classe em seu próprio arquivo `.py` seguindo o padrão existente.
2. Em `janela_gestao_dados.py`, `initUI()`:
   ```python
   from .meu_novo_widget import MeuNovoWidget

   self.widget_novo = MeuNovoWidget(self.conn, self.usuario_logado)
   self.tabs.addTab(self.widget_novo, "Nome da Aba")
   ```
3. Adicionar `elif index == N:` em `ajustar_tamanho_aba()` com as dimensões adequadas para a nova aba.

### 14.9 Adicionar novo módulo ao plugin

1. Criar o arquivo `.py` no diretório raiz do plugin com nome em `snake_case` idêntico ao nome da classe.
2. Usar importações relativas para `ui_tema` e demais módulos do plugin:
   ```python
   from . import ui_tema
   from .outro_modulo import OutraClasse
   ```
3. Importar a nova classe no módulo que a instancia:
   ```python
   from .nome_arquivo import NomeClasse
   ```
4. Não é necessário registrar o novo módulo em nenhum arquivo de configuração central.

### 14.10 Atualizar versão mínima do QGIS

```ini
# metadata.txt
qgisMinimumVersion=3.38
```

### 14.11 Ciclo de testes antes de releases

| Cenário | Verificação esperada |
|---|---|
| Login com credenciais inválidas | Mensagem de erro específica por tipo de falha (autenticação, host, banco) |
| Login com usuário `telemetria_ro` | Aba ETL oculta; cards de cadastro ausentes na `TelaInicial` |
| ETL Etapa 1 com MapServer fora do ar | Mensagem de erro descritiva; sem dados parciais na tabela |
| ETL Etapa 1 com menos de 10.000 feições | Importação abortada com aviso; tabela preservada no estado anterior |
| Edição de medidor com unidade cv | Diálogo disparado; conversão correta (×0,7355) salva na tabela |
| Exportação XLSX com lista vazia | Mensagem de aviso; nenhum arquivo corrompido gerado |
| Cancelamento da `VerificacaoOutorgadoThread` | `conn.cancel()` enviado; thread encerrada sem exceção não tratada |
| Troca de aba para ETL com processo em andamento | `QMessageBox` informativo; processo não reiniciado |

---

## 15. Troubleshooting

### Plugin não aparece no QGIS após instalação

- Verificar se o diretório do plugin está no caminho correto e se `metadata.txt` está presente e bem formado;
- Confirmar que `classFactory` está definido em `__init__.py`;
- Verificar erros no painel **Plugins → Log de Mensagens** do QGIS.

### `ImportError: No module named 'matplotlib'`

```bat
:: OSGeo4W Shell
python -m pip install matplotlib
```
Reiniciar o QGIS após a instalação.

### `ImportError: No module named 'openpyxl'`

```bat
python -m pip install openpyxl
```

### Erro de conexão PostgreSQL: `could not connect to server`

- Verificar VPN institucional ativa (RDS na AWS requer acesso à rede interna);
- Confirmar host, porta e banco digitados em `JanelaLogin`;
- Testar via `psql` ou pgAdmin com as mesmas credenciais antes de abrir o plugin.

### ETL Etapa 1 falha com "Serviço ArcGIS MapServer indisponível"

- Verificar acesso manual à URL do MapServer no navegador;
- O plugin retenta 3× por página com backoff exponencial; falhas persistentes indicam instabilidade no portal SNIRH/ANA;
- Aguardar e tentar novamente — o `TRUNCATE` só ocorre após conexão bem-sucedida ao serviço, preservando os dados anteriores em caso de falha precoce.

### ETL Etapa 2 falha com "driver Oracle não encontrado"

O plugin tenta instalar `oracledb` automaticamente. Se falhar:

```bat
:: OSGeo4W Shell como Administrador
python -m pip install oracledb
```

Reiniciar o QGIS após a instalação.

### `AttributeError` em `CalcMesThread` ou `VerificacaoOutorgadoThread` após atualização de QGIS

Verificar se as assinaturas de `QgsCoordinateTransform`, `QgsVectorLayerUtils` ou de métodos `psycopg2` mudaram na versão instalada. O intervalo 3.38–3.44 é estável para todas as APIs utilizadas pelo plugin.

### Janela congelada durante carregamento de dados

Confirmar que a operação demorada está sendo executada em `QThread` dedicada (ver [Seção 9](#9-processamento-assíncrono-threads-qt)). Operações longas na thread principal bloqueiam o loop de eventos do Qt e congelam toda a interface do QGIS.

### `ModuleNotFoundError` ao importar módulo do plugin

Verificar se o arquivo `.py` do módulo está no diretório raiz do plugin e se a importação usa caminho relativo (`from . import ...`). Importações absolutas sem o prefixo `.` não funcionam dentro de pacotes QGIS.

---

## 16. Referências

| Documento / Recurso | Link |
|---|---|
| Resolução ANA n. 188/2024 | [https://www.gov.br/ana](https://www.gov.br/ana) |
| QGIS PyQGIS Developer Cookbook | [https://docs.qgis.org/3.34/en/docs/pyqgis_developer_cookbook/](https://docs.qgis.org/3.34/en/docs/pyqgis_developer_cookbook/) |
| QGIS API Documentation | [https://api.qgis.org/api/](https://api.qgis.org/api/) |
| PyQt5 Documentation | [https://www.riverbankcomputing.com/static/Docs/PyQt5/](https://www.riverbankcomputing.com/static/Docs/PyQt5/) |
| psycopg2 Documentation | [https://www.psycopg.org/docs/](https://www.psycopg.org/docs/) |
| oracledb Documentation | [https://python-oracledb.readthedocs.io/](https://python-oracledb.readthedocs.io/) |
| ArcGIS REST API Reference | [https://developers.arcgis.com/rest/](https://developers.arcgis.com/rest/) |
| openpyxl Documentation | [https://openpyxl.readthedocs.io/](https://openpyxl.readthedocs.io/) |
| Portal SNIRH / MapServer | `https://portal1.snirh.gov.br/server/rest/services/SFI/` |

---

> **Autor da documentação:** Equipe SFI/ANA  
> **Última atualização:** Março/2026  
> **Versão do plugin documentada:** 2.0
