---
name: arquitetura-plugin
description: |
  Arquitetura do plugin QGIS DURH Diária por Telemetria.
  Usar quando: editar __init__.py, main_plugin.py, JanelaLogin,
  TelaInicial, fluxo de navegação entre janelas, propagação de
  credenciais (conn/usuario/senha), perfis de acesso (telemetria_ro
  vs telemetria_rw), SplashScreen, classFactory, initGui, unload.
---

# Arquitetura do Plugin

## Registro no QGIS

`classFactory(iface)` → `Cadastro(iface)` em `__init__.py`.
`Cadastro` é a única classe que recebe `iface` diretamente.
Janelas filhas usam `from qgis.utils import iface` quando necessário.

```
Cadastro.initGui()        → QAction no menu "ANA" + toolbar
Cadastro.run_with_splash()→ SplashScreen 2,5s → QTimer(150ms) → _abrir_janela_login()
Cadastro.unload()         → remove action, fecha widget
```

Sem `iface.addDockWidget` — plugin abre como janela flutuante.

## Hierarquia de janelas

```
Cadastro
└── SplashScreen (QSplashScreen — fade-in/out via QTimer 50ms, opacity 0→1)
└── JanelaLogin (QDialog modal)
    └── TelaInicial (QWidget)
        ├── TelaCadastroOperadores  → hide(pai) + show(filho)
        ├── TelaCadastroMedidores   → hide(pai) + show(filho)
        ├── JanelaGestaoDados       → hide(pai) + show(filho)
        │   ├── WidgetDashboard        aba 0
        │   ├── WidgetOperadores       aba 1
        │   ├── WidgetMedidores        aba 2
        │   └── WidgetAtualizacaoBase  aba 3 (oculta para telemetria_ro)
        └── JanelaMonitoramento     → hide(pai) + show(filho)
            ├── JanelaGraficosMedidor       (minimiza JanelaMonitoramento)
            └── JanelaMonitoramentoDetalhes (minimiza JanelaMonitoramento)
```

`TelaInicial` nunca fecha — só `hide()`.
`_janelas_abertas: list` garante cleanup no `closeEvent`.

## Autenticação — JanelaLogin

```python
psycopg2.connect(
    host="rds-webgis-dev.cjleec14qixz.sa-east-1.rds.amazonaws.com",
    port=5432, dbname="telemetria", user=usuario, password=senha
)
QSettings("ANA", "DURH_Telemetria")  # auth/usuario, auth/senha, auth/lembrar
```

Senha salva no QSettings **somente após** autenticação bem-sucedida.
Falha de autenticação → remove senha salva do QSettings.
`lembrar_chk` (QCheckBox) controla persistência.

## Propagação de credenciais

```python
# TelaInicial → filhas (assinatura completa)
JanelaGestaoDados(parent, conn, usuario_logado, senha)
JanelaMonitoramento(parent, conn, usuario_logado, senha)
TelaCadastroMedidores(parent, conn, usuario_logado, senha)

# Sem senha
TelaCadastroOperadores(parent, conn)
WidgetMedidores(conexao, usuario)
WidgetOperadores(conexao, usuario)

# Com senha — necessária para abrir conexão dedicada no ETLWorker
WidgetAtualizacaoBase(conexao, usuario, senha, parent_tabs)
```

## Perfis de usuário

| Role | Permissão PG | UI |
|---|---|---|
| `telemetria_ro` | SELECT | TelaInicial 540×220, sem Operadores/Medidores, sem aba ETL |
| `telemetria_rw` | SELECT + DML | UI completa |
| `postgres` | DDL | UI completa + TRUNCATE permitido |

`TelaInicial.verificar_conexao()` executa `SELECT 1` antes de abrir filha.
Sessão expirada → fecha o plugin.

## Convenção de módulos

- Um `.py` por classe, nome snake_case idêntico à classe.
- Imports relativos: `from .modulo import Classe`.
- Fallback absoluto em testes: `try/except ImportError`.
