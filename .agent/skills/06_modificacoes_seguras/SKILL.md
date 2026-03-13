---
name: modificacoes-seguras
description: |
  Guia de modificações seguras no plugin e referência de arquivos.
  Usar quando: adicionar campo em formulário, criar nova aba no
  QTabWidget, adicionar nova exportação XLSX, consultar qual arquivo
  contém qual classe, entender erros comuns (RuntimeError, closure,
  TRUNCATE, UnicodeDecodeError, TypeError unpack, freeze UI, token
  ArcGIS), refatorar código existente sem quebrar fluxo atual.
---

# Guia de Modificações Seguras

## Adicionar campo em formulário de edição

1. Adicionar widget (`QLineEdit`, `QComboBox`, etc.) em `initUI()` da classe alvo.
2. Preencher no slot de seleção (ex: `_preencher_campos_medidor`).
3. Incluir na query SQL de SELECT e no UPDATE.
4. Se campo de unidade física: conectar `editingFinished` + criar/reutilizar `DialogoUnidade*`.

## Adicionar aba no QTabWidget de JanelaGestaoDados

1. Criar `widget_nova_aba.py` com classe `WidgetNovaAba(QWidget)`.
2. Importar em `janela_gestao_dados.py`.
3. `self.tabs.addTab(widget, "Rótulo")`.
4. Se a aba contiver gatilho por seleção: conectar `currentChanged` no `__init__`, passando `parent_tabs`.
5. Ajustar `ajustar_tamanho_aba()` se necessário.
6. **Nunca** remover ou reordenar abas existentes sem verificar `_on_tab_changed`.

## Adicionar nova exportação XLSX

Seguir template em SKILL `gui-pyqt5-padroes` (seção "Exportação XLSX").
Checklist:
- [ ] Guard `if not OPENPYXL_DISPONIVEL: return`
- [ ] Sanitizar filename com `unicodedata.normalize`
- [ ] Título azul `#175cc3`, cabeçalho branco sobre azul, alternância `#eaf2ff`
- [ ] `freeze_panes = 'A5'`
- [ ] Salvar em `~/Downloads/`

## Adicionar nova camada QGIS

- Camadas XYZ/WMS: `QgsRasterLayer(url, nome, "wms")` em `JanelaMonitoramento`.
- MapServer privado (CAR): usar `QgsAuthMethodConfig` com `method="EsriToken"`.
- Token ArcGIS Enterprise: obter com `client='referer'` + `Referer` header correto.
- Nunca embutir token em string de URL — usar autenticação via `QgsAuthMethodConfig`.

## Erros comuns e soluções

| Erro | Causa | Solução |
|---|---|---|
| `RuntimeError: wrapped C++ object deleted` | `deleteLater()` com referência Python ativa | `try/except RuntimeError` |
| Valores errados em closure de loop | Helper function definida dentro do loop | Definir fora do loop |
| Filename com `?` ou caractere errado | Acento não tratado | `unicodedata.normalize('NFKD', ...)` |
| `TypeError: cannot unpack non-sequence bool` | Método ETL retornou `bool` em vez de `(bool, int)` | Garantir tuple `(ok, n)` em todas as branches |
| Freeze de UI em query longa | Query executada na thread principal | Mover para `QThread` / `CalcMesThread` |
| `psycopg2.errors.InsufficientPrivilege` em TRUNCATE | Role não é owner da tabela | Fallback `DELETE FROM` |
| Token ArcGIS Enterprise inválido / 401 | `client='referer'` ausente ou Referer errado | `QgsAuthMethodConfig` com `EsriToken` |
| `UnicodeDecodeError` ao ler Oracle | Banco `WE8MSWIN1252`, driver em modo thick | Manter modo THIN; usar `UTL_RAW.CAST_TO_RAW` |
| `QComboBox` retorna valor errado | Uso de `currentText()` em vez de `itemText` | `itemText(currentIndex())` para valor confirmado |

## Referência de arquivos

| Arquivo | Classe principal | Responsabilidade |
|---|---|---|
| `__init__.py` | `Cadastro`, `SplashScreen` | Registro QGIS, splash, fluxo de login |
| `main_plugin.py` | `JanelaLogin`, `TelaInicial` | Auth PostgreSQL, menu principal com cards |
| `ui_tema.py` | `StyleConfig`, `CardButton` | Design system (cores, CSS, ícones SVG) |
| `janela_gestao_dados.py` | `JanelaGestaoDados` | Hub de abas (Dashboard, Operadores, Medidores, ETL) |
| `widget_dashboard.py` | `WidgetDashboard` | KPIs, gauges, gráfico por sistema hídrico |
| `widget_operadores.py` | `WidgetOperadores` | CRUD operadores + exportação XLSX |
| `widget_medidores.py` | `WidgetMedidores` | CRUD medidores, conversão de unidades, exportação XLSX |
| `widget_atualizacao_base.py` | `ETLWorker`, `WidgetAtualizacaoBase` | ETL ArcGIS + Oracle → PostGIS |
| `janela_monitoramento.py` | `JanelaMonitoramento` | Busca medidores, mapa, verificação outorgado |
| `janela_monitoramento_detalhes.py` | `JanelaMonitoramentoDetalhes` | Grid 15min, correção de anomalias |
| `janela_graficos_medidor.py` | `JanelaGraficosMedidor` | Gráficos de telemetria (PNG 300dpi) |
| `tela_cadastro_operadores.py` | `TelaCadastroOperadores` | Cadastro novo operador |
| `tela_cadastro_medidores.py` | `TelaCadastroMedidores` | Cadastro novo medidor |
| `calc_mes_thread.py` | `CalcMesThread` | Cálculo corrigido de consumo mensal |
| `verificacao_outorgado_thread.py` | `VerificacaoOutorgadoThread` | Comparação consumo vs outorgado |
| `dialogo_unidade_vazao.py` | `DialogoUnidadeVazao` | Confirmação/conversão de unidade de vazão |
| `dialogo_unidade_potencia.py` | `DialogoUnidadePotencia` | Confirmação/conversão de unidade de potência |
| `dialogo_reativacao.py` | `DialogReativacao` | Reativação em lote de medidores desativados |
