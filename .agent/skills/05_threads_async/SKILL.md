---
name: threads-async
description: |
  Padrões de threads assíncronas no plugin. Usar quando: criar ou
  editar CalcMesThread, VerificacaoOutorgadoThread, ETLWorker,
  qualquer QThread ou QObject+moveToThread, cancelamento cooperativo,
  sinais de progresso, conexão PostgreSQL em thread secundária,
  evitar freeze de UI em queries longas, padrão QObject vs QThread.
---

# Threads Assíncronas — Padrões

## Regra fundamental

Usar **QObject + moveToThread()** — nunca herdar QThread e sobrescrever `run()`.
Nunca emitir sinais Qt de `threading.Thread` nativo do Python.

```python
# CORRETO
worker = MinhaClasse()          # herda QObject
thread = QThread(parent_widget) # parent evita memory leak
worker.moveToThread(thread)
thread.started.connect(worker.run)
worker.sinal_concluido.connect(slot_na_main_thread)
thread.start()

# Finalizar sempre com quit + wait
thread.quit()
thread.wait()

# INCORRETO
class MinhaClasse(QThread):
    def run(self):  # antipadrão — não usar
        ...
```

## CalcMesThread(QThread)

Responsabilidade: cálculo corrigido de consumo mensal por medidor.
Arquivo: `calc_mes_thread.py`

```python
# Sinais
progresso    = pyqtSignal(int, int, str)    # (atual, total, descricao)
dia_concluido= pyqtSignal(str, float, bool) # (data_str, consumo_m3, tem_anomalia)
finalizado   = pyqtSignal()
erro         = pyqtSignal(str)
```

Recebe no construtor: `conn_params` (dict), lista de IDs de medidores, `mes`, `ano`, `vazao_nominal`.
Emite `dia_concluido` após cada dia processado — nunca bloqueia UI.
Aplica algoritmo de correção de wrap-around e injeção espúria (ver SKILL modelo-dados-postgis).

## VerificacaoOutorgadoThread(QThread)

Responsabilidade: comparar consumo mensal real vs volume outorgado por interferência.
Arquivo: `verificacao_outorgado_thread.py`

```python
# Sinais
resultado_signal = pyqtSignal(list, str, int) # (lista_resultados, mes_ano, total)
erro_signal      = pyqtSignal(str)
progresso_signal = pyqtSignal(str)
```

Abre **conexão própria** — recebe `(usuario, senha)` no construtor.
Cancelamento cooperativo: `cancelar()` → `conn.cancel()` no PostgreSQL.
Filtros obrigatórios:
- Excluir `rotulo LIKE '%_teste'`
- Excluir operador RHODIA (`nome_operador ILIKE '%RHODIA%'`)

Fonte de dados:
- Consumo: `tb_telemetria_intervencao_diaria` agrupado por mês
- Outorgado: `view_volume_outorgado` (coluna `vol_jan`…`vol_dez` do mês corrente)

## ETLWorker(QObject)

Ver SKILL `etl-arcgis-oracle` para detalhes completos.
Sinais: `log_emitido(str)`, `concluido(bool, bool)`, `erro_fatal(str)`.
Retornos de métodos internos: sempre `(bool, int)` — nunca `bool` sozinho.

## Padrão de conexão em thread secundária

```python
def _get_pg(self):
    return psycopg2.connect(
        host   = PG_BASE['host'],
        port   = PG_BASE['port'],
        dbname = PG_BASE['dbname'],
        user   = self.pg_usuario,
        password = self.pg_senha,
    )
```

Cada thread abre e fecha sua própria conexão.
Nunca compartilhar `psycopg2.connection` entre threads.

## Checklist ao criar nova thread

- [ ] Herda `QObject` (worker) ou `QThread` diretamente (caso simples)
- [ ] Sinais declarados como atributos de classe (`pyqtSignal`)
- [ ] Conexão PG aberta dentro do método `run()`, fechada no `finally`
- [ ] `thread.quit()` + `thread.wait()` em todos os caminhos de saída
- [ ] Flag de cancelamento (`self._cancelar = False`) verificada no loop
- [ ] Parent do `QThread` é o widget dono — evita leak ao fechar janela
