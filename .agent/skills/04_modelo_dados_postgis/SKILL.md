---
name: modelo-dados-postgis
description: |
  Modelo de dados PostGIS e domínio hidrológico do projeto.
  Usar quando: escrever queries SQL, referenciar tabelas/views/colunas,
  entender relacionamentos entre interferência/medidor/operador,
  colunas de outorga, volumes outorgados por mês, joins espaciais
  com BAF/UAM, glossário de termos hídricos (outorga, interferência,
  CNARH, UAM, BAF, UGRH, wrap-around, injeção espúria, automonitoramento,
  DURH, SFI, ANA, Resolução 188/2024), algoritmo de correção de anomalias.
---

# Modelo de Dados — PostgreSQL/PostGIS

Banco: `telemetria` | Host RDS sa-east-1 | Schema: `public`

## Tabelas centrais

### tb_operador_telemetria
PK: `id` (serial)
Campos principais: `nome_operador`, `email`, `cpf_cnpj`, `telefone`, `data_cadastro`
Operador RHODIA: excluir de análises de consumo/excedência.

### tb_intervencao
PK: `id` (serial)
FK: `id_operador → tb_operador_telemetria.id`
Campos principais: `rotulo`, `codigo_interferencia`, `data_cadastro`, `vazao_nominal`
Medidor inativo: `rotulo` termina em `#` — desativação lógica, dados históricos preservados.
Medidor de teste: `rotulo LIKE '%_teste'` — excluir de cálculos de produção.

### tb_telemetria_intervencao
Leituras de 15 minutos por medidor.
PK composta: `(id_intervencao, data_hora_leitura)`
Campos: `acumulado`, `vazao_instantanea`, `data_hora_leitura`
Fonte para `CalcMesThread` — correção de wrap-around e injeção espúria.

### tb_telemetria_intervencao_diaria
Consumo diário consolidado (resultado do `CalcMesThread`).
PK composta: `(id_intervencao, data_leitura, codigo_interferencia)`
Campo principal: `consumo_diario` (m³)
Fonte primária para todas as análises de consumo e excedência.

### tb_mapserver_obrigatoriedade
PK: `codigo_interferencia` (integer)
Alimentada pelo ETL Etapa 1 (ArcGIS MapServer SNIRH).
Campos: `numero_cadastro`, `nome_empreendimento`, `dr_max_telem`, `dr_fim_telem`,
`dr_vencimento_outorga`, `vazao_media_m3_h`, `longitude`, `latitude`,
`bafcd`, `bafnm`, `cdautomonit`, `nmautomonit`, `cdugrh`, `nmugrh`

### tb_mv_sfi_cnarh40
PK: `codigo_interferencia` (integer)
Alimentada pelo ETL Etapa 2 (Oracle DW CNARH40).
Campos de volume outorgado: `vol_jan`, `vol_fev`, ..., `vol_dez` (m³ mensais)
Contém `numero_cadastro` (CNARH), dados de outorga e tipo de interferência.

## Tabelas de referência espacial

| Tabela | CRS | Join produz |
|---|---|---|
| `ft_sishidrico_buffer` | EPSG:4674 (SIRGAS 2000) | `bafcd`, `bafnm` |
| `ft_uam_buffer` | EPSG:4674 | `cdautomonit`, `nmautomonit`, `cdugrh`, `nmugrh` |

Join via `ST_Intersects(geom_interferencia, geom_buffer)`.
Ponto de interferência: `ST_SetSRID(ST_MakePoint(longitude, latitude), 4674)`.

## Views

### view_volume_outorgado
Volume mensal autorizado por interferência.
Colunas: `codigo_interferencia`, `vol_jan`, `vol_fev`, `vol_mar`, `vol_abr`,
`vol_mai`, `vol_jun`, `vol_jul`, `vol_ago`, `vol_set`, `vol_out`, `vol_nov`, `vol_dez`
Todas em m³. Gerada por CASE sobre `tb_mv_sfi_cnarh40`.

## Roles PostgreSQL

| Role | Permissão |
|---|---|
| `telemetria_ro` | SELECT |
| `telemetria_rw` | SELECT + DML |
| `usr_telemetria` | SELECT |
| `iusr_coged_ro` | ALL |
| `postgres` | Superuser / DDL |

## Relacionamentos chave

```
tb_operador_telemetria.id
    ↑ FK
tb_intervencao.id_operador
tb_intervencao.codigo_interferencia
    ↔ (mesmo código)
tb_mapserver_obrigatoriedade.codigo_interferencia
tb_mv_sfi_cnarh40.codigo_interferencia
tb_telemetria_intervencao_diaria.codigo_interferencia

tb_intervencao.id
    ↑ FK
tb_telemetria_intervencao.id_intervencao
tb_telemetria_intervencao_diaria.id_intervencao
```

## Glossário hidrológico

| Termo | Definição |
|---|---|
| **Outorga** | Autorização ANA para uso de recursos hídricos; tem prazo `dr_vencimento_outorga` |
| **Interferência** | Ponto de captação/lançamento; chave `codigo_interferencia` (INT_CD no CNARH) |
| **CNARH** | Cadastro Nacional de Usuários; `numero_cadastro` = `INT_NU_CNARH` |
| **Hidrômetro / Medidor** | Equipamento de telemetria; registrado em `tb_intervencao` |
| **Rótulo** | ID textual do medidor; sufixo `#` = inativo |
| **Automonitoramento** | Obrigação do outorgado de reportar consumo; Res. ANA 188/2024 |
| **UAM** | Unidade de Automonitoramento (`cdautomonit` / `nmautomonit`) |
| **BAF** | Bacia Afluente / Sistema Hídrico (`bafcd` / `bafnm`) |
| **UGRH** | Unidade de Gerenciamento de Recursos Hídricos (`cdugrh` / `nmugrh`) |
| **Wrap-around** | Overflow do contador — acumulado cai a zero e reinicia |
| **Injeção espúria** | Salto positivo absurdo — descartado, substituído por `vazao × duração` |
| **FATOR_SEGURANCA** | Constante `5.0` (500% da capacidade nominal) — limiar para spurious |
| **Vol. outorgado** | Volume mensal autorizado (`view_volume_outorgado`, colunas `vol_jan`…`vol_dez`) |
| **Excedência** | `consumo_real > vol_outorgado` no mês |
| **DURH** | Diretoria de Uso e Regulação de Recursos Hídricos (ANA) |
| **SFI** | Superintendência de Fiscalização (ANA) |

## Algoritmo de correção de anomalias (CalcMesThread)

```
Para cada dia do mês, para cada medidor:
  leituras = SELECT ordenado de tb_telemetria_intervencao

  Para cada par (anterior, atual):
    delta = acumulado_atual − acumulado_anterior

    if delta < 0:                    # wrap-around
        incremento = vazao × duracao
        correcao_acumulada += abs(delta) + incremento

    elif delta > vn × dur × 5:       # injeção espúria (FATOR_SEGURANCA)
        incremento = vn × duracao

    elif overflow_detectado:          # continuação pós-overflow
        incremento = delta + correcao_acumulada
        correcao_acumulada = 0

    else:
        incremento = delta

    total_dia += incremento

  INSERT/UPDATE tb_telemetria_intervencao_diaria
```
