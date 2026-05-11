# GCP Financial Data Platform

## Objetivo
Dois pipelines paralelos sobre a mesma stack medallion no GCP:

1. **Binance (streaming):** Binance WebSocket → Streamer VM → GCS Landing → Cloud Function → BigQuery Raw → Scheduled Query → BigQuery Trusted
2. **BACEN (batch):** BACEN Dados Abertos APIs → Cloud Function (16 endpoints, ThreadPoolExecutor) → GCS Landing → Cloud Function → BigQuery Raw (16 tabelas)

## Projeto GCP
- **Project ID dev:** `gcp-fin-data`
- **Project ID prod:** `gcp-fin-data-prod`

---

## Arquitetura Medallion

### Pipeline Binance (streaming)
```
Binance WebSocket (combined stream endpoint)
      ↓
jobs/0_landing/vm_binance_btcbrl/app.py  (GCE e2-micro VM — systemd service)
      ↓
GCS Landing: gs://us-gs-datalake/landing/binance/{symbol}_trades/year=YYYY/month=MM/day=DD/hour=HH/trades_<epoch_ms>.jsonl
      ↓
jobs/1_raw/cf_binance_btcbrl/  (Cloud Function 2nd gen — daily, 2-day lookback)
      ↓
BigQuery Raw — raw.btcbrl_trades
payload STRING + metadata columns, partitioned by _load_date (90-day expiry)
      ↓
dbt (Cloud Run Job — daily) — raw → trusted + refined (incremental MERGE, dedup, data quality tests)
      ↓
BigQuery Trusted — trusted.binance_btc_trades
typed, partitioned by DATE(trade_time)
```

### Pipeline BACEN (batch)
```
BACEN Dados Abertos (Olinda REST + SGS APIs)
— 5 Olinda services: SPI, IFDATA, taxaJuros, Pix_DadosAbertos, Instituicoes_em_funcionamento
— 1 SGS series: 20715 (taxa média PJ)
      ↓
jobs/0_landing/cf_bacen/  (Cloud Function 2nd gen — 3 schedulers: daily/monthly/quarterly)
  ThreadPoolExecutor(5) — 16 endpoints em paralelo, streaming JSONL page-by-page para GCS
      ↓
GCS Landing: gs://us-gs-datalake/landing/bacen/{entity}/year=YYYY/month=MM/day=DD/{entity}.jsonl
      ↓
jobs/1_raw/cf_bacen/  (Cloud Function 2nd gen — daily, 2-day lookback)
  16 BQ Load Jobs sequenciais, um por entidade
      ↓
BigQuery Raw — raw.bacen_* (16 tabelas)
payload STRING + metadata columns, partitioned by _load_date (90-day expiry)
```

### Camadas
| Camada | Tecnologia | Recursos |
|---|---|---|
| Landing | GCS JSONL | Binance: `landing/binance/{symbol}_trades/…/hour=HH/` · BACEN: `landing/bacen/{entity}/…/day=DD/` |
| Raw | BigQuery (append-only) | `raw.btcbrl_trades` · `raw.bacen_*` (16 tabelas) — particionadas por `_load_date` |
| Trusted | **dbt** (incremental MERGE) | `trusted.*` — owned by dbt, deployed via Cloud Run Job |
| Refined | **dbt** | `refined.*` — owned by dbt |

### Responsabilidades por componente
| Componente | Faz | Não faz |
|---|---|---|
| streamer | Binance WS → GCS landing (JSONL, buffer por símbolo) | nenhuma transformação |
| landing_to_raw (binance) | GCS landing → BQ raw (BQ Load Job) | nenhuma transformação, só append |
| bacen_landing | BACEN APIs → GCS landing (16 endpoints, ThreadPoolExecutor) | nenhuma transformação |
| bacen_raw | GCS landing → BQ raw.bacen_* (16 Load Jobs sequenciais) | nenhuma transformação, só append |
| dbt (Cloud Run Job) | BQ raw → BQ trusted + refined (incremental MERGE, dedup, tests) | não acessa GCS |

---

## Infraestrutura GCP (gerenciada via Terraform)

| Recurso | Nome (dev) | Status |
|---|---|---|
| GCS Bucket (landing) | `us-gs-datalake` | ✓ |
| Artifact Registry | `cloud-run` (us-central1) | ✓ |
| BigQuery Dataset | `raw` | ✓ Terraform owns |
| BigQuery Dataset | `trusted` | ✓ Terraform owns (container only — tables owned by dbt) |
| BigQuery Dataset | `refined` | ✓ Terraform owns (container only — tables owned by dbt) |
| BigQuery Table | `raw.btcbrl_trades` | ✓ partitioned by _load_date — Terraform owns |
| BigQuery Tables | `raw.bacen_*` (16) | ✓ partitioned by _load_date, for_each Terraform |
| BigQuery Tables | `trusted.*` | ✓ dbt owns (do NOT define in Terraform) |
| Cloud Function 2nd gen | `landing-to-raw` | ✓ |
| Cloud Function 2nd gen | `bacen-landing` | ✓ 512Mi, MAX_WORKERS=5 |
| Cloud Function 2nd gen | `bacen-raw` | ✓ |
| Cloud Run Job | `dbt-trusted` | ✓ runs `dbt build --profiles-dir .` |
| Cloud Scheduler | `landing-to-raw-daily` | ✓ 02:00 UTC |
| Cloud Scheduler | `bacen-landing-daily` | ✓ 02:30 UTC — `{"frequency":"daily"}` |
| Cloud Scheduler | `bacen-landing-monthly` | ✓ 03:00 UTC, 1st of month |
| Cloud Scheduler | `bacen-landing-quarterly` | ✓ 03:30 UTC, 1st of Jan/Apr/Jul/Oct |
| Cloud Scheduler | `bacen-raw-daily` | ✓ 04:00 UTC |
| Cloud Scheduler | `dbt-trusted-daily` | ✓ 07:00 UTC (after all raw loads complete) |
| GCE VM | `streamer` (e2-micro, southamerica-east1-a) | ✓ systemd |
| Service Account | `sa-streamer` | ✓ storage.objectAdmin on landing |
| Service Account | `sa-pipeline` | ✓ BQ dataEditor + jobUser + CF invoker + Cloud Run invoker |

---

## Schemas

### raw.btcbrl_trades (partitioned by _load_date, 90-day expiry)

| Campo | Tipo | Nota |
|---|---|---|
| payload | STRING | Binance JSON verbatim |
| _source_file | STRING | GCS blob path — rastreabilidade |
| _ingested_at | TIMESTAMP | Quando a CF rodou |
| _load_date | DATE | Data da carga — campo de partição |

### trusted.binance_btc_trades (partitioned by DATE(trade_time))
| Campo | Tipo | Nota |
|---|---|---|
| trade_id | INTEGER REQUIRED | PK — chave do MERGE |
| event_type | STRING | |
| event_time | TIMESTAMP | TIMESTAMP_MILLIS(E) |
| symbol | STRING | |
| price | NUMERIC | CAST(p AS NUMERIC) |
| quantity | NUMERIC | CAST(q AS NUMERIC) |
| trade_time | TIMESTAMP | TIMESTAMP_MILLIS(T) — campo de partição |
| is_buyer_maker | BOOLEAN | CAST(m AS BOOL) |
| _insert_date | TIMESTAMP | CURRENT_TIMESTAMP() na execução do scheduled query |

### raw.bacen_* — 16 tabelas (partitioned by _load_date, 90-day expiry)

Schema idêntico em todas as 16 tabelas:

| Campo | Tipo | Nota |
|---|---|---|
| payload | STRING | JSON record verbatim (um registro Olinda/SGS por linha) |
| _source_file | STRING | GCS blob path — rastreabilidade |
| _ingested_at | TIMESTAMP | Quando a CF bacen-raw rodou |
| _load_date | DATE | Data da carga — campo de partição |

**Tabelas:** `raw.bacen_spi_pix_liquidados_atual`, `raw.bacen_spi_pix_liquidados_intradia`, `raw.bacen_spi_disponibilidade`, `raw.bacen_spi_interrupcao`, `raw.bacen_taxa_juros_diaria`, `raw.bacen_instituicoes_bancos`, `raw.bacen_instituicoes_cooperativas`, `raw.bacen_instituicoes_consorcios`, `raw.bacen_instituicoes_sociedades`, `raw.bacen_pix_chaves`, `raw.bacen_pix_estatisticas_transacoes`, `raw.bacen_pix_fraudes`, `raw.bacen_pix_transacoes_municipio`, `raw.bacen_sgs_taxa_media_pj_total`, `raw.bacen_ifdata_lista_relatorio`, `raw.bacen_ifdata_cadastro`

Dropped (not in Terraform): `entidades_supervisionadas`, `cooperativas` (BcBase API returns HTTP 500 for all dates), `ifdata_valores` (requires 3 mandatory params — too complex for single-pass streaming design). See `docs/bacen_endpoints.md`.

### BACEN Endpoints por frequência

| Frequência | Domínio | gcs_name | Tipo de call |
|---|---|---|---|
| daily | Instituicoes v1 | `instituicoes_bancos`, `instituicoes_cooperativas`, `instituicoes_consorcios`, `instituicoes_sociedades` | entity set |
| daily | SPI v1 | `spi_pix_liquidados_atual`, `spi_pix_liquidados_intradia`, `spi_disponibilidade`, `spi_interrupcao` | entity set |
| daily | taxaJuros v2 | `taxa_juros_diaria` (`query_date_param=dataInicio`, api_date = run_date − 1 day) | entity set |
| monthly | Pix_DadosAbertos v1 | `pix_chaves` (`func_date_param=Data`), `pix_estatisticas_transacoes` (`Database`), `pix_fraudes` (`Database`), `pix_transacoes_municipio` (`DataBase`) | function import |
| monthly | SGS | `sgs_taxa_media_pj_total` (série 20715) | SGS full fetch |
| quarterly | IFDATA v1 | `ifdata_lista_relatorio` (no-param import), `ifdata_cadastro` (`func_date_param=AnoMes`, int YYYYMM) | function import |

`api_date_for(run_date, frequency)`: daily → `run_date − 1 day`; monthly → last day of prev month; quarterly → last day of prev quarter.

---

## Binance WebSocket

**URL (combined stream):** `wss://stream.binance.com:9443/stream?streams=btcbrl@trade/btcusdt@trade/...`

**Payload raw:**
```json
{
  "e": "trade", "E": 1672515782136, "s": "BTCBRL",
  "t": 12345, "p": "394285.00", "q": "0.001",
  "T": 1672515782136, "m": true, "M": true
}
```
- Timestamps em milissegundos
- `t` é o trade_id único (chave de negócio para MERGE)
- `M` sempre `true` — ignorar
- Combined stream: um único WebSocket para N símbolos; envelope `{"stream": "btcbrl@trade", "data": {...}}`
- Streamer faz reconnect automático em caso de queda

---

## Stack

- Python 3.11, asyncio, websockets, functions-framework
- google-cloud-storage, google-cloud-bigquery
- python-dotenv (dev local)
- Terraform (infra — datasets, raw tables, Cloud Functions, Cloud Run Job, schedulers)
- dbt 1.9.1 + dbt-bigquery (trusted + refined layers — incremental MERGE, data quality tests)
- Cloud Build (image build + push to Artifact Registry — no local Docker needed)

---

## Estrutura do projeto

```
data-streaming-bigquery/
├── jobs/
│   ├── 0_landing/
│   │   ├── vm_binance_btcbrl/       ✓ GCE VM: Binance WS → GCS landing
│   │   │   ├── app.py
│   │   │   └── requirements.txt
│   │   ├── cf_tesouro_leiloes/      ✓ Cloud Function: Tesouro API → GCS landing
│   │   │   ├── tesouro_leiloes.py
│   │   │   └── requirements.txt
│   │   └── cf_bacen/                ✓ Cloud Function: BACEN APIs → GCS landing (16 endpoints)
│   │       ├── main.py              #   entry point + ThreadPoolExecutor + api_date_for()
│   │       ├── clients/
│   │       │   ├── olinda.py        #   streaming paginator (blob.open + $top/$skip, safe-write)
│   │       │   └── sgs.py           #   SGS series fetcher
│   │       ├── domains/
│   │       │   ├── institutions.py  #   Instituicoes entity sets (daily, snapshot)
│   │       │   ├── spi.py           #   SPI stats (daily)
│   │       │   ├── credit_rates.py  #   taxaJuros (daily, query_date_param=dataInicio)
│   │       │   ├── pix.py           #   Pix_DadosAbertos function imports + SGS 20715 (monthly)
│   │       │   └── ifdata.py        #   IFDATA function imports (quarterly)
│   │       ├── config/
│   │       │   └── endpoints.py     #   OlindaEndpoint + SGSEndpoint dataclasses
│   │       └── requirements.txt
│   ├── 1_raw/
│   │   ├── cf_binance_btcbrl/       ✓ Cloud Function: GCS landing → BQ raw
│   │   │   ├── binance_btcbrl.py
│   │   │   └── requirements.txt
│   │   └── cf_bacen/                ✓ Cloud Function: GCS landing → BQ raw.bacen_* (16 tabelas)
│   │       ├── main.py              #   entry point, loop sequencial por RawTarget
│   │       ├── loaders/
│   │       │   └── gcs_to_bigquery.py  # download blob → wrap payload → BQ Load Job
│   │       ├── config/
│   │       │   └── raw_targets.py   #   16 RawTarget(gcs_name, bq_table) mappings
│   │       └── requirements.txt
│   ├── 2_trusted/                   ← owned by dbt (see dbt/ folder)
│   └── 3_refined/                   ← owned by dbt (see dbt/ folder)
├── dbt/
│   ├── models/
│   │   ├── sources.yml              ✓ raw.btcbrl_trades · raw.tesouro_leiloes · raw.bacen_* (5 modeled)
│   │   ├── trusted/                 ✓ 11 models — one .sql + .yml per model
│   │   │   ├── binance_btc_trades.sql / .yml      #   1 binance
│   │   │   ├── tesouro_benchmarks.sql / .yml
│   │   │   ├── tesouro_calendario.sql / .yml
│   │   │   ├── tesouro_comunicados.sql / .yml
│   │   │   ├── tesouro_dealers.sql / .yml
│   │   │   ├── tesouro_resultados.sql / .yml      #   5 tesouro
│   │   │   ├── bacen_meios_pagamento_mensal.sql / .yml
│   │   │   ├── bacen_expectativas_anuais.sql / .yml
│   │   │   ├── bacen_taxa_juros_mensal.sql / .yml
│   │   │   ├── bacen_ifdata_lista_relatorio.sql / .yml
│   │   │   └── bacen_ifdata_cadastro.sql / .yml   #   5 bacen
│   │   └── refined/                 ✓ 3 models (tesouro_leiloes_enriched, tesouro_leiloes_por_titulo, tesouro_leiloes_stress)
│   ├── macros/
│   │   └── generate_schema_name.sql ✓ writes to exact BQ dataset (not dbt_dev_trusted)
│   ├── packages.yml                 ✓ dbt_utils >=1.0.0
│   ├── dbt_project.yml              ✓
│   ├── Dockerfile                   ✓ dbt build --profiles-dir .
│   └── .dockerignore                ✓
├── infra/
│   └── terraform/
│       ├── main.tf                  ✓
│       ├── variables.tf             ✓
│       ├── outputs.tf               ✓
│       ├── streamer_startup.sh.tpl  ✓
│       └── environments/
│           ├── dev.tfvars           ✓
│           └── prod.tfvars
├── CLAUDE.md
├── .env                             (não commitar)
└── .gitignore                       ✓
```

---

## dbt

### Ownership model (CRITICAL)

| Camada | Schema/Dataset | Quem cria e owna as tabelas |
|---|---|---|
| Raw | `raw` | **Terraform** — define `google_bigquery_table` para cada tabela raw |
| Trusted | `trusted` | **dbt** — Terraform cria apenas o `google_bigquery_dataset`; nunca definir `google_bigquery_table` para trusted |
| Refined | `refined` | **dbt** — mesmo que trusted: Terraform cria só o dataset container |

**Regra:** Se você precisa adicionar/modificar uma tabela em `trusted` ou `refined`, faça em dbt. Nunca em Terraform.

### Modelos trusted — BACEN (5 modelos)

| Modelo | Fonte raw | Frequência | unique_key |
|---|---|---|---|
| `bacen_meios_pagamento_mensal` | `raw.bacen_meios_pagamento_mensal` | monthly | `data_referencia` |
| `bacen_expectativas_anuais` | `raw.bacen_expectativas_anuais` | daily | `data_coleta, indicador, data_referencia` |
| `bacen_taxa_juros_mensal` | `raw.bacen_taxa_juros_mensal` | monthly | `data_referencia, modalidade, cnpj8` |
| `bacen_ifdata_lista_relatorio` | `raw.bacen_ifdata_lista_relatorio` | quarterly | `num_relatorio` |
| `bacen_ifdata_cadastro` | `raw.bacen_ifdata_cadastro` | quarterly | `cod_inst, data_referencia, td` |

### Convenções dbt

- `dbt build` em vez de `dbt run && dbt test` — falha por model, não por etapa
- Todos os modelos trusted são `materialized: incremental`, `incremental_strategy: merge`
- **Nunca usar `--full-refresh` em `binance_btc_trades`** — é streaming contínuo; recriar a tabela apaga dados históricos irrecuperáveis
- `var("start_date")` em todos os modelos para reprocessamento pontual
- Macro `generate_schema_name` obrigatória — garante que dbt escreva em `trusted`/`refined` e não em `dbt_dev_trusted`
- NULL em chaves do MERGE causa duplicatas no BigQuery — sempre filtrar NULLs nas chaves do `unique_key`
- QUALIFY `PARTITION BY` deve cobrir exatamente os mesmos campos do `unique_key` — window mais estreita pode descartar rows válidas

### Deployment (Cloud Build — sem Docker local)

```bash
# Build e push da imagem
gcloud builds submit ./dbt \
  --tag us-central1-docker.pkg.dev/gcp-fin-data/cloud-run/dbt-trusted:latest \
  --project gcp-fin-data

# Deploy infra (Cloud Run Job + scheduler)
cd infra/terraform
terraform apply -var-file=environments/dev.tfvars
```

**Nunca** usar `docker build` ou `docker run` localmente — usar Cloud Build sempre.

### Reprocessamento

```bash
# Reprocessar um modelo específico com data
dbt run --select trusted.tesouro_benchmarks --vars '{"start_date": "2024-01-01"}'

# Full refresh (recriar tabela do zero)
dbt run --full-refresh --select trusted.tesouro_benchmarks
```

---

## Terraform

```bash
cd infra/terraform
terraform init
terraform apply -var-file=environments/dev.tfvars
```

**APIs GCP necessárias:** Compute Engine, Cloud Functions, Cloud Build, Cloud Run, Cloud Scheduler, BigQuery

---

## Variáveis de ambiente (streamer VM — /opt/streamer/.env)

```
GCP_PROJECT_ID=gcp-fin-data
GCS_BUCKET=us-gs-datalake
GCS_LANDING_PREFIX=landing/binance
BINANCE_STREAMS=btcbrl@trade
```

---

## Regras do projeto
- Sem overengineering: sem CI/CD por enquanto
- Logs em stdout (Cloud Functions / systemd journal)
- Um arquivo por serviço — sem módulos extras **exceto jobs multi-API** (ex: cf_bacen usa package com clients/, domains/, config/)
- Nomes de arquivos descritivos — nunca `main.py` para jobs simples; para packages multi-domínio `main.py` é o entry point do package
- landing: GCS → BQ raw, sem transformação — só append
- trusted: única porta de entrada para BigQuery Trusted — cast, dedup, MERGE
- Credenciais via ADC local / Workload Identity em produção
- **Repositório público:** sempre adicionar ao `.gitignore` qualquer arquivo que exponha credenciais

---

## Convenções de jobs

### Estrutura de pastas

```
jobs/
  {N}_{layer}/
    {tech}_{source}/
      {source}.py       # ou .sql para BQ jobs
      requirements.txt  # somente Python
```

### Regras de nomenclatura
- **Sem prefixo `job_`** — tudo dentro de `jobs/` já é um job; o prefixo é ruído
- **Número `N` na camada** garante ordem alfabética = ordem do fluxo de dados
- **Prefixo de tech** indica o ambiente de execução (ver tabela abaixo)
- **Nome do arquivo** = nome da pasta sem o prefixo de tech (ex: pasta `cf_tesouro_leiloes` → arquivo `tesouro_leiloes.py`)
- **Exceção**: serviços VM mantêm `app.py` — são serviços de longa duração, não scripts pontuais
- **Entry point** da Cloud Function = nome do arquivo sem extensão para jobs de arquivo único (ex: `tesouro_leiloes`); para packages multi-domínio, entry point é a função `bacen` em `main.py`

### Camadas

| Pasta | Camada | Movimento de dados |
|---|---|---|
| `0_landing` | Landing | Fonte externa → GCS JSONL |
| `1_raw` | Raw | GCS → BigQuery raw (payload STRING, sem transformação) |
| `2_trusted` | Trusted | BQ raw → BQ trusted — **owned by dbt**, não cria jobs aqui |
| `3_refined` | Refined | BQ trusted → BQ refined — **owned by dbt**, não cria jobs aqui |

### Prefixos de tecnologia

| Prefixo | Tecnologia |
|---|---|
| `cf` | Cloud Function (HTTP-triggered, curta duração) |
| `vm` | GCE VM (serviço systemd de longa duração) |
| `bq` | BigQuery Scheduled Query / Data Transfer Service |
| `composer` | Cloud Composer DAG |

### Padrão de path GCS landing

Todo job de landing escreve em GCS com particionamento Hive-style:

```
{prefix}/{entity}/year=YYYY/month=MM/day=DD/{filename}.jsonl
```

- **VM (streaming):** adiciona `hour=HH/` e usa epoch_ms no filename → `trades_{epoch_ms}.jsonl`
- **CF (batch diário):** `year=YYYY/month=MM/day=DD/` apenas, filename descritivo → `{endpoint}.jsonl`
- **Nunca usar** `ingested_at=YYYY-MM-DD` — usar partições Hive separadas por componente

---

## Convenções de nomenclatura de colunas (dbt)

| Tipo de campo | Prefixo | Exemplos |
|---|---|---|
| Identificador numérico | `num_` | `num_edital` |
| Campo data | `data_` | `data_leilao`, `data_vencimento`, `data_inicio_periodo` |
| Quantidade / volume de títulos | `qtd_` | `qtd_aceita`, `qtd_oferta` |
| Percentual / taxa / razão | `pct_` | `pct_taxa_media`, `pct_cobertura`, `pct_spread_taxa` |

---

## Convenções Python

### Declaração de funções

Toda função deve ter:
- Anotação de tipo em todos os parâmetros
- Anotação de retorno sempre presente (incluindo `-> None`)
- Docstring: uma linha de resumo + `:param name:` por parâmetro + `:return:`
- Usar `str | None` para tipos opcionais — não `Optional[str]`

### Logger

```python
import logging, sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)
```

Formato de log — separar contexto com pipe:
```python
logger.info("Loaded records | count=%s | prefix=%s", len(records), prefix)
logger.error("Merge failed | target=%s | error=%s", target_table, str(e))
```

### Design funcional

- **Atomicidade** — uma função faz exatamente uma tarefa
- **Idempotência** — rodar com os mesmos inputs produz o mesmo resultado (MERGE, não append)
- **Sem side effects** — toda escrita é decisão explícita do chamador
- **Parâmetros explícitos** — funções não acessam globais internamente; constantes são lidas no entry point e passadas para baixo
