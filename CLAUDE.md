# BTCBRL Real-Time Data Platform — GCP

## Objetivo
Pipeline de streaming de dados financeiros em tempo real:
Binance WebSocket → Streamer VM → GCS (Landing) → Cloud Function → BigQuery Raw → Scheduled Query → BigQuery Trusted

## Projeto GCP
- **Project ID dev:** `gcp-fin-data`
- **Project ID prod:** `gcp-fin-data-prod`

---

## Arquitetura Medallion

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
BigQuery Scheduled Query — raw → trusted (daily MERGE, dedup on trade_id)
      ↓
BigQuery Trusted — trusted.binance_btc_trades
typed, partitioned by DATE(trade_time)
      ↓
dbt (Refined) — futuro
```

### Camadas
| Camada | Tecnologia | Recurso |
|---|---|---|
| Landing | GCS JSONL | `gs://us-gs-datalake/landing/binance/{symbol}_trades/year=YYYY/month=MM/day=DD/hour=HH/` |
| Raw | BigQuery (append-only) | `raw.btcbrl_trades` |
| Trusted | BigQuery (typed, partitioned) | `trusted.binance_btc_trades` |
| Refined | dbt | `refined.*` (futuro) |

### Responsabilidades por componente
| Componente | Faz | Não faz |
|---|---|---|
| streamer | Binance WS → GCS landing (JSONL, buffer por símbolo) | nenhuma transformação |
| landing_to_raw | GCS landing → BQ raw (BQ Load Job) | nenhuma transformação, só append |
| raw_to_trusted | BQ raw → BQ trusted (MERGE + cast + dedup) | não acessa GCS |

---

## Infraestrutura GCP (gerenciada via Terraform)

| Recurso | Nome (dev) | Status |
|---|---|---|
| GCS Bucket (landing) | `us-gs-datalake` | ✓ |
| Artifact Registry | `cloud-run` (us-central1) | ✓ |
| BigQuery Dataset | `raw` | ✓ |
| BigQuery Dataset | `trusted` | ✓ |
| BigQuery Dataset | `refined` | ✓ |
| BigQuery Table | `raw.btcbrl_trades` | ✓ partitioned by _load_date |
| BigQuery Table | `trusted.binance_btc_trades` | ✓ partitioned by trade_time |
| Cloud Function 2nd gen | `landing-to-raw` | ✓ |
| Cloud Scheduler | `landing-to-raw-daily` | ✓ 02:00 UTC |
| BQ Scheduled Query | `raw-to-trusted-btcbrl` | ✓ daily MERGE |
| GCE VM | `streamer` (e2-micro, southamerica-east1-a) | ✓ systemd |
| Service Account | `sa-streamer` | ✓ storage.objectAdmin on landing |
| Service Account | `sa-pipeline` | ✓ BQ dataEditor + jobUser + CF invoker |

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
- Terraform (infra)
- dbt (futuro — Refined layer)

---

## Estrutura do projeto

```
data-streaming-bigquery/
├── jobs/
│   ├── 0_landing/
│   │   ├── vm_binance_btcbrl/       ✓ GCE VM: Binance WS → GCS landing
│   │   │   ├── app.py
│   │   │   └── requirements.txt
│   │   └── cf_tesouro_leiloes/      ✓ Cloud Function: Tesouro API → GCS landing
│   │       ├── tesouro_leiloes.py
│   │       └── requirements.txt
│   ├── 1_raw/
│   │   └── cf_binance_btcbrl/       ✓ Cloud Function: GCS landing → BQ raw
│   │       ├── binance_btcbrl.py
│   │       └── requirements.txt
│   ├── 2_trusted/
│   │   └── bq_binance_btcbrl/       ✓ BQ Scheduled Query: BQ raw → BQ trusted
│   │       └── binance_btcbrl.sql   (referência — SQL inline no Terraform)
│   └── 3_refined/                   ⬜ futuro (dbt)
├── dbt/
│   ├── models/
│   │   ├── trusted/                 ⬜ futuro
│   │   └── refined/                 ⬜ futuro
│   └── dbt_project.yml
├── infra/
│   └── terraform/
│       ├── main.tf                  ✓
│       ├── variables.tf             ✓
│       ├── outputs.tf               ✓
│       ├── streamer_startup.sh.tpl  ✓
│       └── environments/
│           ├── dev.tfvars           ✓
│           └── prod.tfvars
├── docs/
│   └── technical_document.md
├── CLAUDE.md
├── .env                             (não commitar)
└── .gitignore                       ✓
```

---

## Terraform

```bash
cd infra/terraform
terraform init
terraform apply -var-file=environments/dev.tfvars
```

**APIs GCP necessárias:** Compute Engine, Cloud Functions, Cloud Build, Cloud Scheduler, BigQuery Data Transfer

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
- Um arquivo por serviço — sem módulos extras
- Nomes de arquivos descritivos — nunca `main.py` para jobs
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
- **Entry point** da Cloud Function = nome do arquivo sem extensão (ex: `tesouro_leiloes`)

### Camadas

| Pasta | Camada | Movimento de dados |
|---|---|---|
| `0_landing` | Landing | Fonte externa → GCS JSONL |
| `1_raw` | Raw | GCS → BigQuery raw (payload STRING, sem transformação) |
| `2_trusted` | Trusted | BQ raw → BQ trusted (tipado, dedup, MERGE) |
| `3_refined` | Refined | BQ trusted → BQ refined (dbt) |

### Prefixos de tecnologia

| Prefixo | Tecnologia |
|---|---|
| `cf` | Cloud Function (HTTP-triggered, curta duração) |
| `vm` | GCE VM (serviço systemd de longa duração) |
| `bq` | BigQuery Scheduled Query / Data Transfer Service |
| `composer` | Cloud Composer DAG |

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
