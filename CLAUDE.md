# BTCBRL Real-Time Data Platform — GCP

## Objetivo
Pipeline de streaming de dados financeiros em tempo real:
Binance WebSocket → Cloud Run Producer → Pub/Sub → Cloud Run Consumer → GCS (Raw) → ETL → BigQuery (Trusted)

## Projeto GCP
- **Project ID dev:** `gcp-fin-data`
- **Project ID prod:** `gcp-fin-data-prod`

---

## Arquitetura Medallion

```
Binance WebSocket
      ↓
cloud-run/producer/app.py  (Cloud Run Service)
      ↓
Pub/Sub Topic: btcbrl-trades
      ↓
cloud-run/consumer/app.py  (Cloud Run Service)
      ↓
GCS (Raw)
btcbrl/raw/YYYY/MM/DD/HH/trades_<ts>.jsonl
      ↓
cloud-run/pipelines/btcbrl_raw_trusted.py  (Cloud Run Job — hourly)
      ↓
BigQuery (Trusted)
trusted.binance_btc_trades
      ↓
BigQuery Views (Refined) — futuro
```

### Camadas
| Camada | Tecnologia | Recurso GCP |
|---|---|---|
| Raw | GCS | `gs://<bucket>/btcbrl/raw/YYYY/MM/DD/HH/` |
| Trusted | BigQuery | `trusted.binance_btc_trades` |
| Refined | BigQuery Views | `refined.*` (futuro) |

### Responsabilidades por componente
| Componente | Faz | Não faz |
|---|---|---|
| Producer | Binance WS → Pub/Sub | nenhuma transformação |
| Consumer | Pub/Sub → GCS raw (JSONL) | nenhuma transformação, não escreve no BQ |
| ETL Pipeline | GCS raw → BigQuery Trusted (MERGE) | não acessa Pub/Sub |

---

## Infraestrutura GCP (gerenciada via Terraform)

| Recurso | Nome (dev) | Status |
|---|---|---|
| Pub/Sub Topic | `btcbrl-trades` | ✓ criado |
| Pub/Sub Subscription | `btcbrl-trades-sub` | ✓ criado |
| GCS Bucket | `gcp-fin-data-bucket-dev` | ✓ criado |
| Artifact Registry | `cloud-run` | ⬜ criar via Terraform |
| BigQuery Dataset | `trusted` | ✓ criado (schema a atualizar) |
| BigQuery Dataset | `refined` | ✓ criado |
| BigQuery Table | `trusted.binance_btc_trades` | ⬜ recriar com schema atualizado |
| Cloud Run Service | `producer` | ⬜ deploy após Docker |
| Cloud Run Service | `consumer` | ⬜ deploy após Docker |
| Cloud Run Job | `btcbrl-raw-trusted` | ⬜ deploy após Docker |

### Schema: trusted.binance_btc_trades
| Campo | Tipo | Nota |
|---|---|---|
| trade_id | INTEGER REQUIRED | PK — chave do MERGE |
| event_type | STRING | |
| event_time | TIMESTAMP | ms → TIMESTAMP |
| symbol | STRING | |
| price | NUMERIC | string → NUMERIC (sem perda de precisão) |
| quantity | NUMERIC | string → NUMERIC |
| trade_time | TIMESTAMP | ms → TIMESTAMP |
| is_buyer_maker | BOOLEAN | |
| _file_source | STRING | URI GCS de origem |
| _insert_date | TIMESTAMP | timestamp de execução do ETL |
| _job | STRING | nome do job |

---

## Binance WebSocket

**URL:** `wss://stream.binance.com:9443/ws/btcbrl@trade`

**Payload raw:**
```json
{
  "e": "trade",
  "E": 1672515782136,
  "s": "BTCBRL",
  "t": 12345,
  "p": "394285.00",
  "q": "0.001",
  "T": 1672515782136,
  "m": true,
  "M": true
}
```
- Timestamps em milissegundos
- `t` é o trade_id único (chave de negócio para MERGE)
- `M` sempre `true` — ignorar
- Conexões válidas por 24h → producer precisa de reconnect automático

---

## Stack

- Python 3.11, asyncio, websockets
- google-cloud-pubsub, google-cloud-bigquery, google-cloud-storage
- python-dotenv (dev local)
- Docker, Cloud Run
- Terraform (infra)

---

## Estrutura do projeto

```
data-streaming-bigquery/          ← renomear para gcp-fin-data
├── cloud-run/
│   ├── producer/
│   │   ├── app.py                ✓ implementado e testado
│   │   ├── requirements.txt      ✓
│   │   └── Dockerfile            ✓
│   ├── consumer/
│   │   ├── app.py                ✓ implementado e testado (GCS only)
│   │   ├── requirements.txt      ✓
│   │   └── Dockerfile            ✓
│   └── pipelines/
│       ├── btcbrl_raw_trusted.py ✓ ETL raw → trusted (MERGE)
│       ├── requirements.txt      ✓
│       └── Dockerfile            ✓ (imagem compartilhada por todos os pipelines)
├── infra/
│   └── terraform/
│       ├── main.tf               ✓
│       ├── variables.tf          ✓
│       ├── outputs.tf            ✓
│       └── environments/
│           ├── dev.tfvars        ✓
│           └── prod.tfvars       ✓
├── CLAUDE.md
├── .env.dev                      (não commitar)
└── .gitignore                    ✓
```

> **Nota:** O diretório raiz deve ser renomeado para `gcp-fin-data` para alinhar com o Project ID GCP.

---

## Variáveis de ambiente (.env.dev)

```
GCP_PROJECT_ID=gcp-fin-data
PUBSUB_TOPIC_ID=btcbrl-trades
PUBSUB_SUBSCRIPTION_ID=btcbrl-trades-sub
GCS_BUCKET=gcp-fin-data-bucket-dev
GCS_RAW_PREFIX=btcbrl/raw
BIGQUERY_DATASET_ID=trusted
BIGQUERY_TABLE_ID=binance_btc_trades
# TARGET_HOUR=2026-05-07T23:00:00  # opcional — backfill de hora específica
```

---

## Terraform

Toda infra é gerenciada via Terraform. Recursos existentes criados manualmente precisam ser importados:

```bash
cd infra/terraform
terraform init
terraform apply -var-file=environments/dev.tfvars

# Importar recursos já existentes (executar uma vez)
terraform import -var-file=environments/dev.tfvars \
  google_pubsub_topic.btcbrl_trades projects/gcp-fin-data/topics/btcbrl-trades

terraform import -var-file=environments/dev.tfvars \
  google_storage_bucket.raw gcp-fin-data-bucket-dev

terraform import -var-file=environments/dev.tfvars \
  google_bigquery_dataset.trusted gcp-fin-data/trusted
```

**Nota sobre Cloud Run:** imagens Docker precisam existir no Artifact Registry antes do `apply` com Cloud Run. Ordem:
1. `terraform apply` sem recursos Cloud Run (comentar temporariamente)
2. Build + push das imagens
3. `terraform apply` completo

---

## Status atual

| Componente | Status |
|---|---|
| Producer (`cloud-run/producer/app.py`) | ✓ implementado, testado localmente |
| Consumer (`cloud-run/consumer/app.py`) | ✓ implementado, testado localmente (GCS only) |
| ETL raw → trusted (`cloud-run/pipelines/btcbrl_raw_trusted.py`) | ✓ implementado |
| Terraform (`infra/terraform/`) | ✓ escrito — pendente apply + import |
| BigQuery trusted schema atualizado | ⬜ recriar via Terraform |
| Docker build + Artifact Registry | ⬜ próximo passo |
| Cloud Run deploy | ⬜ após Docker |
| Cloud Scheduler (ETL horário) | ⬜ após Cloud Run Job |
| README.md + technical_document.md | ✓ atualizados |

---

## Regras do projeto
- Sem overengineering: sem CI/CD por enquanto
- Logs em stdout (Cloud Run captura automaticamente)
- Um arquivo por serviço — sem módulos extras
- Consumer: responsabilidade única — Pub/Sub → GCS, sem transformação, sem BQ
- ETL Pipeline: única porta de entrada para BigQuery Trusted
- Credenciais via ADC local / Workload Identity no Cloud Run
- **Repositório público:** sempre adicionar ao `.gitignore` qualquer arquivo que exponha credenciais
- Imagem Docker de pipelines é compartilhada — um Dockerfile, múltiplos scripts `.py`

---

## Convenções Python

### Declaração de funções

Toda função deve ter:
- Anotação de tipo em todos os parâmetros
- Anotação de retorno sempre presente (incluindo `-> None`)
- Docstring: uma linha de resumo + `:param name:` por parâmetro + `:return:`
- Usar `str | None` para tipos opcionais — não `Optional[str]`

```python
def gcs_prefix_for_hour(hour: datetime, raw_prefix: str) -> str:
    """Return the GCS object prefix for the given UTC hour.

    :param hour: Target UTC hour (minute/second/microsecond ignored).
    :param raw_prefix: Base GCS prefix, e.g. "btcbrl/raw".
    :return: Full prefix string ending with "/".
    """
    return f"{raw_prefix}/{hour.year}/{hour.month:02d}/{hour.day:02d}/{hour.hour:02d}/"
```

### Logger

Configurar uma vez, após `load_dotenv()`. Nunca usar `print()` para mensagens operacionais.

```python
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
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
- **Parâmetros explícitos** — funções não acessam globais internamente; constantes são lidas em `main()` e passadas para baixo

### Tratamento de erros

```python
try:
    # lógica
except Exception as e:
    logger.error("Operation failed | target=%s | error=%s", target, str(e))
    raise
```
