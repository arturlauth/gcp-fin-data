# BTCBRL Real-Time Streaming Platform — GCP

## Objetivo
Pipeline de streaming de dados financeiros em tempo real:
Binance WebSocket → Cloud Run Producer → Pub/Sub → Cloud Run Consumer → GCS (Raw) + BigQuery (Trusted)

## Projeto GCP
- **Project ID:** `gcp-fin-data`

---

## Arquitetura Medallion

```
Binance WebSocket
      ↓
producer/app.py  (Cloud Run)
      ↓
Pub/Sub Topic: btcbrl-trades
      ↓
consumer/app.py  (Cloud Run)
     /                    \
GCS (Raw)              BigQuery (Trusted)
gcp-fin-data-bucket-dev   trusted.binance_btc_trades
btcbrl/raw/YYYY/MM/DD/HH/
trades_<ts>.jsonl
                              ↓
                    BigQuery Views (Refined)
                    dataset: refined
                    (próxima sessão)
```

### Camadas
| Camada | Tecnologia | Recurso GCP |
|---|---|---|
| Raw | GCS | `gs://gcp-fin-data-bucket-dev/btcbrl/raw/YYYY/MM/DD/HH/` |
| Trusted | BigQuery | `gcp-fin-data.trusted.binance_btc_trades` |
| Refined | BigQuery Views | `gcp-fin-data.refined.*` (futuro) |

---

## Infraestrutura GCP (já criada)

| Recurso | Nome | Status |
|---|---|---|
| Pub/Sub Topic | `btcbrl-trades` | ✓ criado |
| Pub/Sub Subscription | `btcbrl-trades-sub` | ✓ criado |
| GCS Bucket | `gcp-fin-data-bucket-dev` | ✓ criado |
| BigQuery Dataset | `trusted` | ✓ criado |
| BigQuery Dataset | `refined` | ✓ criado |
| BigQuery Table | `trusted.binance_btc_trades` | ✓ criado |

### Schema: trusted.binance_btc_trades
| Campo | Tipo |
|---|---|
| event_type | STRING |
| event_time | TIMESTAMP |
| symbol | STRING |
| trade_id | INTEGER |
| price | FLOAT |
| quantity | FLOAT |
| trade_time | TIMESTAMP |
| is_buyer_maker | BOOLEAN |
| ingestion_timestamp | TIMESTAMP |

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
- Conexões válidas por 24h → producer precisa de reconnect automático

---

## Stack

- Python 3.11, asyncio, websockets
- google-cloud-pubsub, google-cloud-bigquery, google-cloud-storage
- python-dotenv (dev local)
- Docker, Cloud Run

---

## Estrutura do projeto

```
data-streaming-bigquery/
├── producer/
│   ├── app.py           ✓ implementado e testado localmente
│   ├── requirements.txt ✓
│   └── Dockerfile       ✓
├── consumer/
│   ├── app.py           ← PRÓXIMA SESSÃO
│   ├── requirements.txt
│   └── Dockerfile
├── infra/
│   └── setup_commands.md
├── CLAUDE.md
├── .env                 ✓ (não commitar)
├── .gitignore           ✓
└── README.md
```

---

## Variáveis de ambiente (.env)

```
GCP_PROJECT_ID=gcp-fin-data
PUBSUB_TOPIC_ID=btcbrl-trades
PUBSUB_SUBSCRIPTION_ID=btcbrl-trades-sub
BIGQUERY_DATASET_ID=trusted
BIGQUERY_TABLE_ID=binance_btc_trades
GCS_BUCKET=gcp-fin-data-bucket-dev
GCS_RAW_PREFIX=btcbrl/raw
```

---

## Status atual

| Componente | Status |
|---|---|
| Producer (`producer/app.py`) | ✓ implementado, testado localmente |
| Infra GCP (Pub/Sub, GCS, BigQuery) | ✓ criada |
| Consumer (`consumer/app.py`) | ⬜ próxima sessão |
| Docker build + Artifact Registry | ⬜ após consumer |
| Cloud Run deploy | ⬜ após Docker |
| README.md | ⬜ ao final |

---

## Próxima sessão — Consumer

`consumer/app.py` deve:
1. Pull contínuo do Pub/Sub (`btcbrl-trades-sub`)
2. Para cada mensagem:
   - Transformar tipos (ms → TIMESTAMP, string → FLOAT/INT)
   - Streaming insert em `trusted.binance_btc_trades`
   - Ack no Pub/Sub
3. Micro-batching para GCS (buffer 500 msgs ou 60s):
   - Salvar payload raw (JSON original) em JSONL
   - Path: `gs://gcp-fin-data-bucket-dev/btcbrl/raw/YYYY/MM/DD/HH/trades_<ts>.jsonl`
4. Logs estruturados em stdout
5. Reconnect/retry em caso de erro

**Dependências consumer:**
```
google-cloud-pubsub==2.21.1
google-cloud-bigquery==3.25.0
google-cloud-storage==2.18.2
python-dotenv==1.0.1
```

---

## Regras do projeto
- Sem overengineering: sem Dataflow, sem Terraform, sem CI/CD por enquanto
- Logs em stdout (Cloud Run captura automaticamente)
- Um arquivo por serviço (producer/app.py, consumer/app.py) — sem módulos extras
- Credenciais via ADC local / Workload Identity no Cloud Run
- **Repositório público:** sempre adicionar ao `.gitignore` qualquer arquivo que exponha credenciais ou acesso (`.env`, service account keys, `*.json` de credenciais, etc.)
