# GCP Financial Data Platform

A portfolio project to practice GCP and dbt, explore available services, and show how a data engineer can transition between stacks. Two parallel pipelines ingest financial data from public sources — one streaming, one batch — into BigQuery for analytical use.

The structure is designed to scale at the organizational level (Terraform infrastructure, Medallion architecture, BigQuery partitioning) and at the data volume level. More modern approaches like a lakehouse architecture with open table formats (Iceberg) and Cloud Composer for orchestration were intentionally skipped to keep costs low.

> PT-BR version: [README.pt-br.md](README.pt-br.md)

---

## Architecture

```
Binance WebSocket ──┐
BACEN REST APIs  ───┼──► GCS Landing (JSONL) ──► BigQuery Raw ──► dbt Trusted ──► dbt Refined
Tesouro REST API ───┘
```

**Medallion layers:**

| Layer | Storage | Role |
|---|---|---|
| Landing | GCS JSONL | Raw payloads exactly as received, immutable and replayable |
| Raw | BigQuery (append-only) | Minimal structure: `payload STRING` + metadata columns |
| Trusted | BigQuery (dbt incremental MERGE) | Typed, deduplicated, partitioned, with column descriptions |
| Refined | BigQuery (dbt) | Business-ready models and analytical aggregations |

---

## Data Sources

| Source | Type | Content | Raw Tables |
|---|---|---|---|
| [Binance](https://binance.com) | WebSocket streaming | BTC/BRL and BTC/USDT real-time trades | `raw.btcbrl_trades` |
| [BACEN Dados Abertos](https://dadosabertos.bcb.gov.br) | Batch REST (Olinda) | Credit rates, IFData, payment methods, market expectations | `raw.bacen_*` (5 tables: taxa_juros_mensal, ifdata_cadastro, ifdata_lista_relatorio, meios_pagamento_mensal, expectativas_anuais) |
| [Tesouro Nacional](https://www.tesourotransparente.gov.br) | Batch REST | Government bond auctions (LFT, NTN-B, LTN, NTN-F) | `raw.tesouro_leiloes` |

---

## Ingestion Strategy

### Streaming — Binance

A long-running GCE VM (`e2-micro`) connects to the Binance WebSocket combined stream endpoint via `asyncio` + `websockets`. Trade events are pushed by Binance and buffered in memory per symbol. The buffer flushes to GCS as a JSONL micro-batch when it reaches **500 messages or 60 seconds**, whichever comes first — avoiding both data loss and excessive small files.

- **Landing:** JSONL files partitioned by hour (`year=YYYY/month=MM/day=DD/hour=HH/trades_{epoch_ms}.jsonl`)
- **Raw:** BigQuery table storing the original payload as `STRING` alongside metadata columns (`_source_file`, `_ingested_at`, `_load_date`), partitioned by date with 90-day expiry
- **Trusted:** Daily dbt run with a d-2 lookback. Uses incremental MERGE to guarantee no duplicate events, partitioned by trade date

### Batch — BACEN

A Cloud Function backed by a Python package parametrized for the Olinda REST API. Currently covers **5 endpoints** across 3 domains (credit rates, IFData, and payment expectations), running on monthly or quarterly schedules via `ThreadPoolExecutor`.

Each run performs a **full historical load** for simplicity — the landing is transient:

```
landing cleared → full API history fetched → JSONL written to landing
→ raw load job appends to BigQuery (with _insert_date for traceback)
→ landing cleared
```

- **Landing:** Transient staging area; receives the full dataset on each run
- **Raw:** Append-only BigQuery table; each run adds a new partition with today's `_insert_date`
- **Trusted:** MERGE + deduplication keeping only the record with the latest `_insert_date` per key

### Batch — Tesouro

A Cloud Function fetches **8 Tesouro API endpoints sequentially**: 7 full snapshots (benchmarks, comunicados, dealers, calendario, homologacao, portarias, editais) on every run, plus `resultados` iterated year-by-year from 2019. Daily runs fetch only the current year for `resultados`; a full-load trigger (`{"full_load": true}`) fetches all years from 2019.

- **Landing:** Persistent — files accumulate by ingestion date partition (unlike BACEN's transient staging)
- **Raw:** Append-only BigQuery table with payload + metadata columns
- **Trusted:** dbt incremental MERGE, same pattern as the other pipelines

---

## Infrastructure

All infrastructure is managed via **Terraform** (no manual console clicks).

| Service | Usage |
|---|---|
| GCE `e2-micro` VM | Binance WebSocket streamer — always-on systemd service |
| GCS Bucket | Landing zone for all JSONL files (Hive-partitioned paths) |
| Cloud Functions 2nd gen | Landing jobs (BACEN, Tesouro) + GCS-to-BigQuery raw loaders |
| Cloud Run Job | dbt build — raw → trusted + refined |
| Cloud Scheduler | Triggers each step on a fixed UTC schedule |
| BigQuery | Raw (append-only) + Trusted + Refined analytical layers |
| Artifact Registry | Docker image for the dbt Cloud Run Job |
| Service Accounts | Least-privilege: `sa-streamer` (GCS write), `sa-pipeline` (BQ + CF + CR) |

---

## Orchestration

No workflow orchestrator. Each step is an independent, idempotent unit triggered by Cloud Scheduler with staggered times that provide enough buffer between stages.

| UTC | Step |
|---|---|
| Always on | Binance WebSocket → GCS landing |
| 02:00 | Binance GCS → BigQuery raw |
| 02:30 | BACEN APIs → GCS landing (daily) |
| 03:00 | BACEN APIs → GCS landing (monthly) |
| 03:30 | BACEN APIs → GCS landing (quarterly) |
| 04:00 | BACEN GCS → BigQuery raw |
| 05:00 | Tesouro API → GCS landing |
| 06:00 | Tesouro GCS → BigQuery raw |
| 07:00 | dbt build — trusted + refined |

---

## Monitoring

Observability is handled entirely through **Cloud Logging**. Cloud Functions and the dbt Cloud Run Job write structured logs to stdout (captured automatically by GCP). The GCE VM logs via `systemd journal`, also forwarded to Cloud Logging.

No dedicated monitoring dashboard or alerting was configured — this is a portfolio project and the cost of Cloud Monitoring custom metrics was not justified.

---

## Design Decisions

### What was not used and why

| Tool | Reason not used |
|---|---|
| Apache Iceberg / Delta Lake | Open table formats add significant cost and complexity (Dataproc or Spark required for compaction/maintenance). Not justified for a portfolio at this scale. |
| Cloud Composer (Airflow) | Minimum ~$300/month for the smallest environment. Staggered Cloud Scheduler achieves the same dependency ordering for free. |
| Cloud Dataflow | Per-job pricing makes it expensive for low-volume batch. Cloud Functions cover all ingestion needs at a fraction of the cost. |
| CI/CD pipeline | Not implemented — infrastructure changes are applied manually via `terraform apply`. |

### What was intentionally kept simple

- Landing is immutable and replayable — raw payloads are never modified.
- Raw layer is append-only — no updates, no deletes.
- Typing, deduplication and MERGE happen only in trusted (dbt), so the boundary is explicit.
- Each Cloud Function is a single Python file (or a small package for multi-domain jobs like BACEN). No frameworks, no unnecessary abstraction.

---

## Project Structure

```
.
├── jobs/
│   ├── 0_landing/
│   │   ├── vm_binance_btcbrl/     # Binance WebSocket → GCS (asyncio, systemd)
│   │   ├── cf_bacen/              # BACEN APIs → GCS (ThreadPoolExecutor, 16 endpoints)
│   │   │   ├── main.py
│   │   │   ├── clients/           # olinda.py, sgs.py
│   │   │   ├── domains/           # institutions, spi, credit_rates, pix, ifdata
│   │   │   └── config/            # endpoint definitions
│   │   └── cf_tesouro_leiloes/    # Tesouro API → GCS
│   ├── 1_raw/
│   │   ├── cf_binance_btcbrl/     # GCS → raw.btcbrl_trades
│   │   ├── cf_bacen/              # GCS → raw.bacen_* (16 tables)
│   │   └── cf_tesouro_leiloes/    # GCS → raw.tesouro_leiloes
│   ├── 2_trusted/                 # owned by dbt
│   └── 3_refined/                 # owned by dbt
├── dbt/
│   ├── models/
│   │   ├── trusted/               # incremental MERGE models
│   │   └── refined/               # business-ready aggregations
│   └── macros/
├── infra/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       └── environments/
│           ├── dev.tfvars
│           └── prod.tfvars
└── CLAUDE.md
```

---

## Screenshots

### BigQuery — trusted table schema with column descriptions
![BigQuery trusted table schema](<imgs/Captura de tela 2026-05-10 172820.png>)

### Cloud Run Job — dbt-trusted execution history
![dbt-trusted Cloud Run Job](<imgs/Captura de tela 2026-05-10 173028.png>)

### Cloud Functions — deployed services
![Cloud Functions list](<imgs/Captura de tela 2026-05-10 173036.png>)

### Cloud Scheduler — pipeline jobs
![Cloud Scheduler jobs](<imgs/Captura de tela 2026-05-10 173226.png>)

### GCS — Hive-partitioned landing files (Binance trades)
![GCS landing bucket](<imgs/Captura de tela 2026-05-10 173315.png>)

---

## Status

| Area | Status |
|---|---|
| Binance landing + raw + trusted | Done |
| BACEN landing + raw | Done |
| BACEN trusted (dbt) | In progress |
| Tesouro landing + raw + trusted + refined | Done |
| BACEN / Binance refined | Not planned |
