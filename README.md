# GCP Data Platform

## Overview

`pt-br`

O propósito desse trabalho é demonstrar minha adaptabilidade para construir pipelines e serviços de dados em nuvem independente da stack. A estrutura foi pensada para escalar a nível organizacional, com infraestrutura em Terraform, arquitetura Medallion, separação clara entre ingestão, raw, trusted e refined, e uso de BigQuery para armazenamento analítico particionado.

`en`

The purpose of this work is to demonstrate my adaptability in building cloud-based data pipelines and services, independent of the stack. The structure is designed to scale at the organizational level, using Terraform infrastructure, Medallion architecture, clear separation between ingestion, raw, trusted and refined layers, and BigQuery as the analytical storage layer.

---

## What Data Exists

The platform currently ingests data from three source groups:

| Source | Type | Description | Landing Path | Raw Tables |
|---|---|---|---|---|
| Binance | Streaming WebSocket | Real-time crypto trades, currently focused on BTC/BRL and expandable to more symbols | `landing/binance/{symbol}_trades/` | `raw.btcbrl_trades` |
| BACEN Dados Abertos | Batch APIs | Public financial datasets from Banco Central, including Pix, SPI, institutions, credit rates and IFData | `landing/bacen/{entity}/` | `raw.bacen_*` |
| Tesouro Nacional | Batch API | Public auction data from Tesouro Nacional | `landing/tesouro/leiloes/` | `raw.tesouro_leiloes` |

---

## Data Layers

| Layer | Technology | Purpose |
|---|---|---|
| Landing | GCS JSONL | Stores the original API/WebSocket payloads exactly as received |
| Raw | BigQuery append-only tables | Stores raw records with minimal structure and metadata |
| Trusted | dbt incremental models | Applies typing, validation, deduplication and partitioning via MERGE |
| Refined | dbt views / tables | Business-ready models and analytical views |

---

## High-Level Architecture

~~~mermaid
flowchart LR
    BINANCE["Binance WebSocket"]
    BACEN["BACEN APIs"]
    TESOURO["Tesouro API"]

    LANDING["GCS Landing"]

    RAW["BigQuery Raw"]
    TRUSTED["BigQuery Trusted"]
    REFINED["dbt Refined"]

    BINANCE --> LANDING
    BACEN --> LANDING
    TESOURO --> LANDING

    LANDING --> RAW
    RAW --> TRUSTED
    TRUSTED --> REFINED
~~~

---

## Orchestration

No workflow orchestrator (Airflow, Composer). Each step is an independent, idempotent unit triggered by Cloud Scheduler. Order is enforced by staggered schedules with enough buffer between steps.

| Time (UTC) | Step | Component |
|---|---|---|
| Always on | Binance streaming → GCS landing | GCE VM `systemd` service |
| 02:00 | Binance GCS → BigQuery raw | Cloud Function |
| 02:30 | BACEN APIs → GCS landing (daily endpoints) | Cloud Function |
| 03:00 | BACEN APIs → GCS landing (monthly endpoints) | Cloud Function |
| 03:30 | BACEN APIs → GCS landing (quarterly endpoints) | Cloud Function |
| 04:00 | BACEN GCS → BigQuery raw | Cloud Function |
| 05:00 | Tesouro API → GCS landing | Cloud Function |
| 06:00 | Tesouro GCS → BigQuery raw | Cloud Function |
| 07:00 | dbt build — raw → trusted + refined | Cloud Run Job |

---

## BACEN Datasets

BACEN ingestion is organized by business domain.

| Domain | Examples | Frequency |
|---|---|---|
| Institutions | supervised entities, cooperatives, institutions in operation | Daily |
| Pix | Pix keys, transactions, fraud statistics, municipality-level Pix data | Monthly |
| SPI | Pix settlement and availability statistics | Daily |
| Credit Rates | institutional credit rates and PJ interest rate series | Daily / monthly |
| IFData | financial institution registration and quarterly metrics | Quarterly |

Each dataset lands as JSONL in GCS and is loaded into a dedicated BigQuery raw table.

Example:

~~~text
landing/bacen/pix_chaves/year=2026/month=05/day=08/pix_chaves.jsonl
↓
raw.bacen_pix_chaves
~~~

---

## Binance Data

Binance data is collected from a WebSocket stream and written to GCS in JSONL micro-batches.

Example landing path:

~~~text
landing/binance/btcbrl_trades/year=2026/month=05/day=08/hour=14/
~~~

The raw BigQuery table stores:

- original payload
- source file
- ingestion timestamp
- load date

The trusted layer extracts and types fields such as:

- trade id
- symbol
- price
- quantity
- trade time
- buyer maker flag

---

## Project Structure

~~~text
.
├── jobs/
│   ├── 0_landing/
│   │   ├── vm_binance_btcbrl/     # Binance WebSocket → GCS landing
│   │   ├── cf_tesouro_leiloes/    # Tesouro API → GCS landing
│   │   └── cf_bacen/              # BACEN APIs → GCS landing
│   │       ├── main.py
│   │       ├── clients/
│   │       ├── domains/
│   │       └── config/
│   │
│   ├── 1_raw/
│   │   ├── cf_binance_btcbrl/     # GCS landing → BigQuery raw
│   │   ├── cf_tesouro_leiloes/    # GCS landing → BigQuery raw
│   │   └── cf_bacen/              # GCS landing → BigQuery raw.bacen_*
│   │       ├── main.py
│   │       ├── loaders/
│   │       └── config/
│   │
│   ├── 2_trusted/
│   │   └── bq_binance_btcbrl/     # BigQuery scheduled query: raw → trusted
│   │
│   └── 3_refined/                 # owned by dbt (see dbt/ folder)
│
├── dbt/                           # trusted + refined layers
│
└── infra/
    └── terraform/
        ├── main.tf
        ├── variables.tf
        ├── outputs.tf
        ├── streamer_startup.sh.tpl
        └── environments/
            ├── dev.tfvars
            └── prod.tfvars
~~~

---

## What This Data Tells

### Brazilian Government Debt Market (Tesouro Leilões)

The Treasury holds weekly auctions to finance public debt. Four bond types are sold:

| Bond | Type | Avg rate (2019–2025) | Total issued |
|---|---|---|---|
| LFT | Floating (Selic) | 9.2% nominal | R$ 1.0 trillion |
| NTN-B | Inflation-linked (IPCA+) | 4.9% real | R$ 830 billion |
| LTN | Fixed rate, no coupon | 7.6% | R$ 715 billion |
| NTN-F | Fixed rate + semi-annual coupons | 8.8% | R$ 153 billion |

**Demand signal:** `cobertura_ratio = quantidade_aceita / oferta`. When it drops below 1.0, the market refused to buy all bonds offered — a fiscal stress signal. LTN auctions showed repeated coverage failures in late 2024 as fixed rates climbed from 9.9% (Jan) to 13.7% (Dec), while NTN-B stayed fully subscribed throughout, reflecting persistent inflation hedging demand.

### Real-Time Crypto (Binance)

BTC/BRL and BTC/USDT trades captured tick-by-tick via WebSocket. Useful for correlating Brazilian macro events (rate decisions, fiscal news) with crypto price action in the local market.

### Brazilian Financial System (BACEN)

16 datasets from Banco Central covering Pix transaction volumes, SPI settlement statistics, institution registries, and credit rates. Useful for tracking the growth of instant payments and the evolution of credit conditions across the Brazilian financial system.

---

## Design Principles

- Keep landing immutable and replayable.
- Keep raw append-only.
- Apply typing and deduplication only in trusted.
- Keep ingestion code separated by source/domain.
- Use BigQuery for analytical processing.
- Use Terraform for reproducible infrastructure.
- Use dbt for trusted and refined layers — incremental MERGE, dedup, data quality tests.

---

## Current Status

| Area | Status |
|---|---|
| Binance landing | Implemented |
| Binance raw | Implemented |
| Binance trusted | Implemented |
| BACEN landing | Implemented |
| BACEN raw | Implemented |
| Tesouro landing/raw/trusted | Implemented |
| dbt refined (Tesouro) | Implemented |
| dbt refined (BACEN, Binance) | Planned |