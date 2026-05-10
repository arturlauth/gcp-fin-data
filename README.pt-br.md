# GCP Financial Data Platform

Projeto de portfólio para praticar GCP e dbt, explorar os serviços disponíveis e mostrar como um engenheiro de dados pode transitar entre diferentes stacks. Dois pipelines paralelos ingerem dados financeiros de fontes públicas — um streaming, um batch — no BigQuery para uso analítico.

A estrutura é pensada para escalar a nível organizacional (infraestrutura Terraform, arquitetura Medallion, particionamento no BigQuery) e a nível de volume de dados. Abordagens mais modernas como arquitetura lakehouse com formatos abertos de tabela (Iceberg) e Cloud Composer para orquestração foram intencionalmente descartadas para manter os custos baixos.

> English version: [README.md](README.md)

---

## Arquitetura

```
Binance WebSocket ──┐
APIs REST BACEN  ───┼──► GCS Landing (JSONL) ──► BigQuery Raw ──► dbt Trusted ──► dbt Refined
API REST Tesouro ───┘
```

**Camadas Medallion:**

| Camada | Armazenamento | Função |
|---|---|---|
| Landing | GCS JSONL | Payloads originais exatamente como recebidos, imutáveis e reprocessáveis |
| Raw | BigQuery (append-only) | Estrutura mínima: `payload STRING` + colunas de metadados |
| Trusted | BigQuery (dbt incremental MERGE) | Tipado, deduplicado, particionado, com descrições de colunas |
| Refined | BigQuery (dbt) | Modelos prontos para análise e agregações de negócio |

---

## Fontes de Dados

| Fonte | Tipo | Conteúdo | Tabelas Raw |
|---|---|---|---|
| [Binance](https://binance.com) | Streaming WebSocket | Trades em tempo real BTC/BRL e BTC/USDT | `raw.btcbrl_trades` |
| [BACEN Dados Abertos](https://dadosabertos.bcb.gov.br) | Batch REST (Olinda) | Taxas de crédito, IFData, meios de pagamento, expectativas de mercado | `raw.bacen_*` (5 tabelas) |
| [Tesouro Nacional](https://www.tesourotransparente.gov.br) | Batch REST | Leilões de títulos públicos (LFT, NTN-B, LTN, NTN-F) | `raw.tesouro_leiloes` |

---

## Estratégia de Ingestão

### Streaming — Binance

Uma VM GCE (`e2-micro`) de longa duração conecta ao endpoint de stream combinado da Binance via `asyncio` + `websockets`. Os eventos de trade são enviados pela Binance e bufferizados em memória por símbolo. O buffer é descarregado no GCS como um micro-batch JSONL ao atingir **500 mensagens ou 60 segundos**, o que ocorrer primeiro — evitando perda de dados e excesso de arquivos pequenos.

- **Landing:** Arquivos JSONL particionados por hora (`year=YYYY/month=MM/day=DD/hour=HH/trades_{epoch_ms}.jsonl`)
- **Raw:** Tabela BigQuery armazenando o payload original como `STRING` + colunas de metadados (`_source_file`, `_ingested_at`, `_load_date`), particionada por data com expiração de 90 dias
- **Trusted:** Execução diária dbt com lookback d-2. Usa MERGE incremental para garantir ausência de eventos duplicados, particionado por data do trade

### Batch — BACEN

Uma Cloud Function apoiada por um pacote Python parametrizado para a API REST Olinda. Cobre atualmente **5 endpoints** em 3 domínios (taxas de crédito, IFData e meios de pagamento), rodando em schedules mensais ou trimestrais via `ThreadPoolExecutor`.

Cada execução realiza uma **carga histórica completa** por simplicidade — o landing é transitório:

```
landing limpo → histórico completo da API buscado → JSONL escrito no landing
→ job raw faz append no BigQuery (com _insert_date para rastreabilidade)
→ landing limpo
```

- **Landing:** Área de staging transitória; recebe o dataset completo a cada execução
- **Raw:** Tabela BigQuery append-only; cada execução adiciona uma nova partição com o `_insert_date` do dia
- **Trusted:** MERGE + deduplicação mantendo apenas o registro com o `_insert_date` mais recente por chave

### Batch — Tesouro

Uma Cloud Function busca **8 endpoints da API do Tesouro sequencialmente**: 7 snapshots completos (benchmarks, comunicados, dealers, calendario, homologacao, portarias, editais) a cada execução, mais `resultados` iterado ano a ano a partir de 2019. Execuções diárias buscam apenas o ano corrente para `resultados`; um trigger de carga completa (`{"full_load": true}`) busca todos os anos desde 2019.

- **Landing:** Persistente — arquivos se acumulam por partição de data de ingestão (diferente do staging transitório do BACEN)
- **Raw:** Tabela BigQuery append-only com payload + colunas de metadados
- **Trusted:** dbt incremental MERGE, mesmo padrão dos outros pipelines

---

## Infraestrutura

Toda a infraestrutura é gerenciada via **Terraform** (sem cliques manuais no console).

| Serviço | Uso |
|---|---|
| GCE VM `e2-micro` | Streamer Binance WebSocket — serviço systemd always-on |
| GCS Bucket | Landing zone de todos os arquivos JSONL (paths particionados Hive-style) |
| Cloud Functions 2nd gen | Jobs de landing (BACEN, Tesouro) + loaders GCS → BigQuery raw |
| Cloud Run Job | dbt build — raw → trusted + refined |
| Cloud Scheduler | Dispara cada etapa em horário UTC fixo |
| BigQuery | Raw (append-only) + Trusted + Refined |
| Artifact Registry | Imagem Docker do Cloud Run Job dbt |
| Service Accounts | Menor privilégio: `sa-streamer` (escrita GCS), `sa-pipeline` (BQ + CF + CR) |

---

## Orquestração

Sem orquestrador de workflows. Cada etapa é uma unidade independente e idempotente disparada pelo Cloud Scheduler com horários escalonados que garantem buffer suficiente entre as etapas.

| UTC | Etapa |
|---|---|
| Always on | Binance WebSocket → GCS landing |
| 02:00 | Binance GCS → BigQuery raw |
| 02:30 | APIs BACEN → GCS landing (diário) |
| 03:00 | APIs BACEN → GCS landing (mensal) |
| 03:30 | APIs BACEN → GCS landing (trimestral) |
| 04:00 | BACEN GCS → BigQuery raw |
| 05:00 | API Tesouro → GCS landing |
| 06:00 | Tesouro GCS → BigQuery raw |
| 07:00 | dbt build — trusted + refined |

---

## Monitoramento

A observabilidade é feita inteiramente via **Cloud Logging**. Cloud Functions e o Cloud Run Job dbt escrevem logs estruturados no stdout (capturado automaticamente pelo GCP). A VM GCE loga via `systemd journal`, também encaminhado ao Cloud Logging.

Nenhum dashboard de monitoramento ou alerta foi configurado — este é um projeto de portfólio e o custo de métricas customizadas no Cloud Monitoring não se justificava.

---

## Decisões de Design

### O que não foi usado e por quê

| Ferramenta | Motivo |
|---|---|
| Apache Iceberg / Delta Lake | Formatos abertos de tabela exigem custo e complexidade significativos (Dataproc ou Spark para compactação/manutenção). Não justificado para um portfólio nessa escala. |
| Cloud Composer (Airflow) | Mínimo ~$300/mês para o ambiente mais barato. Cloud Scheduler escalonado resolve a mesma dependência de ordem de graça. |
| Cloud Dataflow | Cobrança por job torna caro para batch de baixo volume. Cloud Functions cobrem toda a ingestão a uma fração do custo. |
| Pipeline CI/CD | Não implementado — mudanças de infraestrutura são aplicadas manualmente via `terraform apply`. |

### O que foi mantido simples intencionalmente

- Landing é imutável e reprocessável — payloads originais nunca são modificados.
- Raw é append-only — sem updates, sem deletes.
- Tipagem, deduplicação e MERGE acontecem apenas no trusted (dbt), com fronteira explícita.
- Cada Cloud Function é um único arquivo Python (ou um pequeno package para jobs multi-domínio como o BACEN). Sem frameworks, sem abstrações desnecessárias.

---

## Estrutura do Projeto

```
.
├── jobs/
│   ├── 0_landing/
│   │   ├── vm_binance_btcbrl/     # Binance WebSocket → GCS (asyncio, systemd)
│   │   ├── cf_bacen/              # APIs BACEN → GCS (ThreadPoolExecutor, 16 endpoints)
│   │   │   ├── main.py
│   │   │   ├── clients/           # olinda.py, sgs.py
│   │   │   ├── domains/           # institutions, spi, credit_rates, pix, ifdata
│   │   │   └── config/            # definições de endpoints
│   │   └── cf_tesouro_leiloes/    # API Tesouro → GCS
│   ├── 1_raw/
│   │   ├── cf_binance_btcbrl/     # GCS → raw.btcbrl_trades
│   │   ├── cf_bacen/              # GCS → raw.bacen_* (16 tabelas)
│   │   └── cf_tesouro_leiloes/    # GCS → raw.tesouro_leiloes
│   ├── 2_trusted/                 # owned by dbt
│   └── 3_refined/                 # owned by dbt
├── dbt/
│   ├── models/
│   │   ├── trusted/               # modelos incremental MERGE
│   │   └── refined/               # agregações prontas para negócio
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

### BigQuery — schema da camada trusted com descrições de colunas
![Schema BigQuery trusted](<imgs/Captura de tela 2026-05-10 172820.png>)

### Cloud Run Job — histórico de execuções do dbt-trusted
![Cloud Run Job dbt-trusted](<imgs/Captura de tela 2026-05-10 173028.png>)

### Cloud Functions — serviços implantados
![Lista de Cloud Functions](<imgs/Captura de tela 2026-05-10 173036.png>)

### Cloud Scheduler — jobs do pipeline
![Cloud Scheduler jobs](<imgs/Captura de tela 2026-05-10 173226.png>)

### GCS — arquivos de landing particionados (trades Binance)
![Bucket GCS landing](<imgs/Captura de tela 2026-05-10 173315.png>)

---

## Status

| Área | Status |
|---|---|
| Binance landing + raw + trusted | Concluído |
| BACEN landing + raw | Concluído |
| BACEN trusted (dbt) | Em andamento |
| Tesouro landing + raw + trusted + refined | Concluído |
| BACEN / Binance refined | Não planejado |
