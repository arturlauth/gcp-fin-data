# Technical Document — Decisões Arquiteturais

---

## Streamer (`apps/streamer/app.py`)

### GCE e2-micro VM em vez de Cloud Run

Cloud Run com `cpu_idle=false` (necessário para manter conexão WebSocket persistente) requer mínimo de 1 vCPU, o que custa ~$62/mês por serviço. Uma VM e2-micro custa ~$7/mês e é suficiente para uma conexão WebSocket contínua — sem autoscaling, sem containers, sem overhead de gestão de imagem Docker para esse componente.

O workload é fundamentalmente diferente de um serviço HTTP: não há bursts de tráfego, não há paralelismo a explorar. É uma conexão persistente de baixo throughput (5–30 msg/s). Cloud Run foi projetado para o oposto disso.

### Systemd como gerenciador de processo

O streamer roda como um serviço systemd (`/etc/systemd/system/streamer.service`) com `Restart=always` e `RestartSec=10`. Isso garante:
- Restart automático em caso de crash
- Start automático no boot da VM
- Logs no journal do sistema (`journalctl -u streamer -f`)

Alternativa considerada: supervisord. Systemd está disponível no Debian 12 sem instalação adicional.

### Binance combined stream endpoint

URL: `wss://stream.binance.com:9443/stream?streams=btcbrl@trade/btcusdt@trade/...`

Um único WebSocket para N símbolos. O envelope é `{"stream": "btcbrl@trade", "data": {...}}` — o campo `stream` identifica o símbolo, permitindo roteamento por símbolo no buffer sem conexões separadas. Adicionar um novo símbolo = adicionar uma entrada em `BINANCE_STREAMS`. Zero novas conexões, zero novas infra.

Alternativa: uma conexão WebSocket por símbolo. Descartada — adicionar 10 símbolos exigiria 10 conexões e 10 tarefas asyncio paralelas com gestão de estado independente.

### `asyncio` + `asyncio.to_thread` para writes GCS

O streamer usa dois coroutines concorrentes via `asyncio.gather`:
- `_reader`: recebe mensagens do WebSocket e acumula no buffer por símbolo
- `_flusher`: verifica a cada 10s se algum símbolo atingiu o limite de tempo (60s) e faz flush

O cliente GCS (`google-cloud-storage`) é síncrono. Chamá-lo diretamente no event loop bloquearia a recepção de mensagens durante o upload. `asyncio.to_thread` delega o upload para uma thread separada mantendo o event loop livre.

### Buffer por símbolo — 500 msgs ou 60s

Gravar no GCS por mensagem geraria milhares de arquivos minúsculos por hora. Buffer de 500 msgs ou 60s equilibra:
- **Latência**: dados chegam ao GCS no máximo 60s depois de gerados
- **Eficiência**: arquivos JSONL com centenas de linhas são mais eficientes para leitura analítica e para o BQ Load Job downstream
- **Períodos de baixo volume**: o timer de 60s garante flush mesmo com poucos trades (madrugada, fins de semana)

### Hive-style path no GCS

Path: `landing/binance/{symbol}_trades/year=YYYY/month=MM/day=DD/hour=HH/trades_{epoch_ms}.jsonl`

O formato Hive (`year=/month=/day=/hour=`) permite no futuro criar uma BigQuery External Table apontando para o bucket com partition discovery automático — sem ETL, o BQ consegue fazer partition pruning por data/hora diretamente no GCS.

### SIGTERM — graceful shutdown

O systemd envia SIGTERM antes de matar o processo (`systemctl stop streamer`). O handler seta `_stop = True`, o loop principal encerra na próxima iteração, e o processo sai limpo. Sem o handler, o processo morreria no meio de um upload GCS, potencialmente corrompendo o arquivo JSONL.

---

## Cloud Function: landing → raw (`jobs/cloud-functions/landing_to_raw/`)

### BQ Load Job em vez de streaming insert

BigQuery streaming inserts custam $0.01/200MB e têm limitações de quota. BQ Load Jobs são gratuitos (até a quota diária, que é generosa) e mais eficientes para cargas batch. Para o volume atual (~15k trades/dia), Load Job é a escolha óbvia.

### GCS como camada intermediária obrigatória

A Cloud Function não lê do WebSocket — lê do GCS. Isso garante:
- **Backfill**: qualquer janela histórica pode ser reprocessada disparando a função com `{"reference_date": "YYYY-MM-DD"}`
- **Schema evolution**: se o Binance mudar o payload, o GCS tem os dados verbatim. O raw não precisa ser alterado — só a query trusted muda
- **Separação de concerns**: o streamer entrega dados, a função carrega, a query transforma

### payload STRING — sem parsing no raw

Os campos Binance são case-insensitive no BigQuery (`e`/`E`, `t`/`T`, `m`/`M` conflitariam como colunas). Armazenar o JSON completo em `payload STRING` elimina esse problema e mantém o raw verdadeiramente schema-agnostic. `JSON_VALUE()` no trusted layer extrai o que for necessário.

### Colunas de metadados — `_source_file`, `_ingested_at`, `_load_date`

- `_source_file`: path completo do blob GCS — rastreabilidade total (qual arquivo originou cada linha)
- `_ingested_at`: timestamp de quando a função rodou — debugging de latência
- `_load_date`: data da carga — campo de partição. Permite `WHERE _load_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)` na query trusted sem depender de `_PARTITIONTIME` (pseudo-coluna de ingestão-time, menos explícita)

### 2-day lookback por padrão

A função processa de `reference_date` até hoje. O default é `today - 2 days` para cobrir:
1. Dados do dia atual (ainda em acumulação)
2. Dados do dia anterior (podem ter chegado tarde ao GCS)
3. Margem para falhas da execução anterior

O MERGE no trusted layer usa `WHERE _load_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)` pelo mesmo motivo — a janela de 2 dias garante que dados atrasados sejam capturados sem escanear a tabela inteira.

---

## BigQuery Scheduled Query: raw → trusted

### MERGE ON trade_id — idempotência

`MERGE ON trade_id` garante que rodar a query N vezes sobre os mesmos dados sempre produz o mesmo resultado. Append simples causaria duplicatas em retries, reprocessamentos manuais ou execuções simultâneas.

### `trade_id` como chave, não `event_time`

`E` (event_time) não é único: múltiplos trades podem ocorrer no mesmo milissegundo. `t` (trade_id) é o inteiro sequencial único do Binance — confirmado em dados de produção (ex: dois eventos com `E: 1778197633284` mas `t: 59869113` e `t: 59869114`).

### Scheduled Query (BQ Data Transfer) em vez de Cloud Function

A query raw → trusted é SQL puro no BigQuery. Não há lógica Python, sem I/O externo. BigQuery Scheduled Query executa nativamente — sem container, sem cold start, sem cobrança de compute adicional. A Cloud Function seria overhead desnecessário para o que é essencialmente uma query SQL agendada.

### Particionamento trusted por `DATE(trade_time)`

O campo de partição do trusted é `trade_time` (quando o trade ocorreu no Binance), não `_load_date` (quando foi carregado). Queries analíticas filtrам por janelas de tempo do negócio ("trades do último mês"), não por quando o dado foi ingerido. Particionar por `trade_time` permite partition pruning nas queries mais comuns.

---

## Infraestrutura (Terraform)

### Por que Terraform em vez de gcloud scripts

Scripts `gcloud` funcionam uma vez, mas não garantem reprodutibilidade. Terraform é declarativo: `terraform plan` mostra o diff exato antes de qualquer mudança, e o estado permite detectar drift (recurso criado manualmente fora do Terraform). `terraform apply` em ambiente novo recria tudo com as mesmas configurações.

### Um projeto GCP por ambiente

`gcp-fin-data` (dev) e `gcp-fin-data-prod` (prod) são projetos separados. IAM separado elimina o risco de acidente de escrita em produção. Billing separado permite monitorar custo por ambiente sem filtros. O mesmo código Terraform serve ambos os ambientes via `.tfvars`.

### Service accounts com least privilege

| Service Account | Permissões |
|---|---|
| `sa-streamer` | `storage.objectAdmin` no bucket landing |
| `sa-pipeline` | `storage.objectViewer` no landing + `bigquery.dataEditor` no raw e trusted + `bigquery.jobUser` no projeto |

O `sa-pipeline` precisa de `bigquery.jobUser` no projeto porque jobs de MERGE (BQ DML) são cobrados contra o projeto, não contra o dataset — a permissão de dataset sozinha não é suficiente.

### Startup script via `templatefile()`

O script de inicialização da VM (`streamer_startup.sh.tpl`) usa variáveis Terraform (`${project_id}`, `${gcs_bucket}`, `${binance_streams}`) interpoladas via `templatefile()`. Isso garante que a VM seja configurada corretamente em qualquer ambiente sem modificação manual pós-deploy.

O app e requirements são carregados do GCS durante o boot (não embutidos na imagem), o que permite atualizar o código sem recriar a VM — basta subir o novo `app.py` no GCS e reiniciar o serviço.

---

## Estrutura de jobs por tecnologia

```
jobs/
├── cloud-functions/    ← executados por Cloud Functions (HTTP trigger / Scheduler)
├── bigquery-schedules/ ← futuro: scheduled queries gerenciadas fora do Terraform
└── cloud-composer/     ← futuro: DAGs Airflow
```

Organizar por tecnologia de execução em vez de por domínio de negócio permite que a estrutura escale conforme o projeto cresce. Um novo pipeline BQ scheduled query vai em `bigquery-schedules/`, não polui `cloud-functions/`. Cada tecnologia tem suas próprias convenções de deploy — agrupar por tecnologia torna isso explícito.

Nomes de arquivo descritivos (`landing_to_raw.py`, não `main.py`) são obrigatórios — Cloud Functions suporta `GOOGLE_FUNCTION_SOURCE` para apontar para qualquer arquivo Python, eliminando a necessidade de `main.py`.
