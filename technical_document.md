# Technical Document — Decisões Arquiteturais

---

## Producer (`cloud-run/producer/app.py`)

### WebSocket
Binance empurra cada trade em tempo real assim que acontece. REST exigiria polling periódico com latência e custo de requests desnecessários. WebSocket mantém uma única conexão aberta e recebe eventos conforme ocorrem.

### `async def` / asyncio
O producer passa a maior parte do tempo esperando I/O (rede → WebSocket, rede → Pub/Sub). Asyncio permite processar a próxima mensagem enquanto aguarda a confirmação da anterior, sem bloquear uma thread inteira. Para o volume do BTCBRL (5–30 msg/s), isso é mais que suficiente sem precisar de multiprocessing.

### `run_in_executor`
O cliente Pub/Sub (`google-cloud-pubsub`) é síncrono — bloqueia até receber o ACK do servidor GCP. `run_in_executor` delega essa chamada bloqueante para uma thread separada do pool padrão, mantendo o event loop asyncio livre para continuar recebendo mensagens do WebSocket sem acumular fila.

### Fail handling — `WebSocketException` vs `Exception`
Separar o erro esperado (queda de conexão de rede) do inesperado (bug, erro de serialização, erro de autenticação) permite tratamentos distintos:
- `websockets.WebSocketException` → `logger.warning` — é normal, reconecta
- `Exception` genérico → `logger.error` — algo está errado, requer investigação

Ambos reconectam, mas o log diferente permite filtrar alertas no Cloud Logging.

### Retry — reconnect loop
O `while not _stop_event.is_set()` garante que o producer seja self-healing: o Binance fecha conexões após 24h por design, e instabilidades de rede são esperadas em produção. O loop reconecta automaticamente sem intervenção humana ou restart do container.

### SIGTERM — graceful shutdown
SIGTERM é o sinal que o sistema operacional (ou Cloud Run) envia para um processo quando quer que ele encerre de forma controlada — antes de mandar SIGKILL, que mata imediatamente. Cloud Run envia SIGTERM em situações como: redeploy, scale-to-zero, ou atualização de configuração. Sem handler, o processo morre no meio de um `publish`, potencialmente perdendo a mensagem em trânsito. Com o handler, o `_stop_event` é ativado, o loop `while` encerra na próxima iteração e o processo sai limpo dentro da janela de 10s que o Cloud Run concede.

---

## Consumer (`cloud-run/consumer/app.py`)

### Responsabilidade única: Pub/Sub → GCS
O consumer não escreve no BigQuery. Essa decisão é arquitetural:

- **Evolução de schema:** se o Binance mudar o payload, só o ETL precisa ser atualizado. Um consumer escrevendo no BQ acoplaria ingestão ao schema.
- **Backfill:** o GCS é a fonte de verdade. Qualquer hora histórica pode ser reprocessada rodando o ETL novamente contra os arquivos raw. Sem isso, seria necessário replay do Pub/Sub (limitado a 7 dias) ou perda de dados.
- **Separação de concerns:** consumer é entrega confiável ao GCS. Tipagem, transformação e deduplicação são responsabilidade do ETL.

O WebSocket é um mecanismo de entrega imposto pelo provedor de dados — não é um requisito de analytics em tempo real.

### Ack após escrita (não antes)
Pub/Sub garante at-least-once delivery: se o consumer der ack antes de gravar e travar, a mensagem se perde permanentemente. Dando ack somente após o GCS write bem-sucedido, o pior caso é reprocessar uma mensagem duplicada — aceitável, pois o ETL usa MERGE com dedup por `trade_id`.

### Poison pill — ack de mensagens inparsáveis
Mensagens que não passam no `json.loads` são imediatamente ackadas com `logger.error`. Sem esse tratamento, uma única mensagem malformada ficaria sendo reentregue indefinidamente (até expirar a retenção de 7 dias do Pub/Sub), bloqueando o processamento de mensagens válidas. Ack intencional de bad data é a decisão correta — a origem do problema deve ser investigada via logs.

### Micro-batching para GCS (500 msgs ou 60s)
Gravar no GCS por mensagem geraria milhares de arquivos minúsculos por hora (custo de operação de PUT + degradação de leitura analítica downstream). Buffer de 500 msgs ou 60s equilibra latência da camada Raw com eficiência de I/O. O timer garante que períodos de baixo volume (madrugada, fins de semana) não acumulem dados órfãos no buffer.

### Final flush no SIGTERM
Ao receber SIGTERM, `_stop = True` encerra o loop principal. Antes de sair, o consumer verifica se há mensagens no buffer e executa um último `flush_to_gcs`. Isso garante zero perda de dados em deploys, restarts e scale events normais do Cloud Run.

### Pull síncrono (não streaming)
O consumer usa `subscriber.pull()` síncrono em vez do `subscriber.subscribe()` assíncrono. Para micro-batching com controle explícito de ack, o pull síncrono é mais simples e previsível: busca N mensagens, processa, decide quando dar ack. O streaming pull exige callback assíncrono e torna o controle de ack por batch mais complexo.

---

## Pub/Sub — por que está no meio

Pub/Sub desacopla o producer do consumer: se o consumer travar ou for reiniciado, as mensagens ficam retidas na subscription (até 7 dias por padrão). Sem Pub/Sub, o producer precisaria de lógica de retry e conhecimento do destino final — violando separação de responsabilidades. O Pub/Sub também absorve bursts: se o Binance mandar 500 msgs/s por alguns segundos (evento de mercado), o producer publica sem pressão e o consumer processa no seu ritmo.

---

## GCS — Camada Raw

### Particionamento por data/hora
O path `btcbrl/raw/YYYY/MM/DD/HH/` cria uma partição natural por hora. Isso permite que o ETL processe exatamente uma pasta por execução (a hora anterior), sem precisar de filtros complexos ou varredura de todo o bucket. Reprocessar uma hora específica é trivial: apontar `TARGET_HOUR` para aquela pasta.

### Por que não particionar só por data
Com ~5-30 trades/s, uma pasta diária acumularia ~1.7M mensagens/dia — mais de 3.000 arquivos (500 msgs cada). Particionar por hora reduz para ~125 arquivos por pasta, tornando o listing do bucket mais eficiente no momento do ETL.

---

## ETL Pipeline (`cloud-run/pipelines/btcbrl_raw_trusted.py`)

### `trade_id` como chave primária, não `E` (event_time)
`E` (event_time) não é único: múltiplos trades podem ocorrer no mesmo milissegundo (ex: dois eventos com `E: 1778197633284` mas `t: 59869113` e `t: 59869114`). `t` (trade_id) é um inteiro sequencial único por trade — é a chave natural do Binance.

### MERGE ao invés de append
Append simples causaria duplicatas se o ETL rodar mais de uma vez para a mesma hora (retry, bug, reprocessamento manual). MERGE `ON trade_id` garante idempotência: rodar o ETL N vezes sobre os mesmos dados sempre resulta no mesmo estado final no Trusted.

### Staging table → MERGE → delete
BigQuery não tem um modo "upsert direto" eficiente para cargas em batch. O padrão é:
1. Carregar os dados novos em uma tabela staging temporária
2. Executar MERGE entre staging e target
3. Deletar a staging

Isso mantém o MERGE atômico (sem concorrência com a tabela de produção durante o load) e limpo (sem tabelas temporárias órfãs).

### `price` e `quantity` como string → NUMERIC
Os campos chegam como strings do Binance (`"p": "395090.00000000"`). Converter para `float()` antes de enviar ao BigQuery introduziria erro de ponto flutuante. Passando a string diretamente no `load_table_from_json`, o BigQuery converte para NUMERIC com precisão exata.

### `TARGET_HOUR` para backfill
O ETL processa a hora anterior por padrão. Para reprocessar horas históricas, basta definir `TARGET_HOUR=2026-05-07T23:00:00` antes de executar o Cloud Run Job. Sem essa flexibilidade, qualquer falha exigiria modificação do código para reprocessar.

---

## Estrutura do Projeto

### `cloud-run/` — agrupamento por serviço GCP
Organizar por serviço GCP (`cloud-run/`) em vez de por domínio (`btcbrl/`) faz sentido para este projeto porque há um único ativo (BTCBRL) e múltiplos serviços GCP. Quando o projeto crescer para múltiplos ativos, a organização seria revisada.

### Imagem Docker compartilhada para pipelines
`cloud-run/pipelines/` tem um único Dockerfile que serve todos os scripts de ETL. O entrypoint é definido no Cloud Run Job (`--command python --args btcbrl_raw_trusted.py`), não no Dockerfile. Isso evita manter N Dockerfiles quase idênticos quando as dependências Python são as mesmas para todos os pipelines.

### `app.py` para serviços, nome descritivo para pipelines
Producer e consumer são serviços Cloud Run com um único entrypoint — `app.py` é idiomático. Pipelines são scripts batch individuais dentro de um diretório compartilhado — nomes descritivos (`btcbrl_raw_trusted.py`) são necessários para distingui-los.

---

## Terraform & Multi-Ambiente

### Por que Terraform em vez de gcloud scripts
`infra/setup_commands.md` (scripts manuais) funciona uma vez, mas não garante que o ambiente possa ser recriado fielmente. Terraform é declarativo: descreve o estado desejado e calcula o diff. `terraform plan` mostra o que será criado/modificado/destruído antes de qualquer mudança.

### Um projeto GCP por ambiente
A separação `gcp-fin-data` (dev) e `gcp-fin-data-prod` (prod) é mais limpa do que prefixar recursos dentro de um mesmo projeto: IAM separado, billing separado, sem risco de acidente de escrita em produção. O Terraform lê o projeto-alvo via `project_id` no `.tfvars`, então o mesmo código serve ambos os ambientes.

### Service accounts com least privilege
Cada componente tem seu próprio service account com apenas as permissões que precisa:
- Producer: `pubsub.publisher` no tópico
- Consumer: `pubsub.subscriber` na subscription + `storage.objectAdmin` no bucket
- Pipeline: `storage.objectViewer` no bucket + `bigquery.dataEditor` no trusted + `bigquery.jobUser` no projeto (necessário para executar MERGE jobs)
