# Technical Document — Architectural Decisions

---

## Producer (`producer/app.py`)

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

## Consumer (`consumer/app.py`) — decisões planejadas

### Ack após escrita (não antes)
Pub/Sub garante at-least-once delivery: se o consumer der ack antes de gravar e travar, a mensagem se perde. Dando ack somente após BigQuery insert + GCS write bem-sucedidos, o pior caso é reprocessar uma mensagem duplicada — aceitável e detectável via `trade_id`.

### Micro-batching para GCS (500 msgs ou 60s)
Gravar no GCS por mensagem geraria milhares de arquivos minúsculos por hora (custo de operação de PUT + degradação de leitura analítica). Buffer de 500 msgs ou 60s equilibra latência da camada Raw com eficiência de I/O.

### Streaming insert no BigQuery (não batch load)
Para o caso de uso de análise em tempo real, streaming insert disponibiliza os dados em segundos. Batch load (via GCS → load job) teria latência de minutos. O custo de streaming insert é aceitável para o volume BTCBRL.

---

## Pub/Sub — por que está no meio

Pub/Sub desacopla o producer do consumer: se o consumer travar ou for reiniciado, as mensagens ficam retidas na subscription (até 7 dias por padrão). Sem Pub/Sub, o producer precisaria de lógica de retry e conhecimento do destino final — violando separação de responsabilidades.
