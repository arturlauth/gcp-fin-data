import asyncio
import json
import logging
import os
import sys

from dotenv import load_dotenv
import websockets

load_dotenv()
from google.cloud import pubsub_v1

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcbrl@trade"
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
TOPIC_ID = os.environ["PUBSUB_TOPIC_ID"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def _publish_sync(data: bytes) -> None:
    publisher.publish(topic_path, data).result()


async def consume() -> None:
    loop = asyncio.get_event_loop()
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as ws:
                logger.info("Connected to Binance WebSocket")
                async for raw in ws:
                    msg = json.loads(raw)
                    data = json.dumps(msg).encode("utf-8")
                    await loop.run_in_executor(None, _publish_sync, data)
                    logger.info("Published trade_id=%s price=%s", msg.get("t"), msg.get("p"))
        except Exception as exc:
            logger.error("WebSocket error: %s — reconnecting in 5s", exc)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(consume())
