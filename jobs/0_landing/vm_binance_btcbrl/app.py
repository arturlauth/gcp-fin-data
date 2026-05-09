import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
import websockets
from google.cloud import storage

load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_LANDING_PREFIX = os.environ["GCS_LANDING_PREFIX"]
BINANCE_STREAMS = [s.strip() for s in os.environ["BINANCE_STREAMS"].split(",")]

BINANCE_WS_URL = (
    f"wss://stream.binance.com:9443/stream?streams={'/'.join(BINANCE_STREAMS)}"
)

BATCH_MAX_MESSAGES = 500
BATCH_MAX_SECONDS = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_stop = False


def _setup_signal_handlers() -> None:
    """Register SIGTERM/SIGINT for graceful shutdown."""
    def _handle(sig, frame):
        global _stop
        logger.info("Shutdown signal received | signal=%s", sig)
        _stop = True
    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def _gcs_flush(
    gcs: storage.Client,
    bucket: str,
    landing_prefix: str,
    symbol: str,
    messages: list[bytes],
) -> None:
    """Write a batch of trade messages for one symbol to GCS landing as JSONL.

    Path: {landing_prefix}/{symbol}_trades/year=YYYY/month=MM/day=DD/hour=HH/trades_<epoch_ms>.jsonl

    :param gcs: Initialized GCS Client.
    :param bucket: GCS bucket name.
    :param landing_prefix: Base GCS prefix, e.g. "landing/binance".
    :param symbol: Lowercased symbol name, e.g. "btcbrl".
    :param messages: Raw JSON bytes, one per trade.
    :return: None
    """
    now = datetime.now(timezone.utc)
    epoch_ms = int(now.timestamp() * 1000)
    blob_path = (
        f"{landing_prefix}/{symbol}_trades"
        f"/year={now.year}/month={now.strftime('%m')}"
        f"/day={now.strftime('%d')}/hour={now.strftime('%H')}"
        f"/trades_{epoch_ms}.jsonl"
    )
    gcs.bucket(bucket).blob(blob_path).upload_from_string(
        b"\n".join(messages), content_type="application/jsonl"
    )
    logger.info("Flushed | symbol=%s | path=%s | count=%d", symbol, blob_path, len(messages))


async def _reader(
    buffers: dict[str, list[bytes]],
    last_flush: dict[str, float],
    gcs: storage.Client,
) -> None:
    """Connect to Binance combined stream and buffer messages per symbol.

    Flushes immediately when a symbol's buffer hits BATCH_MAX_MESSAGES.

    :param buffers: Shared per-symbol message buffers.
    :param last_flush: Shared per-symbol last-flush timestamps.
    :param gcs: Initialized GCS Client.
    :return: None
    """
    while not _stop:
        try:
            async with websockets.connect(BINANCE_WS_URL) as ws:
                logger.info("Connected to Binance WebSocket | streams=%s", BINANCE_STREAMS)
                async for raw in ws:
                    if _stop:
                        break
                    try:
                        envelope = json.loads(raw)
                        data = envelope.get("data", envelope)
                        symbol = data.get("s", "unknown").lower()
                    except Exception as exc:
                        logger.error("Unparseable message | error=%s", exc)
                        continue

                    buffers.setdefault(symbol, []).append(json.dumps(data).encode())
                    last_flush.setdefault(symbol, time.monotonic())

                    if len(buffers[symbol]) >= BATCH_MAX_MESSAGES:
                        msgs, buffers[symbol] = buffers[symbol], []
                        last_flush[symbol] = time.monotonic()
                        await asyncio.to_thread(
                            _gcs_flush, gcs, GCS_BUCKET, GCS_LANDING_PREFIX, symbol, msgs
                        )
        except Exception as exc:
            logger.error("WebSocket error | error=%s | reconnecting in 5s", exc)
            await asyncio.sleep(5)


async def _flusher(
    buffers: dict[str, list[bytes]],
    last_flush: dict[str, float],
    gcs: storage.Client,
) -> None:
    """Periodically flush symbol buffers that have aged past BATCH_MAX_SECONDS.

    Checks every 10 seconds; flushes any symbol whose buffer is non-empty and
    hasn't been flushed for BATCH_MAX_SECONDS. Also handles final flush on shutdown.

    :param buffers: Shared per-symbol message buffers.
    :param last_flush: Shared per-symbol last-flush timestamps.
    :param gcs: Initialized GCS Client.
    :return: None
    """
    while not _stop:
        await asyncio.sleep(10)
        for symbol in list(buffers):
            if not buffers[symbol]:
                continue
            if time.monotonic() - last_flush.get(symbol, 0) >= BATCH_MAX_SECONDS:
                msgs, buffers[symbol] = buffers[symbol], []
                last_flush[symbol] = time.monotonic()
                await asyncio.to_thread(
                    _gcs_flush, gcs, GCS_BUCKET, GCS_LANDING_PREFIX, symbol, msgs
                )

    for symbol, msgs in buffers.items():
        if msgs:
            logger.info("Final flush | symbol=%s | count=%d", symbol, len(msgs))
            try:
                await asyncio.to_thread(
                    _gcs_flush, gcs, GCS_BUCKET, GCS_LANDING_PREFIX, symbol, msgs
                )
            except Exception as exc:
                logger.error("Final flush failed | symbol=%s | error=%s", symbol, exc)


async def _main() -> None:
    """Run reader and flusher concurrently."""
    gcs = storage.Client(project=PROJECT_ID)
    buffers: dict[str, list[bytes]] = {}
    last_flush: dict[str, float] = {}
    await asyncio.gather(
        _reader(buffers, last_flush, gcs),
        _flusher(buffers, last_flush, gcs),
    )


if __name__ == "__main__":
    _setup_signal_handlers()
    asyncio.run(_main())
