import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from google.cloud import pubsub_v1, storage

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
SUBSCRIPTION_ID = os.environ["PUBSUB_SUBSCRIPTION_ID"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_RAW_PREFIX = os.environ["GCS_RAW_PREFIX"]

BATCH_MAX_MESSAGES = 500
BATCH_MAX_SECONDS = 60
PULL_MAX_MESSAGES = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_stop = False


def _setup_signal_handlers() -> None:
    """Register SIGTERM/SIGINT handlers for graceful Cloud Run shutdown."""
    def _handle(sig, frame):
        global _stop
        logger.info("Shutdown signal received | signal=%s", sig)
        _stop = True
    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def flush_to_gcs(
    gcs_client: storage.Client,
    bucket: str,
    raw_prefix: str,
    messages: list[bytes],
) -> None:
    """Write a batch of raw Pub/Sub payloads as JSONL to GCS raw layer.

    Path: {raw_prefix}/YYYY/MM/DD/HH/trades_<epoch_ms>.jsonl

    :param gcs_client: Initialized GCS Client.
    :param bucket: GCS bucket name.
    :param raw_prefix: Base GCS prefix, e.g. "btcbrl/raw".
    :param messages: Raw JSON bytes from Pub/Sub (original Binance payloads).
    :return: None
    """
    now = datetime.now(timezone.utc)
    epoch_ms = int(now.timestamp() * 1000)
    blob_path = f"{raw_prefix}/{now.strftime('%Y/%m/%d/%H')}/trades_{epoch_ms}.jsonl"
    content = b"\n".join(messages)
    gcs_client.bucket(bucket).blob(blob_path).upload_from_string(
        content, content_type="application/jsonl"
    )
    logger.info("Flushed to GCS | path=%s | messages=%d", blob_path, len(messages))


def consume(
    sub_client: pubsub_v1.SubscriberClient,
    gcs_client: storage.Client,
    subscription_path: str,
    bucket: str,
    raw_prefix: str,
) -> None:
    """Pull messages from Pub/Sub and write raw payloads to GCS in micro-batches.

    Acks messages only after GCS write succeeds.
    Flushes when buffer reaches BATCH_MAX_MESSAGES or BATCH_MAX_SECONDS elapsed.

    :param sub_client: Initialized SubscriberClient.
    :param gcs_client: Initialized GCS Client.
    :param subscription_path: Fully qualified Pub/Sub subscription path.
    :param bucket: GCS bucket name.
    :param raw_prefix: Base GCS prefix for raw files.
    :return: None
    """
    buffer: list[bytes] = []
    ack_ids: list[str] = []
    last_flush = time.monotonic()

    logger.info(
        "Consumer started | subscription=%s | batch_size=%d | batch_seconds=%d",
        subscription_path, BATCH_MAX_MESSAGES, BATCH_MAX_SECONDS,
    )

    while not _stop:
        try:
            response = sub_client.pull(
                request={"subscription": subscription_path, "max_messages": PULL_MAX_MESSAGES},
                timeout=10,
            )
        except Exception as exc:
            logger.warning("Pull failed | error=%s | retrying in 5s", exc)
            time.sleep(5)
            continue

        for received in response.received_messages:
            try:
                json.loads(received.message.data)
            except Exception as exc:
                logger.error("Unparseable message — acking to avoid redelivery loop | error=%s", exc)
                sub_client.acknowledge(
                    request={"subscription": subscription_path, "ack_ids": [received.ack_id]}
                )
                continue

            buffer.append(received.message.data)
            ack_ids.append(received.ack_id)

        elapsed = time.monotonic() - last_flush
        should_flush = len(buffer) >= BATCH_MAX_MESSAGES or (elapsed >= BATCH_MAX_SECONDS and buffer)

        if should_flush:
            try:
                flush_to_gcs(gcs_client, bucket, raw_prefix, buffer)
                sub_client.acknowledge(
                    request={"subscription": subscription_path, "ack_ids": ack_ids}
                )
                logger.info("Batch acked | count=%d | elapsed_s=%.1f", len(ack_ids), elapsed)
            except Exception as exc:
                logger.error("Flush failed — not acking, Pub/Sub will redeliver | error=%s", exc)
            finally:
                buffer.clear()
                ack_ids.clear()
                last_flush = time.monotonic()

    if buffer:
        logger.info("Flushing remaining buffer on shutdown | count=%d", len(buffer))
        try:
            flush_to_gcs(gcs_client, bucket, raw_prefix, buffer)
            sub_client.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )
            logger.info("Final flush completed | count=%d", len(ack_ids))
        except Exception as exc:
            logger.error("Final flush failed | error=%s", exc)


if __name__ == "__main__":
    _setup_signal_handlers()

    with pubsub_v1.SubscriberClient() as subscriber:
        gcs = storage.Client(project=PROJECT_ID)
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
        consume(subscriber, gcs, subscription_path, GCS_BUCKET, GCS_RAW_PREFIX)
