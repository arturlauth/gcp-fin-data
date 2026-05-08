import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from google.cloud import bigquery, pubsub_v1, storage

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
SUBSCRIPTION_ID = os.environ["PUBSUB_SUBSCRIPTION_ID"]
DATASET_ID = os.environ["BIGQUERY_DATASET_ID"]
TABLE_ID = os.environ["BIGQUERY_TABLE_ID"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_RAW_PREFIX = os.environ["GCS_RAW_PREFIX"]

BATCH_MAX_MESSAGES = 500
BATCH_MAX_SECONDS = 60
PULL_MAX_MESSAGES = 50

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
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


def _ms_to_iso(ms: int) -> str:
    """Convert millisecond epoch to ISO 8601 UTC string for BigQuery TIMESTAMP.

    :param ms: Unix timestamp in milliseconds.
    :return: ISO 8601 string (e.g. '2023-01-01T00:00:00.000000Z').
    """
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _transform(msg: dict) -> dict:
    """Transform raw Binance trade payload to BigQuery row dict.

    Maps field aliases (e, E, s, t, p, q, T, m) to schema column names.
    Casts price/quantity from string to float, timestamps from ms to ISO.

    :param msg: Parsed Binance trade event.
    :return: Dict matching trusted.binance_btc_trades schema.
    """
    return {
        "event_type": msg["e"],
        "event_time": _ms_to_iso(msg["E"]),
        "symbol": msg["s"],
        "trade_id": int(msg["t"]),
        "price": float(msg["p"]),
        "quantity": float(msg["q"]),
        "trade_time": _ms_to_iso(msg["T"]),
        "is_buyer_maker": bool(msg["m"]),
        "ingestion_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }


def _flush_to_gcs(gcs_client: storage.Client, raw_messages: list[bytes]) -> None:
    """Write a batch of raw Pub/Sub payloads as JSONL to GCS raw layer.

    Path: {GCS_RAW_PREFIX}/YYYY/MM/DD/HH/trades_<epoch_ms>.jsonl

    :param gcs_client: Initialized GCS Client.
    :param raw_messages: Raw JSON bytes from Pub/Sub (original Binance payloads).
    :return: None
    """
    now = datetime.now(timezone.utc)
    epoch_ms = int(now.timestamp() * 1000)
    blob_path = f"{GCS_RAW_PREFIX}/{now.strftime('%Y/%m/%d/%H')}/trades_{epoch_ms}.jsonl"
    content = b"\n".join(raw_messages)
    gcs_client.bucket(GCS_BUCKET).blob(blob_path).upload_from_string(
        content, content_type="application/jsonl"
    )
    logger.info("Flushed to GCS | path=%s | messages=%d", blob_path, len(raw_messages))


def _insert_to_bigquery(bq_client: bigquery.Client, table_ref: str, rows: list[dict]) -> None:
    """Stream insert a batch of rows into BigQuery trusted layer.

    :param bq_client: Initialized BigQuery Client.
    :param table_ref: Fully qualified table ID (project.dataset.table).
    :param rows: List of dicts matching trusted.binance_btc_trades schema.
    :return: None
    """
    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    logger.info("Inserted to BigQuery | table=%s | rows=%d", table_ref, len(rows))


def _flush_batch(
    bq_client: bigquery.Client,
    gcs_client: storage.Client,
    table_ref: str,
    buffer: list[tuple[bytes, dict]],
) -> None:
    """Flush the current buffer to GCS (raw) and BigQuery (trusted).

    Raises on any write failure — caller must NOT ack messages if this raises.

    :param bq_client: Initialized BigQuery Client.
    :param gcs_client: Initialized GCS Client.
    :param table_ref: Fully qualified BigQuery table ID.
    :param buffer: List of (raw_bytes, transformed_row) tuples.
    :return: None
    """
    raw_messages = [item[0] for item in buffer]
    rows = [item[1] for item in buffer]
    _flush_to_gcs(gcs_client, raw_messages)
    _insert_to_bigquery(bq_client, table_ref, rows)


def consume(
    sub_client: pubsub_v1.SubscriberClient,
    bq_client: bigquery.Client,
    gcs_client: storage.Client,
    subscription_path: str,
    table_ref: str,
) -> None:
    """Pull messages from Pub/Sub, transform, and write to GCS + BigQuery in micro-batches.

    Acks messages only after both GCS and BigQuery writes succeed.
    Flushes when buffer reaches BATCH_MAX_MESSAGES or BATCH_MAX_SECONDS elapsed.

    :param sub_client: Initialized SubscriberClient.
    :param bq_client: Initialized BigQuery Client.
    :param gcs_client: Initialized GCS Client.
    :param subscription_path: Fully qualified Pub/Sub subscription path.
    :param table_ref: Fully qualified BigQuery table ID.
    :return: None
    """
    buffer: list[tuple[bytes, dict]] = []
    ack_ids: list[str] = []
    last_flush = time.monotonic()

    logger.info("Consumer started | subscription=%s | batch_size=%d | batch_seconds=%d",
                subscription_path, BATCH_MAX_MESSAGES, BATCH_MAX_SECONDS)

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
            raw = received.message.data
            try:
                msg = json.loads(raw)
                row = _transform(msg)
            except Exception as exc:
                logger.error("Unparseable message — acking to avoid redelivery loop | error=%s", exc)
                sub_client.acknowledge(
                    request={"subscription": subscription_path, "ack_ids": [received.ack_id]}
                )
                continue

            buffer.append((raw, row))
            ack_ids.append(received.ack_id)

        elapsed = time.monotonic() - last_flush
        should_flush = len(buffer) >= BATCH_MAX_MESSAGES or (elapsed >= BATCH_MAX_SECONDS and buffer)

        if should_flush:
            try:
                _flush_batch(bq_client, gcs_client, table_ref, buffer)
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
            _flush_batch(bq_client, gcs_client, table_ref, buffer)
            sub_client.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )
            logger.info("Final flush completed | count=%d", len(ack_ids))
        except Exception as exc:
            logger.error("Final flush failed | error=%s", exc)


if __name__ == "__main__":
    _setup_signal_handlers()

    with pubsub_v1.SubscriberClient() as subscriber:
        bq = bigquery.Client(project=PROJECT_ID)
        gcs = storage.Client(project=PROJECT_ID)
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        consume(subscriber, bq, gcs, subscription_path, table_ref)
