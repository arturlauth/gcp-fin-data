import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from google.cloud import bigquery, storage

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_RAW_PREFIX = os.environ.get("GCS_RAW_PREFIX", "btcbrl/raw")
TRUSTED_DATASET = os.environ["BIGQUERY_DATASET_ID"]
TRUSTED_TABLE = os.environ["BIGQUERY_TABLE_ID"]
JOB_NAME = "etl-raw-trusted-btcbrl"

TRUSTED_SCHEMA = [
    bigquery.SchemaField("trade_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("event_time", "TIMESTAMP"),
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("price", "NUMERIC"),
    bigquery.SchemaField("quantity", "NUMERIC"),
    bigquery.SchemaField("trade_time", "TIMESTAMP"),
    bigquery.SchemaField("is_buyer_maker", "BOOLEAN"),
    bigquery.SchemaField("_file_source", "STRING"),
    bigquery.SchemaField("_insert_date", "TIMESTAMP"),
    bigquery.SchemaField("_job", "STRING"),
]


def get_target_hour() -> datetime:
    """Return the UTC hour to process — previous hour by default, or TARGET_HOUR env var.

    :return: UTC datetime truncated to the hour.
    """
    raw = os.environ.get("TARGET_HOUR")
    if raw:
        return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)


def gcs_prefix_for_hour(hour: datetime, raw_prefix: str) -> str:
    """Return the GCS object prefix for the given UTC hour.

    :param hour: Target UTC hour (minute/second/microsecond ignored).
    :param raw_prefix: Base GCS prefix, e.g. "btcbrl/raw".
    :return: Full prefix string ending with "/".
    """
    return f"{raw_prefix}/{hour.year}/{hour.month:02d}/{hour.day:02d}/{hour.hour:02d}/"


def load_raw_records(
    gcs: storage.Client, bucket: str, raw_prefix: str, hour: datetime
) -> list[tuple[str, dict]]:
    """Read all JSONL files for the given hour from GCS and return parsed records.

    :param gcs: Initialized GCS Client.
    :param bucket: GCS bucket name.
    :param raw_prefix: Base GCS prefix, e.g. "btcbrl/raw".
    :param hour: Target UTC hour.
    :return: List of (gcs_uri, parsed_record) tuples.
    """
    prefix = gcs_prefix_for_hour(hour, raw_prefix)
    records = []
    for blob in gcs.bucket(bucket).list_blobs(prefix=prefix):
        uri = f"gs://{bucket}/{blob.name}"
        for line in blob.download_as_text().splitlines():
            if line.strip():
                records.append((uri, json.loads(line)))
    logger.info("Loaded raw records | count=%d | prefix=%s", len(records), prefix)
    return records


def transform(uri: str, raw: dict, insert_ts: str, job_name: str) -> dict:
    """Transform a raw Binance trade payload to a trusted layer row dict.

    :param uri: GCS URI of the source file.
    :param raw: Parsed Binance trade event.
    :param insert_ts: ISO timestamp of the ETL run.
    :param job_name: ETL job name for lineage tracking.
    :return: Dict matching trusted.binance_btc_trades schema.
    """
    return {
        "trade_id": raw["t"],
        "event_type": raw["e"],
        "event_time": datetime.fromtimestamp(raw["E"] / 1000, tz=timezone.utc).isoformat(),
        "symbol": raw["s"],
        "price": raw["p"],  # string passthrough — BQ casts to NUMERIC without float rounding
        "quantity": raw["q"],
        "trade_time": datetime.fromtimestamp(raw["T"] / 1000, tz=timezone.utc).isoformat(),
        "is_buyer_maker": raw["m"],
        "_file_source": uri,
        "_insert_date": insert_ts,
        "_job": job_name,
    }


def merge_to_trusted(
    bq: bigquery.Client, rows: list[dict], project: str, dataset: str, table: str
) -> int:
    """Load rows into a staging table and MERGE into trusted on trade_id.

    :param bq: Initialized BigQuery Client.
    :param rows: Transformed rows matching trusted schema.
    :param project: GCP project ID.
    :param dataset: BigQuery dataset ID.
    :param table: BigQuery table ID.
    :return: Number of new rows inserted.
    """
    staging_id = f"{project}.{dataset}._staging_{uuid.uuid4().hex}"
    target_id = f"{project}.{dataset}.{table}"

    load_job = bq.load_table_from_json(
        rows,
        staging_id,
        job_config=bigquery.LoadJobConfig(
            schema=TRUSTED_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    load_job.result()
    logger.info("Staging loaded | rows=%d | table=%s", len(rows), staging_id)

    cols = [f.name for f in TRUSTED_SCHEMA]
    merge_sql = f"""
    MERGE `{target_id}` T
    USING `{staging_id}` S ON T.trade_id = S.trade_id
    WHEN NOT MATCHED THEN
        INSERT ({", ".join(cols)})
        VALUES ({", ".join(f"S.{c}" for c in cols)})
    """
    merge_job = bq.query(merge_sql)
    merge_job.result()
    inserted = merge_job.dml_stats.inserted_row_count if merge_job.dml_stats else 0

    bq.delete_table(staging_id)
    logger.info("Merge complete | inserted=%d | target=%s", inserted, target_id)
    return inserted


def main() -> None:
    """Entry point — process one UTC hour from GCS raw into BigQuery trusted."""
    hour = get_target_hour()
    logger.info("ETL start | target_hour=%s", hour.isoformat())

    gcs = storage.Client(project=PROJECT_ID)
    bq = bigquery.Client(project=PROJECT_ID)

    raw_records = load_raw_records(gcs, GCS_BUCKET, GCS_RAW_PREFIX, hour)
    if not raw_records:
        logger.info("No records found | nothing to do")
        return

    insert_ts = datetime.now(timezone.utc).isoformat()
    rows = [transform(uri, raw, insert_ts, JOB_NAME) for uri, raw in raw_records]

    merge_to_trusted(bq, rows, PROJECT_ID, TRUSTED_DATASET, TRUSTED_TABLE)
    logger.info("ETL done")


if __name__ == "__main__":
    main()
