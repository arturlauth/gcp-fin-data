import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

import functions_framework
from google.cloud import bigquery, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

PROJECT = os.environ["GCP_PROJECT_ID"]
BUCKET = os.environ["GCS_BUCKET"]
LANDING_PREFIX = os.environ["GCS_LANDING_PREFIX"]

SCHEMA = [
    bigquery.SchemaField("payload", "STRING"),
    bigquery.SchemaField("_source_file", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_load_date", "DATE"),
]


def get_reference_date(request) -> date:
    """Return the start date for processing — 2 days ago by default.

    Accepts an optional JSON body {"reference_date": "YYYY-MM-DD"} for manual
    reprocessing from any past date forward.

    :param request: HTTP request object.
    :return: Inclusive start date of the processing window.
    """
    body = request.get_json(silent=True) or {}
    raw = body.get("reference_date") or os.environ.get("REFERENCE_DATE")
    if raw:
        return date.fromisoformat(raw)
    return date.today() - timedelta(days=2)


def collect_rows(
    gcs: storage.Client, bucket: str, landing_prefix: str, reference_date: date
) -> list[dict]:
    """Download all JSONL files from landing and build raw rows with metadata columns.

    Stores the raw Binance JSON string verbatim in payload — field extraction happens in
    the trusted layer via JSON_VALUE().

    :param gcs: Initialized GCS Client.
    :param bucket: GCS bucket name.
    :param landing_prefix: Base landing prefix, e.g. "btcbrl/landing".
    :param reference_date: Inclusive start date.
    :return: List of {"payload": <json_string>} dicts.
    """
    rows: list[dict] = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    load_date = date.today().isoformat()
    current = reference_date
    today = date.today()
    while current <= today:
        day_prefix = f"{landing_prefix}/year={current.year}/month={current.strftime('%m')}/day={current.strftime('%d')}/"
        for blob in gcs.bucket(bucket).list_blobs(prefix=day_prefix):
            if not blob.name.endswith(".jsonl"):
                continue
            for line in blob.download_as_text().splitlines():
                if line.strip():
                    rows.append({
                        "payload": line.strip(),
                        "_source_file": blob.name,
                        "_ingested_at": ingested_at,
                        "_load_date": load_date,
                    })
        current += timedelta(days=1)
    logger.info("Collected rows | count=%d | from=%s | to=%s", len(rows), reference_date, today)
    return rows


@functions_framework.http
def binance_btcbrl(request) -> tuple[str, int]:
    """Load JSONL files from GCS landing into BigQuery raw as payload strings.

    Each Binance JSON line is stored verbatim in the payload column — no casting,
    no renaming. Triggered daily by Cloud Scheduler with a 2-day lookback window.
    Duplicates across runs are expected and deduplicated in trusted via MERGE.

    :param request: HTTP request. Accepts optional JSON body {"reference_date": "YYYY-MM-DD"}.
    :return: (message, HTTP status code)
    """
    reference_date = get_reference_date(request)
    logger.info("Load start | reference_date=%s", reference_date)

    gcs = storage.Client(project=PROJECT)
    bq = bigquery.Client(project=PROJECT)

    rows = collect_rows(gcs, BUCKET, LANDING_PREFIX, reference_date)
    if not rows:
        logger.info("No rows found | nothing to load")
        return "No rows found", 200

    target = f"{PROJECT}.raw.btcbrl_trades"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    try:
        job = bq.load_table_from_json(rows, target, job_config=job_config)
        job.result()
        logger.info("Load complete | rows=%d | target=%s", len(rows), target)
        return f"Loaded {len(rows)} rows", 200
    except Exception as e:
        logger.error("Load failed | target=%s | error=%s", target, str(e))
        raise
