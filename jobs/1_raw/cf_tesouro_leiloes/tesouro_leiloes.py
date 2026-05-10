import logging
import os
import sys
from datetime import date, datetime, timezone

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
    bigquery.SchemaField("endpoint_name", "STRING"),
    bigquery.SchemaField("_source_file", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_load_date", "DATE"),
]

ENDPOINTS = [
    "benchmarks", "comunicados", "dealers", "calendario",
    "homologacao", "portarias", "editais", "resultados",
]


def get_reference_date(request) -> date:
    """Return the processing date — today by default.

    Tesouro landing writes files on the same day they are fetched, so
    the raw CF runs same-day (no lookback needed). Accepts an optional
    JSON body {"reference_date": "YYYY-MM-DD"} for manual reprocessing.

    :param request: HTTP request object.
    :return: Date to process.
    """
    body = request.get_json(silent=True) or {}
    raw = body.get("reference_date") or os.environ.get("REFERENCE_DATE")
    if raw:
        return date.fromisoformat(raw)
    return date.today()


def collect_rows(
    gcs: storage.Client, bucket: str, landing_prefix: str, reference_date: date
) -> list[dict]:
    """Download all JSONL files from landing for the given date and build raw rows.

    Iterates over each endpoint prefix using hive-style partitioning
    (year=/month=/day=/) and stores each JSON record verbatim in payload.

    :param gcs: Initialized GCS client.
    :param bucket: GCS bucket name.
    :param landing_prefix: Base landing prefix, e.g. "landing/tesouro_leiloes".
    :param reference_date: Date to process.
    :return: List of row dicts ready for BigQuery load.
    """
    rows: list[dict] = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    load_date = date.today().isoformat()
    day_path = (
        f"year={reference_date.year}"
        f"/month={reference_date.strftime('%m')}"
        f"/day={reference_date.strftime('%d')}"
    )
    for endpoint in ENDPOINTS:
        prefix = f"{landing_prefix}/{endpoint}/{day_path}/"
        endpoint_rows = 0
        for blob in gcs.bucket(bucket).list_blobs(prefix=prefix):
            if not blob.name.endswith(".jsonl"):
                continue
            for line in blob.download_as_text().splitlines():
                if line.strip():
                    rows.append({
                        "payload": line.strip(),
                        "endpoint_name": endpoint,
                        "_source_file": blob.name,
                        "_ingested_at": ingested_at,
                        "_load_date": load_date,
                    })
                    endpoint_rows += 1
        logger.info("Collected | endpoint=%s | rows=%d", endpoint, endpoint_rows)
    logger.info("Collected total | count=%d | date=%s", len(rows), reference_date)
    return rows


@functions_framework.http
def tesouro_leiloes(request) -> tuple[str, int]:
    """Load Tesouro Leilões JSONL files from GCS landing into BigQuery raw.

    Each API record is stored verbatim in payload with endpoint_name as
    context. Triggered daily by Cloud Scheduler after the landing CF runs.
    Duplicates across runs are deduplicated in the trusted layer.

    :param request: HTTP request. Accepts optional JSON body {"reference_date": "YYYY-MM-DD"}.
    :return: (message, HTTP status code).
    """
    reference_date = get_reference_date(request)
    logger.info("Load start | reference_date=%s", reference_date)

    gcs = storage.Client(project=PROJECT)
    bq = bigquery.Client(project=PROJECT)

    rows = collect_rows(gcs, BUCKET, LANDING_PREFIX, reference_date)
    if not rows:
        logger.info("No rows found | nothing to load")
        return "No rows found", 200

    target = f"{PROJECT}.raw.tesouro_leiloes"
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
