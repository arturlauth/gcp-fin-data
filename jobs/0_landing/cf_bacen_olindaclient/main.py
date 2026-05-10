import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import functions_framework
from google.cloud import bigquery, storage

from clients import olinda
from config.endpoints import OlindaEndpoint
from domains.ifdata import ENDPOINTS as IFDATA_ENDPOINTS
from domains.taxa_juros import ENDPOINTS as TAXA_JUROS_ENDPOINTS
from governance.log import IngestionRecord, write_log

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

PROJECT = os.environ["GCP_PROJECT_ID"]
BUCKET = os.environ["GCS_BUCKET"]
LANDING_PREFIX = os.environ["GCS_LANDING_PREFIX"]
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "3"))

ALL_ENDPOINTS: list[OlindaEndpoint] = [
    *TAXA_JUROS_ENDPOINTS,
    *IFDATA_ENDPOINTS,
]


def api_date_for(run_date: date, frequency: str) -> date:
    """Return the API reference date for a given frequency.

    Monthly uses last day of previous month.
    Quarterly uses last day of previous quarter.

    :param run_date: Today's UTC date (the run date).
    :param frequency: "monthly" | "quarterly".
    :return: Reference date to pass to the API.
    """
    if frequency == "quarterly":
        # Look back 2 quarters — BACEN publishes IFDATA ~4 months after quarter end
        q_start = ((run_date.month - 1) // 3) * 3 + 1
        prev_q_end = run_date.replace(month=q_start, day=1) - timedelta(days=1)
        prev_q_start = ((prev_q_end.month - 1) // 3) * 3 + 1
        return prev_q_end.replace(month=prev_q_start, day=1) - timedelta(days=1)
    return run_date.replace(day=1) - timedelta(days=1)  # monthly


def build_blob_path(prefix: str, gcs_name: str, run_date: date) -> str:
    """Build the Hive-style GCS blob path for an endpoint and date.

    :param prefix: GCS landing prefix, e.g. "landing/bacen".
    :param gcs_name: Entity slug used as partition key and filename.
    :param run_date: Ingestion date used in Hive partition.
    :return: Full GCS blob path.
    """
    return (
        f"{prefix}/{gcs_name}"
        f"/year={run_date.year}/month={run_date.strftime('%m')}/day={run_date.strftime('%d')}"
        f"/{gcs_name}.jsonl"
    )


def run_endpoint(
    bucket_obj: storage.Bucket,
    prefix: str,
    ep: OlindaEndpoint,
    run_date: date,
) -> tuple[str, int, str]:
    """Fetch one Olinda endpoint and write JSONL to GCS. Designed for ThreadPoolExecutor.

    :param bucket_obj: GCS Bucket object.
    :param prefix: GCS landing prefix.
    :param ep: Olinda endpoint configuration.
    :param run_date: Run date (today UTC); API reference date is derived per frequency.
    :return: (gcs_name, record_count, blob_path)
    """
    blob_path = build_blob_path(prefix, ep.gcs_name, run_date)
    api_date = api_date_for(run_date, ep.frequency)
    count = olinda.stream_to_gcs(bucket_obj, blob_path, ep, api_date)
    logger.info("Ingested | gcs_name=%s | records=%d | blob=%s", ep.gcs_name, count, blob_path)
    return ep.gcs_name, count, blob_path


@functions_framework.http
def bacen_olinda(request) -> tuple[str, int]:
    """Ingest Olinda BACEN datasets (taxa_juros_mensal + ifdata) into GCS landing.

    Accepts optional JSON body: {"frequency": "monthly|quarterly|all"}.
    Defaults to "all". Endpoints run in parallel via ThreadPoolExecutor.
    A single endpoint failure does not abort the run.

    :param request: HTTP request.
    :return: (summary message, HTTP status code).
    """
    body = request.get_json(silent=True) or {}
    frequency = body.get("frequency", "all")
    raw_date = body.get("run_date")
    run_date = date.fromisoformat(raw_date) if raw_date else datetime.now(timezone.utc).date()

    selected = ALL_ENDPOINTS if frequency == "all" else [ep for ep in ALL_ENDPOINTS if ep.frequency == frequency]
    if not selected:
        return f"No endpoints for frequency={frequency!r}", 400

    logger.info(
        "Ingest start | frequency=%s | endpoints=%d | run_date=%s",
        frequency,
        len(selected),
        run_date,
    )

    gcs = storage.Client(project=PROJECT)
    bucket_obj = gcs.bucket(BUCKET)
    bq = bigquery.Client(project=PROJECT)

    failures: list[str] = []
    total_records = 0
    log_records: list[IngestionRecord] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(run_endpoint, bucket_obj, LANDING_PREFIX, ep, run_date): ep.gcs_name
            for ep in selected
        }
        for future in as_completed(futures):
            gcs_name = futures[future]
            try:
                name, count, blob = future.result()
                total_records += count
                log_records.append(IngestionRecord(
                    project_id=PROJECT, pipeline="bacen", layer="landing",
                    entity=name, reference_date=run_date, record_count=count,
                    status="success", frequency=frequency, blob_path=blob,
                ))
            except Exception as exc:
                logger.error("Failed | gcs_name=%s | error=%s", gcs_name, str(exc))
                failures.append(gcs_name)
                log_records.append(IngestionRecord(
                    project_id=PROJECT, pipeline="bacen", layer="landing",
                    entity=gcs_name, reference_date=run_date, record_count=0,
                    status="failure", frequency=frequency, error_message=str(exc),
                ))

    write_log(bq, log_records)

    if failures:
        logger.error("Ingest finished with failures | failed=%s | total_records=%d", failures, total_records)
        return f"Partial failure: {failures}", 500

    logger.info("Ingest complete | total_records=%d | frequency=%s", total_records, frequency)
    return f"Ingested {total_records} records", 200
