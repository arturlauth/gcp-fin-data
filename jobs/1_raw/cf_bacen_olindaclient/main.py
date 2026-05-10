import logging
import os
import sys
from datetime import date, timedelta

import functions_framework
from google.cloud import bigquery, storage

from config.raw_targets import RAW_TARGETS
from governance.log import IngestionRecord, write_log
from loaders.gcs_to_bigquery import load_to_bq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

PROJECT = os.environ["GCP_PROJECT_ID"]
BUCKET = os.environ["GCS_BUCKET"]
LANDING_PREFIX = os.environ["GCS_LANDING_PREFIX"]


def get_reference_date(request) -> date:
    """Return the reference date for processing — 2 days ago by default.

    Accepts an optional JSON body {"reference_date": "YYYY-MM-DD"} or
    REFERENCE_DATE env var for manual reprocessing.

    :param request: HTTP request object.
    :return: Reference date.
    """
    body = request.get_json(silent=True) or {}
    raw = body.get("reference_date") or os.environ.get("REFERENCE_DATE")
    if raw:
        return date.fromisoformat(raw)
    return date.today() - timedelta(days=2)


@functions_framework.http
def bacen_olinda(request) -> tuple[str, int]:
    """Load Olinda BACEN landing JSONL files into BigQuery raw tables.

    Iterates all 3 RAW_TARGETS sequentially for the reference date.
    Blobs absent for a target (e.g. quarterly entities on a monthly raw run)
    are skipped without error.

    :param request: HTTP request. Optional body: {"reference_date": "YYYY-MM-DD"}.
    :return: (summary message, HTTP status code).
    """
    reference_date = get_reference_date(request)
    logger.info("Load start | reference_date=%s | targets=%d", reference_date, len(RAW_TARGETS))

    gcs = storage.Client(project=PROJECT)
    bq = bigquery.Client(project=PROJECT)

    failures: list[str] = []
    log_records: list[IngestionRecord] = []
    total_rows = 0

    for target in RAW_TARGETS:
        try:
            count = load_to_bq(gcs, bq, BUCKET, LANDING_PREFIX, target, reference_date, PROJECT)
            total_rows += count
            log_records.append(IngestionRecord(
                project_id=PROJECT, pipeline="bacen", layer="raw",
                entity=target.gcs_name, reference_date=reference_date,
                record_count=count, status="success", bq_table=target.bq_table,
            ))
        except Exception as exc:
            logger.error("Load failed | gcs_name=%s | error=%s", target.gcs_name, str(exc))
            failures.append(target.gcs_name)
            log_records.append(IngestionRecord(
                project_id=PROJECT, pipeline="bacen", layer="raw",
                entity=target.gcs_name, reference_date=reference_date,
                record_count=0, status="failure", bq_table=target.bq_table,
                error_message=str(exc),
            ))

    write_log(bq, log_records)

    if failures:
        logger.error("Load finished with failures | failed=%s", failures)
        return f"Partial failure: {failures}", 500

    logger.info("Load complete | total_rows=%d | reference_date=%s", total_rows, reference_date)
    return f"Loaded {total_rows} rows", 200
