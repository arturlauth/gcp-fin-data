import logging
import sys
from datetime import date, datetime, timezone

from google.cloud import bigquery, storage

from config.raw_targets import RawTarget

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

SCHEMA = [
    bigquery.SchemaField("payload",      "STRING"),
    bigquery.SchemaField("_source_file", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_load_date",   "DATE"),
]


def load_to_bq(
    gcs: storage.Client,
    bq: bigquery.Client,
    bucket: str,
    prefix: str,
    target: RawTarget,
    reference_date: date,
    project: str,
) -> int:
    """Load one landing JSONL file into the corresponding BQ raw table.

    Each JSONL line is stored verbatim as a payload STRING — field extraction
    happens in the trusted layer via JSON_VALUE(). Skips silently if the
    blob does not exist for the given reference_date.

    :param gcs: GCS Client.
    :param bq: BigQuery Client.
    :param bucket: GCS bucket name.
    :param prefix: GCS landing prefix, e.g. "landing/bacen".
    :param target: RawTarget with gcs_name and bq_table.
    :param reference_date: Date to look up in the Hive partition path.
    :param project: GCP project ID.
    :return: Number of rows loaded (0 if blob absent or empty).
    """
    blob_path = (
        f"{prefix}/{target.gcs_name}"
        f"/year={reference_date.year}"
        f"/month={reference_date.strftime('%m')}"
        f"/day={reference_date.strftime('%d')}"
        f"/{target.gcs_name}.jsonl"
    )
    blob = gcs.bucket(bucket).blob(blob_path)
    if not blob.exists():
        logger.info("Blob not found | gcs_name=%s | date=%s | skip", target.gcs_name, reference_date)
        return 0

    ingested_at = datetime.now(timezone.utc).isoformat()
    load_date = date.today().isoformat()

    rows = [
        {
            "payload":      line.strip(),
            "_source_file": blob_path,
            "_ingested_at": ingested_at,
            "_load_date":   load_date,
        }
        for line in blob.download_as_bytes().decode("utf-8", errors="replace").splitlines()
        if line.strip()
    ]

    if not rows:
        logger.info("Empty blob | gcs_name=%s | skip", target.gcs_name)
        return 0

    bq_target = f"{project}.{target.bq_table}"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = bq.load_table_from_json(rows, bq_target, job_config=job_config)
    job.result()
    blob.delete()
    logger.info("Loaded | gcs_name=%s | rows=%d | target=%s", target.gcs_name, len(rows), bq_target)
    return len(rows)
