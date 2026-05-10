import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import uuid4

from google.cloud import bigquery

logger = logging.getLogger(__name__)

TABLE = "governance.ingestion_log"


@dataclass
class IngestionRecord:
    project_id: str
    pipeline: str
    layer: str           # "landing" | "raw"
    entity: str          # gcs_name (landing) or bq_table name (raw)
    reference_date: date
    record_count: int
    status: str          # "success" | "failure"
    frequency: str | None = None
    blob_path: str | None = None
    bq_table: str | None = None
    error_message: str | None = None


def write_log(bq_client: bigquery.Client, records: list[IngestionRecord]) -> None:
    """Insert ingestion log rows into governance.ingestion_log.

    Never raises — a log write failure is emitted to stdout but does not
    abort the calling pipeline.

    :param bq_client: BigQuery client.
    :param records: One record per entity ingested in this run.
    :return: None
    """
    now = datetime.now(timezone.utc)
    rows = [
        {
            "log_id": str(uuid4()),
            "project_id": r.project_id,
            "pipeline": r.pipeline,
            "layer": r.layer,
            "entity": r.entity,
            "frequency": r.frequency,
            "reference_date": r.reference_date.isoformat(),
            "record_count": r.record_count,
            "blob_path": r.blob_path,
            "bq_table": r.bq_table,
            "status": r.status,
            "error_message": r.error_message,
            "ingested_at": now.isoformat(),
            "_log_date": now.date().isoformat(),
        }
        for r in records
    ]
    try:
        errors = bq_client.insert_rows_json(TABLE, rows)
        if errors:
            logger.error("ingestion_log write errors | errors=%s", errors)
    except Exception as exc:
        logger.error("ingestion_log write failed | error=%s", str(exc))
