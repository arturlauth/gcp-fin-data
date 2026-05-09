import json
import logging
import os
import sys
from datetime import date, timezone, datetime
from typing import Any

import functions_framework
import requests
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

PROJECT = os.environ["GCP_PROJECT_ID"]
BUCKET = os.environ["GCS_BUCKET"]
LANDING_PREFIX = os.environ["GCS_LANDING_PREFIX"]
API_BASE = os.environ.get(
    "TESOURO_API_BASE",
    "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom",
)

RESULTADOS_START_YEAR = 2019

SIMPLE_ENDPOINTS: list[dict[str, Any]] = [
    {"name": "benchmarks",  "params": {"incluir_historico": "S"}},
    {"name": "comunicados", "params": {}},
    {"name": "dealers",     "params": {}},
    {"name": "calendario",  "params": {}},
    {"name": "homologacao", "params": {}},
    {"name": "portarias",   "params": {}},
    {"name": "editais",     "params": {}},
]


def is_full_load(request) -> bool:
    """Return True when the caller requests a full historical load.

    :param request: HTTP request object.
    :return: True if request body contains full_load=true.
    """
    body = request.get_json(silent=True) or {}
    return bool(body.get("full_load", False))


def fetch_endpoint(session: requests.Session, url: str, params: dict) -> list[dict]:
    """Fetch a single API endpoint and return the registros array.

    :param session: Requests session.
    :param url: Full endpoint URL.
    :param params: Query parameters dict.
    :return: List of record dicts from the "registros" key.
    """
    response = session.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    if data.get("status") != "ok":
        raise ValueError(f"Unexpected API status | url={url} | status={data.get('status')}")
    return data.get("registros", [])


def build_blob_path(prefix: str, endpoint: str, ingested_at: date, suffix: str = "") -> str:
    """Build the GCS blob path for a given endpoint and ingestion date.

    :param prefix: Landing prefix, e.g. "landing/tesouro_leiloes".
    :param endpoint: Endpoint name, e.g. "benchmarks".
    :param ingested_at: Date of ingestion.
    :param suffix: Optional suffix appended before .jsonl, e.g. "_2022".
    :return: Full blob path.
    """
    filename = f"{endpoint}{suffix}.jsonl"
    return f"{prefix}/{endpoint}/ingested_at={ingested_at}/{filename}"


def upload_jsonl(
    gcs: storage.Client, bucket: str, blob_path: str, records: list[dict]
) -> int:
    """Serialize records as JSONL and upload to GCS.

    :param gcs: Initialized GCS client.
    :param bucket: GCS bucket name.
    :param blob_path: Destination blob path.
    :param records: List of dicts to serialize.
    :return: Number of records uploaded.
    """
    content = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    blob = gcs.bucket(bucket).blob(blob_path)
    blob.upload_from_string(content, content_type="application/x-ndjson")
    return len(records)


def ingest_simple(
    session: requests.Session,
    gcs: storage.Client,
    bucket: str,
    prefix: str,
    ingested_at: date,
) -> int:
    """Fetch and upload all simple (non-paginated) endpoints.

    :param session: Requests session.
    :param gcs: GCS client.
    :param bucket: GCS bucket name.
    :param prefix: Landing prefix.
    :param ingested_at: Ingestion date used in blob path.
    :return: Total records uploaded across all endpoints.
    """
    total = 0
    for ep in SIMPLE_ENDPOINTS:
        url = f"{API_BASE}/{ep['name']}"
        try:
            records = fetch_endpoint(session, url, ep["params"])
            blob_path = build_blob_path(prefix, ep["name"], ingested_at)
            count = upload_jsonl(gcs, bucket, blob_path, records)
            logger.info("Ingested | endpoint=%s | records=%d | blob=%s", ep["name"], count, blob_path)
            total += count
        except Exception as e:
            logger.error("Failed | endpoint=%s | error=%s", ep["name"], str(e))
            raise
    return total


def ingest_resultados(
    session: requests.Session,
    gcs: storage.Client,
    bucket: str,
    prefix: str,
    ingested_at: date,
    full_load: bool,
) -> int:
    """Fetch and upload resultados, iterating by year.

    Full load: fetches all years from RESULTADOS_START_YEAR to current.
    Daily: fetches only the current year.

    :param session: Requests session.
    :param gcs: GCS client.
    :param bucket: GCS bucket name.
    :param prefix: Landing prefix.
    :param ingested_at: Ingestion date used in blob path.
    :param full_load: If True, fetch all years; otherwise only current year.
    :return: Total records uploaded.
    """
    current_year = ingested_at.year
    years = range(RESULTADOS_START_YEAR, current_year + 1) if full_load else [current_year]
    url = f"{API_BASE}/resultados"
    total = 0
    for year in years:
        try:
            records = fetch_endpoint(session, url, {"ano": year})
            blob_path = build_blob_path(prefix, "resultados", ingested_at, suffix=f"_{year}")
            count = upload_jsonl(gcs, bucket, blob_path, records)
            logger.info("Ingested | endpoint=resultados | year=%d | records=%d | blob=%s", year, count, blob_path)
            total += count
        except Exception as e:
            logger.error("Failed | endpoint=resultados | year=%d | error=%s", year, str(e))
            raise
    return total


@functions_framework.http
def tesouro_leiloes(request) -> tuple[str, int]:
    """Ingest Tesouro Nacional auction data from ARIA API into GCS landing.

    Triggered daily by Cloud Scheduler. Accepts optional JSON body
    {"full_load": true} to fetch the complete history for resultados
    (years 2019 through current).

    :param request: HTTP request. Optional body: {"full_load": true}.
    :return: (message, HTTP status code).
    """
    full_load = is_full_load(request)
    ingested_at = datetime.now(timezone.utc).date()
    logger.info("Ingest start | full_load=%s | ingested_at=%s", full_load, ingested_at)

    gcs = storage.Client(project=PROJECT)
    session = requests.Session()

    try:
        simple_count = ingest_simple(session, gcs, BUCKET, LANDING_PREFIX, ingested_at)
        resultados_count = ingest_resultados(session, gcs, BUCKET, LANDING_PREFIX, ingested_at, full_load)
        total = simple_count + resultados_count
        logger.info("Ingest complete | total_records=%d | full_load=%s", total, full_load)
        return f"Ingested {total} records", 200
    except Exception as e:
        logger.error("Ingest failed | error=%s", str(e))
        raise
