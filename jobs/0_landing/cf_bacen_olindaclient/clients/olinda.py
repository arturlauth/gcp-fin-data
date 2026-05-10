import json
import logging
import sys
from datetime import date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import storage

from config.endpoints import OlindaEndpoint

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

OLINDA_BASE = "https://olinda.bcb.gov.br/olinda/servico"
PAGE_SIZE = 100


def stream_to_gcs(
    bucket_obj: storage.Bucket,
    blob_path: str,
    ep: OlindaEndpoint,
    run_date: date,
) -> int:
    """Fetch Olinda endpoint page-by-page and stream JSONL directly to GCS.

    Keeps peak memory bounded to one page (PAGE_SIZE records) per thread.
    Creates its own requests.Session, safe for ThreadPoolExecutor.
    If ep.split_filters is set, runs one pagination pass per filter and writes
    all records into a single blob — use this to avoid server-side deep-offset 504s.

    :param bucket_obj: GCS Bucket object.
    :param blob_path: Destination blob path.
    :param ep: Olinda endpoint configuration.
    :param run_date: Reference date; injected as date filter if configured.
    :return: Total records written.
    """
    if ep.func_date_param:
        date_val = run_date.strftime(ep.func_date_format)
        if ep.func_date_quoted:
            entity_path = f"{ep.entity}({ep.func_date_param}='{date_val}')"
        else:
            entity_path = f"{ep.entity}({ep.func_date_param}={date_val})"
    else:
        entity_path = ep.entity

    base_url = f"{OLINDA_BASE}/{ep.service}/versao/{ep.version}/odata/{entity_path}"

    retry = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=2)
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))

    def build_query(skip: int, extra_filter: str | None = None) -> str:
        q = f"$format=json&$top={PAGE_SIZE}&$skip={skip}"
        if ep.query_date_param:
            q += f"&{ep.query_date_param}={run_date.strftime('%Y-%m-%d')}"
        parts: list[str] = []
        if ep.odata_filter_param:
            val = run_date.strftime(ep.odata_filter_format)
            parts.append(f"{ep.odata_filter_param} eq '{val}'")
        if extra_filter:
            parts.append(extra_filter)
        if parts:
            q += f"&$filter={' and '.join(parts)}"
        return q

    split_filters: list[str | None] = ep.split_filters if ep.split_filters else [None]

    # Probe the first slice to decide whether to open the blob at all
    probe = session.get(f"{base_url}?{build_query(0, split_filters[0])}", timeout=300)
    probe.raise_for_status()
    first_page: list[dict] = probe.json().get("value", [])
    if not first_page and len(split_filters) == 1:
        return 0

    blob = bucket_obj.blob(blob_path)
    total = 0

    with blob.open("wt", content_type="application/x-ndjson", encoding="utf-8") as gcs_file:
        for i, extra_filter in enumerate(split_filters):
            if i == 0:
                page = first_page
                skip = len(page)
            else:
                resp = session.get(f"{base_url}?{build_query(0, extra_filter)}", timeout=300)
                resp.raise_for_status()
                page = resp.json().get("value", [])
                skip = len(page)

            if not page:
                logger.info("Empty slice | filter=%s | skip", extra_filter)
                continue

            logger.info("Fetching slice | filter=%s", extra_filter)
            for record in page:
                gcs_file.write(json.dumps(record, ensure_ascii=False) + "\n")
            total += len(page)

            while len(page) == PAGE_SIZE:
                resp = session.get(f"{base_url}?{build_query(skip, extra_filter)}", timeout=300)
                resp.raise_for_status()
                page = resp.json().get("value", [])
                if not page:
                    break
                for record in page:
                    gcs_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                total += len(page)
                skip += len(page)

            logger.info("Slice done | filter=%s | records=%d", extra_filter, skip)

    if total == 0:
        blob.delete()

    return total
