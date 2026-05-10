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


def _iter_months(start: date, end: date):
    """Yield first-of-month dates from start's month to end's month, inclusive."""
    cur = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while cur <= last:
        yield cur
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)


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
    retry = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=2)
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))

    if ep.func_range_start is not None:
        return _stream_range_to_gcs(bucket_obj, blob_path, ep, run_date, session)

    if ep.func_date_param:
        date_val = run_date.strftime(ep.func_date_format)
        if ep.func_date_quoted:
            entity_path = f"{ep.entity}({ep.func_date_param}='{date_val}')"
        else:
            entity_path = f"{ep.entity}({ep.func_date_param}={date_val})"
    else:
        entity_path = ep.entity

    base_url = f"{OLINDA_BASE}/{ep.service}/versao/{ep.version}/odata/{entity_path}"

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


def _stream_range_to_gcs(
    bucket_obj: storage.Bucket,
    blob_path: str,
    ep: OlindaEndpoint,
    run_date: date,
    session: requests.Session,
) -> int:
    """Loop function import one call per month from func_range_start to run_date, all into one blob.

    :param bucket_obj: GCS Bucket object.
    :param blob_path: Destination blob path.
    :param ep: Olinda endpoint configuration (must have func_range_start and func_date_param set).
    :param run_date: Upper bound — iterates up to and including this month.
    :param session: Shared requests Session.
    :return: Total records written.
    """
    months = list(_iter_months(ep.func_range_start, run_date))
    logger.info("Range fetch | entity=%s | months=%d | start=%s | end=%s",
                ep.entity, len(months), months[0], months[-1])

    blob = bucket_obj.blob(blob_path)
    total = 0

    with blob.open("wt", content_type="application/x-ndjson", encoding="utf-8") as gcs_file:
        for month_date in months:
            date_val = month_date.strftime(ep.func_date_format)
            if ep.func_date_quoted:
                entity_path = f"{ep.entity}({ep.func_date_param}='{date_val}')"
            else:
                entity_path = f"{ep.entity}({ep.func_date_param}={date_val})"

            base_url = f"{OLINDA_BASE}/{ep.service}/versao/{ep.version}/odata/{entity_path}"
            skip = 0
            resp = session.get(f"{base_url}?$format=json&$top={PAGE_SIZE}&$skip={skip}", timeout=300)
            resp.raise_for_status()
            page: list[dict] = resp.json().get("value", [])

            if not page:
                logger.info("Empty month | date=%s", date_val)
                continue

            month_total = 0
            while page:
                for record in page:
                    gcs_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                month_total += len(page)
                skip += len(page)
                if len(page) < PAGE_SIZE:
                    break
                resp = session.get(f"{base_url}?$format=json&$top={PAGE_SIZE}&$skip={skip}", timeout=300)
                resp.raise_for_status()
                page = resp.json().get("value", [])

            total += month_total
            logger.info("Month done | date=%s | records=%d", date_val, month_total)

    if total == 0:
        blob.delete()

    return total
