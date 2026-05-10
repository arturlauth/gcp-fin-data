"""Local test for cf_bacen_olindaclient endpoints.

Tests all 3 endpoints (taxa_juros_mensal, ifdata_lista_relatorio, ifdata_cadastro)
using the same request logic as olinda.py, but saving JSONL to local files.
No GCS or BQ credentials required.

Run:
    python test_bacen_endpoint.py
"""

import json
import sys
from datetime import date, timedelta

import requests

OLINDA_BASE = "https://olinda.bcb.gov.br/olinda/servico"
PAGE_SIZE = 100  # small for local inspection; pipeline uses 1_000


def api_date_for_monthly(run_date: date) -> date:
    # Go back 2 months to ensure data is published (BACEN has ~45-day lag)
    first_of_prev = run_date.replace(day=1) - timedelta(days=1)
    return first_of_prev.replace(day=1) - timedelta(days=1)


def api_date_for_quarterly(run_date: date) -> date:
    # Go back 2 quarters to ensure data is published
    quarter_start_month = ((run_date.month - 1) // 3) * 3 + 1
    prev_quarter_end = run_date.replace(month=quarter_start_month, day=1) - timedelta(days=1)
    prev_quarter_start_month = ((prev_quarter_end.month - 1) // 3) * 3 + 1
    return prev_quarter_end.replace(month=prev_quarter_start_month, day=1) - timedelta(days=1)


ENDPOINTS = [
    # (gcs_name, service, version, entity, func_date_param, func_date_quoted, odata_filter_param, frequency)
    (
        "taxa_juros_mensal",
        "taxaJuros", "v2", "TaxasJurosMensalPorMes",
        None, True,
        None,       # no filter — full historical dump
        "monthly",
    ),
    (
        "ifdata_lista_relatorio",
        "IFDATA", "v1", "ListaDeRelatorio()",
        None, True,
        None,       # no date param — static function import
        "quarterly",
    ),
    (
        "ifdata_cadastro",
        "IFDATA", "v1", "IfDataCadastro",
        "AnoMes", False,  # AnoMes=YYYYMM (int, unquoted)
        None,
        "quarterly",
    ),
]

run_date = date.today()

for gcs_name, service, version, entity, func_param, func_quoted, odata_filter, frequency in ENDPOINTS:
    api_date = api_date_for_monthly(run_date) if frequency == "monthly" else api_date_for_quarterly(run_date)

    if func_param:
        date_val = api_date.strftime("%Y%m") if not func_quoted else api_date.strftime("%Y-%m-%d")
        entity_path = (
            f"{entity}({func_param}='{date_val}')" if func_quoted else f"{entity}({func_param}={date_val})"
        )
    else:
        entity_path = entity

    query = f"$format=json&$top={PAGE_SIZE}&$skip=0"
    if odata_filter:
        val = api_date.strftime("%Y-%m")
        query += f"&$filter={odata_filter} eq '{val}'"

    url = f"{OLINDA_BASE}/{service}/versao/{version}/odata/{entity_path}?{query}"

    print(f"\n{'='*60}")
    print(f"Endpoint : {gcs_name}")
    print(f"API date : {api_date} (run_date={run_date})")
    print(f"GET {url}")

    try:
        resp = requests.get(url, timeout=30)
        print(f"Status   : {resp.status_code}")
        resp.raise_for_status()
        records = resp.json().get("value", [])
        print(f"Records  : {len(records)}")
        if records:
            print(f"Sample   : {json.dumps(records[0], ensure_ascii=False)}")
            out = f"sample_{gcs_name}.jsonl"
            with open(out, "w", encoding="utf-8") as f:
                for r in records:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            print(f"Saved → {out}")
        else:
            print("No records returned.")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
