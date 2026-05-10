from datetime import date

from config.endpoints import OlindaEndpoint


def _quarter_slices(start_year: int) -> list[str]:
    """Generate OData $filter expressions splitting by calendar quarter from start_year to current quarter.

    Quarterly slices keep each page-window to ~1500 records, safely below BACEN's deep-offset limit.
    """
    slices: list[str] = []
    cur_year = start_year
    cur_q = 1
    today = date.today()
    current_q = (today.month - 1) // 3 + 1

    while (cur_year, cur_q) <= (today.year, current_q):
        q_start_month = (cur_q - 1) * 3 + 1
        q_end_month = cur_q * 3
        q_start = date(cur_year, q_start_month, 1)
        if q_end_month == 12:
            q_end = date(cur_year + 1, 1, 1)
        else:
            q_end = date(cur_year, q_end_month + 1, 1)
        slices.append(f"Data ge '{q_start.strftime('%Y-%m-%d')}' and Data lt '{q_end.strftime('%Y-%m-%d')}'")
        cur_q += 1
        if cur_q > 4:
            cur_q = 1
            cur_year += 1

    return slices


ENDPOINTS: list[OlindaEndpoint] = [
    OlindaEndpoint(
        "MPV_DadosAbertos", "v1", "MeiosdePagamentosMensalDA", "meios_pagamento_mensal", "monthly",
        func_date_param="AnoMes", func_date_format="%Y%m", func_date_quoted=True,
        func_range_start=date(2005, 1, 1),
    ),
    OlindaEndpoint(
        "Expectativas", "v1", "ExpectativasMercadoAnuais", "expectativas_anuais", "monthly",
        split_filters=_quarter_slices(1999),
    ),
]
