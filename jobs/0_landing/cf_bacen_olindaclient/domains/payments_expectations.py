from datetime import date

from config.endpoints import OlindaEndpoint


def _year_slices(start_year: int) -> list[str]:
    """Generate OData $filter expressions splitting by calendar year from start_year to current year."""
    return [
        f"Data ge '{y}-01-01' and Data lt '{y + 1}-01-01'"
        for y in range(start_year, date.today().year + 1)
    ]


ENDPOINTS: list[OlindaEndpoint] = [
    OlindaEndpoint(
        "MPV_DadosAbertos", "v1", "MeiosdePagamentosMensalDA", "meios_pagamento_mensal", "monthly",
        func_date_param="AnoMes", func_date_format="%Y%m", func_date_quoted=True,
        func_range_start=date(2005, 1, 1),
    ),
    OlindaEndpoint(
        "Expectativas", "v1", "ExpectativasMercadoAnuais", "expectativas_anuais", "monthly",
        split_filters=_year_slices(1999),
    ),
]
