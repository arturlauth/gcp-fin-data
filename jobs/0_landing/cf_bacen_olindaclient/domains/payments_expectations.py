from datetime import date

from config.endpoints import OlindaEndpoint

ENDPOINTS: list[OlindaEndpoint] = [
    OlindaEndpoint(
        "MPV_DadosAbertos", "v1", "MeiosdePagamentosMensalDA", "meios_pagamento_mensal", "monthly",
        func_date_param="AnoMes", func_date_format="%Y%m", func_date_quoted=True,
        func_range_start=date(2005, 1, 1),
    ),
    # Focus survey is published weekly; filter to yesterday's date for incremental daily load.
    # Historical backfill requires a separate one-off job with split_filters=quarterly slices.
    OlindaEndpoint(
        "Expectativas", "v1", "ExpectativasMercadoAnuais", "expectativas_anuais", "daily",
        odata_filter_param="Data", odata_filter_format="%Y-%m-%d",
    ),
]
