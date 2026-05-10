from config.endpoints import OlindaEndpoint

ENDPOINTS: list[OlindaEndpoint] = [
    OlindaEndpoint("IFDATA", "v1", "ListaDeRelatorio()", "ifdata_lista_relatorio", "quarterly"),
    OlindaEndpoint(
        "IFDATA", "v1", "IfDataCadastro", "ifdata_cadastro", "quarterly",
        func_date_param="AnoMes", func_date_format="%Y%m", func_date_quoted=False,
        split_filters=["Td eq 'I'", "Td eq 'C'"],
    ),
]
