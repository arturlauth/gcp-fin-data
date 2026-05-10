from config.endpoints import OlindaEndpoint

ENDPOINTS: list[OlindaEndpoint] = [
    OlindaEndpoint(
        "taxaJuros", "v2", "TaxasJurosMensalPorMes",
        "taxa_juros_mensal", "monthly",
    ),
]
