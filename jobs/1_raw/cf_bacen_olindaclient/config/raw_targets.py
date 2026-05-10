from dataclasses import dataclass


@dataclass
class RawTarget:
    gcs_name: str
    bq_table: str  # fully qualified: "raw.<name>"


RAW_TARGETS: list[RawTarget] = [
    RawTarget("taxa_juros_mensal",      "raw.bacen_taxa_juros_mensal"),
    RawTarget("ifdata_lista_relatorio", "raw.bacen_ifdata_lista_relatorio"),
    RawTarget("ifdata_cadastro",        "raw.bacen_ifdata_cadastro"),
]
