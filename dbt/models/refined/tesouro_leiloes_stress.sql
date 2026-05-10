{{ config(materialized='view') }}

SELECT
    data_leilao,
    titulo,
    benchmark,
    tipo_leilao,
    num_edital,
    ROUND(qtd_oferta, 0)                                  AS qtd_oferta,
    ROUND(qtd_aceita, 0)                                  AS qtd_aceita,
    ROUND(pct_cobertura, 3)                               AS pct_cobertura,
    ROUND(pct_taxa_media, 4)                              AS pct_taxa_media,
    ROUND(pct_taxa_maxima, 4)                             AS pct_taxa_maxima,
    ROUND(pct_spread_taxa, 4)                             AS pct_spread_taxa,
    ROUND(financeiro_total / 1e6, 2)                      AS financeiro_mi,
    ROUND(pct_taxa_media_rolling_4, 4)                    AS pct_taxa_media_rolling_4,
    ROUND(pct_taxa_media - pct_taxa_media_rolling_4, 4)   AS pct_delta_vs_rolling,
    foi_agendado
FROM {{ ref('tesouro_leiloes_enriched') }}
WHERE pct_cobertura < 1
ORDER BY data_leilao DESC
