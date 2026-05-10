{{ config(materialized='view') }}

SELECT
    titulo,
    benchmark,
    COUNT(*)                                              AS num_leiloes,
    MIN(data_leilao)                                      AS data_primeiro_leilao,
    MAX(data_leilao)                                      AS data_ultimo_leilao,
    ROUND(AVG(pct_taxa_media), 4)                         AS pct_taxa_media_historica,
    ROUND(MIN(pct_taxa_media), 4)                         AS pct_taxa_minima_historica,
    ROUND(MAX(pct_taxa_maxima), 4)                        AS pct_taxa_maxima_historica,
    ROUND(AVG(pct_cobertura), 3)                          AS pct_cobertura_media,
    COUNTIF(pct_cobertura < 1)                            AS leiloes_sem_cobertura,
    ROUND(SUM(financeiro_total) / 1e9, 2)                 AS financeiro_total_bi,
    ROUND(AVG(financeiro_total) / 1e6, 2)                 AS ticket_medio_mi,
    ROUND(AVG(dias_ate_vencimento) / 365.0, 1)            AS prazo_medio_anos
FROM {{ ref('tesouro_leiloes_enriched') }}
GROUP BY titulo, benchmark
ORDER BY financeiro_total_bi DESC
