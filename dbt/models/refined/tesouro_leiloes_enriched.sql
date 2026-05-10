{{ config(materialized='table') }}

WITH benchmark_at_auction AS (
    SELECT
        r.num_edital,
        b.data_inicio  AS data_benchmark_inicio,
        b.data_termino AS data_benchmark_termino,
        ROW_NUMBER() OVER (
            PARTITION BY r.num_edital
            ORDER BY b.data_inicio DESC
        ) AS rn
    FROM {{ ref('tesouro_resultados') }} r
    JOIN {{ ref('tesouro_benchmarks') }} b
        ON  r.titulo           = b.titulo
        AND r.data_vencimento  = b.data_vencimento
        AND r.data_leilao BETWEEN b.data_inicio AND b.data_termino
)

SELECT
    -- Identity
    r.num_edital,
    r.titulo,
    r.benchmark,
    r.tipo_leilao,
    r.data_vencimento,
    r.data_leilao,
    r.data_liquidacao,
    r.data_liquidacao_segunda_volta,

    -- Benchmark period active at auction date
    baa.data_benchmark_inicio,
    baa.data_benchmark_termino,

    -- Was this auction pre-scheduled in the official calendar?
    cal.data_leilao IS NOT NULL                                                                    AS foi_agendado,

    -- Quantities
    r.qtd_oferta,
    r.qtd_oferta_segunda_volta,
    r.qtd_aceita,
    r.qtd_aceita_segunda_volta,
    r.qtd_bcb,
    r.qtd_aceita + COALESCE(r.qtd_aceita_segunda_volta, 0)                                        AS qtd_aceita_total,

    -- Rates
    r.pct_taxa_media,
    r.pct_taxa_maxima,
    r.pct_taxa_maxima - r.pct_taxa_media                                                           AS pct_spread_taxa,

    -- Prices
    r.pu_medio,
    r.pu_minimo,
    r.pu_medio - r.pu_minimo                                                                       AS spread_pu,

    -- Financials
    r.financeiro_aceito,
    r.financeiro_aceito_segunda_volta,
    r.financeiro_bcb,
    r.financeiro_aceito + COALESCE(r.financeiro_aceito_segunda_volta, 0)                          AS financeiro_total,

    -- Derived metrics
    DATE_DIFF(r.data_vencimento, r.data_leilao, DAY)                                              AS dias_ate_vencimento,
    SAFE_DIVIDE(r.qtd_aceita, r.qtd_oferta)                                                       AS pct_cobertura,
    SAFE_DIVIDE(r.qtd_aceita_segunda_volta, NULLIF(r.qtd_oferta_segunda_volta, 0))                AS pct_cobertura_segunda_volta,

    -- Rolling last 4 auctions of same titulo (ordered by date)
    AVG(r.pct_taxa_media) OVER (
        PARTITION BY r.titulo
        ORDER BY r.data_leilao
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )                                                                                              AS pct_taxa_media_rolling_4,

    AVG(SAFE_DIVIDE(r.qtd_aceita, r.qtd_oferta)) OVER (
        PARTITION BY r.titulo
        ORDER BY r.data_leilao
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )                                                                                              AS pct_cobertura_rolling_4,

    -- Month-level context columns (window — keeps row grain)
    SUM(r.financeiro_aceito + COALESCE(r.financeiro_aceito_segunda_volta, 0)) OVER (
        PARTITION BY r.titulo, DATE_TRUNC(r.data_leilao, MONTH)
    )                                                                                              AS financeiro_mes_titulo,

    SUM(r.financeiro_aceito + COALESCE(r.financeiro_aceito_segunda_volta, 0)) OVER (
        PARTITION BY DATE_TRUNC(r.data_leilao, MONTH)
    )                                                                                              AS financeiro_mes_mercado,

    r._insert_date

FROM {{ ref('tesouro_resultados') }} r
LEFT JOIN {{ ref('tesouro_calendario') }} cal
    ON  r.titulo          = cal.titulo
    AND r.data_vencimento = cal.data_vencimento
    AND r.data_leilao     = cal.data_leilao
    AND r.tipo_leilao     = cal.tipo_leilao
LEFT JOIN benchmark_at_auction baa
    ON r.num_edital = baa.num_edital AND baa.rn = 1
