{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='num_edital'
) }}

SELECT
    CAST(JSON_VALUE(payload, '$.numero_edital') AS INT64)          AS num_edital,
    JSON_VALUE(payload, '$.titulo')                                AS titulo,
    JSON_VALUE(payload, '$.benchmark')                             AS benchmark,
    JSON_VALUE(payload, '$.tipo_leilao')                           AS tipo_leilao,
    PARSE_DATE('%d/%m/%Y', JSON_VALUE(payload, '$.vencimento'))    AS data_vencimento,
    PARSE_DATE('%d/%m/%Y', JSON_VALUE(payload, '$.data_leilao'))   AS data_leilao,
    PARSE_DATE('%d/%m/%Y', JSON_VALUE(payload, '$.liquidacao'))    AS data_liquidacao,
    PARSE_DATE('%d/%m/%Y', JSON_VALUE(payload, '$.liquidacao_segunda_volta')) AS data_liquidacao_segunda_volta,
    CAST(JSON_VALUE(payload, '$.oferta') AS INT64)                          AS qtd_oferta,
    CAST(JSON_VALUE(payload, '$.oferta_segunda_volta') AS INT64)            AS qtd_oferta_segunda_volta,
    CAST(JSON_VALUE(payload, '$.quantidade_aceita') AS INT64)               AS qtd_aceita,
    CAST(JSON_VALUE(payload, '$.quantidade_aceita_segunda_volta') AS INT64) AS qtd_aceita_segunda_volta,
    CAST(JSON_VALUE(payload, '$.quantidade_bcb') AS INT64)                  AS qtd_bcb,
    CAST(JSON_VALUE(payload, '$.taxa_media') AS NUMERIC)                    AS pct_taxa_media,
    CAST(JSON_VALUE(payload, '$.taxa_maxima') AS NUMERIC)                   AS pct_taxa_maxima,
    CAST(JSON_VALUE(payload, '$.pu_medio') AS NUMERIC)                      AS pu_medio,
    CAST(JSON_VALUE(payload, '$.pu_minimo') AS NUMERIC)                     AS pu_minimo,
    CAST(JSON_VALUE(payload, '$.financeiro_aceito') AS NUMERIC)             AS financeiro_aceito,
    CAST(JSON_VALUE(payload, '$.financeiro_aceito_segunda_volta') AS NUMERIC) AS financeiro_aceito_segunda_volta,
    CAST(JSON_VALUE(payload, '$.financeiro_bcb') AS NUMERIC)                AS financeiro_bcb,
    CURRENT_TIMESTAMP()                                                     AS _insert_date
FROM {{ source('raw', 'tesouro_leiloes') }}
WHERE endpoint_name = 'resultados'
{% if is_incremental() %}
  AND _load_date >= (
      {% if var("start_date", none) %}
          DATE('{{ var("start_date") }}')
      {% else %}
          DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
      {% endif %}
  )
{% endif %}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(payload, '$.numero_edital')
    ORDER BY _ingested_at DESC
) = 1
