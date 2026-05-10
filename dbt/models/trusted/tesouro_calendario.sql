{{ config(unique_key=['benchmark', 'titulo', 'data_vencimento', 'data_leilao', 'tipo_leilao']) }}

SELECT
    JSON_VALUE(payload, '$.BENCHMARK')                          AS benchmark,
    JSON_VALUE(payload, '$.TITULO')                             AS titulo,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.VENCIMENTO')) AS data_vencimento,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.DATA_LEILAO')) AS data_leilao,
    JSON_VALUE(payload, '$.TIPO_LEILAO')                        AS tipo_leilao,
    CURRENT_TIMESTAMP()                                         AS _insert_date
FROM {{ source('raw', 'tesouro_leiloes') }}
WHERE endpoint_name = 'calendario'
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
    PARTITION BY
        JSON_VALUE(payload, '$.TITULO'),
        JSON_VALUE(payload, '$.VENCIMENTO'),
        JSON_VALUE(payload, '$.DATA_LEILAO')
    ORDER BY _ingested_at DESC
) = 1
