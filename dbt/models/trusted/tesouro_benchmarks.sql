{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['benchmark', 'titulo', 'data_vencimento', 'data_inicio', 'data_termino']
) }}

SELECT
    JSON_VALUE(payload, '$.BENCHMARK')                          AS benchmark,
    JSON_VALUE(payload, '$.TÍTULO')                             AS titulo,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.VENCIMENTO')) AS data_vencimento,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.INÍCIO'))     AS data_inicio,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.TERMINO'))    AS data_termino,
    CURRENT_TIMESTAMP()                                         AS _insert_date
FROM {{ source('raw', 'tesouro_leiloes') }}
WHERE endpoint_name = 'benchmarks'
  AND JSON_VALUE(payload, '$.VENCIMENTO') IS NOT NULL
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
        JSON_VALUE(payload, '$.BENCHMARK'),
        JSON_VALUE(payload, '$.TÍTULO'),
        JSON_VALUE(payload, '$.VENCIMENTO'),
        JSON_VALUE(payload, '$.INÍCIO'),
        JSON_VALUE(payload, '$.TERMINO')
    ORDER BY _ingested_at DESC
) = 1
