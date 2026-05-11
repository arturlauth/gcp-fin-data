{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['dealer', 'cnpj', 'data_inicio_periodo', 'data_fim_periodo']
) }}

SELECT
    JSON_VALUE(payload, '$.DEALER')                               AS dealer,
    JSON_VALUE(payload, '$.CNPJ')                                 AS cnpj,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.INICIO_PERIODO')) AS data_inicio_periodo,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.FIM_PERIODO'))    AS data_fim_periodo,
    CURRENT_TIMESTAMP()                                             AS _insert_date
FROM {{ source('raw', 'tesouro_leiloes') }}
WHERE endpoint_name = 'dealers'
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
        JSON_VALUE(payload, '$.DEALER'),
        JSON_VALUE(payload, '$.CNPJ'),
        JSON_VALUE(payload, '$.INICIO_PERIODO'),
        JSON_VALUE(payload, '$.FIM_PERIODO')
    ORDER BY _ingested_at DESC
) = 1
