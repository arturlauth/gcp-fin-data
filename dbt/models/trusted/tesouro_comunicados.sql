{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='link'
) }}

SELECT
    JSON_VALUE(payload, '$.LINK')   AS link,
    JSON_VALUE(payload, '$.TITULO') AS titulo,
    CURRENT_TIMESTAMP()             AS _insert_date
FROM {{ source('raw', 'tesouro_leiloes') }}
WHERE endpoint_name = 'comunicados'
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
    PARTITION BY JSON_VALUE(payload, '$.LINK')
    ORDER BY _ingested_at DESC
) = 1
