{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['num_relatorio']
) }}

SELECT
    CAST(JSON_VALUE(payload, '$.NumeroRelatorio') AS INT64)  AS num_relatorio,
    JSON_VALUE(payload, '$.NomeRelatorio')                    AS nome_relatorio,
    CURRENT_TIMESTAMP()                                       AS _insert_date
FROM {{ source('raw', 'bacen_ifdata_lista_relatorio') }}
WHERE JSON_VALUE(payload, '$.NumeroRelatorio') IS NOT NULL
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
    PARTITION BY JSON_VALUE(payload, '$.NumeroRelatorio')
    ORDER BY _ingested_at DESC
) = 1
