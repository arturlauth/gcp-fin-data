{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['data_coleta', 'indicador', 'data_referencia']
) }}

SELECT
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.Data'))             AS data_coleta,
    JSON_VALUE(payload, '$.Indicador')                                 AS indicador,
    CAST(JSON_VALUE(payload, '$.DataReferencia') AS INT64)             AS data_referencia,
    CAST(JSON_VALUE(payload, '$.Media')          AS NUMERIC)           AS media,
    CAST(JSON_VALUE(payload, '$.Mediana')        AS NUMERIC)           AS mediana,
    CAST(JSON_VALUE(payload, '$.DesvioPadrao')   AS NUMERIC)           AS desvio_padrao,
    CAST(JSON_VALUE(payload, '$.Minimo')         AS NUMERIC)           AS minimo,
    CAST(JSON_VALUE(payload, '$.Maximo')         AS NUMERIC)           AS maximo,
    CAST(JSON_VALUE(payload, '$.numeroRespondentes') AS INT64)         AS num_respondentes,
    CURRENT_TIMESTAMP()                                                AS _insert_date
FROM {{ source('raw', 'bacen_expectativas_anuais') }}
WHERE JSON_VALUE(payload, '$.Data') IS NOT NULL
  AND JSON_VALUE(payload, '$.Indicador') IS NOT NULL
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
        JSON_VALUE(payload, '$.Data'),
        JSON_VALUE(payload, '$.Indicador'),
        JSON_VALUE(payload, '$.DataReferencia')
    ORDER BY _ingested_at DESC
) = 1
