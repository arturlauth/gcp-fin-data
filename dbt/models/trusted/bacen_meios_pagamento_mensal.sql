{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['data_referencia']
) }}

SELECT
    PARSE_DATE('%Y%m%d', CONCAT(JSON_VALUE(payload, '$.AnoMes'), '01'))  AS data_referencia,
    CAST(JSON_VALUE(payload, '$.quantidadePix')    AS INT64)              AS qtd_pix,
    CAST(JSON_VALUE(payload, '$.valorPix')         AS NUMERIC)            AS val_pix,
    CAST(JSON_VALUE(payload, '$.quantidadeTED')    AS INT64)              AS qtd_ted,
    CAST(JSON_VALUE(payload, '$.valorTED')         AS NUMERIC)            AS val_ted,
    CAST(JSON_VALUE(payload, '$.quantidadeBoleto') AS INT64)              AS qtd_boleto,
    CAST(JSON_VALUE(payload, '$.valorBoleto')      AS NUMERIC)            AS val_boleto,
    CAST(JSON_VALUE(payload, '$.quantidadeCheque') AS INT64)              AS qtd_cheque,
    CAST(JSON_VALUE(payload, '$.valorCheque')      AS NUMERIC)            AS val_cheque,
    CAST(JSON_VALUE(payload, '$.quantidadeDOC')    AS INT64)              AS qtd_doc,
    CAST(JSON_VALUE(payload, '$.valorDOC')         AS NUMERIC)            AS val_doc,
    CAST(JSON_VALUE(payload, '$.quantidadeTEC')    AS INT64)              AS qtd_tec,
    CAST(JSON_VALUE(payload, '$.valorTEC')         AS NUMERIC)            AS val_tec,
    CURRENT_TIMESTAMP()                                                    AS _insert_date
FROM {{ source('raw', 'bacen_meios_pagamento_mensal') }}
WHERE JSON_VALUE(payload, '$.AnoMes') IS NOT NULL
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
    PARTITION BY JSON_VALUE(payload, '$.AnoMes')
    ORDER BY _ingested_at DESC
) = 1
