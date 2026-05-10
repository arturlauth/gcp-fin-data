{{ config(unique_key=['data_referencia', 'modalidade', 'cnpj8']) }}

SELECT
    DATE(CONCAT(JSON_VALUE(payload, '$.anoMes'), '-01'))              AS data_referencia,
    JSON_VALUE(payload, '$.Modalidade')                               AS modalidade,
    JSON_VALUE(payload, '$.cnpj8')                                    AS cnpj8,
    JSON_VALUE(payload, '$.InstituicaoFinanceira')                     AS instituicao_financeira,
    CAST(JSON_VALUE(payload, '$.Posicao') AS INT64)                   AS posicao,
    CAST(JSON_VALUE(payload, '$.TaxaJurosAoMes') AS NUMERIC)          AS pct_taxa_juros_mes,
    CAST(JSON_VALUE(payload, '$.TaxaJurosAoAno') AS NUMERIC)          AS pct_taxa_juros_ano,
    CURRENT_TIMESTAMP()                                                AS _insert_date
FROM {{ source('raw', 'bacen_taxa_juros_mensal') }}
WHERE JSON_VALUE(payload, '$.anoMes') IS NOT NULL
  AND JSON_VALUE(payload, '$.Modalidade') IS NOT NULL
  AND JSON_VALUE(payload, '$.cnpj8') IS NOT NULL
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
        JSON_VALUE(payload, '$.anoMes'),
        JSON_VALUE(payload, '$.Modalidade'),
        JSON_VALUE(payload, '$.cnpj8')
    ORDER BY _ingested_at DESC
) = 1
