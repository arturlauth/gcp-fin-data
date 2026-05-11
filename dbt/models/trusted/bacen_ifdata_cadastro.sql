{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['cod_inst', 'data_referencia', 'td']
) }}

SELECT
    JSON_VALUE(payload, '$.CodInst')                                           AS cod_inst,
    PARSE_DATE('%Y%m', JSON_VALUE(payload, '$.Data'))                          AS data_referencia,
    JSON_VALUE(payload, '$.NomeInstituicao')                                   AS nome_instituicao,
    PARSE_DATE('%Y%m', JSON_VALUE(payload, '$.DataInicioAtividade'))           AS data_inicio_atividade,
    JSON_VALUE(payload, '$.Tcb')                                               AS tcb,
    JSON_VALUE(payload, '$.Td')                                                AS td,
    CAST(JSON_VALUE(payload, '$.Tc') AS INT64)                                 AS tc,
    JSON_VALUE(payload, '$.SegmentoTb')                                        AS segmento,
    JSON_VALUE(payload, '$.Uf')                                                AS uf,
    JSON_VALUE(payload, '$.Municipio')                                         AS municipio,
    JSON_VALUE(payload, '$.Sr')                                                AS sr,
    JSON_VALUE(payload, '$.CodConglomeradoFinanceiro')                         AS cod_conglomerado_financeiro,
    JSON_VALUE(payload, '$.CodConglomeradoPrudencial')                         AS cod_conglomerado_prudencial,
    JSON_VALUE(payload, '$.CnpjInstituicaoLider')                              AS cnpj_instituicao_lider,
    JSON_VALUE(payload, '$.Situacao')                                          AS situacao,
    CURRENT_TIMESTAMP()                                                         AS _insert_date
FROM {{ source('raw', 'bacen_ifdata_cadastro') }}
WHERE JSON_VALUE(payload, '$.CodInst') IS NOT NULL
  AND JSON_VALUE(payload, '$.Data') IS NOT NULL
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
        JSON_VALUE(payload, '$.CodInst'),
        JSON_VALUE(payload, '$.Data'),
        JSON_VALUE(payload, '$.Td')
    ORDER BY _ingested_at DESC
) = 1
