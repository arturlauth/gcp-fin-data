{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trade_id',
    partition_by={
        'field': 'trade_time',
        'data_type': 'timestamp',
        'granularity': 'day'
    }
) }}

SELECT
    CAST(JSON_VALUE(payload, '$.t') AS INT64)                   AS trade_id,
    JSON_VALUE(payload, '$.e')                                  AS event_type,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(payload, '$.E') AS INT64)) AS event_time,
    JSON_VALUE(payload, '$.s')                                  AS symbol,
    CAST(JSON_VALUE(payload, '$.p') AS NUMERIC)                 AS price,
    CAST(JSON_VALUE(payload, '$.q') AS NUMERIC)                 AS quantity,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(payload, '$.T') AS INT64)) AS trade_time,
    CAST(JSON_VALUE(payload, '$.m') AS BOOL)                    AS is_buyer_maker,
    CURRENT_TIMESTAMP()                                         AS _insert_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY JSON_VALUE(payload, '$.t')
            ORDER BY JSON_VALUE(payload, '$.E') DESC
        ) AS rn
    FROM {{ source('raw', 'btcbrl_trades') }}
    {% if is_incremental() %}
    WHERE _load_date >= (
        {% if var("start_date", none) %}
            DATE('{{ var("start_date") }}')
        {% else %}
            DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        {% endif %}
    )
    {% endif %}
)
WHERE rn = 1
