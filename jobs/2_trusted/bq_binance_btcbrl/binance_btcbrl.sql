-- raw → trusted: dedup by trade_id, cast all fields, partition by trade_time
-- raw.btcbrl_trades has a single `payload` STRING column (raw Binance JSON).
-- JSON_VALUE() extracts fields; BigQuery field names are case-insensitive so
-- we can't have e/E, t/T, m/M as separate columns — hence the payload approach.
-- Lookback window: last 2 days from raw (_PARTITIONTIME).
-- This SQL will migrate to dbt (models/trusted/binance_btc_trades.sql).

MERGE `PROJECT_ID.trusted.binance_btc_trades` T
USING (
  SELECT
    CAST(JSON_VALUE(payload, '$.t') AS INTEGER)                 AS trade_id,
    JSON_VALUE(payload, '$.e')                                  AS event_type,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(payload, '$.E') AS INT64)) AS event_time,
    JSON_VALUE(payload, '$.s')                                  AS symbol,
    CAST(JSON_VALUE(payload, '$.p') AS NUMERIC)                 AS price,
    CAST(JSON_VALUE(payload, '$.q') AS NUMERIC)                 AS quantity,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(payload, '$.T') AS INT64)) AS trade_time,
    CAST(JSON_VALUE(payload, '$.m') AS BOOL)                    AS is_buyer_maker,
    CURRENT_TIMESTAMP()                                         AS _insert_date
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(payload, '$.t')
        ORDER BY JSON_VALUE(payload, '$.E') DESC
      ) AS rn
    FROM `PROJECT_ID.raw.btcbrl_trades`
    WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  )
  WHERE rn = 1
) S ON T.trade_id = S.trade_id
WHEN NOT MATCHED THEN
  INSERT (trade_id, event_type, event_time, symbol, price, quantity, trade_time, is_buyer_maker, _insert_date)
  VALUES (S.trade_id, S.event_type, S.event_time, S.symbol, S.price, S.quantity, S.trade_time, S.is_buyer_maker, S._insert_date)
