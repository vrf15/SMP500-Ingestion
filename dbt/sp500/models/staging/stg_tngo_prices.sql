WITH daily_ingestion AS (
    SELECT *
    FROM {{ source('raw_smp500', 'raw_tngo_batch1_prices_daily')}}
),

backfill AS (
    SELECT *
    FROM {{ source('raw_smp500', 'raw_tngo_batch1_backfill')}}
),

combined AS (
    SELECT
        date,
        close,
        high,
        low,
        open,
        volume,
        adjClose,
        adjHigh
        adjLow,
        adjOpen,
        adjVolume,
        divCash,
        splitFactor,
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key
    FROM daily_ingestion
    UNION ALL
    SELECT
        date,
        close,
        high,
        low,
        open,
        volume,
        adjClose,
        adjHigh
        adjLow,
        adjOpen,
        adjVolume,
        divCash,
        splitFactor,
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key
    FROM backfill
),

typed AS (
    CAST(date AS DATE) AS price_date,
    CAST(close AS NUMERIC) AS close_price,
    CAST()
)