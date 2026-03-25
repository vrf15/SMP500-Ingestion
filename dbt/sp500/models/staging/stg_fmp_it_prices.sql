WITH daily_ingestion AS (
    SELECT *
    FROM {{ source('raw_smp500', 'raw_fmp_it_prices_daily') }}
),

backfill AS (
    SELECT *
    FROM {{ source('raw_smp500', 'raw_fmp_it_prices_backfill') }}
),

combined AS (
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        change,
        "changePercent",
        vwap,
        requested_symbol,
        source_api,
        run_date AS load_reference,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM daily_ingestion
    UNION ALL
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        change,
        "changePercent",
        vwap,
        requested_symbol,
        source_api,
        backfill_range AS load_reference,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM backfill
),

typed AS (
    SELECT
        CAST(date AS DATE) AS price_date,
        CAST(symbol AS TEXT) AS symbol,
        CAST(open AS NUMERIC) AS open_price,
        CAST(close AS NUMERIC) AS close_price,
        CAST(low AS NUMERIC) AS low_price,
        CAST(high AS NUMERIC) AS high_price,
        CAST(volume AS BIGINT) AS volume,
        CAST(change AS NUMERIC) AS price_change,
        CAST("changePercent" AS NUMERIC) AS change_percent,
        CAST(vwap AS NUMERIC) AS vwap,
        CAST(requested_symbol AS TEXT) AS requested_symbol,
        CAST(source_api AS TEXT) AS source_api,
        CAST(load_reference AS TEXT) AS load_reference,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at,
        CAST(s3_key AS TEXT) AS s3_key,
        CAST(load_type AS TEXT) AS load_type
    FROM combined
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, price_date
            ORDER BY
                CASE WHEN load_type = 'daily' THEN 1 ELSE 2 END,
                ingested_at DESC
        ) AS row_num
    FROM typed
)

SELECT
    price_date,
    symbol,
    open_price,
    close_price,
    low_price,
    high_price,
    volume,
    price_change,
    change_percent,
    vwap,
    requested_symbol,
    source_api,
    load_reference,
    ingested_at,
    s3_key,
    load_type
FROM deduped
WHERE row_num = 1