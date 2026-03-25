-- Adding Daily Ingestion CTEs first; these notes are for debugging in the future
-- FORMATTED WITH EXTRA INDENT SO I CAN COLLAPSE ON VS CODE

WITH batch1_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch1_prices_daily')}}
    ),

    batch2_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch2_prices_daily')}}
    ),

    batch3_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch3_prices_daily')}}
    ),

    batch4_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch4_prices_daily')}}
    ),

    batch5_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch5_prices_daily')}}
    ),

    batch6_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch6_prices_daily')}}
    ),

    batch7_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch7_prices_daily')}}
    ),

    batch8_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch8_prices_daily')}}
    ),

    batch9_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch9_prices_daily')}}
    ),

    batch10_ingestion AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch10_prices_daily')}}
    ),

    -- Backfill Ingestion CTEs start here

    batch1_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch1_prices_backfill')}}
    ),

    batch2_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch2_prices_backfill')}}
    ),

    batch3_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch3_prices_backfill')}}
    ),

    batch4_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch4_prices_backfill')}}
    ),

    batch5_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch5_prices_backfill')}}
    ),

    batch6_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch6_prices_backfill')}}
    ),

    batch7_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch7_prices_backfill')}}
    ),

    batch8_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch8_prices_backfill')}}
    ),

    batch9_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch9_prices_backfill')}}
    ),

    batch10_backfill AS (
        SELECT *
        FROM {{ source('raw_smp500', 'raw_tngo_batch10_prices_backfill')}}
    ),

-- I standardized batch 10 backfill here because it is all TEXT
-- This is because all previous scripts use pandas dataframe (which was too heavy on EC2)
-- I opted for csv on batch 10 backfill because it is more lightweight

batch10_standard AS (
        SELECT
            date::TEXT,
            close::DOUBLE PRECISION,
            high::DOUBLE PRECISION,
            low::DOUBLE PRECISION,
            open::DOUBLE PRECISION,
            volume::BIGINT,
            "adjClose"::DOUBLE PRECISION,
            "adjHigh"::DOUBLE PRECISION,
            "adjLow"::DOUBLE PRECISION,
            "adjOpen"::DOUBLE PRECISION,
            "adjVolume"::BIGINT,
            "divCash"::DOUBLE PRECISION,
            "splitFactor"::DOUBLE PRECISION,
            requested_symbol::TEXT,
            source_api::TEXT,
            run_date::TEXT,
            ingested_at::TEXT,
            s3_key::TEXT
        FROM batch10_backfill
    ),

-- combining all sources plus standardized backfill 10 here

combined AS (
    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch1_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch2_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch3_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch4_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch5_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch6_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch7_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch8_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch9_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'daily' AS load_type
    FROM batch10_ingestion

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch1_backfill

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch2_backfill

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch3_backfill

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch4_backfill

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch5_backfill

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch6_backfill

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch7_backfill

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch8_backfill

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch9_backfill

    UNION ALL

    SELECT
        "date",
        close,
        high,
        low,
        open,
        volume,
        "adjClose",
        "adjHigh",
        "adjLow",
        "adjOpen",
        "adjVolume",
        "divCash",
        "splitFactor",
        requested_symbol,
        source_api,
        run_date,
        ingested_at,
        s3_key,
        'backfill' AS load_type
    FROM batch10_standard
    ),

-- Waiting to get more opinion on how I should format these, there's
-- some redundant columns like "source_api::TEXT as source_api".
-- I will choose to keep it uniformed for now

typed AS (
    SELECT
        date::DATE AS price_date,
        close::NUMERIC AS close_price,
        high::NUMERIC AS high_price,
        low::NUMERIC AS low_price,
        open::NUMERIC AS open_price,
        volume::BIGINT AS volume,
        "adjClose"::NUMERIC AS adjusted_close_price,
        "adjHigh"::NUMERIC AS adjusted_high_price,
        "adjLow"::NUMERIC AS adjusted_low_price,
        "adjOpen"::NUMERIC AS adjusted_open_price,
        "adjVolume"::BIGINT AS adjusted_volume,
        "divCash"::NUMERIC AS div_cash,
        "splitFactor"::NUMERIC AS split_factor,
        requested_symbol::TEXT AS symbol,
        source_api::TEXT AS source_api,
        run_date::DATE AS run_date,
        ingested_at::TIMESTAMP AS ingested_at,
        s3_key::TEXT AS s3_key,
        load_type::TEXT AS load_type
    FROM combined
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, price_date
            ORDER BY
                CASE 
                    WHEN load_type = 'daily' THEN 1
                    WHEN load_type = 'backfill' THEN 2
                    ELSE 3
                END,
                ingested_at DESC
        ) AS row_num
    FROM typed
)

SELECT
    price_date,
    symbol,
    adjusted_open_price,
    adjusted_close_price,
    adjusted_low_price,
    adjusted_high_price,
    adjusted_volume,
    div_cash,
    split_factor,
    open_price,
    close_price,
    low_price,
    high_price,
    volume,
    source_api,
    run_date,
    ingested_at,
    s3_key,
    load_type
FROM deduped
WHERE row_num = 1