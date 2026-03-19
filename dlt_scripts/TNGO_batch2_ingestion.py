# TNGO ingestion script for BATCH 2

# Library imports
import os
import time
from pathlib import Path
from datetime import datetime, timezone

# Non-native libraries
import requests
import pandas as pd
import boto3
from sqlalchemy import create_engine
import pendulum

# API config
API_KEY = os.getenv("TNGO_API_KEY")
BASE_URL = "https://api.tiingo.com/tiingo/daily"

# Container-mounted paths
TICKER_FILE = Path("/opt/airflow/config/smp500_ingestion/sp500_batch2.txt")

# Ingestion configuration; added now_et if/else because pulls return 0 cells after midnight
SLEEP_SECONDS = 0.5
now_et = pendulum.now("America/New_York")
if now_et.hour < 16:
    RUN_DATE = now_et.subtract(days=1).strftime("%Y-%m-%d")
else:
    RUN_DATE = now_et.strftime("%Y-%m-%d")

# RDS env vars
DB_HOST = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__HOST")
DB_PORT = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PORT", "5432")
DB_NAME = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__DATABASE")
DB_USER = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__USERNAME")
DB_PASSWORD = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PASSWORD")
DB_SCHEMA = "raw_smp500"
DB_TABLE = "raw_tngo_batch2_prices_daily"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "smp500_ingestion/tngo_batch2_prices_daily"

# Reading through the ticker file
def read_tickers(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

# Fetching data with 429 retry logic (3 attempts, exponential backoff: 5s, 10s, 20s)
def fetch_ticker_data(ticker, run_date):
    url = f"{BASE_URL}/{ticker}/prices"
    params = {"startDate": run_date, "endDate": run_date, "token": API_KEY}
    max_retries = 3
    backoff_seconds = [5, 10, 20]

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)

            # Handle 429 rate limit with retry
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    wait = backoff_seconds[attempt]
                    print(f"{ticker}: 429 rate limited. Retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait)
                    continue
                else:
                    print(f"{ticker}: FAILED after {max_retries} retries (429 rate limit)")
                    return []

            response.raise_for_status()

            try:
                data = response.json()
            except ValueError:
                print(f"{ticker}: non-JSON response")
                return []

            return data if isinstance(data, list) else []

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = backoff_seconds[attempt]
                print(f"{ticker}: request error ({e}). Retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
                continue
            else:
                print(f"{ticker}: FAILED after {max_retries} retries ({e})")
                return []

    return []

# Metadata fields added
def enrich_rows(rows, ticker):
    ingestion_time = datetime.now(timezone.utc).isoformat()
    for row in rows:
        row["requested_symbol"] = ticker
        row["source_api"] = "tiingo"
        row["run_date"] = RUN_DATE
        row["ingested_at"] = ingestion_time
    return rows

# Uploading to S3
def upload_df_to_s3(df):
    s3 = boto3.client("s3")
    file_name = f"tngo_batch2_prices_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    s3_key = f"{S3_PREFIX}/{file_name}"

    csv_body = df.to_csv(index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_body)

    return s3_key

# Uploading to RSDB
def load_to_postgres(df):
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df.to_sql(DB_TABLE, engine, schema=DB_SCHEMA, if_exists="append", index=False, method="multi")

# Main
def main():
    tickers = read_tickers(TICKER_FILE)
    all_rows = []

    for ticker in tickers:
        print(f"Requesting: {ticker}")
        rows = fetch_ticker_data(ticker, RUN_DATE)
        all_rows.extend(enrich_rows(rows, ticker))
        time.sleep(SLEEP_SECONDS)

    df = pd.DataFrame(all_rows)

    # Guard: skip upload if no data returned (prevents empty tables with wrong schema)
    if df.empty:
        print(f"No data returned for RUN_DATE={RUN_DATE}. Skipping upload.")
        return

    s3_key = upload_df_to_s3(df)

    df["s3_key"] = s3_key
    load_to_postgres(df)

    print(f"Uploaded to S3: {s3_key}")
    print(f"Loaded {len(df)} rows into {DB_SCHEMA}.{DB_TABLE}")

if __name__ == "__main__":
    main()