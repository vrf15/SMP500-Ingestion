# FHUB ingestion script for basic financials api endpoint

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
API_KEY = os.getenv("FHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1/stock/metric"

# Container-mounted paths
TICKER_FILE = Path("/opt/airflow/config/smp500_ingestion/FHUB_SP903_ticker.txt")

# Ingestion configuration; 1s sleep keeps us under 60 calls/min (FHUB api specific limit)
SLEEP_SECONDS = 1
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
DB_TABLE = "raw_fhub_basic_financials"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "smp500_ingestion/fhub_basic_financials"

# Reading through the ticker file
def read_tickers(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

# Fetching data with retry logic
def fetch_ticker_data(ticker):
    params = {"symbol": ticker, "metric": "all", "token": API_KEY}
    max_retries = 3
    backoff_seconds = [5, 10, 20]

    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)

            # Handle 429 rate limit with retry
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    wait = backoff_seconds[attempt]
                    print(f"{ticker}: 429 rate limited. Retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait)
                    continue
                else:
                    print(f"{ticker}: FAILED after {max_retries} retries (429 rate limit)")
                    return {}

            response.raise_for_status()

            try:
                data = response.json()
            except ValueError:
                print(f"{ticker}: non-JSON response")
                return {}

            return data if isinstance(data, dict) else {}

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = backoff_seconds[attempt]
                print(f"{ticker}: request error ({e}). Retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
                continue
            else:
                print(f"{ticker}: FAILED after {max_retries} retries ({e})")
                return {}

    return {}

# Flatten the nested JSON response into skinny rows (this should treat PostgreSQL better bc row-oriented)
def flatten_response(data, ticker):
    rows = []

    # metric section: flat key-value pairs, current snapshot
    metric = data.get("metric", {})
    for metric_name, value in metric.items():
        if value is not None:
            rows.append({
                "requested_symbol": ticker,
                "metric_name": metric_name,
                "period": RUN_DATE,
                "value": value,
                "frequency": "current",
            })

    # series section: annual and quarterly time series
    series = data.get("series", {})
    for frequency in ["annual", "quarterly"]:
        freq_data = series.get(frequency, {})
        for metric_name, observations in freq_data.items():
            for obs in observations:
                rows.append({
                    "requested_symbol": ticker,
                    "metric_name": metric_name,
                    "period": obs.get("period"),
                    "value": obs.get("v"),
                    "frequency": frequency,
                })

    return rows

# Metadata fields added
def enrich_rows(rows, ticker):
    ingestion_time = datetime.now(timezone.utc).isoformat()
    for row in rows:
        row["source_api"] = "finnhub"
        row["run_date"] = RUN_DATE
        row["ingested_at"] = ingestion_time
    return rows

# Uploading to S3
def upload_df_to_s3(df):
    s3 = boto3.client("s3")
    file_name = f"fhub_basic_financials_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    s3_key = f"{S3_PREFIX}/{file_name}"

    csv_body = df.to_csv(index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_body)

    return s3_key

# Uploading to RDBMS
def load_to_postgres(df):
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df.to_sql(DB_TABLE, engine, schema=DB_SCHEMA, if_exists="append", index=False, method="multi")

# Main
def main():
    tickers = read_tickers(TICKER_FILE)
    all_rows = []

    for ticker in tickers:
        print(f"Requesting: {ticker}")
        data = fetch_ticker_data(ticker)
        flat_rows = flatten_response(data, ticker)
        all_rows.extend(enrich_rows(flat_rows, ticker))
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