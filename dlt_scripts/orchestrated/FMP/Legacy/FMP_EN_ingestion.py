# FMP ingestion script for EN COMPANIES

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
API_KEY = os.getenv('FMP_API_KEY')
BASE_URL = "https://financialmodelingprep.com/stable/historical-price-eod/full"

# Container-mounted paths
TICKER_FILE = Path("/opt/airflow/config/smp500_ingestion/FMP_EN_ticker.txt")

# Ingestion configuration
SLEEP_SECONDS = 0.5
RUN_DATE = pendulum.now("America/New_York").strftime("%Y-%m-%d")

# RDS env vars
DB_HOST = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__HOST")
DB_PORT = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PORT", "5432")
DB_NAME = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__DATABASE")
DB_USER = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__USERNAME")
DB_PASSWORD = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PASSWORD")
DB_SCHEMA = "raw_smp500"
DB_TABLE = "raw_fmp_en_prices_daily"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "smp500_ingestion/fmp_en_prices_daily"

# Reading through the ticker file
def read_tickers(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

# Fetching data and handles error
def fetch_ticker_data(ticker, run_date):
    params = {"symbol": ticker, "from": run_date, "to": run_date, "apikey": API_KEY}
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    try:
        data = response.json()
    except ValueError:
        print(f"{ticker}: non-JSON response")
        return []
    return data if isinstance(data, list) else []

# Metadata fields added
def enrich_rows(rows, ticker):
    ingestion_time = datetime.now(timezone.utc).isoformat()
    for row in rows:
        row["requested_symbol"] = ticker
        row["source_api"] = "financial_modeling_prep"
        row["run_date"] = RUN_DATE
        row["ingested_at"] = ingestion_time
    return rows

# Uploading to S3
def upload_df_to_s3(df):
    s3 = boto3.client("s3")
    file_name = f"fmp_en_prices_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
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
    s3_key = upload_df_to_s3(df)

    df["s3_key"] = s3_key
    load_to_postgres(df)

    print(f"Uploaded to S3: {s3_key}")
    print(f"Loaded {len(df)} rows into {DB_SCHEMA}.{DB_TABLE}")

if __name__ == "__main__":
    main()
