# TNGO backfill script for BATCH 6

# Library imports
import os
import csv
import io
import time
from pathlib import Path
from datetime import datetime, timezone

# Non-native libraries
import requests
import boto3
from sqlalchemy import create_engine, text

# API config
API_KEY = os.getenv("TNGO_API_KEY")
BASE_URL = "https://api.tiingo.com/tiingo/daily"

# Container-mounted paths
TICKER_FILE = Path("/opt/airflow/config/smp500_ingestion/TNGO_SP500_BATCH6_ticker.txt")

# Backfill configuration
START_DATE = "2026-03-20"
END_DATE = "2026-03-27"
SLEEP_SECONDS = 0.5
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
CHUNK_SIZE = 10  # ticker progress checkpoint interval
MAX_CHUNK_BYTES = 500 * 1024 * 1024  # 500 MB — flush CSV to S3 if exceeded; added because EC2 keeps crashing, we're going super lightweight

# RDS env vars
DB_HOST = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__HOST")
DB_PORT = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PORT", "5432")
DB_NAME = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__DATABASE")
DB_USER = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__USERNAME")
DB_PASSWORD = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PASSWORD")
DB_SCHEMA = "raw_smp500"
DB_TABLE = "raw_tngo_batch6_prices_backfill"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "smp500_ingestion/tngo_batch6_prices_backfill"

# CSV column order — matches the raw table schema
CSV_COLUMNS = [
    "date", "close", "high", "low", "open", "volume",
    "adjClose", "adjHigh", "adjLow", "adjOpen", "adjVolume",
    "divCash", "splitFactor",
    "requested_symbol", "source_api", "run_date", "ingested_at", "s3_key",
]

# Reading through the ticker file
def read_tickers(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

# Create the table if it doesn't exist — all TEXT columns
def ensure_table_exists(engine):
    col_defs = ", ".join([f'"{col}" TEXT' for col in CSV_COLUMNS])
    create_sql = text(f"CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{DB_TABLE} ({col_defs})")
    with engine.connect() as conn:
        conn.execute(create_sql)
        conn.commit()

# Fetching data with 429 retry logic (3 attempts, exponential backoff: 5s, 10s, 20s)
def fetch_ticker_data(session, ticker, start_date, end_date):
    url = f"{BASE_URL}/{ticker}/prices"
    params = {"startDate": start_date, "endDate": end_date, "token": API_KEY}
    max_retries = 3
    backoff_seconds = [5, 10, 20]

    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=30)

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
def upload_csv_to_s3(csv_body, chunk_number):
    s3 = boto3.client("s3")
    file_name = f"tngo_batch6_backfill_chunk{chunk_number:03d}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    s3_key = f"{S3_PREFIX}/{file_name}"

    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_body)

    return s3_key

# Uploading to RDBMS — chunked to avoid memory spikes on large payloads
def load_to_postgres(csv_body, s3_key, engine):
    reader = csv.DictReader(io.StringIO(csv_body))
    batch = []
    loaded_total = 0

    columns = CSV_COLUMNS
    placeholders = ", ".join([f":{col}" for col in columns])
    col_names = ", ".join([f'"{col}"' for col in columns])
    insert_sql = text(f"INSERT INTO {DB_SCHEMA}.{DB_TABLE} ({col_names}) VALUES ({placeholders})")

    for row in reader:
        row["s3_key"] = s3_key
        batch.append(row)

        if len(batch) >= 10000:
            with engine.connect() as conn:
                conn.execute(insert_sql, batch)
                conn.commit()
            loaded_total += len(batch)
            batch = []

    # Final remaining batch
    if batch:
        with engine.connect() as conn:
            conn.execute(insert_sql, batch)
            conn.commit()
        loaded_total += len(batch)

    return loaded_total

# Main
def main():
    tickers = read_tickers(TICKER_FILE)

    # Reuse one TCP connection across all API calls
    session = requests.Session()

    # Engine created once, disposed at end
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    try:
        ensure_table_exists(engine)

        # Stream rows into a CSV buffer
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=CSV_COLUMNS[:-1], extrasaction='ignore')
        writer.writeheader()

        row_count = 0
        chunk_number = 1
        tickers_in_chunk = 0
        total_loaded = 0

        for ticker in tickers:
            print(f"Requesting: {ticker}")
            rows = fetch_ticker_data(session, ticker, START_DATE, END_DATE)
            enriched = enrich_rows(rows, ticker)

            for row in enriched:
                writer.writerow(row)
                row_count += 1

            tickers_in_chunk += 1
            time.sleep(SLEEP_SECONDS)

            # Progress checkpoint
            if tickers_in_chunk >= CHUNK_SIZE:
                print(f"  Checkpoint: {row_count} rows collected across {tickers.index(ticker) + 1}/{len(tickers)} tickers")
                tickers_in_chunk = 0

            # Buffer safeguard — flush to S3 and Postgres if CSV exceeds threshold
            if csv_buffer.tell() >= MAX_CHUNK_BYTES:
                print(f"  Buffer hit {MAX_CHUNK_BYTES // (1024*1024)} MB — flushing chunk {chunk_number} to S3 and Postgres")
                csv_body = csv_buffer.getvalue()
                csv_buffer.close()

                s3_key = upload_csv_to_s3(csv_body, chunk_number)
                total_loaded += load_to_postgres(csv_body, s3_key, engine)
                print(f"  Chunk {chunk_number} uploaded: {s3_key}")

                chunk_number += 1

                # Reset buffer for next chunk
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=CSV_COLUMNS[:-1], extrasaction='ignore')
                writer.writeheader()

        session.close()

        # Flush final chunk
        if row_count == 0:
            print(f"No data returned for {START_DATE} to {END_DATE}. Skipping upload.")
            return

        csv_body = csv_buffer.getvalue()
        csv_buffer.close()

        s3_key = upload_csv_to_s3(csv_body, chunk_number)
        total_loaded += load_to_postgres(csv_body, s3_key, engine)

        print(f"Uploaded to S3: chunk {chunk_number} final — {s3_key}")
        print(f"Loaded {total_loaded} total rows into {DB_SCHEMA}.{DB_TABLE}")

    finally:
        engine.dispose()

if __name__ == "__main__":
    main()