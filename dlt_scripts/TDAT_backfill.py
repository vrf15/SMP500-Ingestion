# TDAT backfill script for SP MidCap 400 and some overflow

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
API_KEY = os.getenv("TDAT_API_KEY")
BASE_URL = "https://api.twelvedata.com/time_series"

# Container-mounted paths
TICKER_FILE = Path(os.path.expanduser("~/projects/smp500_ingestion/config/TDAT_SP403_ticker.txt"))

# Backfill configuration
# 15s sleep to stay safely under 8 credits/min limit
OUTPUT_SIZE = 5000
SLEEP_SECONDS = 15
MAX_CHUNK_BYTES = 1_000_000_000  # ~1 GB safeguard — flush CSV buffer to S3 if exceeded; sick of EC2 crashing
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# RDS env vars
DB_HOST = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__HOST")
DB_PORT = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PORT", "5432")
DB_NAME = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__DATABASE")
DB_USER = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__USERNAME")
DB_PASSWORD = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PASSWORD")
DB_SCHEMA = "raw_smp500"
DB_TABLE = "raw_tdat_prices_backfill"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "smp500_ingestion/tdat_prices_backfill"

# CSV column order — matches the raw table schema
CSV_COLUMNS = [
    "datetime", "open", "high", "low", "close", "volume",
    "requested_symbol", "source_api", "run_date", "ingested_at", "s3_key",
]

# Reading through the ticker file
def read_tickers(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

# Fetching data with retry logic... free cannot use start/end date
def fetch_ticker_data(session, ticker):
    params = {
        "symbol": ticker,
        "interval": "1day",
        "outputsize": OUTPUT_SIZE,
        "apikey": API_KEY,
    }
    max_retries = 3
    backoff_seconds = [15, 30, 60]

    for attempt in range(max_retries):
        try:
            response = session.get(BASE_URL, params=params, timeout=30)

            # Just in-case for 429 errors
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

            # Twelve Data returns error status on bad tickers or no data
            if data.get("status") == "error":
                print(f"{ticker}: API error — {data.get('message', 'unknown')}")
                return []

            # Single-ticker response
            values = data.get("values", [])
            return values if isinstance(values, list) else []

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
        row["source_api"] = "twelve_data"
        row["run_date"] = RUN_DATE
        row["ingested_at"] = ingestion_time
    return rows

# Uploading to S3
def upload_csv_to_s3(csv_body, chunk_number):
    s3 = boto3.client("s3")
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    file_name = f"tdat_backfill_chunk{chunk_number:03d}_{timestamp}.csv"
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
    col_names = ", ".join(columns)
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

# Flush current CSV buffer to S3 and Postgres, return updated totals
def flush_chunk(csv_buffer, chunk_number, engine):
    csv_body = csv_buffer.getvalue()
    csv_buffer.close()

    s3_key = upload_csv_to_s3(csv_body, chunk_number)
    loaded = load_to_postgres(csv_body, s3_key, engine)

    print(f"  Chunk {chunk_number}: uploaded to S3 ({s3_key}), loaded {loaded} rows to Postgres")

    return loaded

# Main
def main():
    tickers = read_tickers(TICKER_FILE)

    # Reuse one TCP connection across all 403 API calls
    session = requests.Session()

    # Engine created once, disposed at end
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # CSV buffer with 1 GB safeguard — flush and start new chunk if exceeded
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=CSV_COLUMNS[:-1], extrasaction='ignore')
    writer.writeheader()

    row_count = 0
    loaded_total = 0
    chunk_number = 1

    try:
        for i, ticker in enumerate(tickers):
            print(f"Requesting: {ticker} ({i + 1}/{len(tickers)})")
            rows = fetch_ticker_data(session, ticker)
            enriched = enrich_rows(rows, ticker)

            for row in enriched:
                writer.writerow(row)
                row_count += 1

            time.sleep(SLEEP_SECONDS)

            # 1 GB safeguard — flush to S3 + Postgres and start a fresh buffer
            if csv_buffer.tell() >= MAX_CHUNK_BYTES:
                loaded_total += flush_chunk(csv_buffer, chunk_number, engine)
                chunk_number += 1

                # Reset buffer with fresh header
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=CSV_COLUMNS[:-1], extrasaction='ignore')
                writer.writeheader()

        session.close()

        # Guard: skip upload if no data returned
        if row_count == 0:
            print(f"No data returned. Skipping upload.")
            return

        # Flush the final chunk
        loaded_total += flush_chunk(csv_buffer, chunk_number, engine)

        print(f"Backfill complete: {row_count} rows across {chunk_number} chunk(s), {loaded_total} rows loaded to Postgres")

    finally:
        engine.dispose()

if __name__ == "__main__":
    main()