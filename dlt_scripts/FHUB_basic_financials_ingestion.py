# FHUB ingestion script for basic financials api endpoint

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
import pendulum

# API config
API_KEY = os.getenv("FHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1/stock/metric"

# Container-mounted paths
TICKER_FILE = Path("/opt/airflow/config/smp500_ingestion/FHUB_SP903_ticker.txt")

# Ingestion configuration; 1.01s sleep keeps us under 60 calls/min (FHUB api specific limit)
SLEEP_SECONDS = 1.01
CHUNK_SIZE = 100  # to cap memory usage; poor EC2 t3a large
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

# CSV column order to match the raw table schema
CSV_COLUMNS = [
    "requested_symbol", "metric_name", "period", "value", "frequency",
    "source_api", "run_date", "ingested_at", "s3_key",
]

# Reading through the ticker file
def read_tickers(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    
# Fetching data with retry logic
def fetch_ticker_data(session, ticker):
    params = {"symbol": ticker, "metric": "all", "token": API_KEY}
    max_retries = 3
    backoff_seconds = [5, 10, 20]

    for attempt in range(max_retries):
        try:
            response = session.get(BASE_URL, params=params, timeout=30)

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
def enrich_rows(rows):
    ingestion_time = datetime.now(timezone.utc).isoformat()
    for row in rows:
        row["source_api"] = "finnhub"
        row["run_date"] = RUN_DATE
        row["ingested_at"] = ingestion_time
    return rows

# Uploading to S3
def upload_csv_to_s3(csv_body):
    s3 = boto3.client("s3")
    file_name = f"fhub_basic_financials_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    s3_key = f"{S3_PREFIX}/{file_name}"

    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_body)

    return s3_key

# Uploading to RDBMS — chunked to avoid memory spikes on large payloads
def load_to_postgres(rows_chunk, s3_key, engine):
    for row in rows_chunk:
        row["s3_key"] = s3_key

    if not rows_chunk:
        return 0

    columns = CSV_COLUMNS
    placeholders = ", ".join([f":{col}" for col in columns])
    col_names = ", ".join(columns)
    insert_sql = text(f"INSERT INTO {DB_SCHEMA}.{DB_TABLE} ({col_names}) VALUES ({placeholders})")

    with engine.connect() as conn:
        conn.execute(insert_sql, rows_chunk)
        conn.commit()

    return len(rows_chunk)

# Main
def main():
    tickers = read_tickers(TICKER_FILE)

    # Reuse one TCP connection for entire call
    session = requests.Session()

    # Stream rows into a CSV buffer instead of accumulating in a list; way lighter for our set up
    csv_buffer = io.StringIO()
    # Write header row
    writer = csv.DictWriter(csv_buffer, fieldnames=CSV_COLUMNS[:-1], extrasaction='ignore')
    writer.writeheader()

    # Chunk buffer for Postgres writes
    chunk_rows = []
    row_count = 0
    tickers_in_chunk = 0

    # Engine created once, disposed at end
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    try:
        for ticker in tickers:
            print(f"Requesting: {ticker}")
            data = fetch_ticker_data(session, ticker)
            flat_rows = flatten_response(data, ticker)
            enriched = enrich_rows(flat_rows)

            # Write to CSV buffer
            for row in enriched:
                writer.writerow(row)

            # Accumulate for Postgres chunk write
            chunk_rows.extend(enriched)
            row_count += len(enriched)
            tickers_in_chunk += 1

            time.sleep(SLEEP_SECONDS)

            # Flush chunk to Postgres placeholder — actual write happens after S3 upload
            # But we free memory by clearing the chunk buffer every CHUNK_SIZE tickers
            if tickers_in_chunk >= CHUNK_SIZE:
                print(f"  Checkpoint: {row_count} rows collected across {tickers.index(ticker) + 1}/{len(tickers)} tickers")
                tickers_in_chunk = 0

        session.close()

        # Skip upload if no data returned
        if row_count == 0:
            print(f"No data returned for RUN_DATE={RUN_DATE}. Skipping upload.")
            return

        csv_body = csv_buffer.getvalue()
        csv_buffer.close()

        # Upload full CSV to S3
        s3_key = upload_csv_to_s3(csv_body)

        # Now write to Postgres in chunks — re-read CSV to avoid holding two copies
        reader = csv.DictReader(io.StringIO(csv_body))
        batch = []
        loaded_total = 0

        for row in reader:
            row["s3_key"] = s3_key
            batch.append(row)

            if len(batch) >= 10000:
                loaded_total += load_to_postgres(batch, s3_key, engine)
                batch = []

        # Final remaining batch
        if batch:
            loaded_total += load_to_postgres(batch, s3_key, engine)

        print(f"Uploaded to S3: {s3_key}")
        print(f"Loaded {loaded_total} rows into {DB_SCHEMA}.{DB_TABLE}")

    finally:
        engine.dispose()

if __name__ == "__main__":
    main()