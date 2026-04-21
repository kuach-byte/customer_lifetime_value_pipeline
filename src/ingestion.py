"""
Online Retail II Dataset - Raw Data PostgreSQL Importer
Production-ready version:
- Uses .env for credentials
- Uses Kaggle API (no CLI)
- Handles dynamic CSV detection
- Uses chunked ingestion (low memory)
- Avoids table drops (preserves history)
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from pathlib import Path
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from urllib.parse import quote_plus

# ==================== LOGGING ====================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== LOAD ENV ====================
load_dotenv()

# ==================== CONFIG ====================
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

KAGGLE_CONFIG = {
    "username": os.getenv("KAGGLE_USERNAME"),
    "key": os.getenv("KAGGLE_KEY")
}

KAGGLE_DATASET = 'mashlyn/online-retail-ii-uci'

DATA_DIR = Path('./data')
DATA_DIR.mkdir(exist_ok=True)

# ==================== VALIDATION ====================

def validate_config():
    missing_db = [k for k, v in DB_CONFIG.items() if v is None]
    if missing_db:
        raise ValueError(f"Missing DB env vars: {missing_db}")

    if not KAGGLE_CONFIG["username"] or not KAGGLE_CONFIG["key"]:
        raise ValueError("Missing Kaggle API credentials in .env")

# ==================== KAGGLE ====================

def set_kaggle_env():
    os.environ["KAGGLE_USERNAME"] = KAGGLE_CONFIG["username"]
    os.environ["KAGGLE_KEY"] = KAGGLE_CONFIG["key"]

def download_from_kaggle():
    logger.info(f"Downloading dataset: {KAGGLE_DATASET}")

    set_kaggle_env()

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        KAGGLE_DATASET,
        path=DATA_DIR,
        unzip=True
    )

    logger.info("Download complete")

# ==================== FILE HANDLING ====================

def find_csv_file():
    csv_files = list(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError("No CSV file found in data directory")

    logger.info(f"Using CSV file: {csv_files[0]}")
    return csv_files[0]

# ==================== DATABASE ====================

def create_postgres_engine():
    password = quote_plus(DB_CONFIG['password'])

    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{password}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    engine = create_engine(connection_string)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    logger.info("Connected to PostgreSQL")
    return engine

def create_tables(engine):
    logger.info("Ensuring tables exist...")

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS online_retail_raw (
                invoice_no VARCHAR(50),
                stock_code VARCHAR(50),
                description TEXT,
                quantity FLOAT,
                invoice_date TIMESTAMP,
                unit_price FLOAT,
                customer_id VARCHAR(50),
                country VARCHAR(100),
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS import_log (
                id SERIAL PRIMARY KEY,
                import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                filename VARCHAR(255),
                row_count INTEGER,
                file_size_mb FLOAT
            )
        """))

        conn.commit()

    logger.info("Tables ready")

# ==================== INGESTION ====================

def store_raw_data(engine, csv_path):
    logger.info("Starting chunked ingestion...")

    total_rows = 0

    for chunk in pd.read_csv(csv_path, encoding='latin1', chunksize=50000):

        # Rename columns to match DB schema
        chunk.columns = [
            'invoice_no',
            'stock_code',
            'description',
            'quantity',
            'invoice_date',
            'unit_price',
            'customer_id',
            'country'
        ]

        # Convert date safely
        chunk['invoice_date'] = pd.to_datetime(
            chunk['invoice_date'],
            errors='coerce'
        )

        chunk.to_sql(
            'online_retail_raw',
            engine,
            if_exists='append',
            index=False
        )

        total_rows += len(chunk)
        logger.info(f"Inserted {total_rows:,} rows...")

    # Log import
    file_size_mb = csv_path.stat().st_size / (1024 * 1024)

    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO import_log (filename, row_count, file_size_mb)
            VALUES (:filename, :row_count, :file_size_mb)
        """), {
            'filename': csv_path.name,
            'row_count': total_rows,
            'file_size_mb': file_size_mb
        })
        conn.commit()

    logger.info(f"Finished ingestion: {total_rows:,} rows")

# ==================== VERIFICATION ====================

def verify_data(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT invoice_no),
                COUNT(DISTINCT customer_id),
                COUNT(DISTINCT stock_code),
                COUNT(DISTINCT country)
            FROM online_retail_raw
        """))

        row = result.fetchone()

        logger.info("=" * 50)
        logger.info("VERIFICATION")
        logger.info(f"Total rows: {row[0]:,}")
        logger.info(f"Unique invoices: {row[1]:,}")
        logger.info(f"Unique customers: {row[2]:,}")
        logger.info(f"Unique products: {row[3]:,}")
        logger.info(f"Unique countries: {row[4]:,}")
        logger.info("=" * 50)

# ==================== MAIN ====================

def main():
    logger.info("Starting raw ingestion pipeline")

    try:
        validate_config()

        # Download only if no CSV exists
        if not any(DATA_DIR.glob("*.csv")):
            download_from_kaggle()

        csv_path = find_csv_file()

        engine = create_postgres_engine()

        create_tables(engine)

        store_raw_data(engine, csv_path)

        verify_data(engine)

        logger.info("SUCCESS: Raw data pipeline completed")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()