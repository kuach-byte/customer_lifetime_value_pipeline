import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()

required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
missing_vars = [var for var in required_vars if os.getenv(var) is None]

if missing_vars:
    raise ValueError(f"Missing environment variables: {missing_vars}")

print("All environment variables loaded successfully")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ----------------------------
# Create DB connection
# ----------------------------
try:
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print(f"Connection successful: {result.scalar()}")

except Exception as e:
    print("Connection failed")
    raise e


# ----------------------------
# Extract Data Quality Summary
# ----------------------------
query = """
SELECT 
    dimension,
    check_name,
    metric_name,
    metric_value,
    rate,
    n_total
FROM public.dq1_summary
ORDER BY dimension, check_name;
"""

dq_summary = pd.read_sql(query, engine)

print(f"Data extracted: {dq_summary.shape[0]} rows")


# ----------------------------
# Export to CSV (Excel-ready)
# ----------------------------
output_path = "data/exports/dq_summary.csv"

# Create folder if it doesn't exist
os.makedirs(os.path.dirname(output_path), exist_ok=True)

dq_summary.to_csv(output_path, index=False)

print(f"File exported to: {output_path}")