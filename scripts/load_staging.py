import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import types
from sqlalchemy import Float
from sqlalchemy.dialects import oracle
from decimal import Decimal
# --- Configuration ---
DB_USER = 'amit'
DB_PASS = 'texas'
DB_HOST = 'localhost'
DB_PORT = '1521'
DB_SERVICE = 'XEPDB1'
# CONN_STR = f'oracle+cx_oracle://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_SERVICE}'
CONN_STR = f'oracle+oracledb://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/?service_name={DB_SERVICE}'
engine = create_engine(CONN_STR)
TARGET_SCHEMA = 'amit' 
# --- Load CSV into staging tables ---
def load_staging(csv_file, table_name):
    df = pd.read_csv(csv_file)
    df.columns = df.columns.str.strip()
    for col in ['cost', 'total_amount']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: Decimal(str(x)))
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Loaded {len(df)} rows into {table_name}")

def main():
    load_staging(r'C:\SHARED\JOB\healthcare_sample_DW\data\patients.csv', 'STG_PATIENTS')
    load_staging(r'C:\SHARED\JOB\healthcare_sample_DW\data\doctors.csv', 'STG_DOCTORS')
    load_staging(r'C:\SHARED\JOB\healthcare_sample_DW\data\visits.csv', 'STG_VISITS')
    load_staging(r'C:\SHARED\JOB\healthcare_sample_DW\data\treatments.csv', 'STG_TREATMENTS')
    load_staging(r'C:\SHARED\JOB\healthcare_sample_DW\data\billing.csv', 'STG_BILLING')

if __name__ == "__main__":
    main()