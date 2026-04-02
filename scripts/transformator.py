import pandas as pd
from decimal import Decimal
from pathlib import Path

from db_connector import db_connector

engine = db_connector()
def transformator():
    BASE_DIR = Path(__file__).resolve().parent.parent
    path = BASE_DIR / "config" / "db_config.yaml"

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