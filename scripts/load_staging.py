import pandas as pd
from sqlalchemy import create_engine

# --- Configuration ---
DB_USER = 'amit'
DB_PASS = 'texas'
DB_HOST = 'localhost'
DB_PORT = '1521'
DB_SERVICE = 'XEPDB1'
CONN_STR = f'oracle+cx_oracle://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_SERVICE}'
engine = create_engine(CONN_STR)

# --- Load CSV into staging tables ---
def load_staging(csv_file, table_name):
    df = pd.read_csv(csv_file)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} rows into {table_name}")

def main():
    load_staging('data/patients.csv', 'STG_Patients')
    load_staging('data/doctors.csv', 'STG_Doctors')
    load_staging('data/visits.csv', 'STG_Visits')
    load_staging('data/treatments.csv', 'STG_Treatments')
    load_staging('data/billing.csv', 'STG_Billing')

if __name__ == "__main__":
    main()