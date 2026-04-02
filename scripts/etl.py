import pandas as pd
from decimal import Decimal
from pathlib import Path
from sqlalchemy import text
from db_connector import db_connector
engine = db_connector()

def run_sql_file(folder,script_name):

    BASE_DIR = Path(__file__).resolve().parent.parent
    path = BASE_DIR / folder / script_name

    with open(path,"r") as f:
        sql_script = f.read()
        # Remove SQL*Plus `/` lines
        sql_script = "\n".join(line for line in sql_script.splitlines() if line.strip() != "/")

    with engine.connect() as conn:
        conn.execute(text(sql_script))
        # for statement in sql_script.split("/"):
        #     stmt = statement.strip()
        #     if stmt:
        #         conn.execute(text(stmt))
        # conn.commit()

def main():
    #--------------extract--------
    # from load_staging import main
    # main()

    #----------transform
    run_sql_file("sql","03_transfrom_tables.sql")

    #--------Data Warehouse Load--------
    run_sql_file("etl","run_etl_pipeline.sql")

if __name__ == "__main__":
    main()