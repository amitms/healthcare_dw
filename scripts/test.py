import os
from dotenv import load_dotenv
from dotenv import dotenv_values
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from decimal import Decimal
import oracledb
BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / "config" / "db_config.env"

load_dotenv(dotenv_path=env_path)
db_type = os.getenv("DB_TYPE")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_service = os.getenv("DB_SERVICE")
target_schema = os.getenv("TARGET_SCHEMA")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
config = dotenv_values(env_path)
print(db_port)