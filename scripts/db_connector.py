import yaml
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from decimal import Decimal
import oracledb

def db_connector():
    BASE_DIR = Path(__file__).resolve().parent.parent
    yaml_path = BASE_DIR / "config" / "db_config.yaml"
    with open(yaml_path,"r") as file:
        config = yaml.safe_load(file)        
    db = config["database"]  
    CONN_STR = f'oracle+oracledb://{db['db_user']}:{db['db_pass']}@{db['db_host']}:{db['db_port']}/?service_name={db['db_service']}'
    engine = create_engine(CONN_STR)
    return engine
 
