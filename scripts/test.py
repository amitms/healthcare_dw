import yaml
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / "config" / "db_config.yaml"

with open(env_path,"r") as file:
    config = yaml.safe_load(file)
    db = config["database"]
    print(db['db_port'])