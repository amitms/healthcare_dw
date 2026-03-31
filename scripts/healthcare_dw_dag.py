from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

dag = DAG(
    'healthcare_dw_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def run_python_etl():
    subprocess.run(['python3', '/opt/airflow/etl/load_staging.py'], check=True)
    subprocess.run(['python3', '/opt/airflow/etl/run_etl.py'], check=True)

etl_task = PythonOperator(
    task_id='run_healthcare_etl',
    python_callable=run_python_etl,
    dag=dag
)