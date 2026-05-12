from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'dev',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_script(script_name):
    subprocess.run(["python", f"path/to/your/project/{script_name}"], check=True)

with DAG('academic_intelligence_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    
    ingest_api = PythonOperator(task_id='ingest_api_data', python_callable=run_script, op_args=['Api_producer.py'])
    ingest_mongo = PythonOperator(task_id='ingest_mongo_data', python_callable=run_script, op_args=['Mongodb_producer.py'])
    consume_data = PythonOperator(task_id='consume_to_snowflake', python_callable=run_script, op_args=['Api_consumer.py'])

    [ingest_api, ingest_mongo] >> consume_data
