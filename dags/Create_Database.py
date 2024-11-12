from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from components.create_database import ensure_batch_states_table_exists
from components.constants import *

DAG_NAME = 'Create_Database'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
}

# Create the DAG
with DAG(
    DAG_NAME,
    default_args=default_args,
    description='Create a table in the Airflow database.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['database', 'table']
) as dag:
    
    create_table_task = PythonOperator(
        task_id='ensure_table_exists',
        python_callable=ensure_batch_states_table_exists
    )
    
    # Define Dependencies
    create_table_task 