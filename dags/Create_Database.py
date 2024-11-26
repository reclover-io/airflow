from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from components.create_database import create_batch_states_table, ensure_all_columns_exist, ensure_indexes_exist
from components.constants import *

DAG_NAME = 'DB_Migration'

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
    description='Create a batch_states table in the Airflow database.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['database', 'table']
) as dag:
    
    create_batch_states_task = PythonOperator(
        task_id='create_batch_states',
        python_callable=create_batch_states_table
    )
    
    ensure_all_columns_exist_task = PythonOperator(
        task_id='ensure_all_columns_exist',
        python_callable=ensure_all_columns_exist
    )

    ensure_indexes_exist_task = PythonOperator(
        task_id='ensure_indexes_exist',
        python_callable=ensure_indexes_exist
    )

    
    
    # Define Dependencies
    create_batch_states_task >> ensure_all_columns_exist_task >> ensure_indexes_exist_task