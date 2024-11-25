from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from components.file_deletion import delete_old_batch_files

# Path to the directories
DAG_NAME = "Interval_Batch_File_Deletion"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    description='A simple DAG to clean up old batch files with an interval-based schedule.',
    schedule_interval='@daily',
    catchup=False,
    tags=['file_deletion', 'batch_files', 'interval']

) as dag:
    
    delete_old_batch_files_task = PythonOperator(
        task_id='delete_old_batch_files',
        python_callable=delete_old_batch_files,
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: print("Workflow finished successfully!"),
    )

    delete_old_batch_files_task >> end_task
