from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from components.file_deletion import delete_all_batch_files

# Path to the directories
DAG_NAME = "Manual_Batch_File_Deletion"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=1),
}

# Create the DAG
with DAG(
    DAG_NAME,
    default_args=default_args,
    description='DAG for deleting all batch files.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['file_deletion', 'batch_files', 'all']
) as dag:

    delete_all_batch_files_task = PythonOperator(
        task_id='delete_all_batch_files',
        python_callable=delete_all_batch_files,
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: print("\nWorkflow finished successfully!"),
    )

    # Define Task Dependencies
    delete_all_batch_files_task >> end_task
