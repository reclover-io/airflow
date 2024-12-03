from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from components.line_notification import *

from typing import Dict
from datetime import datetime
def call_message():
 
    batch_name = "ExampleBatch"
    run_id = "run_12345"
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    message_text = (
        f"🔔 Batch Process Notification\n"
        f"Batch Name: {batch_name}\n"
        f"Run ID: {run_id}\n"
        f"Start Time: {start_time}\n"
        f"Status: Running"
    )
    message = create_line_text_message(message_text)
    send_line_message(message)


with DAG(
    'Send_Line_Noti_Test',  # Use underscores instead of spaces
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(seconds=1)
    },
    description='DAG to delete other DAGs',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['deleter', 'dynamic']
) as dag:

    call_line_message = PythonOperator(
        task_id='delete_dag_file',
        python_callable=call_message,
        provide_context=True
    )