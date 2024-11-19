
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pytz

from components.notifications import (
    send_running_notification,
    send_success_notification, 
    send_failure_notification
)
from components.process import process_data, check_pause_status
from components.constants import *
from components.uploadtoFTP import *
from components.validators import validate_input_task

API_URL = "http://34.124.138.144:8000/friendMBNotiSpending"
DAG_NAME = 'ELK_Friend_MB_Noti_Spending'

# API Configuration
API_HEADERS = {
    'Authorization': 'R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==',
    'Content-Type': 'application/json'
}

# Output Configuration
OUTPUT_DIR = f'/opt/airflow/data/batch/ELK_{DAG_NAME}'
TEMP_DIR = f'/opt/airflow/data/batch/temp'
CONTROL_DIR = f'/opt/airflow/data/batch/ELK_{DAG_NAME}'
slack_webhook = "https://hooks.slack.com/services/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"

default_emails = {
    'email': ['elk_team@gmail.com'],
    'emailSuccess': [],
    'emailFail': [],
    'emailPause': [],
    'emailResume': [],
    'emailStart': []
}

DEFAULT_CSV_COLUMNS = ['Status', 'RequestDateTime', 'BusinessCode', 'UserToken', 'RequestID', '_id', 'MerchantName', 'Path', 'OriginalAmount', 'CurencyCode']

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
    description='Fetch API data with date range and save to CSV',
    schedule_interval="* 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['api', 'csv', 'backup']
) as dag:
    
    validate_input = PythonOperator(
        task_id='validate_input',
        python_callable=validate_input_task,
        provide_context=True,
        retries=1,
        op_args=[DEFAULT_CSV_COLUMNS]
    )
    
    running_notification = PythonOperator(
        task_id='send_running_notification',
        python_callable=send_running_notification,
        provide_context=True,
        op_args=[default_emails, slack_webhook],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
        retries=3,
        op_args=[API_URL, TEMP_DIR, OUTPUT_DIR, CONTROL_DIR, API_HEADERS, DEFAULT_CSV_COLUMNS]
    )
    
    success_notification = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        provide_context=True,
        op_args=[default_emails, slack_webhook],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    failure_notification = PythonOperator(
        task_id='send_failure_notification',
        python_callable=send_failure_notification,
        provide_context=True,
        op_args=[default_emails, slack_webhook],
        trigger_rule=TriggerRule.ONE_FAILED
    )

    uploadtoFTP = PythonOperator(
        task_id='uploadtoFTP',
        python_callable=upload_csv_ctrl_to_ftp_server,
        provide_context=True
    )
    
    # Define Dependencies
    validate_input >> [running_notification, failure_notification]
    running_notification >> process_task >> [uploadtoFTP, failure_notification]
    uploadtoFTP >> [success_notification, failure_notification]
