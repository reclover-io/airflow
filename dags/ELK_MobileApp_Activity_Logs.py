
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.sensors.time_sensor import TimeSensor
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
from components.check_previous_failed_batch import check_previous_failed_batch

API_URL = "http://34.124.138.144:8000/mobileAppActivity"
DAG_NAME = 'ELK_MobileApp_Activity_Logs'

# API Configuration
API_HEADERS = {
    'Authorization': 'R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==',
    'Content-Type': 'application/json'
}

# Output Configuration
OUTPUT_DIR = f'/opt/airflow/data/batch/{DAG_NAME}'
TEMP_DIR = f'/opt/airflow/data/batch/temp'
CONTROL_DIR = f'/opt/airflow/data/batch/{DAG_NAME}'
slack_webhook = ""

DEFAULT_CSV_COLUMNS = [
    'RequestID', 'Path', 'UserToken', 'RequestDateTime', '_id' , 'Status', 'CounterCode', 'Test'
]

default_emails = {
    'email': [],
    'emailSuccess': [],
    'emailFail': [],
    'emailPause': [],
    'emailResume': [],
    'emailStart': []
}

DEFAULT_CSV_COLUMNS = ['RequestID', 'Path', 'UserToken', 'RequestDateTime', '_id', 'Status', 'CounterCode']

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
    
    check_previous_fails = PythonOperator(
        task_id='check_previous_failed_batch',
        python_callable=check_previous_failed_batch,
        provide_context=True
    )
    
    validate_input = PythonOperator(
        task_id='validate_input',
        python_callable=validate_input_task,
        provide_context=True,
        retries=1,
        op_args=[DEFAULT_CSV_COLUMNS, default_emails]
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
        op_args=[API_URL,TEMP_DIR,OUTPUT_DIR,CONTROL_DIR,API_HEADERS,DEFAULT_CSV_COLUMNS, default_emails, slack_webhook],


    )
    
    success_notification = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        provide_context=True,
        op_args=[default_emails, slack_webhook],
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
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
        provide_context=True,
        op_args=[default_emails, slack_webhook],
        trigger_rule=TriggerRule.ALL_SUCCESS
        
    )
    
    # Define Dependencies
    check_previous_fails >> validate_input >> [running_notification, failure_notification]
    running_notification >> process_task >> [uploadtoFTP, failure_notification]
    uploadtoFTP >> [success_notification, failure_notification]
    process_task >> success_notification