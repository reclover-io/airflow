from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pytz

from components.database import ensure_batch_states_table_exists
from components.notifications import (
    send_running_notification,
    send_success_notification, 
    send_failure_notification
)
from components.process import process_data, check_pause_status
from components.constants import *

API_URL = 'http://34.124.138.144:8000/api/common/authentication'
DAG_NAME = 'MobileApp_Activity_Logs'

# API Configuration
API_HEADERS = {
    'Authorization': 'R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==',
    'Content-Type': 'application/json'
}

# Output Configuration
OUTPUT_DIR = '/opt/airflow/output/batch_process'
TEMP_DIR = '/opt/airflow/output/temp'
CONTROL_DIR = '/opt/airflow/output/control'

DEFAULT_CSV_COLUMNS = [
    'MemberType', 'Latitude', 'Longitude', 'Status', 'DeviceOS', 
    'ModelName', 'UserToken', 'RequestDateTime', '_id'
]

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
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['api', 'csv', 'backup']
) as dag:
    
    create_table_task = PythonOperator(
        task_id='ensure_table_exists',
        python_callable=ensure_batch_states_table_exists
    )
    
    running_notification = PythonOperator(
        task_id='send_running_notification',
        python_callable=send_running_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
        retries=3,
        op_args=[API_URL,TEMP_DIR,OUTPUT_DIR,CONTROL_DIR,API_HEADERS,DEFAULT_CSV_COLUMNS],


    )
    
    check_pause_task = PythonOperator(
        task_id='check_pause',
        python_callable=check_pause_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=0
    )
    
    success_notification = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    failure_notification = PythonOperator(
        task_id='send_failure_notification',
        python_callable=send_failure_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    uploadtoFTP = PythonOperator(
        task_id='uploadtoFTP',
        python_callable=upload_csv_ctrl_to_ftp_server,
        provide_context=True,  
    )
    # Define Dependencies
    create_table_task >> running_notification >> process_task >> check_pause_task >> uploadtoFTP >> [success_notification, failure_notification]