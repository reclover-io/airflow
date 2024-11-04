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
    'batch_api_to_csv_with_dynamic_dates_backup',
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
        retries=3
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
    
    # Define Dependencies
    create_table_task >> running_notification >> process_task >> check_pause_task >> [success_notification, failure_notification]