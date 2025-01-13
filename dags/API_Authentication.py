from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum
from datetime import datetime, timedelta
from croniter import croniter
import pytz
from components.check_previous_failed_batch import check_previous_failed_batch
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils.timezone import utcnow
from airflow.sensors.base import BaseSensorOperator

from components.notifications import (
    send_running_notification,
    send_success_notification, 
    send_failure_notification
)
from components.process_v2 import *
from components.constants import *
from components.uploadtoFTP import *
from components.validators_v2 import *

local_tz = pendulum.timezone("Asia/Bangkok")

schedule_interval = "0 0 * * *"  # Adjust schedule interval as needed
now = datetime.now(pendulum.timezone("Asia/Bangkok"))
cron = croniter(schedule_interval, now)
start_date = cron.get_prev(datetime).astimezone(local_tz)

API_URL = "http://34.124.138.144:8000/mobileAppActivity"
DAG_NAME = 'API_Authentication'

# API Configuration
API_HEADERS = {
    'Authorization': 'R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==',
    'Content-Type': 'application/json'
}

csv_delimiter = ','
host_ftps = 'ftp://34.124.138.144:21'
username_ftps = 'airflow'
password_ftps = 'airflow'
path_ftp = '/ELK/daily/source_data/landing/ELK_Mobile_App_Activity_Logs'

# Output Configuration
OUTPUT_DIR = f'/opt/airflow/data/batch/{DAG_NAME}'
TEMP_DIR = f'/opt/airflow/data/batch/temp'
CONTROL_DIR = f'/opt/airflow/data/batch/{DAG_NAME}'
slack_webhook = ""

default_emails = {
    'email': ['aruethai.c@gmail.com'],
    'emailSuccess': ['test@test.com'],
    'emailFail': [],
    'emailPause': [],
    'emailResume': [],
    'emailStart': []
}

DEFAULT_CSV_COLUMNS = ['RequestID', 'UserToken', 'Path', 'CounterCode', 'Status', 'RequestDateTime']

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300)
}

class WaitUntilTimeSensor(BaseSensorOperator):

    def poke(self, context):
        dag_run_conf = context.get("dag_run").conf

        if dag_run_conf.get("start_run"):
            # Parse the `start_time` from parameters
            target_time = datetime.fromisoformat(dag_run_conf.get("start_run"))
            bangkok_tz = pytz.timezone('Asia/Bangkok')
            target_time = bangkok_tz.localize(target_time)

            # Get the current time in Bangkok timezone
            now = datetime.now(bangkok_tz)

            self.log.info(f"Waiting until {target_time}, current time is {now}")
            return now >= target_time
        else:
            self.log.info("No start_run provided in dag_run configuration.")
            return True

# Create the DAG
with DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=schedule_interval,
    start_date=start_date,
    catchup=False
) as dag:
    
    check_previous_fails = PythonOperator(
        task_id='check_previous_failed_batch',
        python_callable=check_previous_failed_batch,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    validate_input = PythonOperator(
        task_id='validate_input',
        python_callable=validate_input_task,
        provide_context=True,
        retries=1,
        op_args=[DEFAULT_CSV_COLUMNS, default_emails,csv_delimiter]
    )

    wait_for_start_time = WaitUntilTimeSensor(
        task_id="wait_for_start_time",
        poke_interval=60,  # Check every 30 seconds
        mode="reschedule",  # Release worker slot between checks
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    running_notification = PythonOperator(
        task_id='send_running_notification',
        python_callable=send_running_notification,
        provide_context=True,
        op_args=[default_emails, slack_webhook],
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )
    
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
        retries=3,
        op_args=[API_URL,TEMP_DIR,OUTPUT_DIR,CONTROL_DIR,API_HEADERS,DEFAULT_CSV_COLUMNS, default_emails, slack_webhook,csv_delimiter],
        trigger_rule=TriggerRule.ONE_SUCCESS

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
        python_callable=upload_csv_ctrl_to_ftp_server_v2,
        provide_context=True,
        op_args=[default_emails, host_ftps, username_ftps, password_ftps, path_ftp, slack_webhook],
        trigger_rule=TriggerRule.ALL_SUCCESS
        
    )
    
    # Define Dependencies
    # check_previous_fails >> validate_input >> [running_notification, failure_notification]
    validate_input >> [running_notification, failure_notification]
    validate_input >> wait_for_start_time >> check_previous_fails >> [running_notification, process_task, failure_notification]
    process_task >> [uploadtoFTP, failure_notification]
    uploadtoFTP >> [success_notification, failure_notification]
    process_task >> success_notification
