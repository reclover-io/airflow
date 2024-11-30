
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils.timezone import utcnow
from airflow.sensors.base import BaseSensorOperator
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
DAG_NAME = 'ELK_Mobile_App_Activity_Logs'

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
    'email': ['asdfasdfasdfdssadfsd@dasfasdfasdf.com'],
    'emailSuccess': [],
    'emailFail': [],
    'emailPause': [],
    'emailResume': [],
    'emailStart': []
}

DEFAULT_CSV_COLUMNS = ['RequestID', 'Path', 'UserToken', 'RequestDateTime', 'Status', 'CounterCode']

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
}

def process_data2(API_URL: str, TEMP_DIR: str, OUTPUT_DIR: str,CONTROL_DIR: str, API_HEADERS: Dict[str, str], DEFAULT_CSV_COLUMNS: List[str], default_emails: Dict[str, List[str]], slack_webhook: Optional[str] = None, **kwargs):
    """Process the data and handle notifications""" 
    print("Processing data")

def send_success_notification2(default_emails, slack_webhook=None, **context):
    """Send success notification"""
    print("Sending success notification")
    
    
class WaitUntilTimeSensor(BaseSensorOperator):
    """
    Custom sensor to wait until a specific datetime.
    """
    
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
    description='Fetch API data with date range and save to CSV',
    #schedule_interval="0 0 * * *",
    schedule_interval=None,
    start_date=datetime(2024, 11, 25),
    catchup=False,
    tags=['api', 'csv', 'backup']
) as dag:
    
    wait_for_start_time = WaitUntilTimeSensor(
        task_id="wait_for_start_time",
        poke_interval=60,  # Check every 30 seconds
        mode="reschedule",  # Release worker slot between checks
    )
    
    process_task2 = PythonOperator(
        task_id='process_data2',
        python_callable=process_data2,
        provide_context=True,
        retries=3,
        op_args=[API_URL,TEMP_DIR,OUTPUT_DIR,CONTROL_DIR,API_HEADERS,DEFAULT_CSV_COLUMNS, default_emails, slack_webhook],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    success_notification2 = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification2,
        provide_context=True,
        op_args=[default_emails, slack_webhook],
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )
    
    process_task2 >> wait_for_start_time >> success_notification2