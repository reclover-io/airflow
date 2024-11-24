from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# ฟังก์ชันสำหรับสร้างไฟล์ DAG ใหม่
def create_dag_file(**kwargs):
    config = kwargs['dag_run'].conf  # รับค่าคอนฟิกจากการรัน
    api_url = config.get('API_URL', 'http://default.api/url')
    dag_name = config.get('DAG_NAME', 'default_dag_name')
    csv_columns = config.get('DEFAULT_CSV_COLUMNS', ['col1', 'col2', 'col3'])
    authorization = config.get('AUTHORIZATION', 'default_authorization_token')
    schedule_interval = config.get('SCHEDULT_INTERVAL','0 0 * * *')
    email = config.get('EMAIL', [])
    emailSuccess = config.get('EMAIL_SUCCESS', [])
    emailFail = config.get('EMAIL_FAIL', [])
    emailPause = config.get('EMAIL_PAUSE', [])
    emailResume = config.get('EMAIL_RESUME', [])
    emailStart = config.get('EMAIL_START', [])

    # Template ของ DAG ใหม่ที่เหมือนกับ Friend_MB_Noti_Spending.py
    dag_content = f"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum
from datetime import datetime, timedelta
from components.check_previous_failed_batch import check_previous_failed_batch

from components.notifications import (
    send_running_notification,
    send_success_notification, 
    send_failure_notification
)
from components.process import process_data
from components.constants import *
from components.uploadtoFTP import *
from components.validators import validate_input_task

local_tz = pendulum.timezone("Asia/Bangkok")

start_date = (datetime.now(local_tz) - timedelta(days=1))

API_URL = "{api_url}"
DAG_NAME = '{dag_name}'

# API Configuration
API_HEADERS = {{
    'Authorization': '{authorization}',
    'Content-Type': 'application/json'
}}

# Output Configuration
OUTPUT_DIR = f'/opt/airflow/data/batch/{{DAG_NAME}}'
TEMP_DIR = f'/opt/airflow/data/batch/temp'
CONTROL_DIR = f'/opt/airflow/data/batch/{{DAG_NAME}}'
slack_webhook = ""

default_emails = {{
    'email': {email},
    'emailSuccess': {emailSuccess},
    'emailFail': {emailFail},
    'emailPause': {emailPause},
    'emailResume': {emailResume},
    'emailStart': {emailStart}
}}

DEFAULT_CSV_COLUMNS = {csv_columns}

# Default arguments for the DAG
default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
}}

# Create the DAG
with DAG(
    DAG_NAME,
    default_args=default_args,
    description='Fetch API data with date range and save to CSV',
    schedule_interval="{schedule_interval}",
    start_date=start_date,
    catchup=False,
    tags=['api', 'csv', 'backup']
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
        op_args=[DEFAULT_CSV_COLUMNS, default_emails]
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
        op_args=[API_URL,TEMP_DIR,OUTPUT_DIR,CONTROL_DIR,API_HEADERS,DEFAULT_CSV_COLUMNS, default_emails, slack_webhook],
        trigger_rule=TriggerRule.ALL_SUCCESS

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
    # check_previous_fails >> validate_input >> [running_notification, failure_notification]
    validate_input >> [running_notification, failure_notification]
    validate_input >> check_previous_fails >> [running_notification, failure_notification]
    running_notification >> process_task >> [uploadtoFTP, failure_notification]
    uploadtoFTP >> [success_notification, failure_notification]
    process_task >> success_notification
"""



    # สร้างไฟล์ DAG ใหม่
    dag_file_path = f"/opt/airflow/dags/{dag_name}.py"
    with open(dag_file_path, 'w') as f:
        f.write(dag_content)
    print(f"DAG file created at {dag_file_path}")

# สร้าง DAG หลัก
with DAG(
    'Generate_Dags',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(seconds=1)
    },
    description='DAG to generate other DAGs',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['generator', 'dynamic']
) as dag:

    generate_dag_task = PythonOperator(
        task_id='generate_dag_file',
        python_callable=create_dag_file,
        provide_context=True
    )


