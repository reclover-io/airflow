from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
from components.check_previous_failed_batch import check_previous_failed_batch
from components.notifications import (
    send_running_notification,
    send_success_notification, 
    send_failure_notification
)
from components.process import process_data_manual
from components.constants import *
from components.uploadtoFTP import *
from components.validators import validate_input_task_manual

API_HEADERS = {
    'Authorization': 'R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==',
    'Content-Type': 'application/json'
}
slack_webhook=""
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
}

default_emails = {
    'email': ['ebs_olm@aeon.co.th'],
    'emailSuccess': [],
    'emailFail': [],
    'emailPause': [],
    'emailResume': [],
    'emailStart': []
}

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

DAG_NAME="Manual_Run"
# Create the DAG
with DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:

    validate_input = PythonOperator(
        task_id='validate_input_task_manual',
        python_callable=validate_input_task_manual,
        provide_context=True,
        retries=0,
        op_args=[default_emails]
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
        op_args=[default_emails],
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data_manual,
        provide_context=True,
        retries=3,
        op_args=[API_HEADERS,default_emails,slack_webhook],
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
    
    success_notification = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        provide_context=True,
        op_args=[default_emails],
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )
    
    failure_notification = PythonOperator(
        task_id='send_failure_notification',
        python_callable=send_failure_notification,
        provide_context=True,
        op_args=[default_emails],
        trigger_rule=TriggerRule.ONE_FAILED
    )

    uploadtoFTP = PythonOperator(
        task_id='uploadtoFTP',
        python_callable=upload_csv_ctrl_to_ftp_server_manual,
        provide_context=True,
        op_args=[default_emails],
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    validate_input >> [failure_notification]
    validate_input >> wait_for_start_time >> [running_notification, process_task, failure_notification]
    process_task >> [uploadtoFTP, failure_notification]
    uploadtoFTP >> [success_notification, failure_notification]
    process_task >> success_notification