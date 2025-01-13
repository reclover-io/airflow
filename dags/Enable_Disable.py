from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from airflow import settings
from airflow.models import DagModel
import pytz 
import logging
logger = logging.getLogger(__name__)


# ฟังก์ชันสำหรับ Enable DAG
def enable_disable_dags(**kwargs):
    # ดึง dag_list จาก dag_run.conf
    enable = kwargs.get('dag_run').conf.get("enable",[])
    disable = kwargs.get('dag_run').conf.get("disable",[])

    # เปิด session เพื่อปรับสถานะ DAG
    with settings.Session() as session:

        if enable:
            for dag_id in enable:
                dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
                if dag:
                    dag.is_paused = False  # Unpause the DAG
                else:
                    logger.warning(f"DAG ID {dag_id} not found for enabling.")

        if disable:
            for dag_id in disable:
                dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
                if dag:
                    dag.is_paused = True  # Pause the DAG
                else:
                    logger.warning(f"DAG ID {dag_id} not found for disabling.")


        # Commit การเปลี่ยนแปลงใน database
        session.commit()
        session.close()


default_args = {
    'owner': 'airflow',
    'retries': 1,
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

with DAG(
    dag_id="Enable_Disable_DAG_Controller",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    wait_for_start_time = WaitUntilTimeSensor(
        task_id="wait_for_start_time",
        poke_interval=60, 
        mode="reschedule",  
        trigger_rule=TriggerRule.ALL_SUCCESS
    )


    enable_disable_dags_task = PythonOperator(
        task_id="enable_dags",
        python_callable=enable_disable_dags,
        provide_context=True, 
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

wait_for_start_time >> enable_disable_dags_task


