from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os


# ฟังก์ชันสำหรับลบไฟล์ DAG
def delete_dag_file(**kwargs):
    config = kwargs['dag_run'].conf  # รับค่าคอนฟิกจากการรัน
    dag_name = config.get('DAG_NAME', 'default_dag_name')
    
    # Path ของไฟล์ที่ต้องการลบ
    dag_file_path = f"/opt/airflow/dags/{dag_name}.py"
    
    # ตรวจสอบและลบไฟล์
    if os.path.exists(dag_file_path):
        os.remove(dag_file_path)
        print(f"DAG file {dag_file_path} deleted successfully.")
    else:
        print(f"DAG file {dag_file_path} not found.")


# สร้าง DAG สำหรับลบไฟล์
with DAG(
    'Delete_Dags',
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

    delete_dag_task = PythonOperator(
        task_id='delete_dag_file',
        python_callable=delete_dag_file,
        provide_context=True
    )
