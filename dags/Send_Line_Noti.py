from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import requests
from typing import Dict
from datetime import datetime


class LineNotifier:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.api_url = "https://api.line.me/v2/bot/message/push"

    def send_message(self, to: str, messages: Dict) -> None:
        """ส่งข้อความไปยัง LINE OA"""
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "to": to,
            "messages": [messages]
        }
        try:
            response = requests.post(self.api_url, json=payload, headers=headers)
            if response.status_code == 200:
                print("Message sent to LINE successfully")
            else:
                error_msg = f"Failed to send message to LINE: Status code {response.status_code}, Response: {response.text}"
                print(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            print(f"Error sending message to LINE: {str(e)}")
            raise


def create_line_text_message(text: str) -> Dict:
    """สร้างข้อความในรูปแบบ Text Message สำหรับ LINE"""
    return {
        "type": "text",
        "text": text
    }

def call_message():
     # ตัวอย่างการใช้งาน
    ACCESS_TOKEN = "9T/JcMcnS9+pqbdlzZI8vW/UKBldvWeh/2l8CR2D1yNKuS+vawXn9X7IesPRDuslaKECFSUWVwOzgn95FWfzVktrZKp0R1RuQdVZWUJVZySJWyFfDfRL6bpc3nRC6z+8EVwsGfPmWirNaGEdd0u5uQdB04t89/1O/w1cDnyilFU="
    TO_USER_ID = "Uc0f3f827c4145c46392f3bb41a4d2b2c"

    notifier = LineNotifier(ACCESS_TOKEN)

    batch_name = "ExampleBatch"
    run_id = "run_12345"
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    message_text = (
        f"🔔 Batch Process Notification\n"
        f"Batch Name: {batch_name}\n"
        f"Run ID: {run_id}\n"
        f"Start Time: {start_time}\n"
        f"Status: Running"♣
    )

    message = create_line_text_message(message_text)
    notifier.send_message(TO_USER_ID, message)


with DAG(
    'Send_Line_Noti_Test',  # Use underscores instead of spaces
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

    call_line_message = PythonOperator(
        task_id='delete_dag_file',
        python_callable=call_message,
        provide_context=True
    )
