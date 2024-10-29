from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.utils.trigger_rule import TriggerRule

from components.fetch_data_and_save import fetch_data_and_save
from components.csv_file import csv_file
# from components.send_notification_line import send_notification_line
from components.send_notification_email import send_notification_email

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 24, 1, 0),
    'retries': 3,
    'retry_delay': timedelta(seconds=1),
    'email': ['chadaphornt20@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    'Batch_api_to_csv_with_dynamic_dates',
    default_args=default_args,
    description='DAG to fetch paginated data from API and save incrementally to CSV',
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:

    # Define the tasks using the functions from components
    fetch_paginated_api_data = PythonOperator(
        task_id='fetch_paginated_api_data',
        python_callable=fetch_data_and_save,
        dag=dag,
    )

    csv_file_task = PythonOperator(
        task_id='csv_file',
        python_callable=csv_file,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # send_notification_task_line = PythonOperator(
    #     task_id='send_notification_task_line',
    #     python_callable=send_notification_line,
    #     provide_context=True,
    #     dag=dag,
    #     trigger_rule=TriggerRule.ALL_DONE,
    # )

    send_notification_task_email = PythonOperator(
        task_id='send_notification_task_email',
        python_callable=send_notification_email,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define task dependencies
    #fetch_paginated_api_data >> csv_file_task >> [send_notification_task_line, send_notification_task_email]
    fetch_paginated_api_data >> csv_file_task