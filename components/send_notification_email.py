from airflow.utils.email import send_email
import pytz

def get_dag_status(task_instances):
    for task_instance in task_instances:
        if task_instance.state in ['failed', 'up_for_retry', 'upstream_failed', 'skipped']:
            return "Fail"
    return "Successfully"

def send_notification_email(**kwargs):
    ti = kwargs['ti']
    total_records = ti.xcom_pull(key='total_records', task_ids='fetch_paginated_api_data')

    dag_id = kwargs['dag'].dag_id
    execution_date = kwargs['execution_date']
    status = get_dag_status(kwargs['dag_run'].get_task_instances())

    bangkok_tz = pytz.timezone('Asia/Bangkok')
    formatted_time = execution_date.astimezone(bangkok_tz).strftime('%Y/%m/%d %H:%M:%S')

    status_color = "green" if status == "Successfully" else "red"

    email_subject = f'Airflow: DAG "{dag_id}" {status}'
    email_body = f"""
    <html>
        <body>
            <h2>DAG Run Report</h2>
            <p><strong>DAG Name:</strong> {dag_id}</p>
            <p><strong>Time:</strong> {formatted_time}</p>
            <p><strong>Status:</strong> <span style="color: {status_color};">{status}</span></p>
        </body>
    </html>
    """
    recipients = ['chadaphornt20@gmail.com']

    send_email(to=recipients, subject=email_subject, html_content=email_body)
    print("Notification sent successfully via email")
