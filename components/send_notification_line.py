import requests
import pytz

def get_dag_status(task_instances):
    for task_instance in task_instances:
        if task_instance.state in ['failed', 'up_for_retry', 'upstream_failed', 'skipped']:
            return "Fail"
    return "Successfully"

def send_notification_line(**kwargs):
    ti = kwargs['ti']
    total_records = ti.xcom_pull(key='total_records', task_ids='fetch_paginated_api_data')

    dag_id = kwargs['dag'].dag_id
    execution_date = kwargs['execution_date']
    status = get_dag_status(kwargs['dag_run'].get_task_instances())

    bangkok_tz = pytz.timezone('Asia/Bangkok')
    formatted_time = execution_date.astimezone(bangkok_tz).strftime('%Y/%m/%d %H:%M:%S')

    line_token = 'OCez5Tkd6dZQ2jyN5UTYqZKwN5oeyEpsiqUutCylmYl'  # Do this to env
    headers = {
        'Authorization': f'Bearer {line_token}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    message = f"\nDAG Name: {dag_id}\nTime: {formatted_time}\nStatus: {status}"
    response = requests.post(
        'https://notify-api.line.me/api/notify',
        headers=headers,
        data={'message': message}
    )
    response.raise_for_status()
    print("Notification sent successfully to Line")
