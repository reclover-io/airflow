from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pytz
import requests
import pandas as pd
import os
from datetime import timedelta, datetime
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Text
import logging

# Define the directory where the CSV will be saved
OUTPUT_DIR = "/opt/airflow/output"  # Replace with your desired output path
MAX_RECORDS_PER_REQUEST = 10000

# Database connection details (assuming a PostgreSQL database is used)
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

# API endpoint and headers
API_URL = 'http://34.124.138.144:8000/api/common/authentication'
HEADERS = {
    'Authorization': 'R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==', # Do this to env
    'Content-Type': 'application/json'
}

# Default arguments for the DAG with retries
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 24, 1, 0),  # DAG starts one day ago
    'retries': 3,  # Retry up to 3 times if the request fails
    'retry_delay': timedelta(seconds=1),  # Wait 5 minutes between retries
    'email': ['phurinatkantapayao@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# Define the DAG with a daily schedule at 1 AM
with DAG(
    'Batch_api_to_csv_with_dynamic_dates_backup',
    default_args=default_args,
    description='DAG to fetch paginated data from API and save incrementally to CSV',
    schedule_interval="0 1 * * *",  # Run daily at 1 AM
    catchup=False,  # Do not backfill
) as dag:

    # Function to fetch data from the API with pagination and save incrementally
    def fetch_data_and_save(**kwargs):
        search_after = None
        request_count = 0
        total_records = 0
        accumulated_ids = 0

        bangkok_tz = pytz.timezone('Asia/Bangkok')
        current_time = datetime.now(bangkok_tz)
        timestamp = datetime.now(bangkok_tz).strftime("%Y-%m-%d_%H.%M.%S")

        # # Default endDate is 1 AM of the execution date (same day as DAG run)
        end_date = current_time.replace(hour=1, minute=0, second=0, microsecond=0)

        # Default startDate is 1 AM of the day before execution_date
        start_date = end_date - timedelta(days=1)

        # Get startDate and endDate from DAG params (or use the calculated defaults)
        start_date_str = kwargs['dag_run'].conf.get('startDate', start_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        end_date_str = kwargs['dag_run'].conf.get('endDate', end_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])

        print(f"Start Date: {start_date_str}, End Date: {end_date_str}")

        # Prepare the base request body with dynamic startDate and endDate
        BASE_REQUEST_BODY = {
            "startDate": start_date_str,
            "endDate": end_date_str
        }

        # Make the initial request to fetch the total number of records
        initial_request_body = BASE_REQUEST_BODY.copy()
        response = requests.post(API_URL, headers=HEADERS, json=initial_request_body)
        response.raise_for_status()

        # Get the total number of records
        data = response.json()
        total_records = data.get('hits', {}).get('total', {}).get('value', 0)
        print(f"Total records to fetch: {total_records}")
        initial_hits = data.get('hits', {}).get('hits', [])

        # Store the total record count in xcom for use in notification
        kwargs['ti'].xcom_push(key='total_records', value=total_records)

        # Calculate the number of requests needed
        total_requests = (total_records + MAX_RECORDS_PER_REQUEST - 1) // MAX_RECORDS_PER_REQUEST
        print(f"Total requests required: {total_requests}")

        # Paginate through the results
        while request_count < total_requests:
            # Prepare request body for pagination
            request_body = BASE_REQUEST_BODY.copy()
            if search_after:
                request_body['search_after'] = search_after

            # Make the API request
            response = requests.post(API_URL, headers=HEADERS, json=request_body)
            response.raise_for_status()
            status_code = response.status_code

            # Get the response data
            data = response.json()

            # Log fetch details to database
            log_fetch_to_db(request_body, response.json(), status_code, kwargs)

            # Extract records (hits)
            hits = data.get('hits', {}).get('hits', [])

            # If hits is 0, fail immediately
            if not hits:
                error_message = "hits are 0, failing the task."
                logging.error(error_message)
                log_fetch_to_db(initial_request_body, response.json(), response.status_code, kwargs)
                raise ValueError(error_message)

            accumulated_ids += len(hits)
            
            # Prepare data for CSV
            rows = []
            for hit in hits:
                row = {
                    'MemberType': hit.get('MemberType', ''),
                    'Latitude': hit.get('Latitude', ''),
                    'Longitude': hit.get('Longitude', ''),
                    'Status': hit.get('Status', ''),
                    'DeviceOS': hit.get('DeviceOS', ''),
                    'ModelName': hit.get('ModelName', ''),
                    'UserToken': hit.get('UserToken', ''),
                    'RequestDateTime': hit.get('RequestDateTime', ''),
                    '_id': hit.get('_id', '')
                }
                rows.append(row)

            # Convert the list of dictionaries into a Pandas DataFrame
            df = pd.DataFrame(rows)

            # Ensure the output directory exists
            os.makedirs(OUTPUT_DIR, exist_ok=True)

            # Define the CSV file path
            csv_file_path = os.path.join(OUTPUT_DIR, f"batch_api_common_authentication_{timestamp}.csv")

            # Save DataFrame to CSV, append mode if file exists
            df.to_csv(csv_file_path, index=False, mode='a', header=not os.path.exists(csv_file_path))
            print(f"Batch {request_count+1} saved to {csv_file_path}")

            # Prepare the search_after field for the next request if more data exists
            if len(hits) == MAX_RECORDS_PER_REQUEST:
                last_hit = hits[-1]
                search_after = [last_hit['RequestDateTime'], last_hit['_id']]
                request_count += 1
            else:
                break

        # If total_records is 0, fail immediately
        if total_records == 0 or not initial_hits:
            error_message = "Total records are 0, failing the task."
            logging.error(error_message)
            log_fetch_to_db(initial_request_body, response.json(), response.status_code, kwargs)
            raise ValueError(error_message)

        # Verify if accumulated '_id' matches the total_records
        print(f"Total accumulated_ids: {accumulated_ids}")
        if accumulated_ids != total_records:
            error_message = f"Mismatch in total records: expected {total_records}, but got {accumulated_ids}"
            logging.error(error_message)
            raise ValueError(error_message)

    # Function to log fetch details to database
    def log_fetch_to_db(request_body, response_body, status_code, kwargs):
        try:
            engine = create_engine(DATABASE_URL)
            metadata = MetaData()

            # Define the fetch_logs table
            fetch_logs_table = Table('api_common_authentication_fetch_logs', metadata,
                                     Column('id', Integer, primary_key=True, autoincrement=True),
                                     Column('dag_id', String, nullable=False),
                                     Column('execution_date', String, nullable=False),
                                     Column('request_body', Text, nullable=False),
                                     Column('response_body', Text, nullable=False),
                                     Column('status_code', Integer, nullable=False)
                                     )

            metadata.create_all(engine)  # Create the table if it doesn't exist

            # Convert execution date to Bangkok timezone
            execution_date = kwargs['execution_date']
            bangkok_tz = pytz.timezone('Asia/Bangkok')
            execution_date_bangkok = execution_date.astimezone(bangkok_tz).strftime('%Y-%m-%d %H:%M:%S')

            dag_id = kwargs['dag'].dag_id

            # Insert log into the fetch_logs table
            with engine.connect() as connection:
                insert_statement = fetch_logs_table.insert().values(
                    dag_id=dag_id,
                    execution_date=execution_date_bangkok,
                    request_body=str(request_body),
                    response_body=str(response_body),
                    status_code=status_code
                )
                connection.execute(insert_statement)

            logging.info("Fetch log entry added to the database successfully.")

        except Exception as e:
            logging.error(f"Failed to log fetch to the database: {e}")

    # General function to get DAG status
    def get_dag_status(task_instances):
        status = "Successfully"
        for task_instance in task_instances:
            # Check if any task has failed, been up for retry, or skipped
            if task_instance.state in [State.FAILED, State.UP_FOR_RETRY, State.UPSTREAM_FAILED, State.SKIPPED]:
                return "Fail"
        return status

    # Function to simulate moving a file
    def csv_file(**kwargs):
        # Dummy task to simulate moving a file
        print("Simulating file move...")
        pass  # Do nothing for now

    # Function to simulate sending a notification line
    def send_notification_line(**kwargs):
        # Get data from xcom
        ti = kwargs['ti']
        total_records = ti.xcom_pull(key='total_records', task_ids='fetch_paginated_api_data')

        # Get DAG and task details
        dag_id = kwargs['dag'].dag_id
        execution_date = kwargs['execution_date']
        
        status = get_dag_status(kwargs['dag_run'].get_task_instances())

        # Convert execution_date to desired format
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        formatted_time = execution_date.astimezone(bangkok_tz).strftime('%Y/%m/%d %H:%M:%S')

        line_token = 'ZkRaMpRXeiVRPsLzj9Nuwa3BCZnNCuNMBSHHcNQC2lF' # Do this to env
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

    # Function to simulate sending a notification email
    def send_notification_email(**kwargs):
        # Get data from xcom
        ti = kwargs['ti']
        total_records = ti.xcom_pull(key='total_records', task_ids='fetch_paginated_api_data')

        # Get DAG and task details
        dag_id = kwargs['dag'].dag_id
        execution_date = kwargs['execution_date']
        status = "Successfully" if total_records > 0 else "Fail"  # Example status based on record count

        # Convert execution_date to desired format
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        formatted_time = execution_date.astimezone(bangkok_tz).strftime('%Y/%m/%d %H:%M:%S')

        status = get_dag_status(kwargs['dag_run'].get_task_instances())

        status_color = "green" if status == "Successfully" else "red"

        # Send email notification
        email_subject = f'Airflow: DAG "{kwargs["dag"].dag_id}" {status}'
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
        recipients = ['phurinatkantapayao@gmail.com']

        send_email(to=recipients, subject=email_subject, html_content=email_body)
        print("Notification sent successfully via email")

    # Function to simulate log to db
    def log_to_db(**kwargs):
        # Create a database engine using SQLAlchemy
        try:
            engine = create_engine(DATABASE_URL)
            metadata = MetaData()

            # Define the table structure if it doesn't exist
            logs_table = Table('api_common_authentication_logs_overall', metadata,
                               Column('id', Integer, primary_key=True, autoincrement=True),
                               Column('dag_id', String, nullable=False),
                               Column('execution_date', String, nullable=False),
                               Column('total_records', Integer, nullable=False),
                               Column('status', String, nullable=False)
                               )

            metadata.create_all(engine)  # Create the table if it doesn't exist

            # Get data from xcom
            ti = kwargs['ti']
            dag_id = kwargs['dag'].dag_id
            execution_date = kwargs['execution_date']

            # Convert execution_date to Bangkok timezone
            bangkok_tz = pytz.timezone('Asia/Bangkok')
            execution_date_bangkok = execution_date.astimezone(bangkok_tz).strftime('%Y-%m-%d %H:%M:%S')

            total_records = ti.xcom_pull(key='total_records', task_ids='fetch_paginated_api_data')
            
            status = get_dag_status(kwargs['dag_run'].get_task_instances())

            # Insert the log into the table
            with engine.connect() as connection:
                insert_statement = logs_table.insert().values(
                    dag_id=dag_id,
                    execution_date=execution_date_bangkok,
                    total_records=total_records,
                    status=status
                )
                connection.execute(insert_statement)

            logging.info("Log entry added to the database successfully.")

        except Exception as e:
            logging.error(f"Failed to log to the database: {e}")

    # Define the task to fetch data and save to CSV
    fetch_paginated_api_data = PythonOperator(
        task_id='fetch_paginated_api_data',
        python_callable=fetch_data_and_save,
        provide_context=True,  # To access params from the DAG run
        dag=dag,
    )

    # Task 1: Move the file (dummy task)
    csv_file_task = PythonOperator(
        task_id='csv_file',
        python_callable=csv_file,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task 2: Send notification (dummy task)
    send_notification_task_line = PythonOperator(
        task_id='send_notification_task_line',
        python_callable=send_notification_line,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    send_notification_task_email = PythonOperator(
        task_id='send_notification_task_email',
        python_callable=send_notification_email,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    log_to_dbs = PythonOperator(
        task_id='log_to_db',
        python_callable=log_to_db,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define task dependencies
    # fetch_paginated_api_data >> csv_file_task >> send_notification_task_line >> send_notification_task_email

    fetch_paginated_api_data >> csv_file_task >> send_notification_task_line >> log_to_dbs
    fetch_paginated_api_data >> csv_file_task >> send_notification_task_email >> log_to_dbs