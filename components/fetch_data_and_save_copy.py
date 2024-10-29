import pytz
import requests
import pandas as pd
import os
from datetime import timedelta, datetime
import logging
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, JSON
from sqlalchemy.orm import sessionmaker
import json
import shutil  # For moving files
from airflow.models import TaskInstance
from airflow.utils.state import State
from sqlalchemy.exc import IntegrityError

# Define constants
OUTPUT_DIR = "/opt/airflow/output"  # Final output path
TEMP_DIR = "/opt/airflow/output/temp"  # Temporary directory for paused tasks
MAX_RECORDS_PER_REQUEST = 1000
API_URL = 'http://34.124.138.144:8000/api/common/authentication/'
HEADERS = {
    'Authorization': 'R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==', # Do this to env
    'Content-Type': 'application/json'
}
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

# Setup SQLAlchemy engine and session
engine = create_engine(DATABASE_URL)
metadata = MetaData()
Session = sessionmaker(bind=engine)

# Define the state table to store the progress
fetch_state_table = Table('fetch_state', metadata,
                          Column('dag_id', String, primary_key=True),
                          Column('execution_date', String, primary_key=True),
                          Column('request_count', Integer),
                          Column('search_after', JSON),
                          Column('accumulated_ids', Integer),
                          Column('total_records', Integer),
                          Column('file_path', String)  # Add column to store CSV file path
                          )

metadata.create_all(engine)

def is_dag_paused(session, dag_id, response):
    """Check if the given DAG is paused."""
    try:
        if response is None or not response.ok:
            logging.info("Received response status 400. Setting result to True.")
            logging.info(f"400 ////// Query result for DAG {dag_id}")
            query = f"UPDATE dag SET is_paused = true WHERE dag_id =  '{dag_id}'"
            session.execute(query, {'dag_id': dag_id})
            session.commit()  

            result = session.execute(query).fetchone()
 
            return result['is_paused'] if result else False

           

        # ถ้า status code ไม่ใช่ 400, ทำการตรวจสอบสถานะของ DAG
        query = f"SELECT is_paused FROM dag WHERE dag_id = '{dag_id}'"
        result = session.execute(query).fetchone()

        logging.info(f"Query result for DAG {dag_id}: {result}")
        
        # ตรวจสอบว่ามีค่า 'is_paused' และ return ค่า
        return result['is_paused'] if result else False
    except Exception as e:
        logging.error(f"Error retrieving DAG status from database: {e}")
        return False

def fetch_data_and_save(**kwargs):
    # Create a new SQLAlchemy session
    session = Session()

    # Retrieve dag and execution details
    dag_id = kwargs['dag'].dag_id
    execution_date = kwargs['execution_date']
    task_instance = kwargs['ti']
    bangkok_tz = pytz.timezone('Asia/Bangkok')
    execution_date_bangkok = execution_date.astimezone(bangkok_tz).strftime('%Y-%m-%d %H:%M:%S')

    # Initialize progress state
    state = None
    try:
        # Fetch state from the database if it exists
        query = fetch_state_table.select().where(
            (fetch_state_table.c.dag_id == dag_id) &
            (fetch_state_table.c.execution_date == execution_date_bangkok)
        )
        result = session.execute(query).fetchone()

        if result:
            state = {
                'request_count': result['request_count'],
                'search_after': json.loads(result['search_after']) if result['search_after'] else None,
                'accumulated_ids': result['accumulated_ids'],
                'total_records': result['total_records'],
                'file_path': result['file_path']
            }

    except Exception as e:
        logging.error(f"Error retrieving state from database: {e}")

    # Set the initial values
    request_count = state['request_count'] if state else 0
    search_after = state['search_after'] if state else None
    accumulated_ids = state['accumulated_ids'] if state else 0
    total_records = state['total_records'] if state else None
    csv_file_path = state['file_path'] if state and state['file_path'] else None

    bangkok_tz = pytz.timezone('Asia/Bangkok')
    current_time = datetime.now(bangkok_tz)

    # Ensure temp directory exists
    os.makedirs(TEMP_DIR, exist_ok=True)

    # If this is the first time running or file_path is not set, create a new CSV file path in TEMP_DIR
    if request_count == 0 or not csv_file_path:
        timestamp = datetime.now(bangkok_tz).strftime("%Y-%m-%d_%H.%M.%S")
        csv_file_path = os.path.join(TEMP_DIR, f"batch_api_common_authentication_{timestamp}.csv")
    
    # Get startDate and endDate from DAG params (or use the calculated defaults)
    search_after_str = kwargs['dag_run'].conf.get('search_after')
    start_date_str = kwargs['dag_run'].conf.get('startDate')
    end_date_str = kwargs['dag_run'].conf.get('endDate')

    print(f"Start Date: {start_date_str}, End Date: {end_date_str}")

    BASE_REQUEST_BODY = {}
    if search_after_str:
        BASE_REQUEST_BODY["search_after"] = search_after_str
    if start_date_str:
        BASE_REQUEST_BODY["startDate"] = start_date_str
    if end_date_str:
        BASE_REQUEST_BODY["endDate"] = end_date_str

    print(f"Start Date: , End Date: {BASE_REQUEST_BODY}")

    # Make initial API request if it's a new run or if total_records is not yet set
    if request_count == 0 or total_records is None:
        initial_request_body = BASE_REQUEST_BODY.copy()
        response = requests.post(API_URL, headers=HEADERS, json=initial_request_body)
        logging.info(f"response:]{response}")  # Log the request body for debugging

        if response is None or not response.ok:

            # Check if DAG is paused and exit if true
            if is_dag_paused(session, dag_id, response):
                logging.info("DAG is paused. Exiting the task gracefully.")
                session.close()
                task_instance.state = State.UP_FOR_RESCHEDULE  # Indicate it needs to be resumed later
                task_instance.save_state()

        response.raise_for_status()

        data = response.json()
       

        total_records = data.get('hits', {}).get('total', {}).get('value', 0)
        initial_hits = data.get('hits', {}).get('hits', [])

        kwargs['ti'].xcom_push(key='total_records', value=total_records)
        print(f"Total records to fetch: {total_records}")

    if total_records is None:
        # If total_records is still None, fail the task
        logging.error("Total records could not be determined. Failing the task.")
        raise ValueError("Total records could not be determined.")

    total_requests = (total_records + MAX_RECORDS_PER_REQUEST - 1) // MAX_RECORDS_PER_REQUEST
    print(f"Total requests required: {total_requests}")

    # Loop through paginated requests
    while request_count < total_requests:
        # Prepare request body for pagination
        request_body = BASE_REQUEST_BODY.copy()
        if search_after:
            request_body['search_after'] = search_after

        logging.info(f"Request body: {request_body}")  # Log the request body for debugging
        response = requests.post(API_URL, headers=HEADERS, json=request_body)
        logging.info(f"response: {response} {response.status_code}")  # Log the request body for debugging

        # Check if DAG is paused and exit if true
        if is_dag_paused(session, dag_id, response):
            logging.info("DAG is paused. Exiting the task gracefully.")
            session.close()
            task_instance.state = State.UP_FOR_RESCHEDULE  # Indicate it needs to be resumed later
            task_instance.save_state()
                
       

        response.raise_for_status()
        status_code = response.status_code
       

        data = response.json()

        # Extract records (hits)
        hits = data.get('hits', {}).get('hits', [])
        if not hits:
            logging.error("Hits are 0, failing the task.")
            session.close()
            raise ValueError("No records returned from API.")

        accumulated_ids += len(hits)

        # Prepare data for CSV
        rows = [
            {
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
            
            for hit in hits
        ]

        df = pd.DataFrame(rows)
        df.to_csv(csv_file_path, index=False, mode='a', header=not os.path.exists(csv_file_path))
        print(f"Batch {request_count + 1} saved to {csv_file_path}")
        print(f"response***** {response}")

        # Update state after each successful batch
        if len(hits) == MAX_RECORDS_PER_REQUEST:
            last_hit = hits[-1]
            search_after = [last_hit['RequestDateTime'], last_hit['_id']]
            request_count += 1
        else:
            break

        # Save state to the database after every successful iteration
        try:
            # Upsert logic: Insert or Update if exists
            existing_state_query = fetch_state_table.select().where(
                (fetch_state_table.c.dag_id == dag_id) &
                (fetch_state_table.c.execution_date == execution_date_bangkok)
            )
            existing_state = session.execute(existing_state_query).fetchone()

            if existing_state:
                # Update existing entry
                update_stmt = fetch_state_table.update().where(
                    (fetch_state_table.c.dag_id == dag_id) &
                    (fetch_state_table.c.execution_date == execution_date_bangkok)
                ).values(
                    request_count=request_count,
                    search_after=json.dumps(search_after),
                    accumulated_ids=accumulated_ids,
                    total_records=total_records,
                    file_path=csv_file_path
                )
                session.execute(update_stmt)
            else:
                # Insert new entry
                insert_stmt = fetch_state_table.insert().values(
                    dag_id=dag_id,
                    execution_date=execution_date_bangkok,
                    request_count=request_count,
                    search_after=json.dumps(search_after),
                    accumulated_ids=accumulated_ids,
                    total_records=total_records,
                    file_path=csv_file_path
                )
                session.execute(insert_stmt)

            session.commit()

        except IntegrityError as e:
            logging.error(f"Error with database integrity: {e}")
            session.rollback()
        except Exception as e:
            logging.error(f"Error updating state in database: {e}")
            session.rollback()

    # Move file to final output directory if all data is successfully fetched
    if accumulated_ids == total_records:
        try:
            final_csv_path = os.path.join(OUTPUT_DIR, os.path.basename(csv_file_path))
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            shutil.move(csv_file_path, final_csv_path)
            logging.info(f"File moved from {csv_file_path} to {final_csv_path}")

            # Clear the state from the database since the task is done
            delete_stmt = fetch_state_table.delete().where(
                (fetch_state_table.c.dag_id == dag_id) &
                (fetch_state_table.c.execution_date == execution_date_bangkok)
            )
            session.execute(delete_stmt)
            session.commit()
        except Exception as e:
            logging.error(f"Error moving file to output directory or deleting state: {e}")
            session.rollback()
    else:
        error_message = f"Mismatch in total records: expected {total_records}, but got {accumulated_ids}"
        logging.error(error_message)
        session.close()
        raise ValueError(error_message)

    print(f"Total accumulated_ids: {accumulated_ids}")

    # Close the session
    session.close()