from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re
from airflow.exceptions import AirflowException
import requests
import json
import pandas as pd
import os
import shutil
from pathlib import Path
import time
from contextlib import contextmanager
import sqlalchemy
from sqlalchemy import create_engine, text
import pytz
from time import strptime
import csv

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
}

# API Configuration
API_URL = 'http://34.124.138.144:8000/api/common/authentication'
API_HEADERS = {
    'Authorization': 'R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==',
    'Content-Type': 'application/json'
}

# Output Configuration
OUTPUT_DIR = '/opt/airflow/output'
TEMP_DIR = '/opt/airflow/output/temp'
CONTROL_DIR = '/opt/airflow/output/control'
DB_CONNECTION = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'

DEFAULT_CSV_COLUMNS = [
    'MemberType', 'Latitude', 'Longitude', 'Status', 'DeviceOS', 
    'ModelName', 'UserToken', 'RequestDateTime', '_id'
]

THAI_TZ = pytz.timezone('Asia/Bangkok')

class APIException(Exception):
    pass

class NoDataException(Exception):
    pass

def get_thai_time() -> datetime:
    """Get current time in Thai timezone"""
    return datetime.now(THAI_TZ)

def format_thai_time(dt: datetime) -> str:
    """Format datetime to Thai timezone string without timezone info"""
    if dt.tzinfo is None:
        dt = THAI_TZ.localize(dt)
    thai_time = dt.astimezone(THAI_TZ)
    return thai_time.strftime('%Y-%m-%d %H:%M:%S')

def ensure_batch_states_table_exists():
    """Check if batch_states table exists, create if it doesn't"""
    engine = create_engine(DB_CONNECTION)
    
    try:
        with engine.connect() as connection:
            # Check if table exists
            check_table_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'batch_states'
                );
            """)
            
            table_exists = connection.execute(check_table_query).scalar()
            
            if not table_exists:
                print("Creating batch_states table...")
                create_table_query = text("""
                    CREATE TABLE IF NOT EXISTS batch_states (
                        batch_id VARCHAR(255),
                        run_id VARCHAR(255),
                        start_date TIMESTAMP WITH TIME ZONE NOT NULL,
                        end_date TIMESTAMP WITH TIME ZONE NOT NULL,
                        current_page INTEGER NOT NULL,
                        last_search_after JSONB,
                        status VARCHAR(50) NOT NULL,
                        error_message TEXT,
                        total_records INTEGER,
                        fetched_records INTEGER,
                        target_pause_time TIMESTAMP WITH TIME ZONE,
                        initial_start_time TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT timezone('Asia/Bangkok', NOW()),
                        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT timezone('Asia/Bangkok', NOW()),
                        PRIMARY KEY (batch_id, run_id)
                    );

                    CREATE INDEX IF NOT EXISTS idx_batch_states_status 
                    ON batch_states(status);
                    
                    CREATE INDEX IF NOT EXISTS idx_batch_states_updated_at 
                    ON batch_states(updated_at);
                """)
                
                connection.execute(create_table_query)
                print("batch_states table created successfully")
            else:
                print("batch_states table already exists")
                
                # Check if initial_start_time column exists
                check_column_query = text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_name = 'batch_states' 
                        AND column_name = 'initial_start_time'
                    );
                """)
                
                column_exists = connection.execute(check_column_query).scalar()
                
                if not column_exists:
                    print("Adding initial_start_time column...")
                    add_column_query = text("""
                        ALTER TABLE batch_states 
                        ADD COLUMN initial_start_time TIMESTAMP WITH TIME ZONE;

                        -- Update existing rows to set initial_start_time to created_at
                        UPDATE batch_states 
                        SET initial_start_time = created_at 
                        WHERE initial_start_time IS NULL;
                    """)
                    
                    connection.execute(add_column_query)
                    connection.execute(text("COMMIT;"))
                    print("initial_start_time column added successfully")
                else:
                    print("initial_start_time column already exists")
                    
    except Exception as e:
        print(f"Error ensuring table exists: {str(e)}")
        raise e

@contextmanager
def get_db_connection():
    """Get database connection using SQLAlchemy"""
    engine = create_engine(DB_CONNECTION)
    conn = engine.connect()
    trans = conn.begin()
    try:
        yield conn
        trans.commit()
    except:
        trans.rollback()
        raise
    finally:
        conn.close()
        engine.dispose()

def should_pause(pause_time: str, state_status: Optional[str], dag_id: str, run_id: str) -> bool:
    """
    Check if batch should pause based on time and state
    """
    try:
        # แยกส่วนของเวลาและแปลงเป็นตัวเลข
        time_parts = pause_time.split(':')
        pause_hour = int(time_parts[0])
        pause_minute = int(time_parts[1])
        seconds_parts = time_parts[2].split('.')
        pause_second = int(seconds_parts[0])
        pause_millisecond = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
        
        # สร้าง datetime object สำหรับเวลา pause
        pause_datetime = datetime.now(THAI_TZ).replace(
            hour=pause_hour,
            minute=pause_minute,
            second=pause_second,
            microsecond=pause_millisecond * 1000
        )
        
        # ตรวจสอบ state ใน database
        batch_state = get_batch_state(dag_id, run_id)
        current_datetime = datetime.now(THAI_TZ)
        
        print(f"Current datetime: {current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        print(f"Pause datetime: {pause_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        
        # กำหนดเวลาที่จะใช้ pause
        if batch_state and batch_state['status'] == 'PAUSED':
            # ถ้า resume จาก PAUSED ใช้เวลาพรุ่งนี้
            target_datetime = pause_datetime + timedelta(days=1)
            print("Resuming from PAUSED state, using tomorrow's pause time")
        else:
            if current_datetime >= pause_datetime:
                # ถ้าเลยเวลา pause แล้ว ใช้เวลาพรุ่งนี้
                target_datetime = pause_datetime + timedelta(days=1)
                print("Current time is after pause time, using tomorrow's pause time")
            else:
                # ถ้ายังไม่ถึงเวลา pause ใช้เวลาวันนี้
                target_datetime = pause_datetime
                print("Current time is before pause time, using today's pause time")
        
        print(f"Target datetime: {target_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        
        # เปรียบเทียบเวลาปัจจุบันกับเวลา pause ที่กำหนด
        should_pause = current_datetime >= target_datetime
        print(f"Should pause: {should_pause}")
        
        return should_pause
        
    except (ValueError, IndexError) as e:
        print(f"Invalid pause time format: {e}")
        return False
    
def check_current_time_vs_pause(pause_time: str) -> bool:
    """
    Check if current time has reached or passed pause time
    """
    try:
        current = datetime.now(THAI_TZ)
        
        # แยกส่วนของเวลา pause
        time_parts = pause_time.split(':')
        pause_hour = int(time_parts[0])
        pause_minute = int(time_parts[1])
        seconds_parts = time_parts[2].split('.')
        pause_second = int(seconds_parts[0])
        pause_millisecond = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
        
        # สร้าง datetime object สำหรับเวลา pause วันนี้
        pause_datetime = current.replace(
            hour=pause_hour,
            minute=pause_minute,
            second=pause_second,
            microsecond=pause_millisecond * 1000
        )
        
        print(f"Current time: {current.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        print(f"Pause time: {pause_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        
        return current >= pause_datetime
        
    except (ValueError, IndexError) as e:
        print(f"Error parsing pause time: {str(e)}")
        return False
    
def handle_pause(conf: Dict, state_status: Optional[str], dag_id: str, run_id: str) -> bool:
    """
    Handle batch pause if pause time is specified
    Returns True if should pause, False otherwise
    """
    pause_time = conf.get('pause')
    if not pause_time:
        print("No pause time specified")
        return False
    
    print(f"Checking pause time: {pause_time}")
    
    # ดึงข้อมูลจาก Database
    batch_state = get_batch_state(dag_id, run_id)
    current_datetime = datetime.now(THAI_TZ)
    
    # แยกส่วนของเวลา pause
    time_parts = pause_time.split(':')
    pause_hour = int(time_parts[0])
    pause_minute = int(time_parts[1])
    seconds_parts = time_parts[2].split('.')
    pause_second = int(seconds_parts[0])
    pause_millisecond = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
    
    # สร้าง datetime object สำหรับเวลา pause วันนี้
    pause_today = current_datetime.replace(
        hour=pause_hour,
        minute=pause_minute,
        second=pause_second,
        microsecond=pause_millisecond * 1000
    )
    
    # สร้าง datetime object สำหรับเวลา pause พรุ่งนี้
    pause_tomorrow = pause_today + timedelta(days=1)
    
    print(f"Current time: {current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}")
    print(f"Today's pause time: {pause_today.strftime('%Y-%m-%d %H:%M:%S.%f')}")
    
    # ถ้ามี target_pause_time ใน state ให้ใช้ค่านั้น
    target_pause_time = None
    if batch_state and batch_state.get('target_pause_time'):
        if isinstance(batch_state['target_pause_time'], str):
            target_pause_time = datetime.fromisoformat(batch_state['target_pause_time'].replace('Z', '+00:00'))
        else:
            target_pause_time = batch_state['target_pause_time']
        print(f"Using stored target pause time: {target_pause_time.strftime('%Y-%m-%d %H:%M:%S.%f')}")
    else:
        # กำหนด target_pause_time ครั้งแรก
        if current_datetime > pause_today:
            target_pause_time = pause_tomorrow
            print(f"Setting initial target pause time to tomorrow: {target_pause_time.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        else:
            target_pause_time = pause_today
            print(f"Setting initial target pause time to today: {target_pause_time.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        
        # ดึงค่าจาก state หรือใช้ค่าเริ่มต้น
        current_state = {
            'current_page': batch_state['current_page'] if batch_state else 1,
            'last_search_after': batch_state['last_search_after'] if batch_state else None,
            'total_records': batch_state['total_records'] if batch_state else None,
            'fetched_records': batch_state['fetched_records'] if batch_state else 0,
            'start_date': batch_state['start_date'] if batch_state else conf.get('startDate'),
            'end_date': batch_state['end_date'] if batch_state else conf.get('endDate')
        }
        
        # บันทึก target_pause_time ลง state
        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=current_state['start_date'],
            end_date=current_state['end_date'],
            current_page=current_state['current_page'],
            last_search_after=current_state['last_search_after'],
            status='RUNNING',
            error_message=None,
            total_records=current_state['total_records'],
            fetched_records=current_state['fetched_records'],
            target_pause_time=target_pause_time.isoformat()
        )
    
    # เช็คว่าถึงเวลา pause หรือยัง
    should_pause = current_datetime >= target_pause_time
    print(f"Should pause: {should_pause}")
    
    return should_pause
    
def get_next_pause_time(pause_time: str, current_datetime: datetime) -> datetime:
    """
    Calculate next pause time based on current time
    """
    # แยกส่วนของเวลา pause
    time_parts = pause_time.split(':')
    pause_hour = int(time_parts[0])
    pause_minute = int(time_parts[1])
    seconds_parts = time_parts[2].split('.')
    pause_second = int(seconds_parts[0])
    pause_millisecond = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
    
    # สร้าง datetime object สำหรับเวลา pause วันนี้
    pause_datetime = current_datetime.replace(
        hour=pause_hour,
        minute=pause_minute,
        second=pause_second,
        microsecond=pause_millisecond * 1000
    )
    
    # ถ้าเวลา pause อยู่ในอดีต ให้ใช้เวลาพรุ่งนี้
    if current_datetime > pause_datetime:
        return pause_datetime + timedelta(days=1)
    
    return pause_datetime

def save_batch_state(batch_id: str, run_id: str, start_date: str, end_date: str, 
                    current_page: int, last_search_after: Optional[List[str]], 
                    status: str, error_message: Optional[str] = None,
                    total_records: Optional[int] = None,
                    fetched_records: Optional[int] = None,
                    target_pause_time: Optional[str] = None,
                    initial_start_time: Optional[datetime] = None):
    """Save batch state to database"""
    try:
        with get_db_connection() as conn:
            # ตรวจสอบว่ามี state เดิมหรือไม่
            existing_state = get_batch_state(batch_id, run_id)
            
            # ถ้าไม่มี state เดิมและไม่ได้ระบุ initial_start_time ให้ใช้เวลาปัจจุบัน
            if not existing_state and initial_start_time is None:
                initial_start_time = get_thai_time()
            
            query = text("""
                INSERT INTO batch_states (
                    batch_id, run_id, start_date, end_date, current_page, 
                    last_search_after, status, error_message, 
                    total_records, fetched_records, updated_at,
                    target_pause_time, initial_start_time
                ) VALUES (
                    :batch_id, :run_id, :start_date, :end_date, :current_page, 
                    :last_search_after, :status, :error_message,
                    :total_records, :fetched_records, 
                    timezone('Asia/Bangkok', NOW()),
                    :target_pause_time, :initial_start_time
                )
                ON CONFLICT (batch_id, run_id) 
                DO UPDATE SET 
                    current_page = EXCLUDED.current_page,
                    last_search_after = EXCLUDED.last_search_after,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    total_records = EXCLUDED.total_records,
                    fetched_records = EXCLUDED.fetched_records,
                    target_pause_time = EXCLUDED.target_pause_time,
                    initial_start_time = COALESCE(batch_states.initial_start_time, EXCLUDED.initial_start_time),
                    updated_at = timezone('Asia/Bangkok', NOW())
            """)
            
            last_search_after_json = json.dumps(last_search_after) if last_search_after else None
            
            conn.execute(query, {
                'batch_id': str(batch_id),
                'run_id': str(run_id),
                'start_date': start_date,
                'end_date': end_date,
                'current_page': int(current_page) if current_page is not None else 1,
                'last_search_after': last_search_after_json,
                'status': str(status),
                'error_message': str(error_message) if error_message is not None else None,
                'total_records': int(total_records) if total_records is not None else None,
                'fetched_records': int(fetched_records) if fetched_records is not None else None,
                'target_pause_time': target_pause_time,
                'initial_start_time': initial_start_time
            })
            
    except Exception as e:
        print(f"Error saving batch state: {str(e)}")
        raise e

def get_batch_state(batch_id: str, run_id: str) -> Optional[Dict]:
    """Get batch state from database"""
    with get_db_connection() as conn:
        query = text("""
            SELECT start_date, end_date, current_page, last_search_after,
                   status, error_message, total_records, fetched_records,
                   run_id, updated_at, target_pause_time
            FROM batch_states
            WHERE batch_id = :batch_id 
            AND run_id = :run_id
            ORDER BY updated_at DESC
            LIMIT 1
        """)
        
        result = conn.execute(query, {
            'batch_id': batch_id,
            'run_id': run_id
        }).fetchone()
        
        if not result:
            return None
            
        last_search_after = None
        if result[3]:  # if last_search_after is not None
            try:
                if isinstance(result[3], str):
                    last_search_after = json.loads(result[3])
                elif isinstance(result[3], dict):
                    last_search_after = list(result[3].values())
                else:
                    last_search_after = result[3]
            except json.JSONDecodeError:
                print(f"Warning: Could not decode last_search_after value: {result[3]}")
                last_search_after = None
            
        return {
            'start_date': result[0],
            'end_date': result[1],
            'current_page': result[2],
            'last_search_after': last_search_after,
            'status': result[4],
            'error_message': result[5],
            'total_records': result[6],
            'fetched_records': result[7],
            'run_id': result[8],
            'updated_at': result[9],
            'target_pause_time': result[10]  # เพิ่ม target_pause_time
        }

    
def retry_api_call(func, max_retries=3, initial_delay=1):
    """Retry function with exponential backoff"""
    for attempt in range(max_retries):
        try:
            response = func()
            
            # Check status code
            if response.status_code != 200:
                raise APIException(f"API returned status code {response.status_code}")
            
            # Parse and check data
            data = response.json()
            if not data.get('hits', {}).get('hits', []):
                raise NoDataException("API returned no data")
            
            return data
            
        except (APIException, NoDataException, requests.exceptions.RequestException) as e:
            if attempt == max_retries - 1:
                raise e
                
            delay = initial_delay * (2 ** attempt)
            print(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds...")
            time.sleep(delay)

def send_email_notification(to: List[str], subject: str, html_content: str):
    """Send email notification"""
    try:
        send_email(
            to=to,
            subject=subject,
            html_content=html_content
        )
        print(f"Email sent successfully to: {to}")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

def format_running_message(dag_id: str, run_id: str, start_time: datetime, conf: Dict) -> str:
    """Format running notification message"""
    return f"""
        <h2>Batch Process {dag_id} Has Started</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Status:</strong> Starting New Process</p>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
        </ul>
    """

def format_success_message(dag_id: str, run_id: str, current_time: datetime, 
                         conf: Dict, csv_filename: str, control_filename: str,
                         batch_state: Optional[Dict] = None) -> str:
    """Format success notification message with processing details"""
    # Get initial start time
    start_time = get_initial_start_time(dag_id, run_id)
    if not start_time:
        start_time = current_time  # Fallback to current time if no initial time found
        
    # Ensure both times are in Thai timezone
    if current_time.tzinfo is None:
        current_time = THAI_TZ.localize(current_time)
    else:
        current_time = current_time.astimezone(THAI_TZ)
        
    if start_time.tzinfo is None:
        start_time = THAI_TZ.localize(start_time)
    else:
        start_time = start_time.astimezone(THAI_TZ)
    
    # Calculate elapsed time from initial start
    elapsed_time = current_time - start_time
    
    # Ensure elapsed time is positive
    if elapsed_time.total_seconds() < 0:
        print(f"Warning: Negative elapsed time detected. Start: {start_time}, Current: {current_time}")
        elapsed_time = abs(elapsed_time)
    
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    # Get progress information
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    
    # Calculate processing rate
    total_seconds = elapsed_time.total_seconds()
    processing_rate = (fetched_records / total_seconds) if total_seconds > 0 else 0
    
    return f"""
        <h2>Batch Process {dag_id} Has Completed Successfully</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>End Time:</strong> {format_thai_time(current_time)}</p>
        <p><strong>Total Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> Completed</p>
        
        <h3>Processing Summary:</h3>
        <ul>
            <li>Total Records Processed: {fetched_records:,}</li>
            <li>Average Processing Rate: {processing_rate:.2f} records/second</li>
        </ul>
        
        <h3>Output Information:</h3>
        <ul>
            <li>CSV Filename: {csv_filename}</li>
            <li>Control Filename: {control_filename}</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
        </ul>
    """

def format_error_message(dag_id: str, run_id: str, start_time: datetime, end_time: datetime, error_message: str, conf: Dict, batch_state: Optional[Dict] = None) -> str:
    """Format error notification message with progress details"""
    # Get progress information with safe default values
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records') if batch_state else None
    current_page = batch_state.get('current_page', 1) if batch_state else 1
    
    # Calculate progress percentage safely
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0
    
    # Calculate elapsed time
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    # Determine if this was a pause or failure
    if "Pausing batch process" in error_message:
        status = "Paused"
        title = "Has Been Paused"
    else:
        status = "Failed"
        title = "Has Failed"
    
    # Calculate processing rate safely
    elapsed_seconds = elapsed_time.total_seconds()
    if elapsed_seconds > 0 and fetched_records > 0:
        processing_rate = fetched_records / elapsed_seconds
    else:
        processing_rate = 0
    
    return f"""
        <h2>Batch Process {dag_id} {title}</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>End Time:</strong> {format_thai_time(end_time)}</p>
        <p><strong>Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> {status}</p>
        <p><strong>Error Message:</strong> {error_message}</p>
        
        <h3>Progress Information:</h3>
        <ul>
            <li>Records Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Current Page: {current_page}</li>
            <li>Processing Rate: {processing_rate:.2f} records/second</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
            <li>CSV Columns: {conf.get('csvColumns') or conf.get('csvColumn', [])}</li>
        </ul>
    """

def validate_datetime_format(date_str: str, field_name: str) -> Tuple[bool, Optional[str]]:
    """
    Validate datetime string format
    Returns (is_valid, error_message)
    Expected format: YYYY-MM-DD HH:mm:ss.SSS
    """
    try:
        if not date_str:
            return False, f"{field_name} is required"
            
        # แยกส่วนวันที่และเวลา
        date_parts = date_str.split(' ')
        if len(date_parts) != 2:
            return False, f"Invalid {field_name} format. Expected 'YYYY-MM-DD HH:mm:ss.SSS'"
            
        date_part, time_part = date_parts
        
        # ตรวจสอบรูปแบบวันที่
        date_components = date_part.split('-')
        if len(date_components) != 3:
            return False, f"Invalid date format in {field_name}. Expected 'YYYY-MM-DD'"
            
        year, month, day = date_components
        if not (len(year) == 4 and len(month) == 2 and len(day) == 2):
            return False, f"Invalid date components in {field_name}. Year should be 4 digits, month and day should be 2 digits"
            
        # ตรวจสอบรูปแบบเวลา
        if '.' not in time_part:
            return False, f"Missing milliseconds in {field_name}. Expected format 'HH:mm:ss.SSS'"
            
        time_components, milliseconds = time_part.split('.')
        if len(milliseconds) != 3:
            return False, f"Milliseconds should be 3 digits in {field_name}"
            
        hours, minutes, seconds = time_components.split(':')
        if not (len(hours) == 2 and len(minutes) == 2 and len(seconds) == 2):
            return False, f"Invalid time components in {field_name}. Hours, minutes, and seconds should be 2 digits"
            
        # ตรวจสอบค่าที่ถูกต้อง
        datetime.strptime(date_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
        # ตรวจสอบค่าตัวเลข
        if not (1 <= int(month) <= 12 and 1 <= int(day) <= 31):
            return False, f"Invalid date values in {field_name}"
        if not (0 <= int(hours) <= 23 and 0 <= int(minutes) <= 59 and 0 <= int(seconds) <= 59):
            return False, f"Invalid time values in {field_name}"
        if not (0 <= int(milliseconds) <= 999):
            return False, f"Invalid milliseconds value in {field_name}"
            
        return True, None
        
    except ValueError as e:
        return False, f"Invalid {field_name}: {str(e)}"
    except Exception as e:
        return False, f"Error validating {field_name}: {str(e)}"

def validate_date_range(start_date: str, end_date: str) -> Tuple[bool, Optional[str]]:
    """
    Validate that end_date is after start_date
    Returns (is_valid, error_message)
    """
    try:
        start = datetime.strptime(start_date.split('.')[0], '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(end_date.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
        if end <= start:
            return False, "End date must be after start date"
            
        return True, None
        
    except Exception as e:
        return False, f"Error comparing dates: {str(e)}"

def validate_config_dates(conf: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate date configurations
    Returns (is_valid, error_message)
    """
    start_date = conf.get('startDate')
    end_date = conf.get('endDate')
    
    # Validate start date format
    start_valid, start_error = validate_datetime_format(start_date, 'startDate')
    if not start_valid:
        return False, start_error
        
    # Validate end date format
    end_valid, end_error = validate_datetime_format(end_date, 'endDate')
    if not end_valid:
        return False, end_error
        
    # Validate date range
    range_valid, range_error = validate_date_range(start_date, end_date)
    if not range_valid:
        return False, range_error
        
    return True, None

def validate_email_list(emails: List[str]) -> bool:
    """Validate email addresses"""
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return all(re.match(email_pattern, email) for email in emails)

def validate_pause_time(pause_time: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate pause time format
    Returns (is_valid, error_message)
    Expected format: HH:mm:ss.SSS
    """
    if not pause_time:
        return True, None  # pause time is optional
        
    try:
        # Check basic format using regex
        time_pattern = r'^([0-1][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\.(\d{3})$'
        if not re.match(time_pattern, pause_time):
            return False, (
                "Invalid pause time format. Expected 'HH:mm:ss.SSS' where:\n"
                "- HH is hours (00-23)\n"
                "- mm is minutes (00-59)\n"
                "- ss is seconds (00-59)\n"
                "- SSS is milliseconds (000-999)"
            )
        
        # Split into components
        time_part, milliseconds = pause_time.split('.')
        hours, minutes, seconds = time_part.split(':')
        
        # Validate lengths
        if not (len(hours) == 2 and len(minutes) == 2 and len(seconds) == 2 and len(milliseconds) == 3):
            return False, "Each component of pause time must have the correct number of digits (HH:mm:ss.SSS)"
            
        # Convert to numbers and validate ranges
        hours_int = int(hours)
        minutes_int = int(minutes)
        seconds_int = int(seconds)
        milliseconds_int = int(milliseconds)
        
        if not (0 <= hours_int <= 23):
            return False, f"Hours must be between 00 and 23, got {hours}"
        if not (0 <= minutes_int <= 59):
            return False, f"Minutes must be between 00 and 59, got {minutes}"
        if not (0 <= seconds_int <= 59):
            return False, f"Seconds must be between 00 and 59, got {seconds}"
        if not (0 <= milliseconds_int <= 999):
            return False, f"Milliseconds must be between 000 and 999, got {milliseconds}"
            
        return True, None
        
    except ValueError as e:
        return False, f"Invalid pause time value: {str(e)}"
    except Exception as e:
        return False, f"Error validating pause time: {str(e)}"

def validate_config(conf: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate all configuration parameters
    Returns (is_valid, error_message)
    """
    # Validate emails
    is_valid, error_message = validate_email_config(conf)
    if not is_valid:
        return False, error_message
    
    # Validate dates
    is_valid, error_message = validate_config_dates(conf)
    if not is_valid:
        return False, error_message
    
    # Validate CSV columns
    is_valid, error_message = validate_csv_columns(conf)
    if not is_valid:
        return False, error_message
    
    # Validate pause time
    pause_time = conf.get('pause')
    is_valid, error_message = validate_pause_time(pause_time)
    if not is_valid:
        return False, error_message
    
    # Validate filename template
    filename_template = conf.get('csvName')
    is_valid, error_message = validate_filename_template(filename_template)
    if not is_valid:
        return False, error_message
    
    return True, None

def validate_csv_columns(conf: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate CSV columns configuration
    Returns (is_valid, error_message)
    """
    try:
        # ตรวจสอบว่ามีการระบุ csvColumns หรือไม่
        csv_columns = conf.get('csvColumns') or conf.get('csvColumn')
        if not csv_columns:
            return False, "CSV columns not specified in configuration"
        
        # ตรวจสอบว่าเป็น list หรือไม่
        if not isinstance(csv_columns, list):
            return False, "CSV columns must be a list"
        
        # ตรวจสอบว่ามี column หรือไม่
        if len(csv_columns) == 0:
            return False, "CSV columns list is empty"
        
        # ตรวจสอบว่าแต่ละ column เป็น string หรือไม่
        non_string_columns = [col for col in csv_columns if not isinstance(col, str)]
        if non_string_columns:
            return False, f"Invalid column types found: {non_string_columns}. All columns must be strings"
        
        # ตรวจสอบว่า column อยู่ใน DEFAULT_CSV_COLUMNS หรือไม่
        invalid_columns = [col for col in csv_columns if col not in DEFAULT_CSV_COLUMNS]
        if invalid_columns:
            return False, f"Invalid columns specified: {invalid_columns}. Available columns are: {DEFAULT_CSV_COLUMNS}"
            
        return True, None
        
    except Exception as e:
        return False, f"Error validating CSV columns: {str(e)}"
    
def validate_email_config(conf: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate email configurations
    Returns (is_valid, error_message)
    """
    email_types = {
        'email': 'Main email',
        'emailSuccess': 'Success email',
        'emailFail': 'Failure email',
        'emailPause': 'Pause email',
        'emailResume': 'Resume email',
        'emailStart': 'Start email'
    }
    
    # ตรวจสอบ email หลัก (required)
    main_emails = conf.get('email', [])
    if not main_emails:
        return False, "Main email addresses are required"
    
    if not isinstance(main_emails, list):
        return False, "Main email configuration must be a list"
    
    if not validate_email_list(main_emails):
        return False, "Invalid main email address format"
    
    # ตรวจสอบ email ประเภทอื่นๆ (optional)
    for email_key, email_desc in email_types.items():
        if email_key == 'email':
            continue  # ข้ามการตรวจสอบ email หลักเพราะตรวจไปแล้ว
            
        email_list = conf.get(email_key, [])
        if email_list:
            if not isinstance(email_list, list):
                return False, f"{email_desc} configuration must be a list"
            
            if not validate_email_list(email_list):
                return False, f"Invalid {email_desc.lower()} address format"
    
    return True, None

def is_manual_pause(error_message: Optional[str]) -> bool:
    """
    Check if the error is from manual pause (SIGTERM)
    """
    sigterm_messages = [
        "Task received SIGTERM signal",
        "Task received SIGKILL signal",
        "Task was cancelled externally"
    ]
    return error_message and any(msg in error_message for msg in sigterm_messages)

def format_manual_pause_message(dag_id: str, run_id: str, start_time: datetime, 
                              end_time: datetime, conf: Dict, batch_state: Optional[Dict] = None) -> str:
    """Format manual pause notification message with progress details"""
    # Get progress information with safe default values
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records') if batch_state else None
    current_page = batch_state.get('current_page', 1) if batch_state else 1
    
    # Calculate progress percentage safely
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0
    
    # Calculate elapsed time
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    # Calculate processing rate safely
    processing_rate = (fetched_records/elapsed_time.total_seconds()) if elapsed_time.total_seconds() > 0 else 0
    
    return f"""
        <h2>Batch Process {dag_id} Has Been Manually Paused</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Pause Time:</strong> {format_thai_time(end_time)}</p>
        <p><strong>Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> Manually Paused</p>
        <p><strong>Reason:</strong> Process was manually paused by operator</p>
        
        <h3>Progress Information:</h3>
        <ul>
            <li>Records Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Current Page: {current_page}</li>
            <li>Processing Rate: {processing_rate:.2f} records/second</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
            <li>CSV Columns: {conf.get('csvColumns') or conf.get('csvColumn', [])}</li>
        </ul>
        
        <p><em>Note: To resume this process, please run the batch again with the same Run ID.</em></p>
    """

def get_initial_start_time(batch_id: str, run_id: str) -> Optional[datetime]:
    """
    Get the initial start time of the batch from batch_states
    Returns time in Thai timezone
    """
    with get_db_connection() as conn:
        query = text("""
            SELECT created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Bangkok'
            FROM batch_states
            WHERE batch_id = :batch_id 
            AND run_id = :run_id
            ORDER BY created_at ASC
            LIMIT 1
        """)
        
        result = conn.execute(query, {
            'batch_id': batch_id,
            'run_id': run_id
        }).fetchone()
        
        if result and result[0]:
            # ตรวจสอบว่าเวลาที่ได้มา timezone หรือไม่
            if result[0].tzinfo is None:
                # ถ้าไม่มี timezone ให้เพิ่ม Thai timezone
                return THAI_TZ.localize(result[0])
            else:
                # ถ้ามี timezone อยู่แล้ว ให้แปลงเป็น Thai timezone
                return result[0].astimezone(THAI_TZ)
        return None

def get_notification_recipients(conf: Dict, notification_type: str) -> List[str]:
    """
    Get email recipients based on notification type
    notification_type: 'success' | 'normal' | 'fail' | 'pause' | 'resume' | 'start'
    Returns combined list of recipients for the specified type
    """
    # Get default recipients
    all_recipients = conf.get('email', [])
    if not isinstance(all_recipients, list):
        all_recipients = [all_recipients]
    
    if notification_type == 'success':
        # Special handling for success notifications
        success_recipients = conf.get('emailSuccess', [])
        if not isinstance(success_recipients, list):
            success_recipients = [success_recipients]
        return success_recipients
    
    # Get type-specific recipients
    type_map = {
        'fail': 'emailFail',
        'pause': 'emailPause',
        'resume': 'emailResume',
        'start': 'emailStart'
    }
    
    if notification_type in type_map:
        type_recipients = conf.get(type_map[notification_type], [])
        if not isinstance(type_recipients, list):
            type_recipients = [type_recipients]
        
        # Combine with default recipients
        combined_recipients = list(set(all_recipients + type_recipients))
        return combined_recipients
    
    # Return default recipients for unspecified types
    return all_recipients
    
def get_control_file_config(conf: Dict, dag_id: str, timestamp: datetime) -> Tuple[str, str]:
    """
    Get control file path and name configuration
    Returns (control_path, control_filename)
    """
    # ใช้ path จาก config หรือใช้ default
    control_path = conf.get('controlPath', CONTROL_DIR)
    
    # สร้าง directory ถ้ายังไม่มี
    os.makedirs(control_path, exist_ok=True)
    
    # ใช้ template จาก config หรือใช้ default
    control_template = conf.get('controlName')
    if control_template:
        control_filename = get_formatted_filename(control_template, dag_id, timestamp)
        # เปลี่ยนนามสกุลไฟล์เป็น .ctrl
        control_filename = os.path.splitext(control_filename)[0] + '.ctrl'
    else:
        # ใช้ default format
        control_filename = f"{dag_id}_{timestamp.strftime('%Y%m%d%H%M%S')}.ctrl"
    
    return control_path, control_filename

def create_control_file(start_date: str, total_records: int, csv_filename: str,
                       dag_id: str, conf: Dict) -> Tuple[str, str]:
    """
    Create control file with summary information
    Returns (control_path, control_filename)
    """
    try:
        # แปลง start_date เป็น data_dt (เอาเฉพาะวันที่)
        data_dt = datetime.strptime(start_date.split()[0], '%Y-%m-%d').strftime('%Y-%m-%d')
        
        # ใช้เวลาปัจจุบันเป็น process_date
        process_time = get_thai_time()
        process_date = process_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Get control file configuration
        control_path, control_filename = get_control_file_config(conf, dag_id, process_time)
        
        # สร้าง DataFrame สำหรับ control file
        control_data = {
            'data_dt': [data_dt],
            'total_recs': [total_records],
            'process_date': [process_date],
            'file_name': [csv_filename]
        }
        control_df = pd.DataFrame(control_data)
        
        # กำหนด path เต็ม
        full_path = os.path.join(control_path, control_filename)
        
        # บันทึกไฟล์ control
        control_df.to_csv(
            full_path,
            sep='|',
            index=False,
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\',
            doublequote=True
        )
        
        print(f"Created control file: {full_path}")
        print(f"Control file content:")
        print(control_df.to_string(index=False))
        
        return control_path, control_filename
        
    except Exception as e:
        raise AirflowException(f"Error creating control file: {str(e)}")

def get_csv_columns(conf: Dict) -> List[str]:
    """
    Get CSV columns from config or use default if not specified
    """
    # Check both possible keys for backwards compatibility
    csv_columns = conf.get('csvColumns') or conf.get('csvColumn', DEFAULT_CSV_COLUMNS)
    
    # Validate that all specified columns exist in the default columns
    invalid_columns = [col for col in csv_columns if col not in DEFAULT_CSV_COLUMNS]
    if invalid_columns:
        raise AirflowException(f"Invalid columns specified: {invalid_columns}. "
                             f"Available columns are: {DEFAULT_CSV_COLUMNS}")
    
    return csv_columns

def fetch_data_page(start_date: str, end_date: str, search_after: Optional[List[str]] = None) -> Tuple[List[Dict], int, Optional[List[str]]]:
    """Fetch a single page of data from the API with retries"""
    payload = {
        "startDate": start_date,
        "endDate": end_date
    }
    
    if search_after:
        payload["search_after"] = search_after
    
    print(f"Fetching data with payload: {json.dumps(payload, indent=2)}")
    
    def make_request():
        return requests.get(API_URL, headers=API_HEADERS, json=payload)
    
    # Make API call with retries
    data = retry_api_call(make_request)
    
    hits = data.get('hits', {})
    records = hits.get('hits', [])
    total = hits.get('total', {}).get('value', 0)
    
    if records:
        last_record = records[-1]
        next_search_after = [
            last_record.get('RequestDateTime'),
            last_record.get('_id')
        ]
    else:
        next_search_after = None
    
    return records, total, next_search_after

def get_temp_file_path(batch_id: str, start_date: str) -> str:
    """Get the path for temporary CSV file"""
    # ใช้ start_date เป็นส่วนหนึ่งของชื่อไฟล์เพื่อให้แยกแต่ละ batch ได้
    date_str = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y%m%d')
    return os.path.join(TEMP_DIR, f"temp_{batch_id}_{date_str}.csv")

def save_temp_data(records: List[Dict], temp_file: str, headers: bool = False, columns: List[str] = None):
    """
    Save data to temporary CSV file with specified columns in exact order using | as separator
    """
    # Create DataFrame from records
    df = pd.DataFrame(records)
    
    # Use specified columns or default if none provided
    columns_to_use = columns if columns else DEFAULT_CSV_COLUMNS
    
    # Verify all required columns exist in the data
    missing_columns = [col for col in columns_to_use if col not in df.columns]
    if missing_columns:
        raise AirflowException(f"Missing columns in data: {missing_columns}")
    
    # Select and reorder columns - explicitly create new DataFrame with ordered columns
    ordered_df = pd.DataFrame(columns=columns_to_use)
    for col in columns_to_use:
        ordered_df[col] = df[col]
    
    # Save to CSV with | separator
    ordered_df.to_csv(
        temp_file,
        mode='a',
        header=headers,
        index=False,
        sep='|',
        escapechar='\\',  # ใช้ \ เป็น escape character
        doublequote=True,  # ใช้ double quotes สำหรับ field ที่มี | อยู่ในข้อมูล
        quoting=csv.QUOTE_MINIMAL  # ใส่ quotes เฉพาะเมื่อจำเป็น
    )
    print(f"Saved data with ordered columns: {columns_to_use} using | separator")

def fetch_and_save_data(start_date: str, end_date: str, dag_id: str, run_id: str, conf: Dict) -> str:
    """Fetch all data from API using pagination and save to CSV with state management"""

    # Get output configuration
    output_path, filename_template = get_output_config(conf, dag_id)

    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        csv_columns = get_csv_columns(conf)
        print(f"Using CSV columns: {csv_columns}")
    except AirflowException as e:
        print(f"Error in CSV columns configuration: {str(e)}")
        raise
    
    batch_state = get_batch_state(dag_id, run_id)
    state_status = batch_state['status'] if batch_state else None
    
    print(f"Current batch state: {state_status}")
    
    thai_timestamp = get_thai_time().strftime('%Y-%m-%d_%H.%M.%S')
    temp_file_path = os.path.join(TEMP_DIR, f"temp_{dag_id}_{run_id}.csv")
    
    should_write_header = True
    
    # ตรวจสอบ state และกำหนดค่าเริ่มต้น
    if batch_state and batch_state['status'] == 'PAUSED':
        # กรณีที่ resume จาก PAUSED
        search_after = batch_state['last_search_after']
        page = batch_state['current_page']
        total_records = batch_state['total_records']
        fetched_count = batch_state['fetched_records']
        print(f"Resuming from PAUSED state at page {page} with {fetched_count} records already fetched")
        print(f"Last search after: {search_after}")
        
        if os.path.exists(temp_file_path):
            print(f"Found existing temp file from PAUSED state: {temp_file_path}")
            should_write_header = False
            print("Will append to existing file without header")
        else:
            print("Warning: No temp file found from PAUSED state, starting with new file")
            should_write_header = True
            
    elif batch_state and batch_state['status'] == 'RUNNING':
        # กรณีที่ resume จาก RUNNING (error case)
        search_after = batch_state['last_search_after']
        page = batch_state['current_page']
        total_records = batch_state['total_records']
        fetched_count = batch_state['fetched_records']
        print(f"Resuming from RUNNING state at page {page}")
        print(f"Last search after: {search_after}")
        
        if os.path.exists(temp_file_path):
            should_write_header = False
            print("Will append to existing file without header")
        else:
            print("Warning: No temp file found from RUNNING state, starting with new file")
            should_write_header = True
            
    else:
        # กรณีเริ่มใหม่
        search_after = None
        page = 1
        total_records = None
        fetched_count = 0
        should_write_header = True
        print("Starting new batch process")
        
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            print(f"Removed existing temp file: {temp_file_path}")
    
    try:
        # ส่งพารามิเตอร์เพิ่มเติมให้ handle_pause
        if handle_pause(conf, state_status, dag_id, run_id):
            save_batch_state(
                batch_id=dag_id,
                run_id=run_id,
                start_date=start_date,
                end_date=end_date,
                current_page=page,
                last_search_after=search_after,
                status='PAUSED',
                error_message=f"Batch paused at {format_thai_time(get_thai_time())}",
                total_records=total_records,
                fetched_records=fetched_count
            )
            msg = f"Batch paused at {format_thai_time(get_thai_time())} due to configured pause time"
            print(msg)
            if os.path.exists(temp_file_path):
                print(f"Keeping temp file for continuation: {temp_file_path}")
            return {'status': 'paused', 'message': msg}
        
        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            current_page=page,
            last_search_after=search_after,
            status='RUNNING',
            error_message=None,
            total_records=total_records,
            fetched_records=fetched_count
        )
        
        while True:
            # ตรวจสอบเวลา pause ก่อนดึงข้อมูล
            if handle_pause(conf, state_status, dag_id, run_id):
                print("Pause condition met during processing")
                save_batch_state(
                    batch_id=dag_id,
                    run_id=run_id,
                    start_date=start_date,
                    end_date=end_date,
                    current_page=page,
                    last_search_after=search_after,
                    status='PAUSED',
                    error_message=None,
                    total_records=total_records,
                    fetched_records=fetched_count
                )
                msg = f"Batch paused at {format_thai_time(get_thai_time())}"
                print(msg)
                return {'status': 'paused', 'message': msg}
            
            print(f"\nFetching page {page}...")
            
            try:
                records, total, next_search_after = fetch_data_page(start_date, end_date, search_after)
            except (APIException, NoDataException) as e:
                save_batch_state(
                    batch_id=dag_id,
                    run_id=run_id,
                    start_date=start_date,
                    end_date=end_date,
                    current_page=page,
                    last_search_after=search_after,
                    status='FAILED',
                    error_message=str(e),
                    total_records=total_records,
                    fetched_records=fetched_count
                )
                raise e
            
            if handle_pause(conf, state_status, dag_id, run_id):
                save_batch_state(
                    batch_id=dag_id,
                    run_id=run_id,
                    start_date=start_date,
                    end_date=end_date,
                    current_page=page,
                    last_search_after=search_after,
                    status='PAUSED',
                    error_message=f"Batch paused at {format_thai_time(get_thai_time())}",
                    total_records=total_records,
                    fetched_records=fetched_count
                )
                msg = f"Batch paused at {format_thai_time(get_thai_time())} due to configured pause time"
                print(msg)
                if os.path.exists(temp_file_path):
                    print(f"Keeping temp file for continuation: {temp_file_path}")
                return {'status': 'paused', 'message': msg}
            
            if total_records is None:
                total_records = total
                print(f"Total records to fetch: {total_records}")
            
            current_page_size = len(records)
            print(f"Current page size: {current_page_size}")
            
            if current_page_size > 0:
                save_temp_data(records, temp_file_path, headers=should_write_header, columns=csv_columns)
                should_write_header = False
                
                fetched_count += current_page_size
                print(f"Fetched page {page}, got {current_page_size} records. "
                      f"Total fetched: {fetched_count}/{total_records}")
                
                save_batch_state(
                    batch_id=dag_id,
                    run_id=run_id,
                    start_date=start_date,
                    end_date=end_date,
                    current_page=page,
                    last_search_after=next_search_after,
                    status='RUNNING',
                    error_message=None,
                    total_records=total_records,
                    fetched_records=fetched_count
                )
            
            if (fetched_count >= total_records or 
                current_page_size < 1000 or 
                current_page_size == 0):
                print(f"Stopping pagination: fetched_count={fetched_count}, "
                      f"total_records={total_records}, "
                      f"current_page_size={current_page_size}")
                break
                
            search_after = next_search_after
            page += 1
        
        thai_time = get_thai_time()
        final_filename = get_formatted_filename(filename_template, dag_id, thai_time)
        final_path = os.path.join(output_path, final_filename)

        # ถ้า directory ไม่มี ให้สร้างใหม่
        os.makedirs(os.path.dirname(final_path), exist_ok=True)

        
        shutil.move(temp_file_path, final_path)
        print(f"Moved temp file to: {final_path}")
        
        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            current_page=page,
            last_search_after=search_after,
            status='COMPLETED',
            error_message=None,
            total_records=total_records,
            fetched_records=fetched_count
        )
        
        return final_path, final_filename
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        if os.path.exists(temp_file_path):
            print(f"Keeping temp file for continuation: {temp_file_path}")
        
        # อัพเดทสถานะเป็น FAILED เมื่อเกิด error
        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            current_page=page,
            last_search_after=search_after,
            status='FAILED',
            error_message=str(e),
            total_records=total_records,
            fetched_records=fetched_count
        )
        raise e
    
def send_notification(subject: str, html_content: str, conf: Dict, notification_type: str):
    """
    Send notification to appropriate recipients based on type
    """
    recipients = get_notification_recipients(conf, notification_type)
    if not recipients:
        print(f"No recipients specified for notification type: {notification_type}")
        return
    
    try:
        send_email_notification(recipients, subject, html_content)
        print(f"Notification sent to {notification_type} recipients: {recipients}")
    except Exception as e:
        print(f"Failed to send {notification_type} notification: {str(e)}")
    
def send_running_notification(**context):
    """Send notification when DAG starts running or resumes"""
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    start_time = get_thai_time()
    conf = dag_run.conf or {}
    
    context['task_instance'].xcom_push(key='batch_start_time', value=start_time.isoformat())
    
    previous_state = get_batch_state(dag_id, run_id)
    is_resume = previous_state is not None
    
    if is_resume:
        print(f"Found previous state with status: {previous_state.get('status')}")
        subject = f"Batch Process {dag_id} Resumed at {format_thai_time(start_time)}"
        html_content = format_resume_message(dag_id, run_id, start_time, conf, previous_state)
        
        # Send resume notification
        send_notification(subject, html_content, conf, 'resume')
    else:
        print("No previous state found, sending start notification")
        subject = f"Batch Process {dag_id} Started at {format_thai_time(start_time)}"
        html_content = format_running_message(dag_id, run_id, start_time, conf)
        
        # Send start notification
        send_notification(subject, html_content, conf, 'start')

def handle_task_failure(context, pause_exception_class):
    """Handle task failure and determine if retry is needed"""
    exception = context.get('exception')
    if isinstance(exception, pause_exception_class):
        # ถ้าเป็น BatchPauseException จะไม่ retry
        context['task_instance'].max_tries = 1
        print("Task paused - no retry needed")
    else:
        # กรณีอื่นๆ ให้ retry ตามปกติ
        print(f"Task failed with {type(exception).__name__} - will retry")

def process_data(**context):
    """Process the data and handle notifications"""
    try:
        ti = context['task_instance']
        dag_run = context['dag_run']
        conf = dag_run.conf or {}
        
        # Validate configuration
        is_valid, error_message = validate_config(conf)
        if not is_valid:
            print(f"Configuration validation failed: {error_message}")
            ti.xcom_push(key='error_message', value=f"Configuration Error: {error_message}")
            raise AirflowException(f"Configuration Error: {error_message}")
        
        start_date = conf.get('startDate')
        end_date = conf.get('endDate')
        run_id = dag_run.run_id
        
        print(f"Processing with parameters: start_date={start_date}, "
              f"end_date={end_date}, run_id={run_id}")
        
        # Store start time
        ti.xcom_push(key='batch_start_time', value=get_thai_time().isoformat())
        
        try:
            # Check for pause before starting
            if handle_pause(conf, None, dag_run.dag_id, run_id):
                msg = f"Initial pause check at {format_thai_time(get_thai_time())}"
                print(msg)
                return {'status': 'paused', 'message': msg}
            
            result = fetch_and_save_data(
                start_date=start_date,
                end_date=end_date,
                dag_id=dag_run.dag_id,
                run_id=run_id,
                conf=conf
            )
            
            if isinstance(result, dict) and result.get('status') == 'paused':
                return result
            
            output_path, csv_filename = result
            ti.xcom_push(key='output_filename', value=csv_filename)
            
            # Get batch state for total records
            batch_state = get_batch_state(dag_run.dag_id, run_id)
            total_records = batch_state.get('total_records', 0) if batch_state else 0
            
            # Create control file
            control_path, control_filename = create_control_file(
                start_date=start_date,
                total_records=total_records,
                csv_filename=csv_filename,
                dag_id=dag_run.dag_id,
                conf=conf
            )
            
            # Store control filename in XCom
            ti.xcom_push(key='control_filename', value=control_filename)
            
            return (output_path, csv_filename, control_path, control_filename)
            
        except Exception as e:
            error_msg = str(e)
            ti.xcom_push(key='error_message', value=error_msg)
            raise AirflowException(error_msg)
            
    except Exception as e:
        error_msg = str(e)
        ti.xcom_push(key='error_message', value=error_msg)
        raise AirflowException(error_msg)

def send_success_notification(**context):
    """Send success notification"""
    ti = context['task_instance']
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    conf = dag_run.conf or {}
    
    # Get process result
    process_result = ti.xcom_pull(task_ids='process_data')
    
    if isinstance(process_result, tuple) and len(process_result) == 4:
        # Unpack the result (output_path, csv_filename, control_path, control_filename)
        _, csv_filename, _, control_filename = process_result
    else:
        # Fallback to old format or handle error
        csv_filename = ti.xcom_pull(key='output_filename')
        control_filename = ti.xcom_pull(key='control_filename', default='Not available')
    
    # Get batch state for progress information and initial start time
    batch_state = get_batch_state(dag_id, run_id)
    
    if batch_state and batch_state.get('initial_start_time'):
        # Use initial_start_time from batch state
        if isinstance(batch_state['initial_start_time'], str):
            start_time = datetime.fromisoformat(
                batch_state['initial_start_time'].replace('Z', '+00:00')
            )
        else:
            start_time = batch_state['initial_start_time']
    else:
        # Fallback to XCom if initial_start_time not available
        start_time_str = ti.xcom_pull(key='batch_start_time')
        start_time = datetime.fromisoformat(start_time_str)
    
    end_time = get_thai_time()
    
    # Format success message with both CSV and control file information
    subject = f"Batch Process {dag_id} Completed Successfully"
    html_content = format_success_message(
        dag_id, 
        run_id,
        end_time, 
        conf, 
        csv_filename,
        control_filename,
        batch_state
    )
    
    # Send success notification to both groups
    if conf.get('emailSuccess'):
        # Send to success recipients
        send_notification(subject, html_content, conf, 'success')
    
    # Send to normal recipients
    send_notification(subject, html_content, conf, 'normal')

def send_failure_notification(**context):
    """Send failure or pause notification"""
    ti = context['task_instance']
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    conf = dag_run.conf or {}
    batch_state = get_batch_state(dag_id, run_id)
    
    start_time_str = ti.xcom_pull(key='batch_start_time')
    start_time = datetime.fromisoformat(start_time_str)
    end_time = get_thai_time()
    
    process_result = ti.xcom_pull(task_ids='process_data')
    error_message = ti.xcom_pull(key='error_message')
    
    # Check if this is a manual pause
    if is_manual_pause(error_message):
        # Update batch state to PAUSED
        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=conf.get('startDate'),
            end_date=conf.get('endDate'),
            current_page=batch_state.get('current_page', 1) if batch_state else 1,
            last_search_after=batch_state.get('last_search_after') if batch_state else None,
            status='PAUSED',
            error_message="Process was manually paused by operator",
            total_records=batch_state.get('total_records') if batch_state else None,
            fetched_records=batch_state.get('fetched_records', 0) if batch_state else 0
        )
        
        subject = f"Batch Process {dag_id} Has Been Manually Paused"
        html_content = format_manual_pause_message(
            dag_id, run_id, start_time, end_time, conf, batch_state
        )
        
        # Send manual pause notification
        send_notification(subject, html_content, conf, 'pause')
        
    elif isinstance(process_result, dict) and process_result.get('status') == 'paused':
        # Regular pause case (time-based pause)
        pause_message = process_result.get('message', 'Process was paused')
        subject = f"Batch Process {dag_id} Has Been Paused"
        html_content = format_pause_message(
            dag_id, run_id, start_time, end_time, pause_message, conf, batch_state
        )
        
        # Send regular pause notification
        send_notification(subject, html_content, conf, 'pause')
        
    else:
        # Error case
        error_message = error_message or "Unknown error"
        subject = f"Batch Process {dag_id} Failed"
        html_content = format_error_message(
            dag_id, run_id, start_time, end_time, error_message, conf, batch_state
        )
        
        # Send failure notification
        send_notification(subject, html_content, conf, 'fail')

def format_resume_message(dag_id: str, run_id: str, start_time: datetime, conf: Dict, previous_state: Dict) -> str:
    """Format resume notification message"""
    previous_status = previous_state.get('status', 'Unknown')
    previous_fetch_count = previous_state.get('fetched_records', 0)
    total_records = previous_state.get('total_records', 0)
    last_updated = previous_state.get('updated_at')
    
    if isinstance(last_updated, str):
        last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
    
    progress_percentage = (previous_fetch_count / total_records * 100) if total_records > 0 else 0
    
    return f"""
        <h2>Batch Process {dag_id} Has Resumed</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Resume Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Status:</strong> Resuming from {previous_status}</p>
        
        <h3>Previous Progress:</h3>
        <ul>
            <li>Records Processed: {previous_fetch_count:,} / {total_records:,}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Last Updated: {format_thai_time(last_updated) if last_updated else 'Unknown'}</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
        </ul>
    """

def format_pause_message(dag_id: str, run_id: str, start_time: datetime, end_time: datetime, pause_message: str, conf: Dict, batch_state: Optional[Dict] = None) -> str:
    """Format pause notification message with progress details"""
    # Get progress information
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records', 0) if batch_state else 0
    current_page = batch_state.get('current_page', 1) if batch_state else 1
    
    # Calculate progress percentage
    progress_percentage = (fetched_records / total_records * 100) if total_records > 0 else 0
    
    # Calculate elapsed time
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    # Calculate processing rate
    processing_rate = (fetched_records/elapsed_time.total_seconds()) if elapsed_time.total_seconds() > 0 else 0
    
    return f"""
        <h2>Batch Process {dag_id} Has Been Paused</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Pause Time:</strong> {format_thai_time(end_time)}</p>
        <p><strong>Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> Paused</p>
        <p><strong>Pause Reason:</strong> {pause_message}</p>
        
        <h3>Progress Information:</h3>
        <ul>
            <li>Records Processed: {fetched_records:,} / {total_records:,}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Current Page: {current_page}</li>
            <li>Processing Rate: {processing_rate:.2f} records/second</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
        </ul>
    """

def check_pause_status(**context):
    """
    Check if the process was paused and fail this task if it was
    """
    ti = context['task_instance']
    
    # ดึงผลลัพธ์จาก task process_data
    process_result = ti.xcom_pull(task_ids='process_data')
    print(f"Process result: {process_result}")
    
    # ตรวจสอบว่ามีการ return dict หรือ tuple
    if isinstance(process_result, dict):
        # กรณี pause จะ return dict
        if process_result.get('status') == 'paused':
            msg = process_result.get('message', 'Process was paused')
            print(f"Task was paused: {msg}")
            # เก็บข้อความสำหรับ notification
            ti.xcom_push(key='error_message', value=msg)
            # ต้อง raise exception เพื่อให้ task fail
            raise AirflowException(msg)
    elif isinstance(process_result, tuple):
        # กรณีสำเร็จจะ return tuple ของ (path, filename)
        print("Task completed successfully")
        return True
    
    # กรณีอื่นๆ ให้ fail
    msg = f"Invalid process result type: {type(process_result)}"
    print(msg)
    raise AirflowException(msg)

def validate_filename_template(template: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate filename template format
    Returns (is_valid, error_message)
    """
    if not template:
        return True, None
        
    try:
        # ตรวจสอบว่ามี placeholder ที่ไม่รู้จักหรือไม่
        valid_placeholders = ['date_time', 'date', 'time', 'dag_id']
        pattern = r'\{([^}:]+)(?::([^}]+))?\}'
        matches = re.finditer(pattern, template)
        
        for match in matches:
            placeholder = match.group(1)
            format_str = match.group(2)
            
            if placeholder not in valid_placeholders:
                return False, f"Invalid placeholder: {{{placeholder}}}. Valid placeholders are: " + \
                            ", ".join([f"{{{p}}}" for p in valid_placeholders])
            
            # ถ้ามีการระบุ format ให้ทดสอบว่าใช้ได้จริง
            if format_str:
                try:
                    datetime.now().strftime(format_str)
                except ValueError as e:
                    return False, f"Invalid datetime format '{format_str}': {str(e)}"
        
        return True, None
        
    except Exception as e:
        return False, f"Error validating filename template: {str(e)}"

def get_formatted_filename(template: str, dag_id: str, timestamp: datetime) -> str:
    """
    Format filename using template with placeholders
    Available placeholders:
    {date_time:format} - Current datetime with specified format (e.g., {date_time:%Y%m%d%H%M%S})
    {date:format} - Current date with specified format (e.g., {date:%Y%m%d})
    {time:format} - Current time with specified format (e.g., {time:%H%M%S})
    {dag_id} - The DAG ID
    
    If no format specified, defaults to:
    {date_time} -> %Y-%m-%d_%H.%M.%S
    {date} -> %Y-%m-%d
    {time} -> %H.%M.%S
    """
    try:
        # ถ้าไม่ได้ระบุ template ให้ใช้ชื่อ default
        if not template:
            default_format = timestamp.strftime('%Y%m%d%H%M%S')
            return f"{dag_id}_{default_format}.csv"
        
        # สร้าง pattern สำหรับค้นหา placeholders ที่มี format
        pattern = r'\{(date_time|date|time)(?::([^}]+))?\}'
        
        # แทนที่ placeholders ด้วย format ที่กำหนด
        def replace_match(match):
            placeholder = match.group(1)
            format_str = match.group(2)
            
            if not format_str:
                # ใช้ format default ถ้าไม่ได้ระบุ
                if placeholder == 'date_time':
                    format_str = '%Y-%m-%d_%H.%M.%S'
                elif placeholder == 'date':
                    format_str = '%Y-%m-%d'
                elif placeholder == 'time':
                    format_str = '%H.%M.%S'
            
            try:
                if placeholder == 'date_time':
                    return timestamp.strftime(format_str)
                elif placeholder == 'date':
                    return timestamp.strftime(format_str)
                elif placeholder == 'time':
                    return timestamp.strftime(format_str)
            except ValueError as e:
                raise AirflowException(f"Invalid datetime format '{format_str}': {str(e)}")
            
            return match.group(0)
        
        # แทนที่ placeholders ทั้งหมด
        filename = re.sub(pattern, replace_match, template)
        
        # แทนที่ {dag_id}
        filename = filename.replace('{dag_id}', dag_id)
        
        # ถ้าไม่มีนามสกุลไฟล์ ให้เพิ่ม .csv
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        # ตรวจสอบชื่อไฟล์ที่ได้
        if not filename:
            raise AirflowException("Generated filename is empty")
        
        return filename
        
    except Exception as e:
        if isinstance(e, AirflowException):
            raise
        raise AirflowException(f"Error formatting filename: {str(e)}")

def validate_and_create_path(base_path: str) -> str:
    """
    Validate and create output directory path
    Returns the validated path
    """
    try:
        # ถ้าไม่ได้ระบุ path ให้ใช้ path default
        if not base_path:
            return OUTPUT_DIR
        
        # ทำให้ path เป็น absolute path
        abs_path = os.path.abspath(base_path)
        
        # ตรวจสอบว่า path อยู่ภายใต้ /opt/airflow หรือไม่
        if not abs_path.startswith('/opt/airflow'):
            raise AirflowException(
                f"Invalid path: {base_path}. Path must be under /opt/airflow"
            )
        
        # สร้าง directory ถ้ายังไม่มี
        os.makedirs(abs_path, exist_ok=True)
        print(f"Using output directory: {abs_path}")
        
        return abs_path
        
    except Exception as e:
        if isinstance(e, AirflowException):
            raise
        raise AirflowException(f"Error validating path: {str(e)}")
    
def get_output_config(conf: Dict, dag_id: str) -> Tuple[str, str]:
    """
    Get output file configuration from DAG config
    Returns tuple of (output_path, filename_template)
    """
    custom_path = conf.get('csvPath')
    filename_template = conf.get('csvName')
    
    # Validate and create output directory
    output_path = validate_and_create_path(custom_path)
    
    print(f"Output configuration - Path: {output_path}, Template: {filename_template}")
    return output_path, filename_template

# Create the DAG
with DAG(
    'batch_api_to_csv_with_dynamic_dates_backup',
    default_args=default_args,
    description='Fetch API data with date range and save to CSV',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['api', 'csv', 'backup']
) as dag:
    
    # Task สร้าง table (ถ้ายังไม่มี)
    create_table_task = PythonOperator(
        task_id='ensure_table_exists',
        python_callable=ensure_batch_states_table_exists
    )
    
    # Task แจ้งเตือนเริ่มทำงาน - จะถูกข้ามถ้า process_task เป็น success จากการ pause
    running_notification = PythonOperator(
        task_id='send_running_notification',
        python_callable=send_running_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS  # ต้องรอให้ task ก่อนหน้า success
    )
    
    # Task ประมวลผลข้อมูล
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
        retries=3  # ยังคงให้ retry กรณีที่เกิด error จริงๆ
    )
    
    # Task สำหรับตรวจสอบการ pause
    check_pause_task = PythonOperator(
        task_id='check_pause',
        python_callable=check_pause_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=0  # ไม่ต้อง retry เมื่อ fail
    )
    
    # Task แจ้งเตือนสำเร็จ
    success_notification = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # Task แจ้งเตือนล้มเหลว/pause
    failure_notification = PythonOperator(
        task_id='send_failure_notification',
        python_callable=send_failure_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    # กำหนด Dependencies
    create_table_task >> running_notification >> process_task >> check_pause_task >> [success_notification, failure_notification]