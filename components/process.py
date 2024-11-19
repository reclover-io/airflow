from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import shutil
import os

from components.database import (
    get_batch_state, 
    save_batch_state,
)
from components.api import fetch_data_page
from components.file_handlers import (
    save_temp_data,
    create_control_file,
    get_output_config,
    get_formatted_filename
)
from components.validators import (
    validate_config,
    get_csv_columns
)
from components.utils import (
    get_thai_time,
    format_thai_time
)
from components.constants import (
    THAI_TZ,
    PAGE_SIZE
)

class APIException(Exception):
    pass

class NoDataException(Exception):
    pass

# State handling functions
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

# Main process functions
def fetch_and_save_data(start_date: str, end_date: str, dag_id: str, run_id: str, conf: Dict, API_URL: str, TEMP_DIR: str, OUTPUT_DIR: str, DEFAULT_CSV_COLUMNS: List[str], API_HEADERS: Dict[str, str]) -> Tuple[str, str]:
    """Fetch all data from API using pagination and save to CSV with state management"""

    # Get output configuration
    output_path, filename_template = get_output_config(conf, dag_id, OUTPUT_DIR)

    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        csv_columns = get_csv_columns(conf, DEFAULT_CSV_COLUMNS)
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
                records, total, next_search_after = fetch_data_page(start_date, end_date, search_after, API_URL, API_HEADERS)
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
                current_page_size < PAGE_SIZE or 
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

def process_data(API_URL: str, TEMP_DIR: str, OUTPUT_DIR: str,CONTROL_DIR: str, API_HEADERS: Dict[str, str], DEFAULT_CSV_COLUMNS: List[str], **kwargs):
    """Process the data and handle notifications""" 
    try:
        ti = kwargs['task_instance']
        dag_run = kwargs['dag_run']
        conf = dag_run.conf or {}
        
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
                conf=conf,
                API_URL=API_URL,
                TEMP_DIR=TEMP_DIR,
                OUTPUT_DIR=OUTPUT_DIR,
                DEFAULT_CSV_COLUMNS=DEFAULT_CSV_COLUMNS,
                API_HEADERS=API_HEADERS

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
                conf=conf,
                CONTROL_DIR=CONTROL_DIR
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
        if process_result.get('status') == 'paused':
            msg = process_result.get('message', 'Process was paused')
            print(f"Task was paused: {msg}")
            ti.xcom_push(key='error_message', value=msg)
            raise AirflowException(msg)
    elif isinstance(process_result, tuple):
        print("Task completed successfully")
        return True
    
    msg = f"Invalid process result type: {type(process_result)}"
    print(msg)
    raise AirflowException(msg)