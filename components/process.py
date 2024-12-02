from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import shutil
import os
import signal
from contextlib import contextmanager

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

from components.notifications import send_retry_notification

class APIException(Exception):
    pass

class NoDataException(Exception):
    pass

@contextmanager
def handle_termination(batch_id: str, run_id: str, start_date: str, end_date: str, 
                      page: int, search_after: Optional[List[str]], 
                      total_records: Optional[int], fetched_count: int):
    """
    Context manager to handle graceful termination
    """
    sigterm_received = {'terminated': False, 'from_airflow': False}
    original_handler = signal.getsignal(signal.SIGTERM)

    def sigterm_handler(signum, frame):
        print("Received SIGTERM. Finishing current operation...")
        sigterm_received['terminated'] = True
        # เช็คว่าเป็นการ set state จาก Airflow หรือไม่
        import traceback
        stack = traceback.extract_stack()
        sigterm_received['from_airflow'] = any(
            'local_task_job_runner.py' in frame.filename 
            for frame in stack
        )

    try:
        signal.signal(signal.SIGTERM, sigterm_handler)
        yield sigterm_received
    finally:
        signal.signal(signal.SIGTERM, original_handler)
        if sigterm_received['terminated']:
            print("Saving final state before termination...")
            error_msg = "Task received SIGTERM signal" if sigterm_received['from_airflow'] else "Task received SIGTERM signal"
            save_batch_state(
                batch_id=batch_id,
                run_id=run_id,
                start_date=start_date,
                end_date=end_date,
                csv_filename=None,
                ctrl_filename=None,
                current_page=page,
                last_search_after=search_after,
                status='FAILED',
                error_message=error_msg,
                total_records=total_records,
                fetched_records=fetched_count
            )
            raise AirflowException(error_msg)

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
    temp_csv_filename = f"temp_{dag_id}_{run_id}.csv"
    temp_ctrl_filename = f"temp_{dag_id}_{run_id}.ctrl"
    
    should_write_header = True
    
    # ตรวจสอบ state และกำหนดค่าเริ่มต้น
    if batch_state:
        # กรณีที่มี state อยู่แล้ว (ไม่ว่าจะเป็น PAUSED, RUNNING, หรือ FAILED)
        search_after = batch_state['last_search_after']
        page = batch_state['current_page']
        # total_records = batch_state['total_records']
        records, total, next_search_after = fetch_data_page(start_date, end_date, search_after, API_URL, API_HEADERS)
        total_records = total
        fetched_count = batch_state['fetched_records']

        # ใช้ total_records จาก API ถ้ามีความแตกต่าง
        if batch_state['total_records'] != total:
            print(f"Warning: total_records mismatch. Previous: {batch_state['total_records']}, Current: {total}")
            total_records = total
        else:
            total_records = batch_state['total_records']

        print(f"Resuming from state {state_status} at page {page} with {fetched_count} records already fetched")
        print(f"Last search after: {search_after}")
        
        page += 1

        if os.path.exists(temp_file_path):
            print(f"Found existing temp file: {temp_file_path}")
            should_write_header = False
            print("Will append to existing file without header")
        else:
            print("Warning: No temp file found, starting with new file")
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
        
        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            csv_filename=temp_csv_filename,
            ctrl_filename=temp_ctrl_filename,
            current_page=page,
            last_search_after=search_after,
            status='RUNNING',
            error_message=None,
            total_records=total_records,
            fetched_records=fetched_count
        )
        
        while True:
            with handle_termination(
                batch_id=dag_id,
                run_id=run_id,
                start_date=start_date,
                end_date=end_date,
                page=page,
                search_after=search_after,
                total_records=total_records,
                fetched_count=fetched_count
            ) as termination:

                
                print(f"\nFetching page {page}...")
                
                try:
                    records, total, next_search_after = fetch_data_page(start_date, end_date, search_after, API_URL, API_HEADERS)
                except (APIException, NoDataException) as e:
                    save_batch_state(
                        batch_id=dag_id,
                        run_id=run_id,
                        start_date=start_date,
                        end_date=end_date,
                        csv_filename=temp_csv_filename,
                        ctrl_filename=temp_ctrl_filename,
                        current_page=page,
                        last_search_after=search_after,
                        status='FAILED',
                        error_message=str(e),
                        total_records=total_records,
                        fetched_records=fetched_count
                    )

                    raise e
                
                if total_records is None:
                    total_records = total
                    print(f"Total records to fetch: {total_records}")
                
                current_page_size = len(records)
                print(f"Current page size: {current_page_size}")
                
                if current_page_size > 0:

                    # เช็คก่อนเพิ่ม fetched_count
                    if total_records and (fetched_count + current_page_size) > total_records:
                        # ถ้าจะเกิน total_records ให้ปรับจำนวน records ที่จะเพิ่ม
                        current_page_size = total_records - fetched_count
                        records = records[:current_page_size]  # ตัดข้อมูลส่วนเกินทิ้ง

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
                        csv_filename=temp_csv_filename,
                        ctrl_filename=temp_ctrl_filename,
                        current_page=page,
                        last_search_after=next_search_after,
                        status='RUNNING',
                        error_message=None,
                        total_records=total_records,
                        fetched_records=fetched_count
                    )

                if termination['terminated']:
                    break
                
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
        final_filename_csv = final_filename + '.csv'
        final_filename_ctrl = final_filename + '.ctrl'

        final_path = os.path.join(output_path, final_filename_csv)

        # ถ้า directory ไม่มี ให้สร้างใหม่
        os.makedirs(os.path.dirname(final_path), exist_ok=True)

        
        shutil.move(temp_file_path, final_path)
        print(f"Moved temp file to: {final_path}")
        
        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            csv_filename=final_filename_csv,
            ctrl_filename=final_filename_ctrl,
            current_page=page,
            last_search_after=search_after,
            status='COMPLETED',
            error_message=None,
            total_records=total_records,
            fetched_records=fetched_count,
            target_pause_time = None,
            initial_start_time  = None
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
            csv_filename=temp_csv_filename,
            ctrl_filename=temp_ctrl_filename,
            current_page=page,
            last_search_after=search_after,
            status='FAILED',
            error_message=str(e),
            total_records=total_records,
            fetched_records=fetched_count
        )
        raise e

def process_data(API_URL: str, TEMP_DIR: str, OUTPUT_DIR: str,CONTROL_DIR: str, API_HEADERS: Dict[str, str], DEFAULT_CSV_COLUMNS: List[str], default_emails: Dict[str, List[str]], slack_webhook: Optional[str] = None, **kwargs):
    """Process the data and handle notifications""" 
    try:
        ti = kwargs['task_instance']
        dag_run = kwargs['dag_run']
        conf = dag_run.conf or {}
        
        start_date = conf.get('startDate')
        end_date = conf.get('endDate')
        run_id = dag_run.run_id

        batch_state = get_batch_state(dag_run.dag_id, run_id)
        if (batch_state and 
            batch_state.get('csv_filename') and 
            batch_state.get('ctrl_filename')):
            print(f"Batch {run_id} was already completed successfully. Skipping process_data.")

            # ดึงข้อมูลไฟล์เดิม
            csv_filename = batch_state.get('csv_filename')
            control_filename = batch_state.get('ctrl_filename')
            csv_path = os.path.join(OUTPUT_DIR, f"{csv_filename}.csv")
            control_path = os.path.join(OUTPUT_DIR, f"{control_filename}")

            if csv_filename and control_filename:
                # ส่งค่าที่จำเป็นผ่าน XCom
                ti.xcom_push(key='output_filename', value=csv_filename)
                ti.xcom_push(key='control_filename', value=control_filename)
                # ส่งค่าpath และ filename กลับเหมือนการทำงานปกติ
                return (csv_path, csv_filename, control_path, control_filename)
            else:
                error_msg = f"Batch {run_id} is marked as COMPLETED but missing file information"
                ti.xcom_push(key='error_message', value=error_msg)
                raise AirflowException(error_msg)
        
        print(f"Processing with parameters: start_date={start_date}, "
              f"end_date={end_date}, run_id={run_id}")
        
        # Store start time
        ti.xcom_push(key='batch_start_time', value=get_thai_time().isoformat())
        
        try:
            
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
            csv_filename_final = csv_filename + '.csv'
            ctrl_filename_final = csv_filename + '.ctrl'    
            ti.xcom_push(key='output_filename', value=csv_filename_final)
            ti.xcom_push(key='control_filename', value=ctrl_filename_final)
            
            # Get batch state for total records
            batch_state = get_batch_state(dag_run.dag_id, run_id)
            total_records = batch_state.get('total_records', 0) if batch_state else 0
            
            # Create control file
            control_path, control_filename = create_control_file(
                start_date=start_date,
                total_records=total_records,
                csv_filename=csv_filename_final,
                ctrl_filename=ctrl_filename_final,
                dag_id=dag_run.dag_id,
                conf=conf,
                CONTROL_DIR=CONTROL_DIR
            )
            
            return (output_path, csv_filename_final, control_path, ctrl_filename_final)
            
        except Exception as e:
            error_msg = str(e)
            ti.xcom_push(key='error_message', value=error_msg)
            try_number = ti.try_number
            max_retries = ti.max_tries

            # เพิ่มเช็คว่าเป็น SIGTERM หรือไม่
            is_sigterm = any(msg in str(e) for msg in [
                "Task received SIGTERM signal",
                "Task received SIGKILL signal",
                "Task was cancelled externally",
                "Received SIGTERM. Terminating subprocesses",
                "State of this instance has been externally set to failed"
            ])
            
            # Send retry notification if this is not the last retry
            if try_number <= max_retries and not is_sigterm:
                send_retry_notification(
                    dag_id=dag_run.dag_id,
                    run_id=run_id,
                    error_message=error_msg,
                    retry_count=try_number,  # try_number starts from 1
                    max_retries=max_retries,  # exclude the initial try
                    conf=conf,
                    default_emails=default_emails,
                    slack_webhook=slack_webhook,
                    context=kwargs
                )

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