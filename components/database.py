from typing import Dict, Optional, List
from sqlalchemy import text
import pytz
from datetime import datetime
import json
from components.constants import THAI_TZ
from components.utils import get_thai_time
from components.create_database import get_db_connection 

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
    
def get_failed_batch_runs(dag_id: str) -> List[Dict]:
    """Get all failed batch runs ordered by DAG start date (oldest first)"""
    with get_db_connection() as conn:
        print(f"Checking for failed batch runs:")
        print(f"DAG ID: {dag_id}")
        
        query = text("""
            WITH LatestBatchStates AS (
                -- Get the latest batch state for each run_id
                SELECT DISTINCT ON (run_id)
                    batch_id,
                    run_id,
                    start_date as batch_start_date,
                    end_date as batch_end_date,
                    status,
                    error_message,
                    created_at,
                    updated_at
                FROM batch_states
                WHERE batch_id = :dag_id
                ORDER BY run_id, updated_at DESC
            )
            SELECT 
                dr.run_id,
                dr.state as dag_state,
                dr.start_date as dag_start_date,
                dr.end_date as dag_end_date,
                bs.batch_start_date,
                bs.batch_end_date,
                bs.status as batch_status,
                bs.error_message,
                bs.created_at,
                bs.updated_at
            FROM dag_run dr
            LEFT JOIN LatestBatchStates bs ON dr.run_id = bs.run_id AND dr.dag_id = bs.batch_id
            WHERE dr.dag_id = :dag_id
            AND dr.state = 'failed'
            ORDER BY dr.start_date ASC  -- Order by DAG run start date, oldest first
        """)
        
        try:
            result = conn.execute(query, {
                'dag_id': str(dag_id)
            }).fetchall()
            
            # Add debug logging
            if result:
                print(f"\nFound {len(result)} failed batches:")
                for row in result:
                    print(f"\nRun ID: {row['run_id']}")
                    print(f"DAG State: {row['dag_state']}")
                    print(f"DAG Start Date: {row['dag_start_date']}")
                    print(f"Batch Status: {row['batch_status']}")
                    print(f"Start Date: {row['batch_start_date']}")
                    print(f"End Date: {row['batch_end_date']}")
                    print(f"Created At: {row['created_at']}")
                    print(f"Updated At: {row['updated_at']}")
                    print(f"Error: {row['error_message']}")
            else:
                print("No failed batches found in database")
            
            # Convert to list of dictionaries with standardized keys
            failed_batches = []
            for row in result:
                failed_batches.append({
                    'run_id': row['run_id'],
                    'start_date': row['batch_start_date'],
                    'end_date': row['batch_end_date'],
                    'status': row['batch_status'],
                    'error_message': row['error_message'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at'],
                    'dag_state': row['dag_state'],
                    'dag_start_date': row['dag_start_date'],
                    'dag_end_date': row['dag_end_date']
                })
            
            return failed_batches
            
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            raise