from contextlib import contextmanager
from typing import Dict, Optional, List
from sqlalchemy import create_engine, text
import pytz
from datetime import datetime
import json
from components.constants import THAI_TZ, DB_CONNECTION
from components.utils import get_thai_time

# Context manager for database connection
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

# Main functions
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