from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, TIMESTAMP, Index, inspect, text
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from components.constants import DB_CONNECTION
from contextlib import contextmanager


# สร้าง database engine
engine = create_engine(DB_CONNECTION)
metadata = MetaData()

# Schema ของตาราง batch_states
BATCH_STATES_TABLE = Table(
    'batch_states', metadata,
    Column('batch_id', String(255), primary_key=True),
    Column('run_id', String(255), primary_key=True),
    Column('start_date', TIMESTAMP(timezone=True), nullable=False),
    Column('end_date', TIMESTAMP(timezone=True), nullable=False),
    Column('current_page', Integer, nullable=False),
    Column('last_search_after', JSONB),
    Column('status', String(50), nullable=False),
    Column('error_message', String),
    Column('total_records', Integer),
    Column('fetched_records', Integer),
    Column('target_pause_time', TIMESTAMP(timezone=True)),
    Column('initial_start_time', TIMESTAMP(timezone=True)),
    Column(
        'created_at',
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("timezone('Asia/Bangkok', NOW())"),
    ),
    Column(
        'updated_at',
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("timezone('Asia/Bangkok', NOW())"),
    ),
    # เพิ่ม Primary Key Constraint
    extend_existing=True,
)

# เพิ่ม Index
Index('idx_batch_states_status', BATCH_STATES_TABLE.c.status, unique=False)
Index('idx_batch_states_updated_at', BATCH_STATES_TABLE.c.updated_at, unique=False)

# Context manager for database connection
@contextmanager
def get_db_connection():
    """Get database connection using SQLAlchemy"""
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

# ฟังก์ชันสำหรับการสร้างตาราง
def create_batch_states_table():
    """Create the batch_states table if it doesn't exist."""
    try:
        # Check if the table exists, create if not
        if not engine.has_table('batch_states'):
            print("Creating batch_states table...")
            metadata.create_all(engine)  # Create table with indexes
            print("batch_states table and indexes created successfully.")
        else:
            print("batch_states table already exists.")
    except SQLAlchemyError as e:
        print(f"Error creating table: {str(e)}")
        raise

# ฟังก์ชันสำหรับตรวจสอบและเพิ่มคอลัมน์ทั้งหมด
def ensure_all_columns_exist():
    """Ensure all columns in the defined schema exist in the table."""
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)

            # Get the list of existing columns in the table
            existing_columns = {col['name'] for col in inspector.get_columns('batch_states')}
            print(f"Existing columns in 'batch_states': {existing_columns}")

            # Compare with the defined schema
            for column in BATCH_STATES_TABLE.columns:
                if column.name not in existing_columns:
                    print(f"Adding missing column '{column.name}' to 'batch_states'...")
                    alter_query = f"""
                        ALTER TABLE batch_states 
                        ADD COLUMN {column.name} {str(column.type)};
                    """
                    connection.execute(alter_query)
                    print(f"Column '{column.name}' added successfully.")
                else:
                    print(f"Column '{column.name}' already exists in 'batch_states'.")
    except SQLAlchemyError as e:
        print(f"Error ensuring columns exist: {str(e)}")
        raise

# ฟังก์ชันสำหรับตรวจสอบและเพิ่ม Index
def ensure_indexes_exist():
    """Ensure indexes are created if not already present."""
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)

            # ดึงรายการ Index ที่มีอยู่แล้ว
            existing_indexes = {index['name'] for index in inspector.get_indexes('batch_states')}
            if not existing_indexes:
                print("No indexes exist on 'batch_states'.")
            else:
                print(f"Existing indexes: {existing_indexes}")

            # สร้าง Index idx_batch_states_status ถ้ายังไม่มี
            if 'idx_batch_states_status' not in existing_indexes:
                print("Creating index 'idx_batch_states_status'...")
                Index('idx_batch_states_status', BATCH_STATES_TABLE.c.status).create(engine)
                print("Index 'idx_batch_states_status' created successfully.")

            # สร้าง Index idx_batch_states_updated_at ถ้ายังไม่มี
            if 'idx_batch_states_updated_at' not in existing_indexes:
                print("Creating index 'idx_batch_states_updated_at'...")
                Index('idx_batch_states_updated_at', BATCH_STATES_TABLE.c.updated_at).create(engine)
                print("Index 'idx_batch_states_updated_at' created successfully.")
    except SQLAlchemyError as e:
        print(f"Error ensuring indexes exist: {str(e)}")
        raise
