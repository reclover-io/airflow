from contextlib import contextmanager
from sqlalchemy import create_engine, text
from components.constants import DB_CONNECTION

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
