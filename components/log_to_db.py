from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Text, Boolean
import os

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

# Create a table to store DAG state
def create_state_table():
    try:
        engine = create_engine(DATABASE_URL)
        metadata = MetaData()

        # Define a table to store the state of the DAG
        state_table = Table('api_common_authentication_state', metadata,
                            Column('id', Integer, primary_key=True, autoincrement=True),
                            Column('dag_id', String, nullable=False),
                            Column('last_hit_RequestDateTime', String, nullable=True),
                            Column('last_hit__id', String, nullable=True),
                            Column('status', String, nullable=False),  # paused, running, completed
                            Column('start_execution', String, nullable=True),
                            Column('end_execution', String, nullable=True),
                            Column('pause_execution', String, nullable=True),
                            Column('resume_execution', String, nullable=True),
                            Column('start_date', String, nullable=True),
                            Column('end_date', String, nullable=True),
                            Column('total_records', Integer, nullable=True),
                            Column('accumulated_ids', Integer, nullable=True)
                            )

        metadata.create_all(engine)
        print("State table created successfully.")

    except Exception as e:
        print(f"Failed to create state table: {e}")

# Run this function once to create the table
create_state_table()
