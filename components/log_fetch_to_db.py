from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Text
import pytz
import logging
import os

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

def log_fetch_to_db(request_body, response_body, status_code, kwargs):
    try:
        engine = create_engine(DATABASE_URL)
        metadata = MetaData()

        fetch_logs_table = Table('api_common_authentication_fetch_logs', metadata,
                                 Column('id', Integer, primary_key=True, autoincrement=True),
                                 Column('dag_id', String, nullable=False),
                                 Column('execution_date', String, nullable=False),
                                 Column('request_body', Text, nullable=False),
                                 Column('response_body', Text, nullable=False),
                                 Column('status_code', Integer, nullable=False)
                                 )

        metadata.create_all(engine)

        execution_date = kwargs['execution_date']
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        execution_date_bangkok = execution_date.astimezone(bangkok_tz).strftime('%Y-%m-%d %H:%M:%S')
        dag_id = kwargs['dag'].dag_id

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
