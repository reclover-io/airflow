import pytz

# Time zone
THAI_TZ = pytz.timezone('Asia/Bangkok')

# Database Configuration
DB_CONNECTION = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'

# Other Constants
PAGE_SIZE = 1000
