import pytz

# Time zone
THAI_TZ = pytz.timezone('Asia/Bangkok')

# API Configuration
API_URL = 'http://34.124.138.144:8000/api/common/authentication'
API_HEADERS = {
    'Authorization': 'R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==',
    'Content-Type': 'application/json'
}

# Database Configuration
DB_CONNECTION = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'

# Output Configuration
OUTPUT_DIR = '/opt/airflow/output'
TEMP_DIR = '/opt/airflow/output/temp'
CONTROL_DIR = '/opt/airflow/output/control'

# Other Constants
PAGE_SIZE = 1000
DEFAULT_CSV_COLUMNS = [
    'MemberType', 'Latitude', 'Longitude', 'Status', 'DeviceOS', 
    'ModelName', 'UserToken', 'RequestDateTime', '_id'
]