# Run tonight Generate_dag.sh with the following values
# API_URL: Fetch data
# TEMP_DIR: Temporary important to store files
# OUTPUT_DIR: For output files
# CONTROL_DIR: For control files
# API_HEADERS: API header authentication
# DAG_NAME: DAG name to be created
# DEFAULT_CSV_COLUMNS: Request CSV to display

./generate_dag.sh \
    "http://34.124.138.144:8000/mobileAppActivity" \
    "/opt/airflow/output/temp" \
    "/opt/airflow/output" \
    "/opt/airflow/output/control" \
    "R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==" \
    "TEST_Friend_MB_Noti_Spending" \
    "['RequestID', 'Path', 'UserToken', 'RequestDateTime', '_id', 'Status', 'CounterCode']"
