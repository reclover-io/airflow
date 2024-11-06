# รันสคริปต์ generate_dag.sh พร้อมค่าพารามิเตอร์
# API URL ที่ต้องการดึงข้อมูล
# TEMP_DIR: โฟลเดอร์ชั่วคราวสำหรับเก็บไฟล์
# OUTPUT_DIR: โฟลเดอร์สำหรับไฟล์ผลลัพธ์
# CONTROL_DIR: โฟลเดอร์สำหรับเก็บไฟล์ควบคุม
# API_HEADERS: ค่า Authorization ของ API Headers
# DAG_NAME: ชื่อ DAG ที่ต้องการตั้ง (จะเป็นชื่อไฟล์ DAG ด้วย)
# DEFAULT_CSV_COLUMNS: คอลัมน์ของ CSV ที่ต้องการให้แสดง

./generate_dag.sh \
    "http://34.124.138.144:8000/mobileAppActivity" \
    "/opt/airflow/output/temp" \
    "/opt/airflow/output" \
    "/opt/airflow/output/control" \
    "R2pDZVNaRUJnMmt1a0tEVE5raEo6ZTNrYm1WRk1Sb216UGUtU21DS21iZw==" \
    "Friend_MB_Noti_Spending" \
    "['RequestID', 'Path', 'UserToken', 'RequestDateTime', '_id', 'Status', 'CounterCode']"
