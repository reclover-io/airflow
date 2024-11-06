#!/bin/bash

# รับค่า Arguments
API_URL="$1"
TEMP_DIR="$2"
OUTPUT_DIR="$3"
CONTROL_DIR="$4"
API_HEADERS="$5"
DAG_NAME="$6"
DEFAULT_CSV_COLUMNS="$7"

# สร้างไฟล์ DAG ใหม่โดยแทนที่ค่า Placeholder ในเทมเพลต
sed \
    -e "s|{API_URL}|$API_URL|g" \
    -e "s|{TEMP_DIR}|$TEMP_DIR|g" \
    -e "s|{OUTPUT_DIR}|$OUTPUT_DIR|g" \
    -e "s|{CONTROL_DIR}|$CONTROL_DIR|g" \
    -e "s|{API_HEADERS}|$API_HEADERS|g" \
    -e "s|{DAG_NAME}|$DAG_NAME|g" \
    -e "s|{DEFAULT_CSV_COLUMNS}|$DEFAULT_CSV_COLUMNS|g" \
    dag_template.py > /opt/airflow/dags/$DAG_NAME.py

echo "DAG file /opt/airflow/dags/$DAG_NAME.py created successfully."
