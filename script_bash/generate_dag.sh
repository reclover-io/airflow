#!/bin/bash

API_URL="$1"
TEMP_DIR="$2"
OUTPUT_DIR="$3"
CONTROL_DIR="$4"
API_HEADERS="$5"
DAG_NAME="$6"
DEFAULT_CSV_COLUMNS="$7"

sed \
    -e "s|{API_URL}|$API_URL|g" \
    -e "s|{TEMP_DIR}|$TEMP_DIR|g" \
    -e "s|{OUTPUT_DIR}|$OUTPUT_DIR|g" \
    -e "s|{CONTROL_DIR}|$CONTROL_DIR|g" \
    -e "s|{API_HEADERS}|$API_HEADERS|g" \
    -e "s|{DAG_NAME}|$DAG_NAME|g" \
    -e "s|{DEFAULT_CSV_COLUMNS}|$DEFAULT_CSV_COLUMNS|g" \
    dag_template.py > ../dags/$DAG_NAME.py

if [[ -f "../dags/$DAG_NAME.py" ]]; then
    echo "DAG file /opt/airflow/dags/$DAG_NAME.py created successfully."
else
    echo "Error: Failed to create DAG file /opt/airflow/dags/$DAG_NAME.py"
    exit 1
fi

