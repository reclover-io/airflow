from typing import Dict, List, Tuple
from datetime import datetime
import os
import shutil
import pandas as pd
import csv
import re
from airflow.exceptions import AirflowException

from .constants import (
    OUTPUT_DIR, 
    TEMP_DIR, 
    CONTROL_DIR,
    DEFAULT_CSV_COLUMNS
)
from .utils import get_thai_time

def validate_and_create_path(base_path: str) -> str:
    """
    Validate and create output directory path
    Returns the validated path
    """
    try:
        # ถ้าไม่ได้ระบุ path ให้ใช้ path default
        if not base_path:
            return OUTPUT_DIR
        
        # ทำให้ path เป็น absolute path
        abs_path = os.path.abspath(base_path)
        
        # ตรวจสอบว่า path อยู่ภายใต้ /opt/airflow หรือไม่
        if not abs_path.startswith('/opt/airflow'):
            raise AirflowException(
                f"Invalid path: {base_path}. Path must be under /opt/airflow"
            )
        
        # สร้าง directory ถ้ายังไม่มี
        os.makedirs(abs_path, exist_ok=True)
        print(f"Using output directory: {abs_path}")
        
        return abs_path
        
    except Exception as e:
        if isinstance(e, AirflowException):
            raise
        raise AirflowException(f"Error validating path: {str(e)}")

def get_output_config(conf: Dict, dag_id: str) -> Tuple[str, str]:
    """
    Get output file configuration from DAG config
    Returns tuple of (output_path, filename_template)
    """
    custom_path = conf.get('csvPath')
    filename_template = conf.get('csvName')
    
    # Validate and create output directory
    output_path = validate_and_create_path(custom_path)
    
    print(f"Output configuration - Path: {output_path}, Template: {filename_template}")
    return output_path, filename_template

# File name handling
def get_formatted_filename(template: str, dag_id: str, timestamp: datetime) -> str:
    """
    Format filename using template with placeholders
    Available placeholders:
    {date_time:format} - Current datetime with specified format (e.g., {date_time:%Y%m%d%H%M%S})
    {date:format} - Current date with specified format (e.g., {date:%Y%m%d})
    {time:format} - Current time with specified format (e.g., {time:%H%M%S})
    {dag_id} - The DAG ID
    
    If no format specified, defaults to:
    {date_time} -> %Y-%m-%d_%H.%M.%S
    {date} -> %Y-%m-%d
    {time} -> %H.%M.%S
    """
        if not template:
            default_format = timestamp.strftime('%Y%m%d%H%M%S')
            return f"{dag_id}_{default_format}.csv"
        
        # สร้าง pattern สำหรับค้นหา placeholders ที่มี format
        pattern = r'\{(date_time|date|time)(?::([^}]+))?\}'
        
        # แทนที่ placeholders ด้วย format ที่กำหนด
        def replace_match(match):
            placeholder = match.group(1)
            format_str = match.group(2)
            
            if not format_str:
                # ใช้ format default ถ้าไม่ได้ระบุ
                if placeholder == 'date_time':
                    format_str = '%Y-%m-%d_%H.%M.%S'
                elif placeholder == 'date':
                    format_str = '%Y-%m-%d'
                elif placeholder == 'time':
                    format_str = '%H.%M.%S'
            
            try:
                if placeholder == 'date_time':
                    return timestamp.strftime(format_str)
                elif placeholder == 'date':
                    return timestamp.strftime(format_str)
                elif placeholder == 'time':
                    return timestamp.strftime(format_str)
            except ValueError as e:
                raise AirflowException(f"Invalid datetime format '{format_str}': {str(e)}")
            
            return match.group(0)
        
        # แทนที่ placeholders ทั้งหมด
        filename = re.sub(pattern, replace_match, template)
        
        # แทนที่ {dag_id}
        filename = filename.replace('{dag_id}', dag_id)
        
        # ถ้าไม่มีนามสกุลไฟล์ ให้เพิ่ม .csv
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        # ตรวจสอบชื่อไฟล์ที่ได้
        if not filename:
            raise AirflowException("Generated filename is empty")
        
        return filename
        
    except Exception as e:
        if isinstance(e, AirflowException):
            raise
        raise AirflowException(f"Error formatting filename: {str(e)}")

def get_control_file_config(conf: Dict, dag_id: str, timestamp: datetime) -> Tuple[str, str]:
    """
    Get control file path and name configuration
    Returns (control_path, control_filename)
    """
    # ใช้ path จาก config หรือใช้ default
    control_path = conf.get('controlPath', CONTROL_DIR)
    
    # สร้าง directory ถ้ายังไม่มี
    os.makedirs(control_path, exist_ok=True)
    
    # ใช้ template จาก config หรือใช้ default
    control_template = conf.get('controlName')
    if control_template:
        control_filename = get_formatted_filename(control_template, dag_id, timestamp)
        # เปลี่ยนนามสกุลไฟล์เป็น .ctrl
        control_filename = os.path.splitext(control_filename)[0] + '.ctrl'
    else:
        # ใช้ default format
        control_filename = f"{dag_id}_{timestamp.strftime('%Y%m%d%H%M%S')}.ctrl"
    
    return control_path, control_filename

def create_control_file(start_date: str, total_records: int, csv_filename: str,
                       dag_id: str, conf: Dict) -> Tuple[str, str]:
    """
    Create control file with summary information
    Returns (control_path, control_filename)
    """
    try:
        # แปลง start_date เป็น data_dt (เอาเฉพาะวันที่)
        data_dt = datetime.strptime(start_date.split()[0], '%Y-%m-%d').strftime('%Y-%m-%d')
        
        # ใช้เวลาปัจจุบันเป็น process_date
        process_time = get_thai_time()
        process_date = process_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Get control file configuration
        control_path, control_filename = get_control_file_config(conf, dag_id, process_time)
        
        # สร้าง DataFrame สำหรับ control file
        control_data = {
            'data_dt': [data_dt],
            'total_recs': [total_records],
            'process_date': [process_date],
            'file_name': [csv_filename]
        }
        control_df = pd.DataFrame(control_data)
        
        # กำหนด path เต็ม
        full_path = os.path.join(control_path, control_filename)
        
        # บันทึกไฟล์ control
        control_df.to_csv(
            full_path,
            sep='|',
            index=False,
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\',
            doublequote=True
        )
        
        print(f"Created control file: {full_path}")
        print(f"Control file content:")
        print(control_df.to_string(index=False))
        
        return control_path, control_filename
        
    except Exception as e:
        raise AirflowException(f"Error creating control file: {str(e)}")

# File operations
def save_temp_data(records: List[Dict], temp_file: str, headers: bool = False, columns: List[str] = None):
    """
    Save data to temporary CSV file with specified columns in exact order using | as separator
    """
    # Create DataFrame from records
    df = pd.DataFrame(records)
    
    # Use specified columns or default if none provided
    columns_to_use = columns if columns else DEFAULT_CSV_COLUMNS
    
    # Verify all required columns exist in the data
    missing_columns = [col for col in columns_to_use if col not in df.columns]
    if missing_columns:
        raise AirflowException(f"Missing columns in data: {missing_columns}")
    
    # Select and reorder columns - explicitly create new DataFrame with ordered columns
    ordered_df = pd.DataFrame(columns=columns_to_use)
    for col in columns_to_use:
        ordered_df[col] = df[col]
    
    # Save to CSV with | separator
    ordered_df.to_csv(
        temp_file,
        mode='a',
        header=headers,
        index=False,
        sep='|',
        escapechar='\\',  # ใช้ \ เป็น escape character
        doublequote=True,  # ใช้ double quotes สำหรับ field ที่มี | อยู่ในข้อมูล
        quoting=csv.QUOTE_MINIMAL  # ใส่ quotes เฉพาะเมื่อจำเป็น
    )
    print(f"Saved data with ordered columns: {columns_to_use} using | separator")