from typing import Dict, List, Tuple, Optional
from datetime import datetime
import re
from airflow.exceptions import AirflowException

# from components.constants import DEFAULT_CSV_COLUMNS

# Date validation functions
def validate_datetime_format(date_str: str, field_name: str) -> Tuple[bool, Optional[str]]:
    """
    Validate datetime string format
    Returns (is_valid, error_message)
    Expected format: YYYY-MM-DD HH:mm:ss.SSS
    """
    try:
        if not date_str:
            return False, f"{field_name} is required"
            
        # แยกส่วนวันที่และเวลา
        date_parts = date_str.split(' ')
        if len(date_parts) != 2:
            return False, f"Invalid {field_name} format. Expected 'YYYY-MM-DD HH:mm:ss.SSS'"
            
        date_part, time_part = date_parts
        
        # ตรวจสอบรูปแบบวันที่
        date_components = date_part.split('-')
        if len(date_components) != 3:
            return False, f"Invalid date format in {field_name}. Expected 'YYYY-MM-DD'"
            
        year, month, day = date_components
        if not (len(year) == 4 and len(month) == 2 and len(day) == 2):
            return False, f"Invalid date components in {field_name}. Year should be 4 digits, month and day should be 2 digits"
            
        # ตรวจสอบรูปแบบเวลา
        if '.' not in time_part:
            return False, f"Missing milliseconds in {field_name}. Expected format 'HH:mm:ss.SSS'"
            
        time_components, milliseconds = time_part.split('.')
        if len(milliseconds) != 3:
            return False, f"Milliseconds should be 3 digits in {field_name}"
            
        hours, minutes, seconds = time_components.split(':')
        if not (len(hours) == 2 and len(minutes) == 2 and len(seconds) == 2):
            return False, f"Invalid time components in {field_name}. Hours, minutes, and seconds should be 2 digits"
            
        # ตรวจสอบค่าที่ถูกต้อง
        datetime.strptime(date_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
        # ตรวจสอบค่าตัวเลข
        if not (1 <= int(month) <= 12 and 1 <= int(day) <= 31):
            return False, f"Invalid date values in {field_name}"
        if not (0 <= int(hours) <= 23 and 0 <= int(minutes) <= 59 and 0 <= int(seconds) <= 59):
            return False, f"Invalid time values in {field_name}"
        if not (0 <= int(milliseconds) <= 999):
            return False, f"Invalid milliseconds value in {field_name}"
            
        return True, None
        
    except ValueError as e:
        return False, f"Invalid {field_name}: {str(e)}"
    except Exception as e:
        return False, f"Error validating {field_name}: {str(e)}"

def validate_date_range(start_date: str, end_date: str) -> Tuple[bool, Optional[str]]:
    """
    Validate that end_date is after start_date
    Returns (is_valid, error_message)
    """
    try:
        start = datetime.strptime(start_date.split('.')[0], '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(end_date.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
        if end <= start:
            return False, "End date must be after start date"
            
        return True, None
        
    except Exception as e:
        return False, f"Error comparing dates: {str(e)}"

def validate_config_dates(conf: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate date configurations
    Returns (is_valid, error_message)
    """
    start_date = conf.get('startDate')
    end_date = conf.get('endDate')
    
    # Validate start date format
    start_valid, start_error = validate_datetime_format(start_date, 'startDate')
    if not start_valid:
        return False, start_error
        
    # Validate end date format
    end_valid, end_error = validate_datetime_format(end_date, 'endDate')
    if not end_valid:
        return False, end_error
        
    # Validate date range
    range_valid, range_error = validate_date_range(start_date, end_date)
    if not range_valid:
        return False, range_error
        
    return True, None

# Email validation functions
def validate_email_list(emails: List[str]) -> bool:
    """Validate email addresses"""
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return all(re.match(email_pattern, email) for email in emails)

def validate_email_config(conf: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate email configurations
    Returns (is_valid, error_message)
    """
    email_types = {
        'email': 'Main email',
        'emailSuccess': 'Success email',
        'emailFail': 'Failure email',
        'emailPause': 'Pause email',
        'emailResume': 'Resume email',
        'emailStart': 'Start email'
    }
    
    # ตรวจสอบ email หลัก (required)
    main_emails = conf.get('email', [])
    if not main_emails:
        return True, None
    
    if not isinstance(main_emails, list):
        return False, "Main email configuration must be a list"
    
    if not validate_email_list(main_emails):
        return False, "Invalid main email address format"
    
    # ตรวจสอบ email ประเภทอื่นๆ (optional)
    for email_key, email_desc in email_types.items():
        if email_key == 'email':
            continue  # ข้ามการตรวจสอบ email หลักเพราะตรวจไปแล้ว
            
        email_list = conf.get(email_key, [])
        if email_list:
            if not isinstance(email_list, list):
                return False, f"{email_desc} configuration must be a list"
            
            if not validate_email_list(email_list):
                return False, f"Invalid {email_desc.lower()} address format"
    
    return True, None

# Pause time validation
def validate_pause_time(pause_time: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate pause time format
    Returns (is_valid, error_message)
    Expected format: HH:mm:ss.SSS
    """
    if not pause_time:
        return True, None  # pause time is optional
        
    try:
        # Check basic format using regex
        time_pattern = r'^([0-1][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\.(\d{3})$'
        if not re.match(time_pattern, pause_time):
            return False, (
                "Invalid pause time format. Expected 'HH:mm:ss.SSS' where:\n"
                "- HH is hours (00-23)\n"
                "- mm is minutes (00-59)\n"
                "- ss is seconds (00-59)\n"
                "- SSS is milliseconds (000-999)"
            )
        
        # Split into components
        time_part, milliseconds = pause_time.split('.')
        hours, minutes, seconds = time_part.split(':')
        
        # Validate lengths
        if not (len(hours) == 2 and len(minutes) == 2 and len(seconds) == 2 and len(milliseconds) == 3):
            return False, "Each component of pause time must have the correct number of digits (HH:mm:ss.SSS)"
            
        # Convert to numbers and validate ranges
        hours_int = int(hours)
        minutes_int = int(minutes)
        seconds_int = int(seconds)
        milliseconds_int = int(milliseconds)
        
        if not (0 <= hours_int <= 23):
            return False, f"Hours must be between 00 and 23, got {hours}"
        if not (0 <= minutes_int <= 59):
            return False, f"Minutes must be between 00 and 59, got {minutes}"
        if not (0 <= seconds_int <= 59):
            return False, f"Seconds must be between 00 and 59, got {seconds}"
        if not (0 <= milliseconds_int <= 999):
            return False, f"Milliseconds must be between 000 and 999, got {milliseconds}"
            
        return True, None
        
    except ValueError as e:
        return False, f"Invalid pause time value: {str(e)}"
    except Exception as e:
        return False, f"Error validating pause time: {str(e)}"

# File related validation
def validate_filename_template(template: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate filename template format
    Returns (is_valid, error_message)
    """
    if not template:
        return True, None
        
    try:
        # ตรวจสอบว่ามี placeholder ที่ไม่รู้จักหรือไม่
        valid_placeholders = ['date_time', 'date', 'time', 'dag_id']
        pattern = r'\{([^}:]+)(?::([^}]+))?\}'
        matches = re.finditer(pattern, template)
        
        for match in matches:
            placeholder = match.group(1)
            format_str = match.group(2)
            
            if placeholder not in valid_placeholders:
                return False, f"Invalid placeholder: {{{placeholder}}}. Valid placeholders are: " + \
                            ", ".join([f"{{{p}}}" for p in valid_placeholders])
            
            # ถ้ามีการระบุ format ให้ทดสอบว่าใช้ได้จริง
            if format_str:
                try:
                    datetime.now().strftime(format_str)
                except ValueError as e:
                    return False, f"Invalid datetime format '{format_str}': {str(e)}"
        
        return True, None
        
    except Exception as e:
        return False, f"Error validating filename template: {str(e)}"

# CSV validation functions
def validate_csv_columns(conf: Dict, DEFAULT_CSV_COLUMNS: List[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate CSV columns configuration
    Returns (is_valid, error_message)
    """
    try:
        # ตรวจสอบว่ามีการระบุ csvColumns หรือไม่
        csv_columns = conf.get('csvColumns') or conf.get('csvColumn')
        if not csv_columns:
            return False, "CSV columns not specified in configuration"
        
        # ตรวจสอบว่าเป็น list หรือไม่
        if not isinstance(csv_columns, list):
            return False, "CSV columns must be a list"
        
        # ตรวจสอบว่ามี column หรือไม่
        if len(csv_columns) == 0:
            return False, "CSV columns list is empty"
        
        # ตรวจสอบว่าแต่ละ column เป็น string หรือไม่
        non_string_columns = [col for col in csv_columns if not isinstance(col, str)]
        if non_string_columns:
            return False, f"Invalid column types found: {non_string_columns}. All columns must be strings"
        
        # ตรวจสอบว่า column อยู่ใน DEFAULT_CSV_COLUMNS หรือไม่
        invalid_columns = [col for col in csv_columns if col not in DEFAULT_CSV_COLUMNS]
        if invalid_columns:
            return False, f"Invalid columns specified: {invalid_columns}. Available columns are: {DEFAULT_CSV_COLUMNS}"
            
        return True, None
        
    except Exception as e:
        return False, f"Error validating CSV columns: {str(e)}"

def get_csv_columns(conf: Dict , DEFAULT_CSV_COLUMNS: List[str]) -> List[str]:
    """
    Get CSV columns from config or use default if not specified
    """
    # Check both possible keys for backwards compatibility
    csv_columns = conf.get('csvColumns') or conf.get('csvColumn', DEFAULT_CSV_COLUMNS)
    
    # Validate that all specified columns exist in the default columns
    invalid_columns = [col for col in csv_columns if col not in DEFAULT_CSV_COLUMNS]
    if invalid_columns:
        raise AirflowException(f"Invalid columns specified: {invalid_columns}. "
                             f"Available columns are: {DEFAULT_CSV_COLUMNS}")
    
    return csv_columns

# Main validation function
def validate_config(conf: Dict, DEFAULT_CSV_COLUMNS: List[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate all configuration parameters
    Returns (is_valid, error_message)
    """
    # Validate emails
    is_valid, error_message = validate_email_config(conf)
    if not is_valid:
        return False, error_message
    
    # Validate dates
    is_valid, error_message = validate_config_dates(conf)
    if not is_valid:
        return False, error_message
    
    # Validate CSV columns
    is_valid, error_message = validate_csv_columns(conf, DEFAULT_CSV_COLUMNS)
    if not is_valid:
        return False, error_message
    
    # Validate pause time
    pause_time = conf.get('pause')
    is_valid, error_message = validate_pause_time(pause_time)
    if not is_valid:
        return False, error_message
    
    # Validate filename template
    filename_template = conf.get('csvName')
    is_valid, error_message = validate_filename_template(filename_template)
    if not is_valid:
        return False, error_message
    
    return True, None