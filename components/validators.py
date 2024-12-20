from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import re
from airflow.exceptions import AirflowException
from components.utils import get_thai_time
from airflow.utils.session import create_session
from dateutil.relativedelta import relativedelta
from airflow.models import DagRun
from components.constants import THAI_TZ

def validate_datetime_format(date_str: str, field_name: str) -> Tuple[bool, Optional[str]]:
    """
    Validate datetime string format
    Returns (is_valid, error_message)
    Expected format: YYYY-MM-DD HH:mm:ss.SSS
    """
    try:
        if not date_str:
            return False, f"{field_name} is required"
            
        date_parts = date_str.split(' ')
        if len(date_parts) != 2:
            return False, f"Invalid {field_name} format. Expected 'YYYY-MM-DD HH:mm:ss.SSS'"
            
        date_part, time_part = date_parts
        
        date_components = date_part.split('-')
        if len(date_components) != 3:
            return False, f"Invalid date format in {field_name}. Expected 'YYYY-MM-DD'"
            
        year, month, day = date_components
        if not (len(year) == 4 and len(month) == 2 and len(day) == 2):
            return False, f"Invalid date components in {field_name}. Year should be 4 digits, month and day should be 2 digits"
            
        if '.' not in time_part:
            return False, f"Missing milliseconds in {field_name}. Expected format 'HH:mm:ss.SSS'"
            
        time_components, milliseconds = time_part.split('.')
        if len(milliseconds) != 3:
            return False, f"Milliseconds should be 3 digits in {field_name}"
            
        hours, minutes, seconds = time_components.split(':')
        if not (len(hours) == 2 and len(minutes) == 2 and len(seconds) == 2):
            return False, f"Invalid time components in {field_name}. Hours, minutes, and seconds should be 2 digits"
            
        datetime.strptime(date_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
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
    errors = []
    start_date = conf.get('startDate')
    end_date = conf.get('endDate')
    
    # Validate start date format
    start_valid, start_error = validate_datetime_format(start_date, 'startDate')
    if not start_valid:
        errors.append(start_error)
        
    # Validate end date format
    end_valid, end_error = validate_datetime_format(end_date, 'endDate')
    if not end_valid:
        errors.append(end_error)
        
    # Validate date range
    if start_valid and end_valid:
        range_valid, range_error = validate_date_range(start_date, end_date)
        if not range_valid:
            errors.append(range_error)
    
    if errors:
        return False, "\n".join(errors)
        
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
    
    main_emails = conf.get('email', [])
    if not main_emails:
        return True, None
    
    if not isinstance(main_emails, list):
        return False, "Main email configuration must be a list"
    
    if not validate_email_list(main_emails):
        return False, "Invalid main email address format"
    
    for email_key, email_desc in email_types.items():
        if email_key == 'email':
            continue 
            
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
        return True, None 
        
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

        valid_placeholders = ['date_time', 'date', 'time', 'dag_id']
        pattern = r'\{([^}:]+)(?::([^}]+))?\}'
        matches = re.finditer(pattern, template)
        
        for match in matches:
            placeholder = match.group(1)
            format_str = match.group(2)
            
            if placeholder not in valid_placeholders:
                return False, f"Invalid placeholder: {{{placeholder}}}. Valid placeholders are: " + \
                            ", ".join([f"{{{p}}}" for p in valid_placeholders])
            
            if format_str:
                try:
                    datetime.now().strftime(format_str)
                except ValueError as e:
                    return False, f"Invalid datetime format '{format_str}': {str(e)}"
        
        return True, None
        
    except Exception as e:
        return False, f"Error validating filename template: {str(e)}"

# CSV validation functions
def validate_csv_columns(conf: Dict, default_csv_columns: List[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate CSV columns configuration
    Returns (is_valid, error_message)
    """
    try:
        # ถ้าไม่ได้ระบุ csvColumns ให้ใช้ default
        csv_columns = conf.get('csvColumn')
        if not csv_columns:
            return True, None  # ไม่จำเป็นต้องตรวจสอบถ้าใช้ default
        
        # ตรวจสอบว่าเป็น list หรือไม่
        if not isinstance(csv_columns, list):
            return False, "CSV columns must be a list"
        
        # ตรวจสอบว่าแต่ละ column เป็น string หรือไม่
        non_string_columns = [col for col in csv_columns if not isinstance(col, str)]
        if non_string_columns:
            return False, f"Invalid column types found: {non_string_columns}. All columns must be strings"
            
        return True, None
        
    except Exception as e:
        return False, f"Error validating CSV columns: {str(e)}"

def get_csv_columns(conf: Dict , DEFAULT_CSV_COLUMNS: List[str]) -> List[str]:
    """
    Get CSV columns from config or use default if not specified
    """
    # Check both possible keys for backwards compatibility
    csv_columns = conf.get('csvColumn', DEFAULT_CSV_COLUMNS)
    
    return csv_columns

def get_default_config(execution_date: datetime) -> Dict:
    """
    Generate default configuration based on execution date
    For daily batch, will get data from previous day
    """
    # Get previous day
    current_time = get_thai_time()
    #prev_day = execution_date - timedelta(days=1)
    prev_day = current_time - timedelta(days=1)
    
    # Format start date and end date
    start_date = prev_day.replace(
        hour=0, 
        minute=0, 
        second=0, 
        microsecond=0
    ).strftime('%Y-%m-%d %H:%M:%S.000')
    
    end_date = prev_day.replace(
        hour=23, 
        minute=59, 
        second=59, 
        microsecond=999000
    ).strftime('%Y-%m-%d %H:%M:%S.999')
    
    return {
        'startDate': start_date,
        'endDate': end_date
    }

def get_default_config_monthly(execution_date: datetime) -> Dict:
    """
    Generate default configuration based on execution date
    For monthly batch, startDate is 1 month ago from current time
    and endDate is 1 day ago.
    """
    # Get current time in Thai timezone
    current_time = get_thai_time()

    # Calculate start date (1 month ago)
    start_date = (current_time - relativedelta(months=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
    ).strftime('%Y-%m-%d %H:%M:%S.000')

    # Calculate end date (1 day ago)
    end_date = (current_time - timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=999000
    ).strftime('%Y-%m-%d %H:%M:%S.999')

    return {
        'startDate': start_date,
        'endDate': end_date
    }

def validate_start_run(start_run: Optional[str]) -> Tuple[bool, Optional[str]]:
    """Validate start_run time"""
    if not start_run:
        return True, None

    try:
        is_valid, error_message = validate_datetime_format(start_run, 'start_run')
        if not is_valid:
            return False, error_message
            
        # Validate format
        start_time = datetime.strptime(start_run, '%Y-%m-%d %H:%M:%S.%f')
        # Convert to Thai timezone
        start_time = THAI_TZ.localize(start_time)
        
        # Get current time in Thai timezone
        current_time = get_thai_time()

        print(f"Validating start_run:")
        print(f"Start time: {start_time}")
        print(f"Current time: {current_time}")

        # Check if start_time is in the future
        if start_time <= current_time:
            return False, (
                f"start_run must be in the future.\n"
                f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S.%f')}\n"
                f"Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S.%f')}"
            )

        return True, None
        
    except ValueError as e:
        return False, (
            f"Invalid start_run format: {str(e)}.\n"
            f"Expected format: YYYY-MM-DD HH:mm:ss.SSS"
        )
    except Exception as e:
        return False, f"Error validating start_run: {str(e)}"
    
def validate_run_ids(run_id: str, dag_id: str) -> Tuple[bool, Optional[str]]:
    """Validate run_ids exist and are in failed state"""
    if not run_id:
        return True, None

    with create_session() as session:
        errors = []
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.run_id == run_id
        ).first()

        if not dag_run:
            errors.append(f"Run ID not found: {run_id}")
        elif dag_run.state != 'failed':
            errors.append(f"Run ID {run_id} is in {dag_run.state} state. Only failed runs can be resumed.")

        if errors:
            return False, "\n".join(errors)

    return True, None

# Main validation function
def validate_config(conf: Dict, DEFAULT_CSV_COLUMNS: List[str], context: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate all configuration parameters
    Returns (is_valid, error_message)
    """

    errors = []

    # Validate emails
    is_valid, error_message = validate_email_config(conf)
    if not is_valid:
        errors.append(f"Email Configuration Error: {error_message}")
    
    run_ids = conf.get('run_id')
    if not run_ids:
    # Validate dates
        is_valid, error_message = validate_config_dates(conf)
        if not is_valid:
            errors.append(f"Date Configuration Error: {error_message}")
    
    # Validate CSV columns
    if 'csvColumn' in conf:
        is_valid, error_message = validate_csv_columns(conf, DEFAULT_CSV_COLUMNS)
        if not is_valid:
            errors.append(f"CSV Columns Error: {error_message}")
    
    # Validate pause time
    pause_time = conf.get('pause')
    is_valid, error_message = validate_pause_time(pause_time)
    if not is_valid:
        errors.append(f"Pause Time Error: {error_message}")
    
    # Validate filename template
    filename_template = conf.get('csvName')
    is_valid, error_message = validate_filename_template(filename_template)
    if not is_valid:
        errors.append(f"Filename Template Error: {error_message}")
    
    is_valid, error_message = validate_ftp_config(conf, context)
    if not is_valid:
        errors.append(f"FTP Configuration Error: {error_message}")

    if run_ids:
        is_valid, error_message = validate_run_ids(run_ids, context['dag'].dag_id)
        if not is_valid:
            errors.append(f"Invalid run_ids: {error_message}")

    start_run = conf.get('start_run')
    if start_run:
        is_valid, error_message = validate_start_run(start_run)
        if not is_valid:
            errors.append(f"Invalid start_run: {error_message}")

    is_valid, error_message = validate_check_fail(conf)
    if not is_valid:
        errors.append(f"Invalid check_fail: {error_message}")

    is_valid, error_message = validate_slack_config(conf, context)
    if not is_valid:
        errors.append(f"Slack Configuration Error: {error_message}")

    errors.reverse()

    if errors:
        error_message = "Configuration Validation Failed:<ol>"
        for error in errors:
            error_message += f"<li>{error}</li>"
        error_message += "</ol>"

        error_message_format = "Configuration Validation Failed:\n"

        for i, error in enumerate(errors, 1):
            error_message_format += f"\n{i}. {error}\n"
        return False, error_message , error_message_format
    
    context['task_instance'].xcom_push(key='error_message', value=error_message)
    
    return True, None, None

def validate_input_task(default_csv_columns: List[str], default_emails: Dict[str, List[str]], **context):
    """Validate input configuration using existing validate_config function"""
    try:
        dag_run = context['dag_run']
        
        # Get execution date from context
        execution_date = context['execution_date']
        
        # If no config provided, use default config
        if not dag_run.conf:
            default_config = get_default_config(execution_date)
            # Add default email configuration
            # default_config.update({
            #     'email': default_emails.get('email', []),
            #     'emailSuccess': default_emails.get('emailSuccess', []),
            #     'emailFail': default_emails.get('emailFail', []),
            #     'emailPause': default_emails.get('emailPause', []),
            #     'emailResume': default_emails.get('emailResume', []),
            #     'emailStart': default_emails.get('emailStart', [])
            # })
            dag_run.conf = default_config
            print(f"Using default configuration: {default_config}")
        
        conf = dag_run.conf
        
        # Use existing validate_config function
        is_valid, error_message , error_message_format = validate_config(conf, default_csv_columns, context)
        
        if not is_valid:
            context['task_instance'].xcom_push(key='error_message', value=error_message)
            raise AirflowException(f"{error_message_format}")
            
        # If validation passed, store result in XCom
        context['task_instance'].xcom_push(key='validation_result', value=True)
        
    except Exception as e:
        error_msg = str(e)
        if not isinstance(e, AirflowException):
            error_msg = f"Unexpected error during validation: {error_msg}"
        raise AirflowException(error_msg)
    
def validate_ftp_config(conf: Dict, context: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate FTP configuration and store setting in XCom
    Returns (is_valid, error_message)
    """
    try:
        ftp_value = conf.get('ftp')
        if ftp_value is not None and not isinstance(ftp_value, bool):
            return False, "FTP configuration must be a boolean (true/false)"

        # Store FTP setting in XCom with default as True if not specified
        should_upload_ftp = conf.get('ftp', True)
        print(should_upload_ftp)
        ti = context.get('task_instance')
        if ti:
            ti.xcom_push(key='should_upload_ftp', value=should_upload_ftp)
            print(f"Stored FTP setting in XCom: {should_upload_ftp}")
        else:
            print("Warning: task_instance not found in context")
        return True, None

    except Exception as e:
        return False, f"Failed to validate FTP configuration: {str(e)}"
    
def validate_check_fail(conf: Dict) -> Tuple[bool, Optional[str]]:
    """Validate check_fail configuration"""
    check_fail = conf.get('check_fail')
    if check_fail is not None and not isinstance(check_fail, bool):
        return False, "check_fail must be a boolean value"
    return True, None

def validate_slack_config(conf: Dict, context: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate FTP configuration and store setting in XCom
    Returns (is_valid, error_message)
    """
    try:
        slack_value = conf.get('slack')
        if slack_value is not None and not isinstance(slack_value, bool):
            return False, "Slack configuration must be a boolean (true/false)"

        should_slack = conf.get('slack', False)
        ti = context.get('task_instance')
        if ti:
            ti.xcom_push(key='should_slack', value=should_slack)
            print(f"Stored FTP setting in XCom: {should_slack}")
        else:
            print("Warning: task_instance not found in context")
        return True, None

    except Exception as e:
        return False, f"Failed to validate Slack configuration: {str(e)}"
    
def validate_input_task_manual(default_emails, **context):
   """Validate that required fields are present in the configuration"""
   try:
        dag_run = context['dag_run']
        conf = dag_run.conf
        run_ids = conf.get('run_id')

        if not conf:
            raise AirflowException("Configuration is required")

        # เก็บ errors ไว้ในรูปแบบ list
        errors = []
        
        # เช็คว่ามี required fields ครบ 
        required_fields = ['API_URL', 'CSV_COLUMNS', 'FTP_PATH', 'DAG_NAME', 'csvName']
        missing_fields = []
        
        if not run_ids:
            
            for field in required_fields:
                if not conf.get(field):
                    missing_fields.append(field)

            if missing_fields:
                errors.append(f"Missing required fields: {', '.join(missing_fields)}")

            # เช็คว่า CSV_COLUMNS เป็น array
            csv_columns = conf.get('CSV_COLUMNS', [])
            if not isinstance(csv_columns, list):
                errors.append("CSV_COLUMNS must be an array")
            
            # Validate dates ด้วย validate_config_dates
            is_valid, error_message = validate_config_dates(conf)
            if not is_valid:
                # แยก error messages ที่ได้จาก validate_config_dates ออกเป็นข้อๆ
                date_errors = error_message.split('\n')
                errors.extend(date_errors)

            is_valid, error_message = validate_ftp_config(conf, context)
            if not is_valid:
                errors.append(f"FTP Configuration Error: {error_message}")


        if run_ids:
            is_valid, error_message = validate_run_ids(run_ids, context['dag'].dag_id)
            if not is_valid:
                errors.append(f"Invalid run_ids: {error_message}")

        start_run = conf.get('start_run')
        if start_run:
            is_valid, error_message = validate_start_run(start_run)
            if not is_valid:
                errors.append(f"{error_message}")

        if errors:
            # กรองออกข้อที่เป็นค่าว่าง
            errors = [error for error in errors if error.strip()]
            
            error_message = "Configuration Validation Failed:<ol>"
            for error in errors:
                error_message += f"<li>{error}</li>"
            error_message += "</ol>"

            error_message_format = "Configuration Validation Failed:\n\n"
            for i, error in enumerate(errors, 1):
                error_message_format += f"{i}. {error}\n"

            context['task_instance'].xcom_push(key='error_message', value=error_message)
            raise AirflowException(error_message_format)

       # Push validation result to XCom
        ti = context['task_instance']
        ti.xcom_push(key='validation_result', value=True)

   except Exception as e:
       if not isinstance(e, AirflowException):
           error_msg = f"Unexpected error during validation: {str(e)}"
           context['task_instance'].xcom_push(key='error_message', value=error_msg)
           raise AirflowException(error_msg)
       raise

def validate_input_task_monthly(default_csv_columns: List[str], default_emails: Dict[str, List[str]], **context):
    """Validate input configuration using existing validate_config function"""
    try:
        dag_run = context['dag_run']
        
        # Get execution date from context
        execution_date = context['execution_date']
        
        # If no config provided, use default config
        if not dag_run.conf:
            default_config = get_default_config_monthly(execution_date)
            # Add default email configuration
            # default_config.update({
            #     'email': default_emails.get('email', []),
            #     'emailSuccess': default_emails.get('emailSuccess', []),
            #     'emailFail': default_emails.get('emailFail', []),
            #     'emailPause': default_emails.get('emailPause', []),
            #     'emailResume': default_emails.get('emailResume', []),
            #     'emailStart': default_emails.get('emailStart', [])
            # })
            dag_run.conf = default_config
            print(f"Using default configuration: {default_config}")
        
        conf = dag_run.conf
        
        # Use existing validate_config function
        is_valid, error_message , error_message_format = validate_config(conf, default_csv_columns, context)
        
        if not is_valid:
            context['task_instance'].xcom_push(key='error_message', value=error_message)
            raise AirflowException(f"{error_message_format}")
            
        # If validation passed, store result in XCom
        context['task_instance'].xcom_push(key='validation_result', value=True)
        
    except Exception as e:
        error_msg = str(e)
        if not isinstance(e, AirflowException):
            error_msg = f"Unexpected error during validation: {error_msg}"
        raise AirflowException(error_msg)