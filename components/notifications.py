from typing import Dict, List
from datetime import datetime
from airflow.utils.email import send_email
from airflow.exceptions import AirflowException
import pytz
from typing import Dict, List, Optional, Tuple
from components.constants import THAI_TZ
from components.process import is_manual_pause
from components.database import save_batch_state

from components.database import get_batch_state , get_initial_start_time
from components.utils import get_thai_time

# Utility functions
def format_thai_time(dt: datetime) -> str:
    """Format datetime to Thai timezone string without timezone info"""
    if dt.tzinfo is None:
        dt = THAI_TZ.localize(dt)
    thai_time = dt.astimezone(THAI_TZ)
    return thai_time.strftime('%Y-%m-%d %H:%M:%S')

def get_notification_recipients(conf: Dict, notification_type: str) -> List[str]:
    """
    Get email recipients based on notification type
    notification_type: 'success' | 'normal' | 'fail' | 'pause' | 'resume' | 'start'
    Returns combined list of recipients for the specified type
    """
    all_recipients = conf.get('email', [])
    if not isinstance(all_recipients, list):
        all_recipients = [all_recipients]
    
    if notification_type == 'success':
        # Special handling for success notifications
        success_recipients = conf.get('emailSuccess', [])
        if not isinstance(success_recipients, list):
            success_recipients = [success_recipients]
        return success_recipients
    
    # Get type-specific recipients
    type_map = {
        'fail': 'emailFail',
        'pause': 'emailPause',
        'resume': 'emailResume',
        'start': 'emailStart'
    }
    
    if notification_type in type_map:
        type_recipients = conf.get(type_map[notification_type], [])
        if not isinstance(type_recipients, list):
            type_recipients = [type_recipients]
        
        combined_recipients = list(set(all_recipients + type_recipients))
        return combined_recipients
    
    return all_recipients

# Message formatting functions
def format_running_message(dag_id: str, run_id: str, start_time: datetime, conf: Dict) -> str:
    """Format running notification message"""
    return f"""
        <h2>Batch Process {dag_id} Has Started</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Status:</strong> Starting New Process</p>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
        </ul>
    """

def format_success_message(dag_id: str, run_id: str, current_time: datetime, 
                         conf: Dict, csv_filename: str, control_filename: str,
                         batch_state: Optional[Dict] = None) -> str:
    """Format success notification message with processing details"""
    # Get initial start time
    start_time = get_initial_start_time(dag_id, run_id)
    if not start_time:
        start_time = current_time 
        
    # Ensure both times are in Thai timezone
    if current_time.tzinfo is None:
        current_time = THAI_TZ.localize(current_time)
    else:
        current_time = current_time.astimezone(THAI_TZ)
        
    if start_time.tzinfo is None:
        start_time = THAI_TZ.localize(start_time)
    else:
        start_time = start_time.astimezone(THAI_TZ)
    
    elapsed_time = current_time - start_time
    
    # Ensure elapsed time is positive
    if elapsed_time.total_seconds() < 0:
        print(f"Warning: Negative elapsed time detected. Start: {start_time}, Current: {current_time}")
        elapsed_time = abs(elapsed_time)
    
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    
    total_seconds = elapsed_time.total_seconds()
    processing_rate = (fetched_records / total_seconds) if total_seconds > 0 else 0
    
    return f"""
        <h2>Batch Process {dag_id} Has Completed Successfully</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>End Time:</strong> {format_thai_time(current_time)}</p>
        <p><strong>Total Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> Completed</p>
        
        <h3>Processing Summary:</h3>
        <ul>
            <li>Total Records Processed: {fetched_records:,}</li>
            <li>Average Processing Rate: {processing_rate:.2f} records/second</li>
        </ul>
        
        <h3>Output Information:</h3>
        <ul>
            <li>CSV Filename: {csv_filename}</li>
            <li>Control Filename: {control_filename}</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
        </ul>
    """

def format_error_message(dag_id: str, run_id: str, start_time: datetime, end_time: datetime, error_message: str, conf: Dict, batch_state: Optional[Dict] = None) -> str:
    """Format error notification message with progress details"""

    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records') if batch_state else None
    current_page = batch_state.get('current_page', 1) if batch_state else 1
    
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0
    
    # Calculate elapsed time
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    # Determine if this was a pause or failure
    if "Pausing batch process" in error_message:
        status = "Paused"
        title = "Has Been Paused"
    else:
        status = "Failed"
        title = "Has Failed"
    
    # Calculate processing rate safely
    elapsed_seconds = elapsed_time.total_seconds()
    if elapsed_seconds > 0 and fetched_records > 0:
        processing_rate = fetched_records / elapsed_seconds
    else:
        processing_rate = 0
    
    return f"""
        <h2>Batch Process {dag_id} {title}</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>End Time:</strong> {format_thai_time(end_time)}</p>
        <p><strong>Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> {status}</p>
        <p><strong>Error Message:</strong> {error_message}</p>
        
        <h3>Progress Information:</h3>
        <ul>
            <li>Records Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Current Page: {current_page}</li>
            <li>Processing Rate: {processing_rate:.2f} records/second</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
            <li>CSV Columns: {conf.get('csvColumns') or conf.get('csvColumn', [])}</li>
        </ul>
    """

def format_pause_message(dag_id: str, run_id: str, start_time: datetime, end_time: datetime, pause_message: str, conf: Dict, batch_state: Optional[Dict] = None) -> str:
    """Format pause notification message with progress details"""
    # Get progress information
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records', 0) if batch_state else 0
    current_page = batch_state.get('current_page', 1) if batch_state else 1
    
    # Calculate progress percentage
    progress_percentage = (fetched_records / total_records * 100) if total_records > 0 else 0
    
    # Calculate elapsed time
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    # Calculate processing rate
    processing_rate = (fetched_records/elapsed_time.total_seconds()) if elapsed_time.total_seconds() > 0 else 0
    
    return f"""
        <h2>Batch Process {dag_id} Has Been Paused</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Pause Time:</strong> {format_thai_time(end_time)}</p>
        <p><strong>Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> Paused</p>
        <p><strong>Pause Reason:</strong> {pause_message}</p>
        
        <h3>Progress Information:</h3>
        <ul>
            <li>Records Processed: {fetched_records:,} / {total_records:,}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Current Page: {current_page}</li>
            <li>Processing Rate: {processing_rate:.2f} records/second</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
        </ul>
    """

def format_manual_pause_message(dag_id: str, run_id: str, start_time: datetime, 
                              end_time: datetime, conf: Dict, batch_state: Optional[Dict] = None) -> str:
    """Format manual pause notification message with progress details"""
    # Get progress information with safe default values
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records') if batch_state else None
    current_page = batch_state.get('current_page', 1) if batch_state else 1
    
    # Calculate progress percentage safely
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0
    
    # Calculate elapsed time
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    # Calculate processing rate safely
    processing_rate = (fetched_records/elapsed_time.total_seconds()) if elapsed_time.total_seconds() > 0 else 0
    
    return f"""
        <h2>Batch Process {dag_id} Has Been Manually Paused</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Pause Time:</strong> {format_thai_time(end_time)}</p>
        <p><strong>Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> Manually Paused</p>
        <p><strong>Reason:</strong> Process was manually paused by operator</p>
        
        <h3>Progress Information:</h3>
        <ul>
            <li>Records Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Current Page: {current_page}</li>
            <li>Processing Rate: {processing_rate:.2f} records/second</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
            <li>CSV Columns: {conf.get('csvColumns') or conf.get('csvColumn', [])}</li>
        </ul>
        
        <p><em>Note: To resume this process, please run the batch again with the same Run ID.</em></p>
    """

def format_resume_message(dag_id: str, run_id: str, start_time: datetime, conf: Dict, previous_state: Dict) -> str:
    """Format resume notification message"""
    previous_status = previous_state.get('status', 'Unknown')
    previous_fetch_count = previous_state.get('fetched_records', 0)
    total_records = previous_state.get('total_records', 0)
    last_updated = previous_state.get('updated_at')
    
    if isinstance(last_updated, str):
        last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
    
    progress_percentage = (previous_fetch_count / total_records * 100) if total_records > 0 else 0
    
    return f"""
        <h2>Batch Process {dag_id} Has Resumed</h2>
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Resume Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Status:</strong> Resuming from {previous_status}</p>
        
        <h3>Previous Progress:</h3>
        <ul>
            <li>Records Processed: {previous_fetch_count:,} / {total_records:,}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Last Updated: {format_thai_time(last_updated) if last_updated else 'Unknown'}</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            <li>Pause Time: {conf.get('pause', 'Not specified')}</li>
        </ul>
    """

# Main notification functions
def send_email_notification(to: List[str], subject: str, html_content: str):
    """Send email notification"""
    try:
        send_email(
            to=to,
            subject=subject,
            html_content=html_content
        )
        print(f"Email sent successfully to: {to}")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

def send_notification(subject: str, html_content: str, conf: Dict, notification_type: str):
    """
    Send notification to appropriate recipients based on type
    """
    recipients = get_notification_recipients(conf, notification_type)
    if not recipients:
        print(f"No recipients specified for notification type: {notification_type}")
        return
    
    try:
        send_email_notification(recipients, subject, html_content)
        print(f"Notification sent to {notification_type} recipients: {recipients}")
    except Exception as e:
        print(f"Failed to send {notification_type} notification: {str(e)}")

# Task notification handlers
def send_running_notification(**context):
    """Send notification when DAG starts running or resumes"""
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    start_time = get_thai_time()
    conf = dag_run.conf or {}
    
    context['task_instance'].xcom_push(key='batch_start_time', value=start_time.isoformat())
    
    previous_state = get_batch_state(dag_id, run_id)
    is_resume = previous_state is not None
    
    if is_resume:
        print(f"Found previous state with status: {previous_state.get('status')}")
        subject = f"Batch Process {dag_id} Resumed at {format_thai_time(start_time)}"
        html_content = format_resume_message(dag_id, run_id, start_time, conf, previous_state)
        
        # Send resume notification
        send_notification(subject, html_content, conf, 'resume')
    else:
        print("No previous state found, sending start notification")
        subject = f"Batch Process {dag_id} Started at {format_thai_time(start_time)}"
        html_content = format_running_message(dag_id, run_id, start_time, conf)
        
        # Send start notification
        send_notification(subject, html_content, conf, 'start')

def send_success_notification(**context):
    """Send success notification"""
    ti = context['task_instance']
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    conf = dag_run.conf or {}
    
    # Get process result
    process_result = ti.xcom_pull(task_ids='process_data')
    
    if isinstance(process_result, tuple) and len(process_result) == 4:
        _, csv_filename, _, control_filename = process_result
    else:
        csv_filename = ti.xcom_pull(key='output_filename')
        control_filename = ti.xcom_pull(key='control_filename', default='Not available')
    
    # Get batch state for progress information and initial start time
    batch_state = get_batch_state(dag_id, run_id)
    
    if batch_state and batch_state.get('initial_start_time'):
        if isinstance(batch_state['initial_start_time'], str):
            start_time = datetime.fromisoformat(
                batch_state['initial_start_time'].replace('Z', '+00:00')
            )
        else:
            start_time = batch_state['initial_start_time']
    else:
        start_time_str = ti.xcom_pull(key='batch_start_time')
        start_time = datetime.fromisoformat(start_time_str)
    
    end_time = get_thai_time()
    
    # Format success message with both CSV and control file information
    subject = f"Batch Process {dag_id} Completed Successfully"
    html_content = format_success_message(
        dag_id, 
        run_id,
        end_time, 
        conf, 
        csv_filename,
        control_filename,
        batch_state
    )
    
    # Send success notification to both groups
    if conf.get('emailSuccess'):
        send_notification(subject, html_content, conf, 'success')
    
    # Send to normal recipients
    send_notification(subject, html_content, conf, 'normal')

def send_failure_notification(**context):
    """Send failure or pause notification"""
    ti = context['task_instance']
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    conf = dag_run.conf or {}
    batch_state = get_batch_state(dag_id, run_id)
    
    start_time_str = ti.xcom_pull(key='batch_start_time')
    start_time = datetime.fromisoformat(start_time_str)
    end_time = get_thai_time()
    
    process_result = ti.xcom_pull(task_ids='process_data')
    error_message = ti.xcom_pull(key='error_message')
    
    # Check if this is a manual pause
    if is_manual_pause(error_message):
        save_batch_state(
            batch_id=dag_id,
            run_id=run_id,
            start_date=conf.get('startDate'),
            end_date=conf.get('endDate'),
            current_page=batch_state.get('current_page', 1) if batch_state else 1,
            last_search_after=batch_state.get('last_search_after') if batch_state else None,
            status='PAUSED',
            error_message="Process was manually paused by operator",
            total_records=batch_state.get('total_records') if batch_state else None,
            fetched_records=batch_state.get('fetched_records', 0) if batch_state else 0
        )
        
        subject = f"Batch Process {dag_id} Has Been Manually Paused"
        html_content = format_manual_pause_message(
            dag_id, run_id, start_time, end_time, conf, batch_state
        )
        
        # Send manual pause notification
        send_notification(subject, html_content, conf, 'pause')
        
    elif isinstance(process_result, dict) and process_result.get('status') == 'paused':
        # Regular pause case (time-based pause)
        pause_message = process_result.get('message', 'Process was paused')
        subject = f"Batch Process {dag_id} Has Been Paused"
        html_content = format_pause_message(
            dag_id, run_id, start_time, end_time, pause_message, conf, batch_state
        )
        
        # Send regular pause notification
        send_notification(subject, html_content, conf, 'pause')
        
    else:
        error_message = error_message or "Unknown error"
        subject = f"Batch Process {dag_id} Failed"
        html_content = format_error_message(
            dag_id, run_id, start_time, end_time, error_message, conf, batch_state
        )
        
        # Send failure notification
        send_notification(subject, html_content, conf, 'fail')