from typing import Dict, List
from datetime import datetime
from airflow.utils.email import send_email
from airflow.exceptions import AirflowException
import pytz
from typing import Dict, List, Optional, Tuple
from components.constants import THAI_TZ
from components.database import save_batch_state

from components.database import get_batch_state , get_initial_start_time
from components.utils import get_thai_time
from components.slack_notifications import *


def format_thai_time(dt: datetime) -> str:
    """Format datetime to Thai timezone string without timezone info"""
    if dt.tzinfo is None:
        dt = THAI_TZ.localize(dt)
    thai_time = dt.astimezone(THAI_TZ)
    return thai_time.strftime('%Y-%m-%d %H:%M:%S')

def format_csv_columns(conf: Dict) -> str:
    """Generate the HTML for CSV columns."""
    if 'csvColumns' in conf or 'csvColumn' in conf:
        csv_columns = conf.get('csvColumns') or conf.get('csvColumn', [])
        return f"<li>CSV Column: [ {', '.join(csv_columns)} ]</li>"
    return ""

def get_notification_recipients(conf: Dict, notification_type: str, default_emails: Dict[str, List[str]]) -> List[str]:
    """
    Get email recipients based on notification type
    notification_type: 'success' | 'normal' | 'fail' | 'pause' | 'resume' | 'start'
    Returns combined list of recipients for the specified type
    """
    all_recipients = conf.get('email', default_emails.get('email', []))
    if not isinstance(all_recipients, list):
        all_recipients = [all_recipients]
    
    if notification_type == 'success':
        success_recipients = conf.get('emailSuccess', default_emails.get('emailSuccess', []))
        if not isinstance(success_recipients, list):
            success_recipients = [success_recipients]
        return success_recipients
    
    type_map = {
        'success': 'emailSuccess',
        'fail': 'emailFail',
        'pause': 'emailPause',
        'resume': 'emailResume',
        'start': 'emailStart'
    }
    
    if notification_type in type_map:
        config_key = type_map[notification_type]
        type_recipients = conf.get(config_key, default_emails.get(config_key, []))
        if not isinstance(type_recipients, list):
            type_recipients = [type_recipients]
        
        combined_recipients = list(set(all_recipients + type_recipients))
        return combined_recipients
    
    return all_recipients

def format_running_message(dag_id: str, run_id: str, start_time: datetime, conf: Dict) -> str:
    """Format running notification message"""

    csv_columns_html = format_csv_columns(conf)

    return f"""
        <p><strong>Batch Name:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Status:</strong> Starting New Process</p>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            {csv_columns_html}
        </ul>
    """

def format_success_message(dag_id: str, run_id: str, current_time: datetime, 
                         conf: Dict, csv_filename: str, control_filename: str,
                         batch_state: Optional[Dict] = None, ftp_status: str = None) -> str:
    """Format success notification message with processing details"""

    start_time = get_initial_start_time(dag_id, run_id)
    if not start_time:
        start_time = current_time 
        
    if current_time.tzinfo is None:
        current_time = THAI_TZ.localize(current_time)
    else:
        current_time = current_time.astimezone(THAI_TZ)
        
    if start_time.tzinfo is None:
        start_time = THAI_TZ.localize(start_time)
    else:
        start_time = start_time.astimezone(THAI_TZ)
    
    elapsed_time = current_time - start_time
    
    if elapsed_time.total_seconds() < 0:
        print(f"Warning: Negative elapsed time detected. Start: {start_time}, Current: {current_time}")
        elapsed_time = abs(elapsed_time)
    
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    
    total_seconds = elapsed_time.total_seconds()
    processing_rate = (fetched_records / total_seconds) if total_seconds > 0 else 0
    
    ftp_path = (
        "ftps://10.250.1.101/ELK/daily/source_data/landing/ELK_MobileApp_Activity_Logs/"
        if conf.get("ftp") else
        "/data/batch/ELK_MobileApp_Activity_Logs/"
    )

    csv_columns_html = format_csv_columns(conf)

    ftp_config = (
        "" if conf.get('ftp') else (
            "<h3>Batch Configuration:</h3>"
            "<ul>"
            f"<li>Start Date: {conf.get('startDate')}</li>"
            f"<li>End Date: {conf.get('endDate')}</li>"
            f"{csv_columns_html}"
            "</ul>"
        )
    )

    return f"""
        <p><strong>Batch Name:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>End Time:</strong> {format_thai_time(current_time)}</p>
        <p><strong>Total Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> Completed</p>
        
        <h3>Processing Summary:</h3>
        <ul>
            <li>Total Records Processed: {fetched_records:,}</li>
            <li>Range Time: {conf.get('startDate')} - {conf.get('endDate')}</li>
        </ul>
        
        <h3>Output Information:</h3>
        <ul>
            <li>Path: {ftp_path}</li>
            <li>CSV Filename: {csv_filename}</li>
            <li>Control Filename: {control_filename}</li>
        </ul>
        
        {ftp_config}
        
    """

def format_error_message(dag_id: str, run_id: str, start_time: datetime, end_time: datetime, error_message: str, conf: Dict, batch_state: Optional[Dict] = None) -> str:
    """Format error notification message with progress details"""


    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records') if batch_state else 0
    current_page = batch_state.get('current_page', 1) if batch_state else 1

    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0
    
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    if "Pausing batch process" in error_message:
        status = "Paused"
        title = "Has Been Paused"
    else:
        status = "Failed"
        title = "Has Failed"
    
    elapsed_seconds = elapsed_time.total_seconds()
    if elapsed_seconds > 0 and fetched_records > 0:
        processing_rate = fetched_records / elapsed_seconds
    else:
        processing_rate = 0

    csv_columns_html = format_csv_columns(conf)


    
    return f"""
        <p><strong>Batch Name:</strong> {dag_id}</p>
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
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            {csv_columns_html}
        </ul>

        <p><em>Note: To resume this process, please run the batch again with the same Run ID.</em></p>
    """

def format_pause_message(dag_id: str, run_id: str, start_time: datetime, end_time: datetime, pause_message: str, conf: Dict, batch_state: Optional[Dict] = None) -> str:
    """Format pause notification message with progress details"""
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records', 0) if batch_state else 0
    current_page = batch_state.get('current_page', 1) if batch_state else 1
    
    progress_percentage = (fetched_records / total_records * 100) if total_records > 0 else 0
    
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    processing_rate = (fetched_records/elapsed_time.total_seconds()) if elapsed_time.total_seconds() > 0 else 0
    csv_columns_html = format_csv_columns(conf)

    return f"""
        <p><strong>Batch Name:</strong> {dag_id}</p>
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
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            {csv_columns_html}
        </ul>

        <p><em>Note: To resume this process, please run the batch again with the same Run ID.</em></p>
    """

def format_manual_pause_message(dag_id: str, run_id: str, start_time: datetime, 
                              end_time: datetime, conf: Dict, batch_state: Optional[Dict] = None) -> str:
    """Format manual pause notification message with progress details"""
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records') if batch_state else None
    current_page = batch_state.get('current_page', 1) if batch_state else 1
    
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0
    
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    processing_rate = (fetched_records/elapsed_time.total_seconds()) if elapsed_time.total_seconds() > 0 else 0
    
    csv_columns_html = format_csv_columns(conf)

    return f"""
        <p><strong>Batch Name:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Start Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Pause Time:</strong> {format_thai_time(end_time)}</p>
        <p><strong>Elapsed Time:</strong> {elapsed_str}</p>
        <p><strong>Status:</strong> Manually Paused</p>
        <p><strong>Pause Reason::</strong> Process was manually paused by operator</p>
        
        <h3>Progress Information:</h3>
        <ul>
            <li>Records Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Current Page: {current_page}</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            {csv_columns_html}
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
    
    if total_records and total_records > 0:
        progress_percentage = (previous_fetch_count / total_records * 100)
    else:
        progress_percentage = 0

    csv_columns_html = format_csv_columns(conf)

    return f"""
        <p><strong>Batch Name:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Resume Time:</strong> {format_thai_time(start_time)}</p>
        <p><strong>Status:</strong> Resuming from {previous_status}</p>
        
        <h3>Previous Progress:</h3>
        <ul>
            <li>Records Processed: {previous_fetch_count:,} {f'/ {total_records:,}' if total_records else ''}</li>
            <li>Progress: {progress_percentage:.2f}%</li>
            <li>Last Updated: {format_thai_time(last_updated) if last_updated else 'Unknown'}</li>
        </ul>
        
        <h3>Batch Configuration:</h3>
        <ul>
            <li>Start Date: {conf.get('startDate')}</li>
            <li>End Date: {conf.get('endDate')}</li>
            {csv_columns_html}
        </ul>
    """
def format_retry_message(dag_id: str, run_id: str, error_message: str, 
                        retry_count: int, max_retries: int, current_time: datetime,
                        batch_state: Optional[Dict] = None) -> str:
    """Format retry notification message"""
    
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records') if batch_state else None
    current_page = batch_state.get('current_page', 1) if batch_state else 1
    
    progress = (fetched_records / total_records * 100) if total_records and total_records > 0 else 0
    
    return f"""
        <p><strong>Batch Process:</strong> {dag_id}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Time:</strong> {format_thai_time(current_time)}</p>
        <p><strong>Status:</strong> Retry {retry_count} of {max_retries}</p>
        <p><strong>Error Message:</strong> {error_message}</p>
        
        <h3>Progress Information:</h3>
        <ul>
            <li>Records Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}</li>
            <li>Progress: {progress:.2f}%</li>
            <li>Current Page: {current_page}</li>
        </ul>
        
        <p><em>Note: System will automatically retry the process.</em></p>
    """

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
        error_msg = f"Failed to send email to {to}: {str(e)}"
        print(f"Failed to send email: {str(e)}")
        raise AirflowException(error_msg)

def send_notification(
    subject: str, 
    html_content: str, 
    conf: Dict, 
    notification_type: str, 
    default_emails: Dict[str, List[str]],
    slack_webhook: Optional[str] = None,
    context: Optional[Dict] = None,
    current_time: Optional[datetime] = None,
    retry_count: Optional[int] = None,
    max_retries: Optional[int] = None,
    previous_state: Optional[Dict] = None, **kwargs
    
):
    """Send notification to appropriate recipients based on type"""

    email_sent = False
    slack_sent = False
    errors = []
    ti = context['task_instance']
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    conf = dag_run.conf or {}

    recipients = get_notification_recipients(conf, notification_type, default_emails)
    if recipients:
        try:
            send_email_notification(recipients, subject, html_content)
            print(f"Email notification sent to {notification_type} recipients: {recipients}")
            email_sent = True

        except Exception as e:
            error_msg = f"Failed to send email notification: {str(e)}"
            print(f"Failed to send email {notification_type} notification: {str(e)}")
            errors.append(error_msg)
        
    should_slack = ti.xcom_pull(key='should_slack', task_ids='validate_input')

    if should_slack:
        if slack_webhook and context:
            try:
                dag_run = context['dag_run']
                ti = context['task_instance']
                
                batch_state = get_batch_state(dag_run.dag_id, dag_run.run_id)
                
                start_time_str = ti.xcom_pull(key='batch_start_time')
                start_time = datetime.fromisoformat(start_time_str) if start_time_str else get_thai_time()

                notifier = SlackNotifier(slack_webhook)

                if notification_type == "start":
                    slack_message = create_slack_running_message(
                        dag_id=dag_run.dag_id,
                        run_id=dag_run.run_id,
                        start_time=start_time,
                        conf=conf
                    )

                elif notification_type == "success" or notification_type == "SUCCESS" or notification_type == "normal":
                    csv_filename = ti.xcom_pull(key='output_filename')
                    control_filename = ti.xcom_pull(key='control_filename', default='Not available')
                    slack_message = create_slack_success_message(
                        dag_id=dag_run.dag_id,
                        run_id=dag_run.run_id,
                        start_time=start_time,
                        end_time=current_time,
                        conf=conf,
                        csv_filename=ti.xcom_pull(key='output_filename'),
                        control_filename=ti.xcom_pull(key='control_filename', default='Not available'),
                        batch_state=batch_state,
                    )
                    
                elif notification_type == "fail" and retry_count and max_retries:
                    error_message = ti.xcom_pull(key='error_message', default='Unknown error')
                    slack_message = create_slack_retry_message(
                        title=subject,
                        dag_id=dag_run.dag_id,
                        run_id=dag_run.run_id,
                        retry_count=retry_count,
                        max_retries=max_retries,
                        current_time=current_time,
                        error_message=error_message,
                        conf=conf,
                        batch_state=batch_state
                    )

                elif notification_type == "fail":
                    error_message = ti.xcom_pull(key='error_message', default='Unknown error')
                    slack_message = create_slack_err_message(
                        title=subject,
                        dag_id=dag_run.dag_id,
                        run_id=dag_run.run_id,
                        start_time=start_time,
                        end_time=current_time,
                        error_message=error_message,
                        conf=conf,
                        batch_state=batch_state
                    )

                elif notification_type == "pause":
                    slack_message = create_slack_manual_pause_message(
                        title=subject,
                        dag_id=dag_run.dag_id,
                        run_id=dag_run.run_id,
                        start_time=start_time,
                        end_time=current_time,
                        conf=conf,
                        batch_state=batch_state
                    )

                elif notification_type == "resume":
                    slack_message = create_slack_resume_message(
                        title=subject,
                        dag_id=dag_run.dag_id,
                        run_id=dag_run.run_id,
                        start_time=start_time,
                        conf=conf,
                        previous_state=previous_state,
                        batch_state=batch_state
                    )

                else:
                    raise ValueError(f"Unsupported notification type: {notification_type}")

                notifier.send_message(slack_message)
                slack_sent = True
            except Exception as e:
                error_msg = f"Failed to send Slack notification: {str(e)}"
                print(f"Failed to send Slack notification: {str(e)}")
                errors.append(error_msg)
                raise AirflowException(error_msg)
        
    # if (not email_sent and not slack_sent) or (slack_webhook and not slack_sent):
    #     error_msg = "Failed to send notifications:\n" + "\n".join(errors)
    #     raise AirflowException(error_msg)
    if (not email_sent):
        error_msg = "Failed to send notifications:\n" + "\n".join(errors)
        raise AirflowException(error_msg)

def send_running_notification(default_emails, slack_webhook=None, **context):
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
        
        send_notification(subject, html_content, conf, 'resume', default_emails, slack_webhook, context ,None,None,None,previous_state)
    else:
        print("No previous state found, sending start notification")
        subject = f"Batch Process {dag_id} Started at {format_thai_time(start_time)}"
        html_content = format_running_message(dag_id, run_id, start_time, conf)
        
        send_notification(subject, html_content, conf, 'start', default_emails, slack_webhook, context)

def send_success_notification(default_emails, slack_webhook=None, **context):
    """Send success notification"""
    ti = context['task_instance']
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    conf = dag_run.conf or {}
    
    process_result = ti.xcom_pull(task_ids='process_data')
    
    if isinstance(process_result, tuple) and len(process_result) == 4:
        _, csv_filename, _, control_filename = process_result
    else:
        csv_filename = ti.xcom_pull(key='output_filename')
        control_filename = ti.xcom_pull(key='control_filename', default='Not available')
    
    batch_state = get_batch_state(dag_id, run_id)

    should_upload_ftp = ti.xcom_pull(key='should_upload_ftp', task_ids='validate_input')
    ftp_status = "FTP upload skipped (disabled in config)" if not should_upload_ftp else "FTP upload completed"
    
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
    
    current_time = get_thai_time()
    
    subject = f"Batch Process {dag_id} Completed Successfully"
    html_content = format_success_message(
        dag_id, 
        run_id,
        current_time, 
        conf, 
        csv_filename,
        control_filename,
        batch_state,
        ftp_status
    )
    
    if conf.get('emailSuccess'):
        send_notification(subject, html_content, conf, 'success', default_emails, slack_webhook, context,current_time=current_time)
    else:
        send_notification(subject, html_content, conf, 'normal', default_emails, slack_webhook, context,current_time=current_time)

def is_manual_pause(error_message: Optional[str]) -> bool:
    """
    Check if the error is from manual pause (SIGTERM)
    """
    sigterm_messages = [
        "Task received SIGTERM signal",
        "Task received SIGKILL signal",
        "Task was cancelled externally"
    ]
    return error_message and any(msg in error_message for msg in sigterm_messages)

def send_failure_notification(default_emails, slack_webhook=None, **context):
    """Send failure or pause notification"""
    ti = context['task_instance']
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    conf = dag_run.conf or {}
    batch_state = get_batch_state(dag_id, run_id)    

    start_time_str = ti.xcom_pull(key='batch_start_time')
    if start_time_str:
        start_time = datetime.fromisoformat(start_time_str)
    else:
        start_time = get_thai_time()

    end_time = get_thai_time()
    
    process_result = ti.xcom_pull(task_ids='process_data')
    error_message = ti.xcom_pull(key='error_message')

    failed_task_id = context.get('task_instance').task_id
    if failed_task_id == 'validate_input':
        subject = f"Batch Process {dag_id} Validation Failed"
        html_content = format_error_message(
            dag_id=dag_id,
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            error_message=error_message,
            conf=conf,
            batch_state=None
        )
        
        send_notification(subject, html_content, conf, 'fail', default_emails, slack_webhook, context, current_time=end_time)
        return
    
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
        
        send_notification(subject, html_content, conf, 'pause', default_emails, slack_webhook, context, current_time=end_time)
        
    elif isinstance(process_result, dict) and process_result.get('status') == 'paused':
        pause_message = process_result.get('message', 'Process was paused')
        subject = f"Batch Process {dag_id} Has Been Paused"
        html_content = format_pause_message(
            dag_id, run_id, start_time, end_time, pause_message, conf, batch_state
        )
        
        send_notification(subject, html_content, conf, 'pause', default_emails, slack_webhook, context, current_time=end_time)
        
    else:
        error_message = error_message or "Unknown error"
        subject = f"Batch Process {dag_id} Failed"
        html_content = format_error_message(
            dag_id, run_id, start_time, end_time, error_message, conf, batch_state
        )
        
        send_notification(subject, html_content, conf, 'fail', default_emails, slack_webhook, context, current_time=end_time)

def send_retry_notification(dag_id: str, run_id: str, error_message: str,
                          retry_count: int, max_retries: int,
                          conf: Dict, default_emails: Dict[str, List[str]],
                          slack_webhook: Optional[str] = None,
                          context: Optional[Dict] = None):
    """Send notification for retry attempts"""
    batch_state = get_batch_state(dag_id, run_id)

    current_time = get_thai_time()

    subject = f"Batch Process {dag_id} Failed - Retry {retry_count}/{max_retries}"
    html_content = format_retry_message(
        dag_id=dag_id,
        run_id=run_id,
        error_message=error_message,
        retry_count=retry_count,
        max_retries=max_retries,
        current_time=current_time,
        batch_state=batch_state
    )

    send_notification(subject, html_content, conf, 'fail', default_emails, slack_webhook, context ,current_time=current_time,retry_count=retry_count ,max_retries=max_retries )