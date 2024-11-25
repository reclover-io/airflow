import requests
from typing import Dict, Optional
from datetime import datetime
from airflow.exceptions import AirflowException
from components.database import get_initial_start_time
from components.constants import THAI_TZ

class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_message(self, message: Dict) -> None:
        """Send message to Slack"""
        try:
            response = requests.post(self.webhook_url, json=message)
            if response.status_code == 200:
                print("Message sent to Slack successfully")
            else:
                error_msg = f"Failed to send message to Slack: Status code {response.status_code}"
                print(error_msg)
                raise AirflowException(error_msg)
        except Exception as e:
            error_msg = f"Error sending message to Slack: {str(e)}"
            print(error_msg)
            raise AirflowException(error_msg)
        
def get_csv_column_text(conf: Dict) -> str:
    """
    Generate the text for CSV Columns if they exist in the configuration.
    """
    if 'csvColumn' in conf and conf['csvColumn']:
        return f"\t\t\t\t•\tCSV Column: [ {', '.join(conf['csvColumn'])} ]\n"
    return ""


def format_thai_time(dt: datetime) -> str:
    """Format datetime to Thai timezone string without timezone info"""
    if dt.tzinfo is None:
        dt = THAI_TZ.localize(dt)
    thai_time = dt.astimezone(THAI_TZ)
    return thai_time.strftime('%Y-%m-%d %H:%M:%S')

def add_note_and_divider(note: str) -> list:
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": note
            }
        },
        {
            "type": "divider"
        }
    ]

def create_slack_header(title: str) -> Dict:
    return {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": title
        }
    }

def create_slack_section(text: str) -> Dict:
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": text
        }
    }


def create_slack_running_message(
    dag_id: str,
    run_id: str,
    start_time: datetime,
    conf: Optional[Dict] = None,
) -> Dict:
    """Create Slack message matching email format"""

    csv_column_text = get_csv_column_text(conf)

    blocks = []
    blocks.extend([
        create_slack_header(f"Batch Process {dag_id} Has Started at {format_thai_time(start_time)}"),
        create_slack_section(f"*Batch Name:* {dag_id}"),
        create_slack_section(f"*Run ID:* {run_id}"),
        create_slack_section(f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_slack_section(f"*Status:* Starting New Process"),
        create_slack_section(
                        f"*Batch Configuration:*\n\n"
                        f"\t\t\t\t•\tStart Date: {conf.get('startDate', 'Not specified')}\n"
                        f"\t\t\t\t•\tEnd Date: {conf.get('endDate', 'Not specified')}\n"
                        f"{csv_column_text}"),
        {
            "type": "divider"
        }
        
        ])

    return {
        "blocks": blocks
    }

def create_slack_resume_message(title: str, dag_id: str, run_id: str, start_time: datetime, conf: Dict, previous_state: Dict, batch_state: Optional[Dict] = None
) -> Dict:
    """Create Slack message matching email format"""

    previous_status = previous_state.get('status', 'Unknown')
    csv_column_text = get_csv_column_text(conf)
    fetched_records = batch_state.get('fetched_records', 0)
    total_records = batch_state.get('total_records')
    last_updated = previous_state.get('updated_at')
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0
    csv_column_text = get_csv_column_text(conf)

    blocks = []
    blocks.extend([
        create_slack_header(title),
        create_slack_section(f"*Batch Name:* {dag_id}"),
        create_slack_section(f"*Run ID:* {run_id}"),
        create_slack_section(f"*Resume Time:* {format_thai_time(start_time)}"),
        create_slack_section(f"*Status:* Resuming from {previous_status}"),
        create_slack_section(
                    f"*Previous Progress:*\n\n"
                    f"\t\t\t\t•\tRecords Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}\n"
                    f"\t\t\t\t•\tProgress: {progress_percentage:.2f}%\n"
                    f"\t\t\t\t•\tLast Updated: {format_thai_time(last_updated) if last_updated else 'Unknown'}"),
        create_slack_section(
                    f"*Batch Configuration:*\n\n"
                    f"\t\t\t\t•\tStart Date: {conf.get('startDate', 'Not specified')}\n"
                    f"\t\t\t\t•\tEnd Date: {conf.get('endDate', 'Not specified')}\n"
                    f"{csv_column_text}"),
        {
            "type": "divider"
        }
        ])

    return {
        "blocks": blocks
    }

def create_slack_pause_message(
    title: str,
    status: str,
    dag_id: str,
    run_id: str,
    start_time: datetime,
    end_time: Optional[datetime] = None,
    pause_message: Optional[str] = None,
    conf: Optional[Dict] = None,
    batch_state: Optional[Dict] = None
) -> Dict:
    """Create Slack message matching email format"""
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    fetched_records = batch_state.get('fetched_records', 0)
    total_records = batch_state.get('total_records')
    last_updated = previous_state.get('updated_at')
    current_page = batch_state.get('current_page', 1)
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0

    csv_column_text = get_csv_column_text(conf)

    blocks = [
        create_slack_header(title),
        create_slack_section(f"*Batch Name:* {dag_id}"),
        create_slack_section(f"*Run ID:* {run_id}"),
        create_slack_section(f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_slack_section(f"*Pause Time:* {end_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_slack_section(f"*Elapsed Time:* {elapsed_str}"),
        create_slack_section(f"*Status:* {status}"),
        create_slack_section(f"*Pause Reason:* {pause_message}")
    ]

    if batch_state:
        blocks.append(
            create_slack_section(
                f"*Previous Progress:*\n\n"
                f"\t\t\t\t•\tRecords Processed: {fetched_records:,} / {total_records:,}\n"
                f"\t\t\t\t•\tProgress: {progress_percentage:.2f}%\n"
                f"\t\t\t\t•\tCurrent Page: {current_page}"
            )
        )
    
    if conf:
        blocks.append(
            create_slack_section(
                f"*Batch Configuration:*\n\n"
                f"\t\t\t\t•\tStart Date: {conf.get('startDate', 'Not specified')}\n"
                f"\t\t\t\t•\tEnd Date: {conf.get('endDate', 'Not specified')}\n"
                f"{csv_column_text}"
            )
        )

    blocks.extend(add_note_and_divider("_Note: To resume this process, please run the batch again with the same Run ID._"))

    return {
        "blocks": blocks
    }


def create_slack_manual_pause_message(
    title: str,
    dag_id: str,
    run_id: str,
    start_time: datetime,
    end_time: Optional[datetime] = None,
    conf: Optional[Dict] = None,
    batch_state: Optional[Dict] = None
) -> Dict:
    """Create Slack message matching email format"""
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    
    current_page = batch_state.get('current_page', 1)
    fetched_records = batch_state.get('fetched_records', 0)
    total_records = batch_state.get('total_records', 0)
    progress_percentage = (fetched_records / total_records * 100) if total_records > 0 else 0

    csv_column_text = get_csv_column_text(conf)

    blocks = [
        create_slack_header(title),
        create_slack_section(f"*Batch Name:* {dag_id}"),
        create_slack_section(f"*Run ID:* {run_id}"),
        create_slack_section(f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_slack_section(f"*Pause Time:* {end_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_slack_section(f"*Elapsed Time:* {elapsed_str}"),
        create_slack_section(f"*Status:* Manually Paused"),
        create_slack_section(f"*Pause Reason:* Process was manually paused by operator")
    ]

    if batch_state:
        blocks.append(
            create_slack_section(
                f"*Progress Information:*\n\n"
                f"\t\t\t\t•\tRecords Processed: {fetched_records:,} / {total_records:,}\n"
                f"\t\t\t\t•\tProgress: {progress_percentage:.2f}%\n"
                f"\t\t\t\t•\tCurrent Page: {current_page}"
            )
        )
    
    if conf:
        blocks.append(
            create_slack_section(
                f"*Batch Configuration:*\n\n"
                f"\t\t\t\t•\tStart Date: {conf.get('startDate', 'Not specified')}\n"
                f"\t\t\t\t•\tEnd Date: {conf.get('endDate', 'Not specified')}\n"
                f"{csv_column_text}"
            )
        )

    blocks.extend(add_note_and_divider("_Note: To resume this process, please run the batch again with the same Run ID._"))

    return {
        "blocks": blocks
    }
    
def create_slack_retry_message(
    title: str,
    dag_id: str,
    run_id: str,
    retry_count: int,
    max_retries: int,
    current_time: datetime,
    error_message: Optional[str] = None,
    conf: Optional[Dict] = None,
    batch_state: Optional[Dict] = None
) -> Dict:
    """Create Slack message matching email format"""

    current_page = batch_state.get('current_page', 1)
    fetched_records = batch_state.get('fetched_records', 0)
    total_records = batch_state.get('total_records')
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0
            
    blocks = []
    blocks.extend([create_slack_header(title),
                   create_slack_section(f"*Batch Name:* {dag_id}"),
                   create_slack_section(f"*Run ID:* {run_id}"),
                   create_slack_section(f"*Time:* {format_thai_time(current_time)}"),
                   create_slack_section(f"*Status:* Retry {retry_count} of {max_retries}"),
                   create_slack_section(f"*Error Message:* {error_message}"),
                   create_slack_section(
                        f"*Progress Information:*\n\n"
                        f"\t\t\t\t•\tRecords Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}\n"
                        f"\t\t\t\t•\tProgress: {progress_percentage:.2f}%\n"
                        f"\t\t\t\t•\tCurrent Page: {current_page}"),       
    ])
    blocks.extend(add_note_and_divider("_Note: System will automatically retry the process._"))

    return {
        "blocks": blocks
    }

def create_slack_success_message(dag_id: str, run_id: str, start_time: datetime, end_time: datetime,
                         conf: Dict, csv_filename: str, control_filename: str,
                         batch_state: Optional[Dict] = None
) -> Dict:
    """Create Slack message matching email format"""
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"
    start_time = get_initial_start_time(dag_id, run_id)
    fetched_records = batch_state.get('fetched_records', 0)
    range_time_start = conf.get('startDate')
    range_time_end = conf.get('endDate')
    ftp_path = (
            "ftps://10.250.1.101/ELK/daily/source_data/landing/ELK_MobileApp_Activity_Logs/"
            if conf.get("ftp") else
            "/data/batch/ELK_MobileApp_Activity_Logs/"
        )
    csv_column_text = get_csv_column_text(conf)
    current_page = batch_state.get('current_page', 1)

    blocks = []
    blocks.extend([
        create_slack_header(f"Batch Process {dag_id} Completed Successfully"),
        create_slack_section(f"*Batch Name:* {dag_id}"),
        create_slack_section(f"*Run ID:* {run_id}"),
        create_slack_section(f"*Start Time:* {format_thai_time(start_time)}"),
        create_slack_section(f"*End Time:* {format_thai_time(end_time)}"),
        create_slack_section(f"*Total Elapsed Time:* {elapsed_str}"),
        create_slack_section(f"*Status:* Completed"),
        create_slack_section(
                    f"*Processing Summary:*\n\n"
                    f"\t\t\t\t•\tTotal Records Processed: {fetched_records:,}\n"
                    f"\t\t\t\t•\tRange Time: {range_time_start} - {range_time_end}"),
        create_slack_section(
                    f"*Output Information:*\n\n"
                    f"\t\t\t\t•\tPath: {ftp_path}\n"
                    f"\t\t\t\t•\tCSV Filename: {csv_filename}\n"
                    f"\t\t\t\t•\tControl Filename: {control_filename}"),
        create_slack_section(
                    f"*Batch Configuration:*\n\n"
                    f"\t\t\t\t•\tStart Date: {conf.get('startDate', 'Not specified')}\n"
                    f"\t\t\t\t•\tEnd Date: {conf.get('endDate', 'Not specified')}\n"
                    f"{csv_column_text}"),
        {"type": "divider"}
        ])

    return {
        "blocks": blocks
    }

def create_slack_err_message(
    title: str,
    dag_id: str,
    run_id: str,
    start_time: datetime,
    end_time: Optional[datetime] = None,
    error_message: Optional[str] = None,
    conf: Optional[Dict] = None,
    batch_state: Optional[Dict] = None
) -> Dict:

    if "Pausing batch process" in error_message:
        status = "Paused"
    else:
        status = "Failed"
    
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"

    fetched_records = batch_state.get('fetched_records', 0)
    total_records = batch_state.get('total_records')
    current_page = batch_state.get('current_page', 1)
    csv_column_text = get_csv_column_text(conf)
    
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0

    blocks = []
    blocks = [
        create_slack_header(title),
        create_slack_section(f"*Batch Name:* {dag_id}"),
        create_slack_section(f"*Run ID:* {run_id}"),
        create_slack_section(f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_slack_section(f"*End Time:* {end_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_slack_section(f"*Elapsed Time:* {elapsed_str}"),
        create_slack_section(f"*Status:* {status}"),
        create_slack_section(f"*Error Message:* {error_message}")
    ]

    blocks.append(
        create_slack_section(
            f"*Progress Information:*\n\n"
            f"\t\t\t\t•\tRecords Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}\n"
            f"\t\t\t\t•\tProgress: {progress_percentage:.2f}%\n"
            f"\t\t\t\t•\tCurrent Page: {current_page}"
        )
    )

    if conf:
        blocks.append(
            create_slack_section(
                f"*Batch Configuration:*\n\n"
                f"\t\t\t\t•\tStart Date: {conf.get('startDate', 'Not specified')}\n"
                f"\t\t\t\t•\tEnd Date: {conf.get('endDate', 'Not specified')}\n"
                f"{csv_column_text}"
            )
        )

    blocks.extend(add_note_and_divider("_Note: To resume this process, please run the batch again with the same Run ID._"))

    return {
        "blocks": blocks
    }
