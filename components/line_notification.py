import requests
from typing import Optional, Dict
from datetime import datetime
from components.database import get_initial_start_time
from components.constants import THAI_TZ, ACCESS_TOKEN, TO_USER_ID

api_url = "https://api.line.me/v2/bot/message/push"
access_token = ACCESS_TOKEN

def send_message(messages: Dict) -> None:
    """ส่งข้อความไปยัง LINE OA"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "to": TO_USER_ID,
        "messages": [messages]
    }
    try:
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 200:
            print("Message sent to LINE successfully")
        else:
            error_msg = f"Failed to send message to LINE: Status code {response.status_code}, Response: {response.text}"
            print(error_msg)
            raise Exception(error_msg)
    except Exception as e:
        print(f"Error sending message to LINE: {str(e)}")
        raise

def create_line_message_header(text: str) -> Dict:
    """สร้างข้อความหัวข้อในรูปแบบของ LINE"""
    return {
        "type": "text",
        "text": text
    }

def create_line_section(text: str) -> Dict:
    """สร้างข้อความ section สำหรับ LINE"""
    return {
        "type": "text",
        "text": text
    }

def create_line_divider() -> Dict:
    """สร้างข้อความ divider สำหรับ LINE"""
    return {
        "type": "text",
        "text": "------------------------------"
    }

def get_csv_column_text(conf: Dict) -> str:
    """Generate the text for CSV Columns if they exist in the configuration."""
    if 'csvColumn' in conf and conf['csvColumn']:
        return f"\t\t\t\t•\tCSV Column: [ {', '.join(conf['csvColumn'])} ]\n"
    return ""

def format_thai_time(dt: datetime) -> str:
    """Format datetime to Thai timezone string without timezone info"""
    if dt.tzinfo is None:
        dt = THAI_TZ.localize(dt)
    thai_time = dt.astimezone(THAI_TZ)
    return thai_time.strftime('%Y-%m-%d %H:%M:%S')




def create_line_running_message(dag_id: str, run_id: str, start_time: datetime, conf: Optional[Dict] = None) -> None:
    """สร้างข้อความในรูปแบบ LINE สำหรับการเริ่มต้น Batch Process"""
    message_text = [
        create_line_message_header(f"🔔 Batch Process {dag_id} Has Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_line_section(f"*Batch Name:* {dag_id}"),
        create_line_section(f"*Run ID:* {run_id}"),
        create_line_section(f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_line_section(f"*Status:* Starting New Process"),
    ]
    
    if conf:
        csv_column_text = get_csv_column_text(conf)  # แปลงข้อมูล conf ถ้ามี
        message_text.append(create_line_section(f"*Batch Configuration:*\n\n"
                                                f"• Start Date: {conf.get('startDate', 'Not specified')}\n"
                                                f"• End Date: {conf.get('endDate', 'Not specified')}\n"
                                                f"{csv_column_text}"))
    
    # เพิ่ม Divider
    message_text.append(create_line_divider())
    send_message(message_text)




def create_line_resume_message(title: str, dag_id: str, run_id: str, start_time: datetime, conf: Dict, previous_state: Dict, batch_state: Optional[Dict] = None) -> None:
    """สร้างข้อความ LINE สำหรับ resume การทำงานของ batch"""
    previous_status = previous_state.get('status', 'Unknown')
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records', 0) if batch_state else 0
    last_updated = previous_state.get('updated_at', 'Unknown')
    
    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0
    
    message_text = [
        f"🔔 *{title}*\n",
        f"*Batch Name:* {dag_id}\n",
        f"*Run ID:* {run_id}\n",
        f"*Resume Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n",
        f"*Status:* Resuming from {previous_status}\n",
        f"\n*Previous Progress:*\n"
        f"• Records Processed: {fetched_records:,} {f'/ {total_records:,}' if total_records else ''}\n"
        f"• Progress: {progress_percentage:.2f}%\n"
        f"• Last Updated: {last_updated}\n"
        f"\n*Batch Configuration:*\n"
        f"• Start Date: {conf.get('startDate', 'Not specified')}\n"
        f"• End Date: {conf.get('endDate', 'Not specified')}\n"
        f"{get_csv_column_text(conf)}\n"
    ]
    
    send_message(message_text)




def create_line_pause_message(title: str, status: str, dag_id: str, run_id: str, start_time: datetime, end_time: Optional[datetime] = None, pause_message: Optional[str] = None, conf: Optional[Dict] = None, batch_state: Optional[Dict] = None) -> None:
    """สร้างข้อความ LINE สำหรับการ pause การทำงานของ batch"""
    elapsed_time = end_time - start_time if end_time else 0
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s" if elapsed_time else 'Unknown'
    
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records', 0) if batch_state else 0
    current_page = batch_state.get('current_page', 1) if batch_state else 1

    if total_records and total_records > 0:
        progress_percentage = (fetched_records / total_records * 100)
    else:
        progress_percentage = 0

    message_text = [
        f"🔔 *{title}*\n",
        f"*Batch Name:* {dag_id}\n",
        f"*Run ID:* {run_id}\n",
        f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n",
        f"*Pause Time:* {end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'Unknown'}\n",
        f"*Elapsed Time:* {elapsed_str}\n",
        f"*Status:* {status}\n",
        f"*Pause Reason:* {pause_message}\n"
    ]
    
    if batch_state:
        message_text.append(
            f"\n*Previous Progress:*\n"
            f"• Records Processed: {fetched_records:,} / {total_records:,}\n"
            f"• Progress: {progress_percentage:.2f}%\n"
            f"• Current Page: {current_page}\n"
        )
    
    if conf:
        message_text.append(
            f"\n*Batch Configuration:*\n"
            f"• Start Date: {conf.get('startDate', 'Not specified')}\n"
            f"• End Date: {conf.get('endDate', 'Not specified')}\n"
            f"{get_csv_column_text(conf)}\n"
        )

    message_text.append("\n_Note: To resume this process, please run the batch again with the same Run ID._")
    send_message(message_text)




def create_line_retry_message(title: str, dag_id: str, run_id: str, retry_count: int, max_retries: int, current_time: datetime, error_message: Optional[str] = None, conf: Optional[Dict] = None, batch_state: Optional[Dict] = None) -> None:
    """สร้างข้อความ LINE สำหรับ retry การทำงานของ batch"""

    current_page = batch_state.get('current_page', 1) if batch_state else 1
    fetched_records = batch_state.get('fetched_records', 0) if batch_state else 0
    total_records = batch_state.get('total_records', 0) if batch_state else 0
    progress_percentage = (fetched_records / total_records * 100) if total_records > 0 else 0

    message_text = [
        f"🔄 *{title}*\n",
        f"*Batch Name:* {dag_id}\n",
        f"*Run ID:* {run_id}\n",
        f"*Time:* {format_thai_time(current_time)}\n",
        f"*Status:* Retry {retry_count} of {max_retries}\n",
        f"*Progress:* {progress_percentage:.2f}%\n",
        f"• Current Page: {current_page}\n"
    ]
    
    if error_message:
        message_text.append(f"\n*Error Message:* {error_message}")
    
    if conf:
        message_text.append(f"\n*Batch Configuration:*\n"
                            f"• Start Date: {conf.get('startDate', 'Not specified')}\n"
                            f"• End Date: {conf.get('endDate', 'Not specified')}\n"
                            f"{get_csv_column_text(conf)}\n")

    send_message(message_text)




def create_line_manual_pause_message(dag_id: str, run_id: str, start_time: datetime, pause_message: str, conf: Optional[Dict] = None) -> None:
    """สร้างข้อความ LINE สำหรับการ pause การทำงานด้วยตนเอง"""
    message_text = [
        create_line_message_header(f"🔔 Batch Process {dag_id} Paused"),
        create_line_section(f"*Batch Name:* {dag_id}"),
        create_line_section(f"*Run ID:* {run_id}"),
        create_line_section(f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}"),
        create_line_section(f"*Pause Reason:* {pause_message}"),
    ]
    
    if conf:
        message_text.append(create_line_section(f"*Batch Configuration:*\n\n"
                                                f"• Start Date: {conf.get('startDate', 'Not specified')}\n"
                                                f"• End Date: {conf.get('endDate', 'Not specified')}\n"
                                                f"{get_csv_column_text(conf)}"))

    message_text.append(create_line_divider())
    send_message(message_text)



def create_line_success_message(dag_id: str, run_id: str, start_time: datetime, end_time: Optional[datetime] = None, conf: Optional[Dict] = None) -> None:
    """สร้างข้อความ LINE เมื่อ batch ทำงานสำเร็จ"""
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'Unknown'
    elapsed_time = end_time - start_time if end_time else 0
    hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s" if elapsed_time else 'Unknown'
    
    message_text = [
        f"✅ *Batch {dag_id} Success*\n",
        f"*Run ID:* {run_id}\n",
        f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n",
        f"*End Time:* {end_time_str}\n",
        f"*Elapsed Time:* {elapsed_str}\n",
        f"*Status:* Success\n"
    ]
    
    if conf:
        message_text.append(f"\n*Batch Configuration:*\n"
                            f"• Start Date: {conf.get('startDate', 'Not specified')}\n"
                            f"• End Date: {conf.get('endDate', 'Not specified')}\n"
                            f"{get_csv_column_text(conf)}\n")
    
    message_text.append("\n_This batch has successfully completed._")
    send_message(message_text)



def create_line_error_message(dag_id: str, run_id: str, error_message: str, start_time: datetime, conf: Optional[Dict] = None) -> None:
    """สร้างข้อความ LINE เมื่อเกิดข้อผิดพลาด"""
    message_text = [
        f"❌ *Batch {dag_id} Failed*\n",
        f"*Run ID:* {run_id}\n",
        f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n",
        f"*Status:* Failed\n",
        f"\n*Error Message:* {error_message}\n"
    ]
    
    if conf:
        message_text.append(f"\n*Batch Configuration:*\n"
                            f"• Start Date: {conf.get('startDate', 'Not specified')}\n"
                            f"• End Date: {conf.get('endDate', 'Not specified')}\n"
                            f"{get_csv_column_text(conf)}\n")
    
    message_text.append("\n_Please check the logs for further details._")
    send_message(message_text)
