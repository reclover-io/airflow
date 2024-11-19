import requests
from typing import Dict, Optional
from datetime import datetime

class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_message(self, message: Dict) -> bool:
        """Send message to Slack"""
        try:
            response = requests.post(self.webhook_url, json=message)
            if response.status_code == 200:
                print("Message sent to Slack successfully")
                return True
            else:
                print(f"Failed to send message to Slack: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error sending message to Slack: {str(e)}")
            return False

def create_slack_message(
    title: str,
    status: str,
    dag_id: str,
    run_id: str,
    start_time: datetime,
    end_time: Optional[datetime] = None,
    error_message: Optional[str] = None,
    conf: Optional[Dict] = None,
    batch_state: Optional[Dict] = None
) -> Dict:
    """Create Slack message matching email format"""
    color_map = {
        'SUCCESS': '#36a64f',
        'FAILED': '#ff0000',
        'RUNNING': '#3AA3E3',
        'PAUSED': '#FFA500',
        'RESUMED': '#3AA3E3',
        'STARTED': '#3AA3E3'
    }

    # Build basic blocks
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": title
            }
        }
    ]

    # Basic information - each field in its own section
    basic_info = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Batch Name:* {dag_id}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Run ID:* {run_id}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Start Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            }
        }
    ]

    if end_time:
        basic_info.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*End Time:* {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
            }
        })

    basic_info.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*Status:* {status}"
        }
    })

    blocks.extend(basic_info)

    # Add divider before processing information
    blocks.append({"type": "divider"})

    # Processing information if available
    if batch_state:
        fetched_records = batch_state.get('fetched_records', 0)
        total_records = batch_state.get('total_records')
        current_page = batch_state.get('current_page', 1)

        if end_time and start_time:
            elapsed_time = end_time - start_time
            hours, remainder = divmod(int(elapsed_time.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            elapsed_str = f"{hours}h {minutes}m {seconds}s"
            
            blocks.extend([
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Total Elapsed Time:* {elapsed_str}"
                    }
                }
            ])

        if total_records:
            progress = (fetched_records / total_records * 100) if total_records > 0 else 0
            blocks.extend([
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Records Processed:* {fetched_records:,} / {total_records:,}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Progress:* {progress:.2f}%"
                    }
                }
            ])

    # Add configuration information if available
    if conf:
        blocks.append({"type": "divider"})
        blocks.extend([
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Batch Configuration:*"
                }
            }
        ])
        blocks.extend([
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Start Date:* {conf.get('startDate', 'Not specified')}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*End Date:* {conf.get('endDate', 'Not specified')}"
                }
            }
        ])
        
        if 'csvColumns' in conf:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*CSV Columns:* {conf['csvColumns']}"
                }
            })

    # Add error message if available
    if error_message:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error Message:*\n```{error_message}```"
            }
        })

    return {
        "blocks": blocks,
        "color": color_map.get(status, '#808080')
    }