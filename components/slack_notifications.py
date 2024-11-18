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
    additional_info: Optional[Dict] = None
) -> Dict:
    """Create Slack message with blocks format"""
    color_map = {
        'success': '#36a64f',  # green
        'failed': '#ff0000',   # red
        'running': '#3AA3E3',  # blue
        'paused': '#FFA500',   # orange
        'resumed': '#3AA3E3'   # blue
    }
    
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": title
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*DAG:*\n{dag_id}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Run ID:*\n{run_id}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Start Time:*\n{start_time.strftime('%Y-%m-%d %H:%M:%S')}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Status:*\n{status}"
                }
            ]
        }
    ]

    if end_time:
        duration = end_time - start_time
        blocks[1]["fields"].append({
            "type": "mrkdwn",
            "text": f"*Duration:*\n{str(duration).split('.')[0]}"
        })

    if error_message:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error Message:*\n```{error_message}```"
            }
        })

    if additional_info:
        info_fields = []
        for key, value in additional_info.items():
            info_fields.append({
                "type": "mrkdwn",
                "text": f"*{key}:*\n{value}"
            })
        
        blocks.append({
            "type": "section",
            "fields": info_fields
        })

    return {
        "blocks": blocks,
        "color": color_map.get(status.lower(), '#808080')
    }