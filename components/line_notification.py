import requests
from typing import Optional, Dict
from datetime import datetime
from components.database import get_initial_start_time
from components.constants import THAI_TZ, ACCESS_TOKEN, TO_USER_ID

api_url = "https://api.line.me/v2/bot/message/push"

def send_line_message(messages: Dict) -> None:
    """Send Message to LINE OA"""
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
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

def create_line_text_message(text: str) -> Dict:
    """Create Text Message for LINE"""
    return {
        "type": "text",
        "text": text
    }
