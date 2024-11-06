from datetime import datetime
from components.constants import THAI_TZ

def get_thai_time() -> datetime:
    """Get current time in Thai timezone"""
    return datetime.now(THAI_TZ)

def format_thai_time(dt: datetime) -> str:
    """Format datetime to Thai timezone string without timezone info"""
    if dt.tzinfo is None:
        dt = THAI_TZ.localize(dt)
    thai_time = dt.astimezone(THAI_TZ)
    return thai_time.strftime('%Y-%m-%d %H:%M:%S')