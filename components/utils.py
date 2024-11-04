from datetime import datetime
import pytz

# Constants from main DAG file
from .constants import THAI_TZ

# Time handling functions
def get_thai_time() -> datetime:
    """Get current time in Thai timezone"""
    return datetime.now(THAI_TZ)

def format_thai_time(dt: datetime) -> str:
    """Format datetime to Thai timezone string without timezone info"""
    if dt.tzinfo is None:
        dt = THAI_TZ.localize(dt)
    thai_time = dt.astimezone(THAI_TZ)
    return thai_time.strftime('%Y-%m-%d %H:%M:%S')

# เนื่องจากเห็นว่าฟังก์ชันส่วนใหญ่ที่เกี่ยวกับ utility ได้ย้ายไปอยู่ใน components อื่นๆ แล้ว 
# ในไฟล์นี้จึงเหลือเพียงฟังก์ชันที่เกี่ยวกับการจัดการเวลาที่ใช้ร่วมกันหลายๆที่

# หมายเหตุ: ฟังก์ชัน get_thai_time() อาจจะย้ายไปอยู่ที่นี่แทนที่จะอยู่ใน database.py 
# เพราะเป็นฟังก์ชันที่ใช้งานร่วมกันหลายๆที่ไม่ได้เกี่ยวข้องกับฐานข้อมูลโดยตรง