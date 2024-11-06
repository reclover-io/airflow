import requests
import json
import time
from typing import Dict, List, Optional, Tuple

# Custom exceptions
class APIException(Exception):
    pass

class NoDataException(Exception):
    pass

def retry_api_call(func, max_retries=3, initial_delay=1):
    """Retry function with exponential backoff"""
    for attempt in range(max_retries):
        try:
            response = func()
            
            # Check status code
            if response.status_code != 200:
                raise APIException(f"API returned status code {response.status_code}")
            
            # Parse and check data
            data = response.json()
            if not data.get('hits', {}).get('hits', []):
                raise NoDataException("API returned no data")
            
            return data
        except (APIException, NoDataException, requests.exceptions.RequestException) as e:
            if attempt == max_retries - 1:
                raise e
            delay = initial_delay * (2 ** attempt)
            print(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds...")
            time.sleep(delay)

def fetch_data_page(start_date: str, end_date: str, search_after: Optional[List[str]], API_URL: str, API_HEADERS: Dict[str, str]) -> Tuple[List[Dict], int, Optional[List[str]]]:
    """Fetch a single page of data from the API with retries"""
    payload = {
        "startDate": start_date,
        "endDate": end_date
    }
    
    if search_after:
        payload["search_after"] = search_after
    
    print(f"Fetching data with payload: {json.dumps(payload, indent=2)}")
    
    def make_request():
        return requests.get(API_URL, headers=API_HEADERS, json=payload, timeout=10)
    
    # Make API call with retries
    data = retry_api_call(make_request)
    
    hits = data.get('hits', {})
    records = hits.get('hits', [])
    total = hits.get('total', {}).get('value', 0)
    
    next_search_after = None
    if records:
        last_record = records[-1]
        next_search_after = [
            last_record.get('RequestDateTime'),
            last_record.get('_id')
        ]
    
    return records, total, next_search_after