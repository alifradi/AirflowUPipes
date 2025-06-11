import urllib.request
import urllib.parse
import urllib.error
import json
import ssl
from typing import Optional, List, Dict
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_data(
    resource_id: str,
    limit: Optional[int] = None,
    batch_size: int = 100,
    ssl_verify: bool = False,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    request_timeout: float = 30.0
) -> List[Dict]:
    """
    Fetch data from Québec Open Data API with improved error handling and retries.
    
    Args:
        resource_id: Dataset ID (UUID format)
        limit: Max records to fetch (None for all)
        batch_size: Records per request (100-1000 recommended)
        ssl_verify: Verify SSL certificates
        max_retries: Max retry attempts for failed requests
        retry_delay: Initial delay between retries in seconds
        request_timeout: Request timeout in seconds
    
    Returns:
        List of records (dictionaries)
    """
    API_URL = "https://www.donneesquebec.ca/recherche/api/3/action/datastore_search"
    all_records = []
    
    # Validate parameters
    if not isinstance(resource_id, str) or len(resource_id) != 36:
        raise ValueError("Invalid resource_id format. Expected UUID.")
    
    if batch_size < 1 or batch_size > 1000:
        raise ValueError("batch_size must be between 1 and 1000")

    # Configure SSL context
    context = ssl.create_default_context()
    if not ssl_verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    def make_request(url: str) -> Optional[dict]:
        """Helper function with retry logic"""
        for attempt in range(max_retries + 1):
            try:
                with urllib.request.urlopen(
                    url, 
                    context=context,
                    timeout=request_timeout
                ) as response:
                    return json.loads(response.read().decode("utf-8"))
            except (urllib.error.HTTPError, urllib.error.URLError) as e:
                if attempt == max_retries:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(retry_delay * (2 ** attempt))
            except Exception as e:
                logger.error(f"Non-retryable error: {e}")
                raise
        return None

    try:
        # Handle limited request
        if limit is not None:
            params = {"resource_id": resource_id, "limit": limit}
            query = urllib.parse.urlencode(params)
            url = f"{API_URL}?{query}"
            
            data = make_request(url)
            return data.get("result", {}).get("records", []) if data else []

        # Handle full dataset with pagination
        # Get total count efficiently
        params = {
            'resource_id': resource_id,
            'limit': 0
        }
        query = urllib.parse.urlencode(params)
        count_url = f"{API_URL}?{query}"
        count_data = make_request(count_url)
        if not count_data:
            return []
            
        total = count_data.get("result", {}).get("total", 0)
        logger.info(f"Total records to fetch: {total}")

        # Fetch in batches
        for offset in range(0, total, batch_size):
            params = {
                "resource_id": resource_id,
                "offset": offset,
                "limit": min(batch_size, total - offset)
            }
            query = urllib.parse.urlencode(params)
            url = f"{API_URL}?{query}"
            
            batch_data = make_request(url)
            if not batch_data:
                continue
                
            all_records.extend(batch_data.get("result", {}).get("records", []))
            
            # Progress logging
            if (offset // batch_size) % 10 == 0:  # Log every 10 batches
                logger.info(f"Progress: {len(all_records)}/{total} records fetched")

        return all_records

    except Exception as e:
        logger.error(f"Failed to fetch data: {str(e)}")
        raise
