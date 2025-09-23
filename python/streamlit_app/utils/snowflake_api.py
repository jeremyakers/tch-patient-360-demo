"""
Snowflake API utilities that work in both warehouse and SPCS runtime environments.
"""

import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Try to determine the runtime environment
try:
    import _snowflake
    RUNTIME_TYPE = "warehouse"
    logger.info("Running in warehouse runtime environment")
except ImportError:
    _snowflake = None
    RUNTIME_TYPE = "spcs"
    logger.info("Running in SPCS container runtime environment")

# SPCS imports
if RUNTIME_TYPE == "spcs":
    try:
        import snowflake.snowpark.context as snowpark_context
        from snowflake.snowpark import Session
        import requests
        SPCS_AVAILABLE = True
        logger.info("SPCS dependencies available")
    except ImportError as e:
        SPCS_AVAILABLE = False
        logger.error(f"SPCS dependencies not available: {e}")
else:
    snowpark_context = None
    Session = None
    requests = None
    SPCS_AVAILABLE = False


def send_snow_api_request(
    method: str,
    endpoint: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    payload: Optional[Dict[str, Any]] = None,
    files: Optional[Dict] = None,
    timeout: int = 60000
) -> Any:
    """
    Send API request to Snowflake, handling both warehouse and SPCS environments.
    
    Args:
        method: HTTP method (GET, POST, etc.)
        endpoint: API endpoint (relative or absolute)
        headers: HTTP headers
        params: URL parameters
        payload: JSON payload
        files: Files to upload
        timeout: Timeout in milliseconds
        
    Returns:
        API response
    """
    if RUNTIME_TYPE == "warehouse" and _snowflake:
        # Use warehouse runtime _snowflake module
        logger.debug(f"Using warehouse runtime for {method} {endpoint}")
        return _snowflake.send_snow_api_request(
            method, endpoint, headers or {}, params or {}, payload, files, timeout
        )
    
    elif RUNTIME_TYPE == "spcs" and SPCS_AVAILABLE:
        # Use SPCS with snowpark session and requests
        logger.debug(f"Using SPCS runtime for {method} {endpoint}")
        return _send_spcs_api_request(method, endpoint, headers, params, payload, files, timeout)
    
    else:
        raise RuntimeError(f"Cannot send API request in {RUNTIME_TYPE} runtime - dependencies not available")


def _send_spcs_api_request(
    method: str,
    endpoint: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    payload: Optional[Dict[str, Any]] = None,
    files: Optional[Dict] = None,
    timeout: int = 60000
) -> Any:
    """
    Send API request using SPCS environment with snowpark session.
    """
    try:
        # Get the active Snowpark session
        session = snowpark_context.get_active_session()
        
        # Get session token for authentication
        session_token = session.get_session_token()
        
        # Build full URL if endpoint is relative
        if not endpoint.startswith('http'):
            account_url = session.get_current_account_url()
            full_url = f"https://{account_url}{endpoint}"
        else:
            full_url = endpoint
        
        # Prepare headers
        request_headers = {
            "Authorization": f"Snowflake Token=\"{session_token}\"",
            "Content-Type": "application/json"
        }
        if headers:
            request_headers.update(headers)
        
        logger.debug(f"SPCS API request: {method} {full_url}")
        
        # Make the request
        kwargs = {
            'timeout': timeout / 1000,  # Convert ms to seconds
            'headers': request_headers
        }
        
        if params:
            kwargs['params'] = params
            
        if payload:
            kwargs['json'] = payload
            
        if files:
            kwargs['files'] = files
            # Remove content-type for file uploads
            if 'Content-Type' in kwargs['headers']:
                del kwargs['headers']['Content-Type']
        
        response = requests.request(method, full_url, **kwargs)
        
        # Create response object that mimics _snowflake response format
        class SPCSResponse:
            def __init__(self, requests_response):
                self.status = requests_response.status_code
                self.reason = requests_response.reason
                try:
                    self.content = requests_response.json()
                except:
                    self.content = requests_response.text
        
        return SPCSResponse(response)
        
    except Exception as e:
        logger.error(f"SPCS API request failed: {e}")
        raise


def get_current_session():
    """Get the current Snowflake session."""
    if RUNTIME_TYPE == "spcs" and SPCS_AVAILABLE:
        return snowpark_context.get_active_session()
    else:
        raise RuntimeError(f"Session not available in {RUNTIME_TYPE} runtime")


def is_spcs_runtime() -> bool:
    """Check if running in SPCS container runtime."""
    return RUNTIME_TYPE == "spcs"


def is_warehouse_runtime() -> bool:
    """Check if running in warehouse runtime."""
    return RUNTIME_TYPE == "warehouse"
