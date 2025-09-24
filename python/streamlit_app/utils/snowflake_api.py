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
    Send API request using SPCS environment with proper authentication.
    
    In SPCS, we can make external REST API calls using requests library,
    but we need to get authentication credentials properly.
    """
    logger.info(f"SPCS API request: {method} {endpoint}")
    
    try:
        import streamlit as st
        
        # Get Snowflake connection details from st.connection
        conn = st.connection("snowflake")
        
        # Debug: Log the connection object structure to understand the API
        logger.info(f"SPCS: SnowflakeConnection type: {type(conn)}")
        logger.info(f"SPCS: SnowflakeConnection attributes: {dir(conn)}")
        
        # Try to access the raw connection in different ways
        raw_conn = None
        account_url = None
        session_token = None
        
        # Method 1: Try _connection attribute
        if hasattr(conn, '_connection'):
            raw_conn = conn._connection
            logger.info(f"SPCS: Found _connection attribute, type: {type(raw_conn)}")
        
        # Method 2: Try raw_connection method/property
        elif hasattr(conn, 'raw_connection'):
            try:
                raw_conn = conn.raw_connection
                logger.info(f"SPCS: Found raw_connection, type: {type(raw_conn)}")
            except Exception as e:
                logger.debug(f"raw_connection failed: {e}")
        
        # Method 3: Try _instance attribute
        elif hasattr(conn, '_instance'):
            instance = conn._instance
            logger.info(f"SPCS: Found _instance, type: {type(instance)}")
            if hasattr(instance, '_connection'):
                raw_conn = instance._connection
                logger.info(f"SPCS: Found _instance._connection, type: {type(raw_conn)}")
        
        if not raw_conn:
            logger.error(f"SPCS: Cannot access underlying connection. Available attributes: {[attr for attr in dir(conn) if not attr.startswith('__')]}")
            raise RuntimeError("Cannot access underlying Snowflake connection from st.connection")
        
        # Try to get account URL and session token
        try:
            if hasattr(raw_conn, 'host'):
                account_url = raw_conn.host
            elif hasattr(raw_conn, 'account'):
                account_url = f"{raw_conn.account}.snowflakecomputing.com"
            
            if hasattr(raw_conn, 'get_session_token'):
                session_token = raw_conn.get_session_token()
            elif hasattr(raw_conn, 'session_token'):
                session_token = raw_conn.session_token
                
        except Exception as e:
            logger.error(f"Failed to get connection details: {e}")
            logger.error(f"Raw connection attributes: {[attr for attr in dir(raw_conn) if not attr.startswith('__')]}")
            raise RuntimeError(f"Cannot extract connection details: {e}")
        
        if not account_url or not session_token:
            raise RuntimeError(f"Missing connection details - account_url: {bool(account_url)}, session_token: {bool(session_token)}")
        
        full_url = f"https://{account_url}{endpoint}"
        
        # Set up proper authentication headers
        auth_headers = {
            "Authorization": f"Snowflake Token=\"{session_token}\"",
            "Content-Type": "application/json"
        }
        
        # Add any additional headers
        if headers:
            auth_headers.update(headers)
        
        logger.debug(f"SPCS: Making request to {full_url}")
        
        # Make the request using requests library
        kwargs = {
            'timeout': timeout / 1000,  # Convert ms to seconds
            'headers': auth_headers
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
        
        # Create response object that matches _snowflake response format
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
        raise RuntimeError(f"Failed to make SPCS API request: {e}")




def get_current_session():
    """Get the current Snowflake session."""
    if RUNTIME_TYPE == "spcs":
        # In SPCS, use st.connection instead of direct session access
        try:
            import streamlit as st
            return st.connection("snowflake")
        except Exception as e:
            logger.error(f"Failed to get SPCS connection: {e}")
            raise RuntimeError(f"Cannot get Snowflake connection in SPCS: {e}")
    elif RUNTIME_TYPE == "warehouse" and snowpark_context:
        return snowpark_context.get_active_session()
    else:
        raise RuntimeError(f"Session not available in {RUNTIME_TYPE} runtime")


def is_spcs_runtime() -> bool:
    """Check if running in SPCS container runtime."""
    return RUNTIME_TYPE == "spcs"


def is_warehouse_runtime() -> bool:
    """Check if running in warehouse runtime."""
    return RUNTIME_TYPE == "warehouse"
