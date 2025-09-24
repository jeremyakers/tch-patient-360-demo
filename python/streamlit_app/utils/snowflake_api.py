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
        
        # Use the built-in OAuth token and environment variables provided by Snowflake in SPCS
        import os
        
        # Read OAuth token from the file provided by Snowflake
        token_path = "/snowflake/session/token"
        try:
            with open(token_path, 'r') as token_file:
                oauth_token = token_file.read().strip()
            logger.info("SPCS: Successfully read OAuth token from Snowflake")
        except Exception as e:
            raise RuntimeError(f"Cannot read Snowflake OAuth token from {token_path}: {e}")
        
        # Get account and host information from environment variables
        snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
        snowflake_host = os.getenv('SNOWFLAKE_HOST')
        
        if not snowflake_host:
            raise RuntimeError("SNOWFLAKE_HOST environment variable not set by Snowflake")
        
        # Build full URL using the Snowflake-provided host
        full_url = f"https://{snowflake_host}{endpoint}"
        
        logger.info(f"SPCS: Making authenticated REST API call to {full_url}")
        
        # Set up proper authentication headers using OAuth token
        auth_headers = {
            "Authorization": f"Bearer {oauth_token}",
            "X-Snowflake-Authorization-Token-Type": "OAUTH",
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
