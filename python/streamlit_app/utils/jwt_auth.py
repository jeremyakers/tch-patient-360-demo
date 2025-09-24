"""
JWT Authentication utility for Snowflake REST API calls in SPCS.
Uses the existing keypair for proper authentication.
"""

import jwt
import time
import hashlib
import base64
import logging
from typing import Optional
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

logger = logging.getLogger(__name__)

def generate_jwt_token(
    account_identifier: str,
    username: str,
    private_key_path: str,
    private_key_passphrase: Optional[str] = None
) -> str:
    """
    Generate a JWT token for Snowflake REST API authentication.
    
    Args:
        account_identifier: Snowflake account identifier (e.g., "SFSENORTHAMERICA-DEMO_JAKERS")
        username: Snowflake username
        private_key_path: Path to the private key file
        private_key_passphrase: Optional passphrase for the private key
        
    Returns:
        JWT token string
    """
    try:
        # Read the private key
        with open(private_key_path, 'rb') as key_file:
            private_key_data = key_file.read()
        
        # Load the private key
        private_key = load_pem_private_key(
            private_key_data,
            password=private_key_passphrase.encode() if private_key_passphrase else None
        )
        
        # Get the public key and generate fingerprint
        public_key = private_key.public_key()
        public_key_der = public_key.public_key_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        # Generate SHA256 fingerprint
        sha256_hash = hashlib.sha256(public_key_der).digest()
        public_key_fingerprint = base64.b64encode(sha256_hash).decode('utf-8')
        
        # Current time
        now = int(time.time())
        
        # JWT payload
        payload = {
            'iss': f"{account_identifier.upper()}.{username.upper()}.SHA256:{public_key_fingerprint}",
            'sub': f"{account_identifier.upper()}.{username.upper()}",
            'iat': now,
            'exp': now + 3600  # 1 hour expiration
        }
        
        # Generate JWT token
        token = jwt.encode(payload, private_key, algorithm='RS256')
        
        logger.debug(f"Generated JWT token for {username}@{account_identifier}")
        return token
        
    except Exception as e:
        logger.error(f"Failed to generate JWT token: {e}")
        raise


def get_snowflake_jwt_token() -> str:
    """
    Get JWT token for the current Snowflake environment.
    Uses the existing keypair and connection details.
    """
    # These should match the deployment configuration
    account_identifier = "SFSENORTHAMERICA-DEMO_JAKERS"
    username = "TCH_PATIENT_360_USER"
    
    # Option 1: Try to get private key from Snowflake secret
    try:
        import streamlit as st
        conn = st.connection("snowflake")
        
        # Try to read the private key from a Snowflake secret
        result = conn.query("SELECT SYSTEM$GET_SECRET('keypair_secret') as private_key")
        if not result.empty:
            private_key_content = result.iloc[0]['PRIVATE_KEY']
            if private_key_content:
                return generate_jwt_token_from_content(account_identifier, username, private_key_content)
    except Exception as e:
        logger.debug(f"Could not read private key from secret: {e}")
    
    # Option 2: Try environment variable
    import os
    private_key_env = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    if private_key_env:
        try:
            return generate_jwt_token_from_content(account_identifier, username, private_key_env)
        except Exception as e:
            logger.error(f"Failed to use private key from environment: {e}")
    
    # Option 3: Try file system (last resort, not recommended for production)
    possible_paths = [
        "/opt/streamlit-runtime/keypair/rsa_key.p8",  # Container path
        "./keypair/rsa_key.p8",  # Relative path
        "keypair/rsa_key.p8",    # Alternative relative path
        "/app/keypair/rsa_key.p8"  # Alternative container path
    ]
    
    for key_path in possible_paths:
        try:
            return generate_jwt_token(account_identifier, username, key_path)
        except FileNotFoundError:
            logger.debug(f"Private key not found at: {key_path}")
            continue
        except Exception as e:
            logger.error(f"Failed to generate JWT with key at {key_path}: {e}")
            continue
    
    raise RuntimeError("Private key not available via secret, environment variable, or file system")


def generate_jwt_token_from_content(
    account_identifier: str,
    username: str,
    private_key_content: str,
    private_key_passphrase: Optional[str] = None
) -> str:
    """
    Generate a JWT token from private key content (string).
    
    Args:
        account_identifier: Snowflake account identifier
        username: Snowflake username
        private_key_content: Private key content as string
        private_key_passphrase: Optional passphrase for the private key
        
    Returns:
        JWT token string
    """
    try:
        # Load the private key from content
        private_key_bytes = private_key_content.encode('utf-8')
        private_key = load_pem_private_key(
            private_key_bytes,
            password=private_key_passphrase.encode() if private_key_passphrase else None
        )
        
        # Get the public key and generate fingerprint
        public_key = private_key.public_key()
        public_key_der = public_key.public_key_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        # Generate SHA256 fingerprint
        sha256_hash = hashlib.sha256(public_key_der).digest()
        public_key_fingerprint = base64.b64encode(sha256_hash).decode('utf-8')
        
        # Current time
        now = int(time.time())
        
        # JWT payload
        payload = {
            'iss': f"{account_identifier.upper()}.{username.upper()}.SHA256:{public_key_fingerprint}",
            'sub': f"{account_identifier.upper()}.{username.upper()}",
            'iat': now,
            'exp': now + 3600  # 1 hour expiration
        }
        
        # Generate JWT token
        token = jwt.encode(payload, private_key, algorithm='RS256')
        
        logger.debug(f"Generated JWT token from content for {username}@{account_identifier}")
        return token
        
    except Exception as e:
        logger.error(f"Failed to generate JWT token from content: {e}")
        raise
