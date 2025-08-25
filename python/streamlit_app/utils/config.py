"""
Configuration Management for TCH Patient 360 PoC

Handles application configuration, environment variables,
and setup for different deployment environments.
"""

import os
import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

def get_app_config() -> Dict[str, Any]:
    """
    Get application configuration settings
    
    Returns:
        Dictionary containing app configuration
    """
    try:
        config = {
            'app_name': os.getenv('APP_NAME', 'TCH Patient 360 PoC'),
            'app_version': os.getenv('APP_VERSION', '1.0.0'),
            'environment': os.getenv('ENVIRONMENT', 'development'),
            'debug': os.getenv('DEBUG', 'true').lower() == 'true',
            'log_level': os.getenv('LOG_LEVEL', 'INFO'),
            'session_timeout': int(os.getenv('SESSION_TIMEOUT', '3600')),  # 1 hour
            'cache_timeout': int(os.getenv('CACHE_TIMEOUT', '300')),       # 5 minutes
            'max_file_size': int(os.getenv('MAX_FILE_SIZE', '10485760')),  # 10MB
            'pagination_size': int(os.getenv('PAGINATION_SIZE', '25')),
            'max_search_results': int(os.getenv('MAX_SEARCH_RESULTS', '500'))
        }
        
        return config
        
    except Exception as e:
        logger.error(f"Error loading app configuration: {e}")
        return {
            'app_name': 'TCH Patient 360 PoC',
            'environment': 'development',
            'debug': True
        }

def get_database_config() -> Dict[str, Any]:
    """
    Get database configuration settings
    
    Returns:
        Dictionary containing database configuration
    """
    try:
        config = {
            'snowflake_account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'snowflake_user': os.getenv('SNOWFLAKE_USER'),
            'snowflake_password': os.getenv('SNOWFLAKE_PASSWORD'),
            'snowflake_database': os.getenv('SNOWFLAKE_DATABASE', 'TCH_PATIENT_360'),
            'snowflake_schema': os.getenv('SNOWFLAKE_SCHEMA', 'PRESENTATION'),
            'snowflake_warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'snowflake_role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '60')),
            'query_timeout': int(os.getenv('QUERY_TIMEOUT', '300')),
            'max_connections': int(os.getenv('MAX_CONNECTIONS', '10'))
        }
        
        # Validate required fields
        required_fields = ['snowflake_account', 'snowflake_user']
        missing_fields = [field for field in required_fields if not config.get(field)]
        
        if missing_fields:
            logger.warning(f"Missing required database config fields: {missing_fields}")
        
        return config
        
    except Exception as e:
        logger.error(f"Error loading database configuration: {e}")
        return {}

def get_cortex_config() -> Dict[str, Any]:
    """
    Get Cortex AI configuration settings
    
    Returns:
        Dictionary containing Cortex configuration
    """
    try:
        config = {
            'cortex_enabled': os.getenv('CORTEX_ENABLED', 'true').lower() == 'true',
            'cortex_analyst_enabled': os.getenv('CORTEX_ANALYST_ENABLED', 'true').lower() == 'true',
            'cortex_search_enabled': os.getenv('CORTEX_SEARCH_ENABLED', 'true').lower() == 'true',
            'cortex_agents_enabled': os.getenv('CORTEX_AGENTS_ENABLED', 'true').lower() == 'true',
            'cortex_timeout': int(os.getenv('CORTEX_TIMEOUT', '30')),
            'cortex_max_retries': int(os.getenv('CORTEX_MAX_RETRIES', '3')),
            'cortex_fallback_enabled': os.getenv('CORTEX_FALLBACK_ENABLED', 'true').lower() == 'true',
            
            # Cortex Analyst Apps
            'analyst_patient_app': os.getenv('CORTEX_ANALYST_PATIENT_APP', 'TCH_PATIENT_ANALYTICS_APP'),
            'analyst_clinical_app': os.getenv('CORTEX_ANALYST_CLINICAL_APP', 'TCH_CLINICAL_METRICS_APP'),
            'analyst_population_app': os.getenv('CORTEX_ANALYST_POPULATION_APP', 'TCH_POPULATION_HEALTH_APP'),
            'analyst_quality_app': os.getenv('CORTEX_ANALYST_QUALITY_APP', 'TCH_QUALITY_MEASURES_APP'),
            
            # Cortex Search Services
            'search_clinical_notes': os.getenv('CORTEX_SEARCH_CLINICAL_NOTES', 'TCH_CLINICAL_NOTES_SEARCH'),
            'search_patient_data': os.getenv('CORTEX_SEARCH_PATIENT_DATA', 'TCH_PATIENT_SEARCH'),
            'search_lab_results': os.getenv('CORTEX_SEARCH_LAB_RESULTS', 'TCH_LAB_SEARCH'),
            'search_procedures': os.getenv('CORTEX_SEARCH_PROCEDURES', 'TCH_PROCEDURE_SEARCH'),
            'search_medications': os.getenv('CORTEX_SEARCH_MEDICATIONS', 'TCH_MEDICATION_SEARCH'),
            'search_diagnoses': os.getenv('CORTEX_SEARCH_DIAGNOSES', 'TCH_DIAGNOSIS_SEARCH')
        }
        
        return config
        
    except Exception as e:
        logger.error(f"Error loading Cortex configuration: {e}")
        return {
            'cortex_enabled': False,
            'cortex_fallback_enabled': True
        }

def get_security_config() -> Dict[str, Any]:
    """
    Get security configuration settings
    
    Returns:
        Dictionary containing security configuration
    """
    try:
        config = {
            'enable_auth': os.getenv('ENABLE_AUTH', 'false').lower() == 'true',
            'enable_encryption': os.getenv('ENABLE_ENCRYPTION', 'true').lower() == 'true',
            'enable_audit_logging': os.getenv('ENABLE_AUDIT_LOGGING', 'true').lower() == 'true',
            'session_secret': os.getenv('SESSION_SECRET', 'tch-patient-360-secret'),
            'allowed_origins': os.getenv('ALLOWED_ORIGINS', '*').split(','),
            'rate_limit_enabled': os.getenv('RATE_LIMIT_ENABLED', 'false').lower() == 'true',
            'rate_limit_requests': int(os.getenv('RATE_LIMIT_REQUESTS', '100')),
            'rate_limit_window': int(os.getenv('RATE_LIMIT_WINDOW', '3600')),  # 1 hour
        }
        
        return config
        
    except Exception as e:
        logger.error(f"Error loading security configuration: {e}")
        return {
            'enable_auth': False,
            'enable_encryption': True,
            'enable_audit_logging': True
        }

def get_healthcare_config() -> Dict[str, Any]:
    """
    Get healthcare-specific configuration settings
    
    Returns:
        Dictionary containing healthcare configuration
    """
    try:
        config = {
            'hospital_name': os.getenv('HOSPITAL_NAME', "Texas Children's Hospital"),
            'hospital_system': os.getenv('HOSPITAL_SYSTEM', 'TCH'),
            'pediatric_focus': os.getenv('PEDIATRIC_FOCUS', 'true').lower() == 'true',
            'max_patient_age': int(os.getenv('MAX_PATIENT_AGE', '21')),
            'hipaa_compliance': os.getenv('HIPAA_COMPLIANCE', 'true').lower() == 'true',
            'phi_masking': os.getenv('PHI_MASKING', 'true').lower() == 'true',
            'audit_trail': os.getenv('AUDIT_TRAIL', 'true').lower() == 'true',
            
            # Clinical data retention
            'data_retention_days': int(os.getenv('DATA_RETENTION_DAYS', '2555')),  # 7 years
            'cache_phi_data': os.getenv('CACHE_PHI_DATA', 'false').lower() == 'true',
            
            # Pediatric age groups
            'infant_max_age': int(os.getenv('INFANT_MAX_AGE', '1')),
            'toddler_max_age': int(os.getenv('TODDLER_MAX_AGE', '2')),
            'preschool_max_age': int(os.getenv('PRESCHOOL_MAX_AGE', '5')),
            'school_age_max_age': int(os.getenv('SCHOOL_AGE_MAX_AGE', '12')),
            'adolescent_max_age': int(os.getenv('ADOLESCENT_MAX_AGE', '17')),
            'young_adult_max_age': int(os.getenv('YOUNG_ADULT_MAX_AGE', '21'))
        }
        
        return config
        
    except Exception as e:
        logger.error(f"Error loading healthcare configuration: {e}")
        return {
            'hospital_name': "Texas Children's Hospital",
            'pediatric_focus': True,
            'hipaa_compliance': True
        }

def is_development() -> bool:
    """Check if running in development environment"""
    return os.getenv('ENVIRONMENT', 'development').lower() == 'development'

def is_production() -> bool:
    """Check if running in production environment"""
    return os.getenv('ENVIRONMENT', 'development').lower() == 'production'

def is_testing() -> bool:
    """Check if running in testing environment"""
    return os.getenv('ENVIRONMENT', 'development').lower() == 'testing'

def get_log_level() -> str:
    """Get configured log level"""
    level = os.getenv('LOG_LEVEL', 'INFO').upper()
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    return level if level in valid_levels else 'INFO'

def setup_logging() -> None:
    """Setup application logging configuration"""
    try:
        log_level = get_log_level()
        log_format = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_level),
            format=log_format,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('tch_patient_360.log') if not is_development() else logging.NullHandler()
            ]
        )
        
        # Set specific logger levels
        if is_development():
            logging.getLogger('snowflake').setLevel(logging.WARNING)
            logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        logger.info(f"Logging configured - Level: {log_level}, Environment: {os.getenv('ENVIRONMENT', 'development')}")
        
    except Exception as e:
        print(f"Error setting up logging: {e}")

def validate_configuration() -> bool:
    """
    Validate that all required configuration is present
    
    Returns:
        True if configuration is valid
    """
    try:
        validation_errors = []
        
        # Check database configuration
        db_config = get_database_config()
        required_db_fields = ['snowflake_account', 'snowflake_user']
        
        for field in required_db_fields:
            if not db_config.get(field):
                validation_errors.append(f"Missing required database configuration: {field}")
        
        # Check authentication requirements for production
        if is_production():
            security_config = get_security_config()
            if not security_config.get('enable_auth'):
                validation_errors.append("Authentication must be enabled in production")
        
        # Log validation results
        if validation_errors:
            logger.error(f"Configuration validation failed: {validation_errors}")
            return False
        else:
            logger.info("Configuration validation successful")
            return True
            
    except Exception as e:
        logger.error(f"Error validating configuration: {e}")
        return False

def get_feature_flags() -> Dict[str, bool]:
    """
    Get feature flag configuration
    
    Returns:
        Dictionary of feature flags
    """
    try:
        flags = {
            'enable_cortex_analyst': os.getenv('FEATURE_CORTEX_ANALYST', 'true').lower() == 'true',
            'enable_cortex_search': os.getenv('FEATURE_CORTEX_SEARCH', 'true').lower() == 'true',
            'enable_cortex_agents': os.getenv('FEATURE_CORTEX_AGENTS', 'true').lower() == 'true',
            'enable_advanced_analytics': os.getenv('FEATURE_ADVANCED_ANALYTICS', 'true').lower() == 'true',
            'enable_clinical_timeline': os.getenv('FEATURE_CLINICAL_TIMELINE', 'true').lower() == 'true',
            'enable_population_health': os.getenv('FEATURE_POPULATION_HEALTH', 'true').lower() == 'true',
            'enable_cohort_builder': os.getenv('FEATURE_COHORT_BUILDER', 'true').lower() == 'true',
            'enable_document_search': os.getenv('FEATURE_DOCUMENT_SEARCH', 'true').lower() == 'true',
            'enable_chat_interface': os.getenv('FEATURE_CHAT_INTERFACE', 'true').lower() == 'true',
            'enable_export_functionality': os.getenv('FEATURE_EXPORT', 'false').lower() == 'true',
            'enable_bulk_operations': os.getenv('FEATURE_BULK_OPS', 'false').lower() == 'true'
        }
        
        return flags
        
    except Exception as e:
        logger.error(f"Error loading feature flags: {e}")
        return {}

def load_environment_file(env_file: str = '.env') -> bool:
    """
    Load environment variables from file
    
    Args:
        env_file: Path to environment file
        
    Returns:
        True if file was loaded successfully
    """
    try:
        env_path = Path(env_file)
        
        if not env_path.exists():
            logger.warning(f"Environment file not found: {env_file}")
            return False
        
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())
        
        logger.info(f"Environment file loaded: {env_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error loading environment file: {e}")
        return False

def load_app_config() -> Dict[str, Any]:
    """
    Load and initialize application configuration
    
    This function:
    - Sets up logging
    - Validates configuration
    - Loads environment variables if needed
    - Returns the complete app configuration
    
    Returns:
        Dictionary containing application configuration
    """
    try:
        # Setup logging
        setup_logging()
        
        # Load environment file in development
        if is_development():
            load_environment_file()
        
        # Validate configuration
        if not validate_configuration():
            logger.warning("Configuration validation failed, using defaults")
        
        # Get complete configuration
        config = {
            'app': get_app_config(),
            'database': get_database_config(),
            'cortex': get_cortex_config(),
            'security': get_security_config(),
            'healthcare': get_healthcare_config(),
            'features': get_feature_flags()
        }
        
        logger.info("Application configuration loaded successfully")
        return config
        
    except Exception as e:
        logger.error(f"Error loading application configuration: {e}")
        # Return minimal configuration on error
        return {
            'app': get_app_config(),
            'database': {},
            'cortex': {},
            'security': {},
            'healthcare': {},
            'features': {}
        }

# Initialize configuration on import
if is_development():
    load_environment_file()

setup_logging()