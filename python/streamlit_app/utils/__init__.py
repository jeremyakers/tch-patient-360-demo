"""
Utils Module for TCH Patient 360 PoC

This module contains utility functions and helpers used throughout the application.
Includes data formatting, validation, configuration management, and common helpers.

Modules:
- helpers: Common utility functions and data formatting helpers
- validators: Input validation and data validation functions
- config: Configuration management and environment setup
"""

from .helpers import (
    format_date, format_currency, format_phone_number,
    calculate_age, get_pediatric_age_group, format_query_params,
    handle_database_errors, safe_divide, truncate_text
)

from .validators import (
    validate_patient_id, validate_mrn, validate_search_criteria,
    validate_date_range, validate_phone_number, is_valid_email
)

from .config import (
    get_app_config, get_database_config, get_cortex_config,
    is_development, is_production, get_log_level
)

__all__ = [
    # Helpers
    'format_date', 'format_currency', 'format_phone_number',
    'calculate_age', 'get_pediatric_age_group', 'format_query_params',
    'handle_database_errors', 'safe_divide', 'truncate_text',
    
    # Validators
    'validate_patient_id', 'validate_mrn', 'validate_search_criteria',
    'validate_date_range', 'validate_phone_number', 'is_valid_email',
    
    # Config
    'get_app_config', 'get_database_config', 'get_cortex_config',
    'is_development', 'is_production', 'get_log_level'
]