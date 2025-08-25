"""
Input Validation Utilities for TCH Patient 360 PoC

Validation functions for user inputs, data integrity checks,
and healthcare-specific validation rules.
"""

import re
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
import logging

logger = logging.getLogger(__name__)

def validate_patient_id(patient_id: str) -> Tuple[bool, str]:
    """
    Validate patient ID format
    
    Args:
        patient_id: Patient ID to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        if not patient_id or pd.isna(patient_id):
            return False, "Patient ID is required"
        
        patient_id = str(patient_id).strip()
        
        if len(patient_id) < 3:
            return False, "Patient ID must be at least 3 characters"
        
        if len(patient_id) > 50:
            return False, "Patient ID must be less than 50 characters"
        
        # Check for valid characters (alphanumeric, hyphens, underscores)
        if not re.match(r'^[A-Za-z0-9_-]+$', patient_id):
            return False, "Patient ID can only contain letters, numbers, hyphens, and underscores"
        
        return True, ""
        
    except Exception as e:
        logger.error(f"Error validating patient ID: {e}")
        return False, "Invalid patient ID format"

def validate_mrn(mrn: str) -> Tuple[bool, str]:
    """
    Validate Medical Record Number format
    
    Args:
        mrn: MRN to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        if not mrn or pd.isna(mrn):
            return False, "MRN is required"
        
        mrn = str(mrn).strip().upper()
        
        # Remove MRN prefix if present for validation
        if mrn.startswith('MRN'):
            mrn_number = mrn[3:]
        else:
            mrn_number = mrn
        
        # Check if remaining part is numeric and appropriate length
        if not mrn_number.isdigit():
            return False, "MRN must contain only numbers after MRN prefix"
        
        if len(mrn_number) < 6 or len(mrn_number) > 12:
            return False, "MRN number must be between 6 and 12 digits"
        
        return True, ""
        
    except Exception as e:
        logger.error(f"Error validating MRN: {e}")
        return False, "Invalid MRN format"

def validate_search_criteria(criteria: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate search criteria dictionary
    
    Args:
        criteria: Search criteria to validate
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    try:
        errors = []
        
        if not criteria:
            return False, ["Search criteria cannot be empty"]
        
        # Validate age ranges
        age_min = criteria.get('age_min')
        age_max = criteria.get('age_max')
        
        if age_min is not None:
            if not isinstance(age_min, (int, float)) or age_min < 0:
                errors.append("Minimum age must be a non-negative number")
            elif age_min > 150:
                errors.append("Minimum age must be less than 150")
        
        if age_max is not None:
            if not isinstance(age_max, (int, float)) or age_max < 0:
                errors.append("Maximum age must be a non-negative number")
            elif age_max > 150:
                errors.append("Maximum age must be less than 150")
        
        if age_min is not None and age_max is not None:
            if age_min > age_max:
                errors.append("Minimum age cannot be greater than maximum age")
        
        # Validate date ranges
        date_from = criteria.get('date_from')
        date_to = criteria.get('date_to')
        
        if date_from and date_to:
            if isinstance(date_from, str):
                try:
                    date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
                except:
                    errors.append("Invalid 'from' date format")
            
            if isinstance(date_to, str):
                try:
                    date_to = datetime.strptime(date_to, '%Y-%m-%d').date()
                except:
                    errors.append("Invalid 'to' date format")
            
            if isinstance(date_from, date) and isinstance(date_to, date):
                if date_from > date_to:
                    errors.append("'From' date cannot be after 'to' date")
                
                # Check for reasonable date ranges (not more than 10 years)
                if (date_to - date_from).days > 3650:
                    errors.append("Date range cannot exceed 10 years")
        
        # Validate gender
        gender = criteria.get('gender')
        if gender and gender not in ['M', 'F', 'Other', 'Unknown']:
            errors.append("Invalid gender value")
        
        # Validate MRN if provided
        mrn = criteria.get('mrn')
        if mrn:
            is_valid, mrn_error = validate_mrn(mrn)
            if not is_valid:
                errors.append(f"MRN validation failed: {mrn_error}")
        
        # Validate names (basic checks)
        for name_field in ['first_name', 'last_name']:
            name = criteria.get(name_field)
            if name:
                name = str(name).strip()
                if len(name) > 100:
                    errors.append(f"{name_field.replace('_', ' ').title()} must be less than 100 characters")
                if not re.match(r'^[A-Za-z\s\'-]+$', name):
                    errors.append(f"{name_field.replace('_', ' ').title()} contains invalid characters")
        
        return len(errors) == 0, errors
        
    except Exception as e:
        logger.error(f"Error validating search criteria: {e}")
        return False, ["Error validating search criteria"]

def validate_date_range(start_date: Union[date, datetime, str], 
                       end_date: Union[date, datetime, str],
                       max_days: int = 3650) -> Tuple[bool, str]:
    """
    Validate date range
    
    Args:
        start_date: Start date
        end_date: End date
        max_days: Maximum allowed days in range
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        # Convert strings to dates
        if isinstance(start_date, str):
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            except:
                return False, "Invalid start date format (expected YYYY-MM-DD)"
        
        if isinstance(end_date, str):
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            except:
                return False, "Invalid end date format (expected YYYY-MM-DD)"
        
        # Extract date part if datetime
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()
        
        # Validate dates
        if start_date > end_date:
            return False, "Start date cannot be after end date"
        
        # Check range size
        days_diff = (end_date - start_date).days
        if days_diff > max_days:
            return False, f"Date range cannot exceed {max_days} days"
        
        # Check for future dates
        today = date.today()
        if start_date > today:
            return False, "Start date cannot be in the future"
        
        return True, ""
        
    except Exception as e:
        logger.error(f"Error validating date range: {e}")
        return False, "Error validating date range"

def validate_phone_number(phone: str) -> Tuple[bool, str]:
    """
    Validate phone number format
    
    Args:
        phone: Phone number to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        if not phone or pd.isna(phone):
            return True, ""  # Phone is optional
        
        phone = str(phone).strip()
        
        # Remove all non-digits
        digits = re.sub(r'\D', '', phone)
        
        # Check length
        if len(digits) == 10:
            return True, ""
        elif len(digits) == 11 and digits[0] == '1':
            return True, ""
        else:
            return False, "Phone number must be 10 digits or 11 digits starting with 1"
        
    except Exception as e:
        logger.error(f"Error validating phone number: {e}")
        return False, "Invalid phone number format"

def is_valid_email(email: str) -> bool:
    """
    Validate email address format
    
    Args:
        email: Email address to validate
        
    Returns:
        True if valid email format
    """
    try:
        if not email or pd.isna(email):
            return True  # Email is optional
        
        email = str(email).strip().lower()
        
        # Basic email regex pattern
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        return bool(re.match(pattern, email))
        
    except Exception as e:
        logger.error(f"Error validating email: {e}")
        return False

def validate_age(age: Union[int, float, str]) -> Tuple[bool, str]:
    """
    Validate age value
    
    Args:
        age: Age to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        if pd.isna(age) or age is None:
            return True, ""  # Age can be optional
        
        # Convert to numeric
        try:
            age_num = float(age)
        except:
            return False, "Age must be a number"
        
        # Check range for pediatric hospital
        if age_num < 0:
            return False, "Age cannot be negative"
        
        if age_num > 150:
            return False, "Age cannot exceed 150 years"
        
        # For pediatric focus, warn about adults
        if age_num > 21:
            return True, "Note: Age exceeds typical pediatric range (0-21 years)"
        
        return True, ""
        
    except Exception as e:
        logger.error(f"Error validating age: {e}")
        return False, "Invalid age value"

def validate_medication_name(medication: str) -> Tuple[bool, str]:
    """
    Validate medication name
    
    Args:
        medication: Medication name to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        if not medication or pd.isna(medication):
            return False, "Medication name is required"
        
        medication = str(medication).strip()
        
        if len(medication) < 2:
            return False, "Medication name must be at least 2 characters"
        
        if len(medication) > 200:
            return False, "Medication name must be less than 200 characters"
        
        # Allow letters, numbers, spaces, hyphens, parentheses, and common symbols
        if not re.match(r'^[A-Za-z0-9\s\-\(\)\.\,\/\+]+$', medication):
            return False, "Medication name contains invalid characters"
        
        return True, ""
        
    except Exception as e:
        logger.error(f"Error validating medication name: {e}")
        return False, "Invalid medication name"

def validate_diagnosis_code(code: str, code_type: str = "ICD-10") -> Tuple[bool, str]:
    """
    Validate diagnosis code format
    
    Args:
        code: Diagnosis code to validate
        code_type: Type of code (ICD-10, ICD-9, etc.)
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        if not code or pd.isna(code):
            return False, "Diagnosis code is required"
        
        code = str(code).strip().upper()
        
        if code_type == "ICD-10":
            # Basic ICD-10 format validation (simplified)
            if not re.match(r'^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$', code):
                return False, "Invalid ICD-10 code format (expected: A00 or A00.0000)"
        
        elif code_type == "ICD-9":
            # Basic ICD-9 format validation (simplified)
            if not re.match(r'^[0-9]{3}(\.[0-9]{1,2})?$', code):
                return False, "Invalid ICD-9 code format (expected: 000 or 000.00)"
        
        return True, ""
        
    except Exception as e:
        logger.error(f"Error validating diagnosis code: {e}")
        return False, f"Invalid {code_type} code"

def validate_lab_value(value: str, test_type: str = None) -> Tuple[bool, str]:
    """
    Validate laboratory value
    
    Args:
        value: Lab value to validate
        test_type: Type of test (for specific validation rules)
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        if not value or pd.isna(value):
            return False, "Lab value is required"
        
        value_str = str(value).strip()
        
        # Check for common lab value patterns
        # Allow numbers, decimals, ranges, and common lab notation
        if not re.match(r'^[0-9\.\-\<\>\=\s\+\/]+$', value_str):
            # Also allow some text values like "Positive", "Negative", etc.
            text_values = ['POSITIVE', 'NEGATIVE', 'NORMAL', 'ABNORMAL', 
                          'HIGH', 'LOW', 'CRITICAL', 'PENDING']
            if value_str.upper() not in text_values:
                return False, "Lab value format not recognized"
        
        return True, ""
        
    except Exception as e:
        logger.error(f"Error validating lab value: {e}")
        return False, "Invalid lab value format"

def validate_risk_level(risk_level: str) -> Tuple[bool, str]:
    """
    Validate risk level value
    
    Args:
        risk_level: Risk level to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        if not risk_level or pd.isna(risk_level):
            return True, ""  # Risk level can be optional
        
        valid_levels = ['LOW', 'MEDIUM', 'HIGH', 'UNKNOWN']
        
        if str(risk_level).upper() not in valid_levels:
            return False, f"Risk level must be one of: {', '.join(valid_levels)}"
        
        return True, ""
        
    except Exception as e:
        logger.error(f"Error validating risk level: {e}")
        return False, "Invalid risk level"

def validate_data_completeness(data: pd.DataFrame, 
                              required_columns: List[str]) -> Tuple[bool, List[str]]:
    """
    Validate data completeness for required columns
    
    Args:
        data: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        Tuple of (is_complete, list_of_missing_columns)
    """
    try:
        if data.empty:
            return False, ["Data is empty"]
        
        missing_columns = []
        
        for col in required_columns:
            if col not in data.columns:
                missing_columns.append(f"Missing column: {col}")
            elif data[col].isna().all():
                missing_columns.append(f"Column '{col}' has no data")
        
        return len(missing_columns) == 0, missing_columns
        
    except Exception as e:
        logger.error(f"Error validating data completeness: {e}")
        return False, ["Error validating data completeness"]