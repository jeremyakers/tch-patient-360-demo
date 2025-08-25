"""
Helper Utilities for TCH Patient 360 PoC

Common utility functions for data formatting, date handling,
pediatric calculations, and general application helpers.
"""

import pandas as pd
import streamlit as st
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
import re
import logging

logger = logging.getLogger(__name__)

def format_date(date_value: Union[datetime, date, str], format_str: str = "%Y-%m-%d") -> str:
    """
    Format date values consistently
    
    Args:
        date_value: Date to format
        format_str: Format string
        
    Returns:
        Formatted date string
    """
    try:
        if pd.isna(date_value) or date_value is None:
            return "N/A"
        
        if isinstance(date_value, str):
            # Try to parse string date
            try:
                date_value = pd.to_datetime(date_value)
            except:
                return date_value  # Return original if can't parse
        
        if isinstance(date_value, (datetime, date)):
            return date_value.strftime(format_str)
        
        return str(date_value)
        
    except Exception as e:
        logger.error(f"Error formatting date: {e}")
        return "Invalid Date"

def format_currency(amount: Union[int, float], currency: str = "$") -> str:
    """
    Format currency values
    
    Args:
        amount: Amount to format
        currency: Currency symbol
        
    Returns:
        Formatted currency string
    """
    try:
        if pd.isna(amount) or amount is None:
            return "N/A"
        
        return f"{currency}{amount:,.2f}"
        
    except Exception as e:
        logger.error(f"Error formatting currency: {e}")
        return "N/A"

def format_phone_number(phone: str) -> str:
    """
    Format phone numbers consistently
    
    Args:
        phone: Phone number string
        
    Returns:
        Formatted phone number
    """
    try:
        if not phone or pd.isna(phone):
            return "N/A"
        
        # Remove all non-digits
        digits = re.sub(r'\D', '', str(phone))
        
        # Format as (XXX) XXX-XXXX if 10 digits
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            # Handle 1-XXX-XXX-XXXX format
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        else:
            return phone  # Return original if not standard format
            
    except Exception as e:
        logger.error(f"Error formatting phone number: {e}")
        return phone

def calculate_age(birth_date: Union[datetime, date, str], 
                 reference_date: Union[datetime, date] = None) -> int:
    """
    Calculate age from birth date
    
    Args:
        birth_date: Date of birth
        reference_date: Reference date (default: today)
        
    Returns:
        Age in years
    """
    try:
        if pd.isna(birth_date) or birth_date is None:
            return None
        
        # Convert to date if string
        if isinstance(birth_date, str):
            birth_date = pd.to_datetime(birth_date).date()
        elif isinstance(birth_date, datetime):
            birth_date = birth_date.date()
        
        # Use today if no reference date provided
        if reference_date is None:
            reference_date = date.today()
        elif isinstance(reference_date, datetime):
            reference_date = reference_date.date()
        
        # Calculate age
        age = reference_date.year - birth_date.year
        
        # Adjust if birthday hasn't occurred this year
        if reference_date.month < birth_date.month or \
           (reference_date.month == birth_date.month and reference_date.day < birth_date.day):
            age -= 1
        
        return age
        
    except Exception as e:
        logger.error(f"Error calculating age: {e}")
        return None

def get_pediatric_age_group(age: int) -> str:
    """
    Get pediatric age group classification
    
    Args:
        age: Age in years
        
    Returns:
        Pediatric age group
    """
    try:
        if age is None or pd.isna(age):
            return "Unknown"
        
        if age < 1:
            return "Infant"
        elif age < 2:
            return "Toddler"
        elif age < 6:
            return "Preschool"
        elif age < 13:
            return "School Age"
        elif age < 18:
            return "Adolescent"
        elif age < 22:
            return "Young Adult"
        else:
            return "Adult"
            
    except Exception as e:
        logger.error(f"Error determining pediatric age group: {e}")
        return "Unknown"

def format_query_params(params: Dict[str, Any]) -> List[Any]:
    """
    Format parameters for SQL queries
    
    Args:
        params: Dictionary of parameters
        
    Returns:
        List of formatted parameters
    """
    try:
        formatted_params = []
        
        for key, value in params.items():
            if value is None:
                formatted_params.append(None)
            elif isinstance(value, (list, tuple)):
                # Handle list parameters (for IN clauses)
                formatted_params.extend(value)
            elif isinstance(value, (datetime, date)):
                # Format dates
                formatted_params.append(value.strftime('%Y-%m-%d'))
            else:
                formatted_params.append(value)
        
        return formatted_params
        
    except Exception as e:
        logger.error(f"Error formatting query params: {e}")
        return []

def handle_database_errors(func):
    """
    Decorator for handling database errors gracefully
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function with error handling
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Database error in {func.__name__}: {e}")
            # Return appropriate default based on function type
            if 'get_' in func.__name__ or 'fetch_' in func.__name__:
                return pd.DataFrame()  # Return empty DataFrame for data functions
            else:
                return None
    
    return wrapper

def safe_divide(numerator: Union[int, float], denominator: Union[int, float], 
               default: Union[int, float] = 0) -> Union[int, float]:
    """
    Safely divide two numbers, handling division by zero
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        default: Default value if division by zero
        
    Returns:
        Division result or default value
    """
    try:
        if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
            return default
        return numerator / denominator
    except Exception as e:
        logger.error(f"Error in safe division: {e}")
        return default

def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate text to specified length
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated text
    """
    try:
        if not text or pd.isna(text):
            return ""
        
        text = str(text)
        if len(text) <= max_length:
            return text
        
        return text[:max_length - len(suffix)] + suffix
        
    except Exception as e:
        logger.error(f"Error truncating text: {e}")
        return str(text) if text else ""

def format_medical_record_number(mrn: str) -> str:
    """
    Format Medical Record Number consistently
    
    Args:
        mrn: Raw MRN string
        
    Returns:
        Formatted MRN
    """
    try:
        if not mrn or pd.isna(mrn):
            return "N/A"
        
        # Remove non-alphanumeric characters
        clean_mrn = re.sub(r'[^A-Za-z0-9]', '', str(mrn))
        
        # Ensure MRN prefix
        if not clean_mrn.upper().startswith('MRN'):
            clean_mrn = f"MRN{clean_mrn}"
        
        return clean_mrn.upper()
        
    except Exception as e:
        logger.error(f"Error formatting MRN: {e}")
        return str(mrn) if mrn else "N/A"

def get_risk_level_color(risk_level: str) -> str:
    """
    Get color code for risk levels
    
    Args:
        risk_level: Risk level string
        
    Returns:
        Color code or emoji
    """
    risk_colors = {
        'HIGH': '🔴',
        'MEDIUM': '🟡', 
        'LOW': '🟢',
        'UNKNOWN': '⚪'
    }
    
    return risk_colors.get(str(risk_level).upper(), '⚪')

def format_lab_value(value: Union[str, int, float], unit: str = None, 
                    reference_range: str = None) -> str:
    """
    Format laboratory values with units and reference ranges
    
    Args:
        value: Lab value
        unit: Unit of measurement
        reference_range: Reference range
        
    Returns:
        Formatted lab value string
    """
    try:
        if pd.isna(value) or value is None:
            return "N/A"
        
        formatted_value = str(value)
        
        if unit and not pd.isna(unit):
            formatted_value += f" {unit}"
        
        if reference_range and not pd.isna(reference_range):
            formatted_value += f" (Ref: {reference_range})"
        
        return formatted_value
        
    except Exception as e:
        logger.error(f"Error formatting lab value: {e}")
        return str(value) if value else "N/A"

def create_breadcrumbs(current_page: str, patient_name: str = None) -> str:
    """
    Create breadcrumb navigation string
    
    Args:
        current_page: Current page name
        patient_name: Optional patient name for context
        
    Returns:
        Breadcrumb string
    """
    try:
        breadcrumbs = ["TCH Patient 360"]
        
        if patient_name:
            breadcrumbs.append(f"Patient: {patient_name}")
        
        breadcrumbs.append(current_page)
        
        return " > ".join(breadcrumbs)
        
    except Exception as e:
        logger.error(f"Error creating breadcrumbs: {e}")
        return current_page

def format_medication_dosage(medication_name: str, dosage: str, 
                           frequency: str) -> str:
    """
    Format medication information consistently
    
    Args:
        medication_name: Name of medication
        dosage: Dosage amount
        frequency: Frequency of administration
        
    Returns:
        Formatted medication string
    """
    try:
        parts = [medication_name] if medication_name else []
        
        if dosage and not pd.isna(dosage):
            parts.append(str(dosage))
        
        if frequency and not pd.isna(frequency):
            parts.append(str(frequency))
        
        return " - ".join(parts) if parts else "N/A"
        
    except Exception as e:
        logger.error(f"Error formatting medication: {e}")
        return medication_name if medication_name else "N/A"

def calculate_length_of_stay(admit_date: Union[datetime, date, str], 
                           discharge_date: Union[datetime, date, str]) -> Optional[int]:
    """
    Calculate length of stay in days
    
    Args:
        admit_date: Admission date
        discharge_date: Discharge date
        
    Returns:
        Length of stay in days
    """
    try:
        if pd.isna(admit_date) or pd.isna(discharge_date):
            return None
        
        # Convert to datetime if string
        if isinstance(admit_date, str):
            admit_date = pd.to_datetime(admit_date)
        if isinstance(discharge_date, str):
            discharge_date = pd.to_datetime(discharge_date)
        
        # Calculate difference
        los = (discharge_date - admit_date).days
        
        # Ensure non-negative
        return max(0, los)
        
    except Exception as e:
        logger.error(f"Error calculating length of stay: {e}")
        return None

def generate_summary_stats(data: pd.DataFrame, numeric_columns: List[str]) -> Dict[str, Any]:
    """
    Generate summary statistics for numeric columns
    
    Args:
        data: DataFrame to analyze
        numeric_columns: List of numeric column names
        
    Returns:
        Dictionary of summary statistics
    """
    try:
        if data.empty:
            return {}
        
        stats = {}
        
        for col in numeric_columns:
            if col in data.columns:
                series = pd.to_numeric(data[col], errors='coerce')
                
                stats[col] = {
                    'count': series.count(),
                    'mean': series.mean(),
                    'median': series.median(),
                    'std': series.std(),
                    'min': series.min(),
                    'max': series.max(),
                    'q25': series.quantile(0.25),
                    'q75': series.quantile(0.75)
                }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error generating summary stats: {e}")
        return {}

def create_filter_summary(filters: Dict[str, Any]) -> str:
    """
    Create a human-readable summary of applied filters
    
    Args:
        filters: Dictionary of filter values
        
    Returns:
        Filter summary string
    """
    try:
        if not filters:
            return "No filters applied"
        
        summary_parts = []
        
        for key, value in filters.items():
            if value:
                if isinstance(value, list):
                    if len(value) > 3:
                        summary_parts.append(f"{key}: {len(value)} selected")
                    else:
                        summary_parts.append(f"{key}: {', '.join(map(str, value))}")
                elif isinstance(value, tuple) and len(value) == 2:
                    summary_parts.append(f"{key}: {value[0]} to {value[1]}")
                else:
                    summary_parts.append(f"{key}: {value}")
        
        return " | ".join(summary_parts) if summary_parts else "No filters applied"
        
    except Exception as e:
        logger.error(f"Error creating filter summary: {e}")
        return "Filter summary unavailable"