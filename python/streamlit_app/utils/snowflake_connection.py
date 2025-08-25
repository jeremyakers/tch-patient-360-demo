"""
Texas Children's Hospital Patient 360 PoC - Snowflake Connection Utilities

This module provides utilities for connecting to Snowflake and executing queries
within the Streamlit application.
"""

import streamlit as st
import snowflake.connector
import pandas as pd
import json
import os
from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnowflakeConnection:
    """Manages Snowflake connections and query execution for the Streamlit app."""
    
    def __init__(self):
        """Initialize the Snowflake connection manager."""
        self.connection = None
        self.database = "TCH_PATIENT_360_POC"
        self.schema = "PRESENTATION"
        self.warehouse = "ANALYTICS_WH"
        
    def get_connection_config(self) -> Dict[str, str]:
        """Get Snowflake connection configuration from environment or Streamlit secrets."""
        try:
            # Try to get from Streamlit secrets first
            if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
                return {
                    'account': st.secrets.snowflake.account,
                    'user': st.secrets.snowflake.user,
                    'password': st.secrets.snowflake.password,
                    'database': st.secrets.snowflake.get('database', self.database),
                    'warehouse': st.secrets.snowflake.get('warehouse', self.warehouse),
                    'schema': st.secrets.snowflake.get('schema', self.schema)
                }
        except Exception as e:
            logger.warning(f"Could not load from Streamlit secrets: {e}")
        
        # Fall back to environment variables
        return {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'database': os.getenv('SNOWFLAKE_DATABASE', self.database),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', self.warehouse),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', self.schema)
        }
    
    def connect(self) -> bool:
        """Establish connection to Snowflake."""
        try:
            config = self.get_connection_config()
            
            # Validate required config
            required_fields = ['account', 'user', 'password']
            missing_fields = [field for field in required_fields if not config.get(field)]
            
            if missing_fields:
                st.error(f"Missing required Snowflake configuration: {', '.join(missing_fields)}")
                return False
            
            # Create connection
            self.connection = snowflake.connector.connect(
                account=config['account'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
                warehouse=config['warehouse'],
                schema=config['schema']
            )
            
            logger.info("Successfully connected to Snowflake")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            st.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Optional[pd.DataFrame]:
        """Execute a SQL query and return results as a pandas DataFrame."""
        try:
            if not self.connection:
                if not self.connect():
                    return None
            
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Fetch results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=columns)
            
            cursor.close()
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            st.error(f"Query execution failed: {e}")
            return None
    
    def execute_single_value(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a query and return a single value."""
        df = self.execute_query(query, params)
        if df is not None and not df.empty:
            return df.iloc[0, 0]
        return None
    
    def test_connection(self) -> bool:
        """Test the Snowflake connection."""
        try:
            result = self.execute_single_value("SELECT CURRENT_TIMESTAMP()")
            if result:
                st.success(f"✅ Connected to Snowflake at {result}")
                return True
            else:
                st.error("❌ Failed to test Snowflake connection")
                return False
        except Exception as e:
            st.error(f"❌ Connection test failed: {e}")
            return False
    
    def get_patient_summary(self, patient_id: str) -> Optional[Dict]:
        """Get comprehensive patient summary from Patient 360 view."""
        query = """
        SELECT 
            patient_id,
            mrn,
            full_name,
            current_age,
            gender,
            race,
            ethnicity,
            primary_insurance,
            risk_category,
            condition_count,
            total_encounters,
            last_encounter_date,
            days_since_last_visit,
            patient_status,
            age_group
        FROM PATIENT_360
        WHERE patient_id = %s
        """
        
        df = self.execute_query(query, {'patient_id': patient_id})
        if df is not None and not df.empty:
            return df.iloc[0].to_dict()
        return None
    
    def search_patients(self, search_term: str, limit: int = 20) -> pd.DataFrame:
        """Search for patients by name, MRN, or patient ID."""
        query = """
        SELECT 
            patient_id,
            mrn,
            full_name,
            current_age,
            gender,
            risk_category,
            total_encounters,
            last_encounter_date,
            patient_status
        FROM PATIENT_360
        WHERE UPPER(full_name) LIKE UPPER(%s)
           OR UPPER(mrn) LIKE UPPER(%s)
           OR UPPER(patient_id) LIKE UPPER(%s)
        ORDER BY full_name
        LIMIT %s
        """
        
        search_pattern = f"%{search_term}%"
        return self.execute_query(query, {
            'search_term_1': search_pattern,
            'search_term_2': search_pattern, 
            'search_term_3': search_pattern,
            'limit': limit
        })
    
    def get_patient_encounters(self, patient_id: str, limit: int = 50) -> pd.DataFrame:
        """Get encounter history for a patient."""
        query = """
        SELECT 
            encounter_date,
            encounter_type,
            department,
            attending_provider,
            chief_complaint_category,
            length_of_stay_days,
            total_charges,
            readmission_flag
        FROM ENCOUNTER_ANALYTICS
        WHERE patient_id = %s
        ORDER BY encounter_date DESC
        LIMIT %s
        """
        
        return self.execute_query(query, {'patient_id': patient_id, 'limit': limit})
    
    def get_patient_diagnoses(self, patient_id: str, limit: int = 50) -> pd.DataFrame:
        """Get diagnosis history for a patient."""
        query = """
        SELECT 
            diagnosis_date,
            diagnosis_description,
            diagnosis_category,
            age_at_diagnosis,
            is_chronic_condition,
            encounter_type,
            department
        FROM DIAGNOSIS_ANALYTICS
        WHERE patient_id = %s
        ORDER BY diagnosis_date DESC
        LIMIT %s
        """
        
        return self.execute_query(query, {'patient_id': patient_id, 'limit': limit})
    
    def get_population_metrics(self) -> Dict:
        """Get population health metrics."""
        query = """
        SELECT 
            total_patients,
            newborn_count,
            infant_count,
            toddler_count,
            preschool_count,
            school_age_count,
            adolescent_count,
            young_adult_count,
            male_patients,
            female_patients,
            medicaid_patients,
            commercial_patients,
            high_risk_patients,
            asthma_patients,
            diabetes_patients,
            obesity_patients
        FROM POPULATION_HEALTH_SUMMARY
        ORDER BY report_date DESC
        LIMIT 1
        """
        
        df = self.execute_query(query)
        if df is not None and not df.empty:
            return df.iloc[0].to_dict()
        return {}
    
    def get_department_metrics(self) -> pd.DataFrame:
        """Get department performance metrics."""
        query = """
        SELECT 
            department_name,
            service_line,
            total_encounters,
            unique_patients,
            avg_length_of_stay,
            readmission_rate_percent,
            total_revenue,
            avg_patient_age
        FROM DEPARTMENT_PERFORMANCE
        ORDER BY total_encounters DESC
        """
        
        return self.execute_query(query)
    
    def get_quality_metrics(self) -> Dict:
        """Get quality metrics dashboard data."""
        query = """
        SELECT 
            total_encounters,
            readmissions,
            readmission_rate_percent,
            avg_length_of_stay_days,
            total_ed_visits,
            ed_throughput_rate_percent,
            asthma_patients_seen,
            diabetes_patients_seen,
            total_charges,
            avg_charge_per_encounter
        FROM QUALITY_METRICS_DASHBOARD
        """
        
        df = self.execute_query(query)
        if df is not None and not df.empty:
            return df.iloc[0].to_dict()
        return {}
    
    def get_encounter_trends(self, months: int = 12) -> pd.DataFrame:
        """Get encounter volume trends over time."""
        query = f"""
        SELECT 
            encounter_month_year,
            COUNT(*) as encounter_count,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(CASE WHEN encounter_type = 'Emergency' THEN 1 END) as emergency_visits,
            COUNT(CASE WHEN encounter_type = 'Inpatient' THEN 1 END) as inpatient_visits,
            COUNT(CASE WHEN encounter_type = 'Outpatient' THEN 1 END) as outpatient_visits,
            AVG(length_of_stay_days) as avg_los,
            SUM(total_charges) as total_revenue
        FROM ENCOUNTER_ANALYTICS
        WHERE encounter_date >= DATEADD('month', -{months}, CURRENT_DATE())
        GROUP BY encounter_month_year, encounter_year, encounter_month
        ORDER BY encounter_year, encounter_month
        """
        
        return self.execute_query(query)
    
    def close_connection(self):
        """Close the Snowflake connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Snowflake connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self.connection = None

# Global connection instance for the Streamlit app
@st.cache_resource
def get_snowflake_connection():
    """Get or create a cached Snowflake connection."""
    return SnowflakeConnection()

def format_currency(value: float) -> str:
    """Format a numeric value as currency."""
    if value is None:
        return "N/A"
    return f"${value:,.2f}"

def format_percentage(value: float, decimals: int = 1) -> str:
    """Format a numeric value as percentage."""
    if value is None:
        return "N/A"
    return f"{value:.{decimals}f}%"

def format_number(value: float, decimals: int = 0) -> str:
    """Format a numeric value with commas."""
    if value is None:
        return "N/A"
    if decimals == 0:
        return f"{int(value):,}"
    else:
        return f"{value:,.{decimals}f}"