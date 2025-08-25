"""
Streamlit in Snowflake (SiS) Connection Manager

This module provides utilities for working with Snowpark sessions
within Streamlit in Snowflake applications.
"""

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark.functions import col, lit, count, sum as snowpark_sum, avg
import pandas as pd
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class SiSConnectionManager:
    """Connection manager for Streamlit in Snowflake applications"""
    
    def __init__(self, session: Session):
        """Initialize with an active Snowpark session"""
        self.session = session
        self.database = "TCH_PATIENT_360_POC"
        self.compute_warehouse = "TCH_COMPUTE_WH"
        self.analytics_warehouse = "TCH_ANALYTICS_WH"
        self.ai_ml_warehouse = "TCH_AI_ML_WH"
        
    def get_session(self) -> Session:
        """Get the active Snowpark session"""
        return self.session
    
    def query_table(self, table_name: str, schema: str = "PRESENTATION") -> SnowparkDataFrame:
        """Query a table and return Snowpark DataFrame"""
        try:
            full_table_name = f"{self.database}.{schema}.{table_name}"
            return self.session.table(full_table_name)
        except Exception as e:
            logger.error(f"Error querying table {table_name}: {e}")
            raise
    
    def execute_sql(self, sql: str) -> SnowparkDataFrame:
        """Execute SQL and return Snowpark DataFrame"""
        try:
            return self.session.sql(sql)
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            raise
    
    def get_patient_360_view(self, patient_id: Optional[str] = None) -> SnowparkDataFrame:
        """Get data from Patient 360 view"""
        try:
            df = self.query_table("PATIENT_360")
            if patient_id:
                df = df.filter(col("PATIENT_ID") == patient_id)
            return df
        except Exception as e:
            logger.error(f"Error getting Patient 360 data: {e}")
            raise
    
    def get_encounter_analytics(self, patient_id: Optional[str] = None) -> SnowparkDataFrame:
        """Get encounter analytics data"""
        try:
            df = self.query_table("ENCOUNTER_ANALYTICS")
            if patient_id:
                df = df.filter(col("PATIENT_KEY") == patient_id)
            return df
        except Exception as e:
            logger.error(f"Error getting encounter analytics: {e}")
            raise
    
    def get_diagnosis_analytics(self, patient_id: Optional[str] = None) -> SnowparkDataFrame:
        """Get diagnosis analytics data"""
        try:
            df = self.query_table("DIAGNOSIS_ANALYTICS")
            if patient_id:
                df = df.filter(col("PATIENT_KEY") == patient_id)
            return df
        except Exception as e:
            logger.error(f"Error getting diagnosis analytics: {e}")
            raise
    
    def get_population_summary(self) -> SnowparkDataFrame:
        """Get population health summary"""
        try:
            return self.query_table("POPULATION_HEALTH_SUMMARY")
        except Exception as e:
            logger.error(f"Error getting population summary: {e}")
            raise
    
    def get_department_performance(self) -> SnowparkDataFrame:
        """Get department performance metrics"""
        try:
            return self.query_table("DEPARTMENT_PERFORMANCE")
        except Exception as e:
            logger.error(f"Error getting department performance: {e}")
            raise
    
    def search_patients(self, search_term: str, limit: int = 20) -> SnowparkDataFrame:
        """Search for patients by name or MRN"""
        try:
            df = self.query_table("PATIENT_360")
            # Use Snowpark functions for filtering
            search_filter = (
                col("FULL_NAME").like(f"%{search_term}%") |
                col("MRN").like(f"%{search_term}%") |
                col("PATIENT_ID").like(f"%{search_term}%")
            )
            return df.filter(search_filter).limit(limit)
        except Exception as e:
            logger.error(f"Error searching patients: {e}")
            raise
    
    def get_patient_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics about patients"""
        try:
            df = self.query_table("PATIENT_360")
            
            stats = {
                'total_patients': df.count(),
                'avg_age': df.agg(avg(col("CURRENT_AGE"))).collect()[0][0],
                'gender_breakdown': df.group_by("GENDER").agg(count("*").alias("count")).collect(),
                'insurance_breakdown': df.group_by("PRIMARY_INSURANCE").agg(count("*").alias("count")).collect()
            }
            
            return stats
        except Exception as e:
            logger.error(f"Error getting patient stats: {e}")
            return {}
    
    def test_connection(self) -> bool:
        """Test the Snowpark session"""
        try:
            result = self.session.sql("SELECT CURRENT_TIMESTAMP()").collect()
            if result:
                st.success(f"✅ Connected to Snowflake at {result[0][0]}")
                return True
            else:
                st.error("❌ Failed to test Snowflake connection")
                return False
        except Exception as e:
            st.error(f"❌ Connection test failed: {e}")
            return False