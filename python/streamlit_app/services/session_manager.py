"""
Session Manager for TCH Patient 360 PoC

Manages Snowflake connections, session state, and Cortex AI service availability.
Implements enterprise-grade session management with connection pooling and caching.
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class SessionManager:
    """Manages Snowflake sessions and application state"""
    
    def __init__(self):
        self.session = None
        self.cortex_services = {
            'analyst': True,
            'search': True, 
            'agents': True
        }
        self.connection_cache = {}
        self.last_health_check = None
        
    def initialize_services(self):
        """Initialize all required services for the application"""
        try:
            # Get active Snowflake session
            self.session = get_active_session()
            
            # Test connection
            self._test_connection()
            
            # Skip Cortex AI service status probing (removed System Status UI)
            
            # Set up caching
            self._setup_caching()
            
            logger.info("Services initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {str(e)}")
            # Don't fail completely - allow graceful degradation
            self.session = None
    
    def get_session(self) -> Optional[Session]:
        """Get the active Snowflake session"""
        if self.session is None:
            try:
                self.session = get_active_session()
            except Exception as e:
                logger.error(f"Failed to get active session: {str(e)}")
                return None
        return self.session
    
    def check_connection(self) -> bool:
        """Check if Snowflake connection is healthy"""
        try:
            # Check if we need to perform health check
            now = datetime.now()
            if (self.last_health_check is None or 
                now - self.last_health_check > timedelta(minutes=5)):
                
                session = self.get_session()
                if session:
                    # Simple health check query
                    result = session.sql("SELECT CURRENT_TIMESTAMP()").collect()
                    if result:
                        self.last_health_check = now
                        return True
                return False
            return True
            
        except Exception as e:
            logger.error(f"Connection health check failed: {str(e)}")
            return False
    
    def get_cortex_status(self) -> Dict[str, bool]:
        """Get status of Cortex AI services"""
        return self.cortex_services.copy()
    
    def execute_query(self, query: str, use_cache: bool = True) -> Optional[Any]:
        """Execute a Snowflake SQL query with caching"""
        try:
            # Check cache first if enabled
            if use_cache and query in self.connection_cache:
                cache_entry = self.connection_cache[query]
                if datetime.now() - cache_entry['timestamp'] < timedelta(minutes=30):
                    logger.debug(f"Cache hit for query: {query[:50]}...")
                    return cache_entry['result']
            
            # Execute query
            session = self.get_session()
            if not session:
                logger.error("No active session available")
                return None
                
            result = session.sql(query).collect()
            
            # Cache result if enabled
            if use_cache:
                self.connection_cache[query] = {
                    'result': result,
                    'timestamp': datetime.now()
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return None
    
    def execute_query_to_pandas(self, query: str, use_cache: bool = True):
        """Execute query and return as pandas DataFrame"""
        try:
            session = self.get_session()
            if not session:
                return None
                
            # Check cache first
            cache_key = f"pandas_{query}"
            if use_cache and cache_key in self.connection_cache:
                cache_entry = self.connection_cache[cache_key]
                if datetime.now() - cache_entry['timestamp'] < timedelta(minutes=30):
                    return cache_entry['result']
            
            # Execute and convert to pandas
            result = session.sql(query).to_pandas()
            
            # Cache result
            if use_cache:
                self.connection_cache[cache_key] = {
                    'result': result,
                    'timestamp': datetime.now()
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Pandas query execution failed: {str(e)}")
            return None
    
    def clear_cache(self):
        """Clear all cached queries"""
        self.connection_cache.clear()
        logger.info("Query cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'cache_size': len(self.connection_cache),
            'last_health_check': self.last_health_check,
            'services_status': self.cortex_services
        }
    
    def _test_connection(self):
        """Test Snowflake connection with comprehensive checks"""
        if not self.session:
            raise Exception("No active session available")
        
        # Test basic connectivity
        result = self.session.sql("SELECT CURRENT_TIMESTAMP()").collect()
        if not result:
            raise Exception("Basic connectivity test failed")
        
        # Test database access
        databases = self.session.sql("SHOW DATABASES").collect()
        tch_db_found = any('TCH_PATIENT_360_POC' in str(row) for row in databases)
        if not tch_db_found:
            logger.warning("TCH Patient 360 database not found")
        
        # Test warehouse access
        warehouses = self.session.sql("SHOW WAREHOUSES").collect()
        if not warehouses:
            logger.warning("No warehouses available")
            
        logger.info("Connection test completed successfully")
    
    def _initialize_cortex_services(self):
        """Initialize and test Cortex AI services"""
        try:
            session = self.get_session()
            if not session:
                return
            
            # Set proper context for Cortex services
            try:
                session.sql("USE DATABASE TCH_PATIENT_360_POC").collect()
                session.sql("USE SCHEMA AI_ML").collect()
            except Exception as e:
                logger.warning(f"Could not set database context for Cortex checks: {e}")
            
            # Test Cortex Analyst availability
            try:
                # Try to access the semantic model directly
                result = session.sql("SHOW CORTEX ANALYST SEMANTIC MODELS").collect()
                self.cortex_services['analyst'] = len(result) > 0
                if self.cortex_services['analyst']:
                    logger.info("Cortex Analyst available")
                else:
                    logger.info("No Cortex Analyst semantic models found")
            except Exception as e:
                self.cortex_services['analyst'] = False
                logger.info(f"Cortex Analyst not available: {e}")
            
            # Test Cortex Search availability  
            try:
                # Check for search services in AI_ML schema
                search_services = session.sql("SHOW CORTEX SEARCH SERVICES").collect()
                self.cortex_services['search'] = len(search_services) > 0
                if self.cortex_services['search']:
                    logger.info(f"Cortex Search available with {len(search_services)} services")
                else:
                    logger.info("No Cortex Search services found")
            except Exception as e:
                self.cortex_services['search'] = False
                logger.info(f"Cortex Search not available: {e}")
            
            # Cortex Agents is a combination service
            self.cortex_services['agents'] = (
                self.cortex_services['analyst'] and 
                self.cortex_services['search']
            )
            
            if self.cortex_services['agents']:
                logger.info("Cortex Agents available (Analyst + Search)")
            else:
                logger.info("Cortex Agents not available (requires both Analyst and Search)")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cortex services: {str(e)}")
            # Set all services as unavailable on error
            for service in self.cortex_services:
                self.cortex_services[service] = False
    
    def _setup_caching(self):
        """Set up intelligent caching for queries"""
        # Common query patterns that benefit from caching
        self.cacheable_patterns = [
            'SELECT COUNT(*) FROM CONFORMED.PATIENT_MASTER',
            'SELECT * FROM CONFORMED.ENCOUNTER_SUMMARY',
            'SELECT * FROM CONFORMED.DIAGNOSIS_FACT'
        ]
        
        # Pre-warm cache with common queries
        self._prewarm_cache()
    
    def _prewarm_cache(self):
        """Pre-warm cache with commonly used queries"""
        try:
            common_queries = [
                "SELECT COUNT(*) AS total_patients FROM CONFORMED.PATIENT_MASTER",
                "SELECT COUNT(*) AS total_encounters FROM CONFORMED.ENCOUNTER_SUMMARY WHERE ENCOUNTER_DATE >= DATEADD('day', -30, CURRENT_DATE())",
                "SELECT COUNT(DISTINCT PATIENT_KEY) AS chronic_patients FROM CONFORMED.DIAGNOSIS_FACT WHERE IS_CHRONIC_CONDITION = TRUE"
            ]
            
            for query in common_queries:
                self.execute_query(query, use_cache=True)
                
        except Exception as e:
            logger.error(f"Cache pre-warming failed: {str(e)}")
    
    def get_semantic_model_info(self) -> Dict[str, Any]:
        """Get information about available semantic models for Cortex Analyst"""
        try:
            session = self.get_session()
            if not session:
                return {}
            
            # Get semantic model information
            models = session.sql("""
                SHOW CORTEX ANALYST SEMANTIC MODELS 
                IN SCHEMA TCH_PATIENT_360_POC.AI_ML
            """).collect()
            
            return {
                'models_available': len(models),
                'models': [str(row) for row in models] if models else []
            }
            
        except Exception as e:
            logger.error(f"Failed to get semantic model info: {str(e)}")
            return {'models_available': 0, 'models': []}
    
    def get_search_services_info(self) -> Dict[str, Any]:
        """Get information about available Cortex Search services"""
        try:
            session = self.get_session()
            if not session:
                return {}
            
            services = session.sql("SHOW CORTEX SEARCH SERVICES").collect()
            
            return {
                'services_available': len(services),
                'services': [str(row) for row in services] if services else []
            }
            
        except Exception as e:
            logger.error(f"Failed to get search services info: {str(e)}")
            return {'services_available': 0, 'services': []}