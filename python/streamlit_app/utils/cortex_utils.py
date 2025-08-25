"""
Texas Children's Hospital Patient 360 PoC - Cortex Utilities

This module provides utilities for working with Snowflake Cortex features including
Cortex Analyst for structured data queries and Cortex Search for unstructured data.
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
import json
import logging
from datetime import datetime

from .snowflake_connection import SnowflakeConnection

logger = logging.getLogger(__name__)

class CortexAnalystClient:
    """Client for interacting with Cortex Analyst for natural language queries."""
    
    def __init__(self, snowflake_conn: SnowflakeConnection):
        """Initialize the Cortex Analyst client."""
        self.conn = snowflake_conn
        self.semantic_model = "TCH_Patient_360_Analytics"
    
    def query_natural_language(self, question: str, context: Optional[Dict] = None) -> Dict:
        """
        Send a natural language question to Cortex Analyst.
        
        Args:
            question: Natural language question about the data
            context: Optional context to help with the query
            
        Returns:
            Dictionary containing the response, SQL, and results
        """
        try:
            # For demo purposes, we'll simulate Cortex Analyst responses
            # In a real implementation, this would use the actual Cortex Analyst API
            
            response = self._simulate_cortex_analyst(question, context)
            return response
            
        except Exception as e:
            logger.error(f"Cortex Analyst query failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'response': f"I encountered an error processing your question: {e}",
                'sql': None,
                'results': None
            }
    
    def _simulate_cortex_analyst(self, question: str, context: Optional[Dict] = None) -> Dict:
        """Simulate Cortex Analyst responses for common healthcare questions."""
        
        question_lower = question.lower()
        
        # Patient demographics queries
        if any(word in question_lower for word in ['patients', 'demographics', 'age group', 'population']):
            return self._handle_demographics_query(question)
        
        # Encounter volume queries
        elif any(word in question_lower for word in ['encounters', 'visits', 'volume', 'activity']):
            return self._handle_encounters_query(question)
        
        # Diagnosis/condition queries
        elif any(word in question_lower for word in ['diagnosis', 'condition', 'asthma', 'diabetes', 'adhd', 'disease']):
            return self._handle_diagnosis_query(question)
        
        # Financial/charges queries
        elif any(word in question_lower for word in ['cost', 'charge', 'revenue', 'financial', 'billing']):
            return self._handle_financial_query(question)
        
        # Quality metrics queries
        elif any(word in question_lower for word in ['quality', 'readmission', 'length of stay', 'los']):
            return self._handle_quality_query(question)
        
        # Department performance queries
        elif any(word in question_lower for word in ['department', 'service', 'emergency', 'icu', 'ed']):
            return self._handle_department_query(question)
        
        # Default response for unrecognized queries
        else:
            return self._handle_general_query(question)
    
    def _handle_demographics_query(self, question: str) -> Dict:
        """Handle patient demographics related queries."""
        sql = """
        SELECT 
            age_group,
            COUNT(DISTINCT patient_key) as patient_count,
            ROUND(COUNT(DISTINCT patient_key) * 100.0 / SUM(COUNT(DISTINCT patient_key)) OVER(), 1) as percentage
        FROM AI_ML.PATIENT_ANALYTICS_BASE
        GROUP BY age_group
        ORDER BY patient_count DESC
        """
        
        results = self.conn.execute_query(sql)
        
        if results is not None and not results.empty:
            response = "Here's the breakdown of our patient population by age group:\n\n"
            for _, row in results.iterrows():
                response += f"• {row['AGE_GROUP']}: {row['PATIENT_COUNT']:,} patients ({row['PERCENTAGE']}%)\n"
        else:
            response = "I couldn't retrieve the patient demographics data at this time."
        
        return {
            'success': True,
            'response': response,
            'sql': sql,
            'results': results
        }
    
    def _handle_encounters_query(self, question: str) -> Dict:
        """Handle encounter volume related queries."""
        sql = """
        SELECT 
            encounter_type,
            COUNT(*) as encounter_count,
            COUNT(DISTINCT patient_key) as unique_patients,
            ROUND(AVG(length_of_stay_days), 1) as avg_los_days
        FROM AI_ML.ENCOUNTER_ANALYTICS_BASE
        WHERE encounter_date >= DATEADD('month', -12, CURRENT_DATE())
        GROUP BY encounter_type
        ORDER BY encounter_count DESC
        """
        
        results = self.conn.execute_query(sql)
        
        if results is not None and not results.empty:
            total_encounters = results['ENCOUNTER_COUNT'].sum()
            response = f"In the past 12 months, we've had {total_encounters:,} total encounters:\n\n"
            for _, row in results.iterrows():
                pct = (row['ENCOUNTER_COUNT'] / total_encounters) * 100
                response += f"• {row['ENCOUNTER_TYPE']}: {row['ENCOUNTER_COUNT']:,} encounters ({pct:.1f}%) serving {row['UNIQUE_PATIENTS']:,} unique patients\n"
                if row['AVG_LOS_DAYS'] and row['AVG_LOS_DAYS'] > 0:
                    response += f"  Average length of stay: {row['AVG_LOS_DAYS']} days\n"
        else:
            response = "I couldn't retrieve the encounter data at this time."
        
        return {
            'success': True,
            'response': response,
            'sql': sql,
            'results': results
        }
    
    def _handle_diagnosis_query(self, question: str) -> Dict:
        """Handle diagnosis and condition related queries."""
        question_lower = question.lower()
        
        if 'asthma' in question_lower:
            condition_filter = "condition_category = 'Asthma'"
            condition_name = "asthma"
        elif 'diabetes' in question_lower:
            condition_filter = "condition_category = 'Diabetes'"
            condition_name = "diabetes"
        elif 'adhd' in question_lower:
            condition_filter = "condition_category = 'ADHD'"
            condition_name = "ADHD"
        else:
            condition_filter = "is_chronic_condition = TRUE"
            condition_name = "chronic conditions"
        
        sql = f"""
        SELECT 
            condition_category,
            COUNT(DISTINCT patient_key) as patient_count,
            COUNT(*) as diagnosis_count,
            ROUND(AVG(age_at_diagnosis), 1) as avg_age_at_diagnosis
        FROM AI_ML.DIAGNOSIS_ANALYTICS_BASE
        WHERE {condition_filter}
        GROUP BY condition_category
        ORDER BY patient_count DESC
        """
        
        results = self.conn.execute_query(sql)
        
        if results is not None and not results.empty:
            if len(results) == 1:
                row = results.iloc[0]
                response = f"For {condition_name}:\n"
                response += f"• {row['PATIENT_COUNT']:,} patients affected\n"
                response += f"• {row['DIAGNOSIS_COUNT']:,} total diagnoses\n"
                response += f"• Average age at diagnosis: {row['AVG_AGE_AT_DIAGNOSIS']} years\n"
            else:
                response = f"Here's the breakdown for {condition_name}:\n\n"
                for _, row in results.iterrows():
                    response += f"• {row['CONDITION_CATEGORY']}: {row['PATIENT_COUNT']:,} patients\n"
        else:
            response = f"I couldn't find specific data about {condition_name} at this time."
        
        return {
            'success': True,
            'response': response,
            'sql': sql,
            'results': results
        }
    
    def _handle_financial_query(self, question: str) -> Dict:
        """Handle financial and billing related queries."""
        sql = """
        SELECT 
            encounter_type,
            COUNT(*) as encounter_count,
            SUM(total_charges) as total_revenue,
            AVG(total_charges) as avg_charge_per_encounter,
            MEDIAN(total_charges) as median_charge
        FROM AI_ML.ENCOUNTER_ANALYTICS_BASE
        WHERE encounter_date >= DATEADD('month', -12, CURRENT_DATE())
        AND total_charges > 0
        GROUP BY encounter_type
        ORDER BY total_revenue DESC
        """
        
        results = self.conn.execute_query(sql)
        
        if results is not None and not results.empty:
            total_revenue = results['TOTAL_REVENUE'].sum()
            response = f"Financial summary for the past 12 months (total revenue: ${total_revenue:,.2f}):\n\n"
            for _, row in results.iterrows():
                response += f"• {row['ENCOUNTER_TYPE']}:\n"
                response += f"  - {row['ENCOUNTER_COUNT']:,} encounters\n"
                response += f"  - ${row['TOTAL_REVENUE']:,.2f} total revenue\n"
                response += f"  - ${row['AVG_CHARGE_PER_ENCOUNTER']:,.2f} average per encounter\n"
                response += f"  - ${row['MEDIAN_CHARGE']:,.2f} median charge\n\n"
        else:
            response = "I couldn't retrieve the financial data at this time."
        
        return {
            'success': True,
            'response': response,
            'sql': sql,
            'results': results
        }
    
    def _handle_quality_query(self, question: str) -> Dict:
        """Handle quality metrics related queries."""
        sql = """
        SELECT 
            COUNT(*) as total_encounters,
            COUNT(CASE WHEN readmission_flag THEN 1 END) as readmissions,
            ROUND(COUNT(CASE WHEN readmission_flag THEN 1 END) * 100.0 / COUNT(*), 2) as readmission_rate,
            ROUND(AVG(length_of_stay_days), 1) as avg_length_of_stay,
            ROUND(MEDIAN(length_of_stay_days), 1) as median_length_of_stay
        FROM AI_ML.ENCOUNTER_ANALYTICS_BASE
        WHERE encounter_date >= DATEADD('month', -12, CURRENT_DATE())
        AND encounter_type IN ('Inpatient', 'Observation')
        """
        
        results = self.conn.execute_query(sql)
        
        if results is not None and not results.empty:
            row = results.iloc[0]
            response = "Quality metrics for inpatient encounters (past 12 months):\n\n"
            response += f"• Total encounters: {row['TOTAL_ENCOUNTERS']:,}\n"
            response += f"• Readmissions: {row['READMISSIONS']:,} ({row['READMISSION_RATE']}%)\n"
            response += f"• Average length of stay: {row['AVG_LENGTH_OF_STAY']} days\n"
            response += f"• Median length of stay: {row['MEDIAN_LENGTH_OF_STAY']} days\n"
        else:
            response = "I couldn't retrieve the quality metrics at this time."
        
        return {
            'success': True,
            'response': response,
            'sql': sql,
            'results': results
        }
    
    def _handle_department_query(self, question: str) -> Dict:
        """Handle department performance related queries."""
        sql = """
        SELECT 
            department,
            service_line,
            COUNT(*) as encounter_count,
            COUNT(DISTINCT patient_key) as unique_patients,
            ROUND(AVG(length_of_stay_days), 1) as avg_los,
            COUNT(CASE WHEN readmission_flag THEN 1 END) as readmissions
        FROM AI_ML.ENCOUNTER_ANALYTICS_BASE
        WHERE encounter_date >= DATEADD('month', -12, CURRENT_DATE())
        GROUP BY department, service_line
        ORDER BY encounter_count DESC
        LIMIT 10
        """
        
        results = self.conn.execute_query(sql)
        
        if results is not None and not results.empty:
            response = "Top departments by encounter volume (past 12 months):\n\n"
            for _, row in results.iterrows():
                response += f"• {row['DEPARTMENT']} ({row['SERVICE_LINE']}):\n"
                response += f"  - {row['ENCOUNTER_COUNT']:,} encounters\n"
                response += f"  - {row['UNIQUE_PATIENTS']:,} unique patients\n"
                if row['AVG_LOS'] and row['AVG_LOS'] > 0:
                    response += f"  - {row['AVG_LOS']} days average LOS\n"
                if row['READMISSIONS'] and row['READMISSIONS'] > 0:
                    response += f"  - {row['READMISSIONS']} readmissions\n"
                response += "\n"
        else:
            response = "I couldn't retrieve the department data at this time."
        
        return {
            'success': True,
            'response': response,
            'sql': sql,
            'results': results
        }
    
    def _handle_general_query(self, question: str) -> Dict:
        """Handle general queries with a helpful response."""
        response = """I can help you analyze our pediatric healthcare data! Here are some examples of questions I can answer:

**Patient Demographics:**
• "How many patients do we have by age group?"
• "What's our patient population breakdown?"

**Clinical Activity:**
• "What's our encounter volume this year?"
• "How many emergency department visits have we had?"

**Common Conditions:**
• "How many patients have asthma?"
• "What are our most common diagnoses?"

**Quality Metrics:**
• "What's our readmission rate?"
• "What's the average length of stay?"

**Financial Analysis:**
• "What's our total revenue by encounter type?"
• "What are the average charges per encounter?"

**Department Performance:**
• "Which departments see the most patients?"
• "How is our ICU performing?"

Please ask me a specific question about any of these topics!"""
        
        return {
            'success': True,
            'response': response,
            'sql': None,
            'results': None
        }

class CortexSearchClient:
    """Client for interacting with Cortex Search for unstructured data queries."""
    
    def __init__(self, snowflake_conn: SnowflakeConnection):
        """Initialize the Cortex Search client."""
        self.conn = snowflake_conn
    
    def search_clinical_notes(self, query: str, patient_id: Optional[str] = None, 
                            max_results: int = 5) -> Dict:
        """
        Search clinical notes using Cortex Search.
        
        Args:
            query: Search query
            patient_id: Optional patient ID to filter results
            max_results: Maximum number of results to return
            
        Returns:
            Dictionary containing search results and metadata
        """
        try:
            # Use the search function we created in the Cortex setup
            if patient_id:
                sql = f"""
                SELECT * FROM TABLE(
                    AI_ML.SEARCH_CLINICAL_NOTES(
                        '{query}', 
                        '{patient_id}', 
                        NULL, 
                        NULL, 
                        {max_results}
                    )
                )
                """
            else:
                sql = f"""
                SELECT * FROM TABLE(
                    AI_ML.SEARCH_CLINICAL_NOTES(
                        '{query}', 
                        NULL, 
                        NULL, 
                        NULL, 
                        {max_results}
                    )
                )
                """
            
            results = self.conn.execute_query(sql)
            
            return {
                'success': True,
                'query': query,
                'results': results,
                'count': len(results) if results is not None else 0
            }
            
        except Exception as e:
            logger.error(f"Clinical notes search failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'query': query,
                'results': None,
                'count': 0
            }
    
    def search_radiology_reports(self, query: str, patient_id: Optional[str] = None,
                                max_results: int = 5) -> Dict:
        """Search radiology reports using Cortex Search."""
        try:
            if patient_id:
                sql = f"""
                SELECT * FROM TABLE(
                    AI_ML.SEARCH_RADIOLOGY_REPORTS(
                        '{query}', 
                        '{patient_id}', 
                        NULL, 
                        NULL, 
                        {max_results}
                    )
                )
                """
            else:
                sql = f"""
                SELECT * FROM TABLE(
                    AI_ML.SEARCH_RADIOLOGY_REPORTS(
                        '{query}', 
                        NULL, 
                        NULL, 
                        NULL, 
                        {max_results}
                    )
                )
                """
            
            results = self.conn.execute_query(sql)
            
            return {
                'success': True,
                'query': query,
                'results': results,
                'count': len(results) if results is not None else 0
            }
            
        except Exception as e:
            logger.error(f"Radiology reports search failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'query': query,
                'results': None,
                'count': 0
            }
    
    def search_all_clinical_docs(self, query: str, patient_id: Optional[str] = None,
                                max_results: int = 10) -> Dict:
        """Search all clinical documentation using Cortex Search."""
        try:
            if patient_id:
                sql = f"""
                SELECT * FROM TABLE(
                    AI_ML.SEARCH_ALL_CLINICAL_DOCS(
                        '{query}', 
                        '{patient_id}', 
                        NULL, 
                        NULL, 
                        {max_results}
                    )
                )
                """
            else:
                sql = f"""
                SELECT * FROM TABLE(
                    AI_ML.SEARCH_ALL_CLINICAL_DOCS(
                        '{query}', 
                        NULL, 
                        NULL, 
                        NULL, 
                        {max_results}
                    )
                )
                """
            
            results = self.conn.execute_query(sql)
            
            return {
                'success': True,
                'query': query,
                'results': results,
                'count': len(results) if results is not None else 0
            }
            
        except Exception as e:
            logger.error(f"Clinical documentation search failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'query': query,
                'results': None,
                'count': 0
            }
    
    def get_clinical_context_for_rag(self, query: str, patient_id: Optional[str] = None,
                                   max_items: int = 3) -> List[str]:
        """Get relevant clinical context for RAG applications."""
        try:
            if patient_id:
                sql = f"""
                SELECT context_item
                FROM TABLE(
                    AI_ML.GET_CLINICAL_CONTEXT_FOR_RAG(
                        '{query}', 
                        '{patient_id}', 
                        {max_items}
                    )
                )
                ORDER BY relevance_score DESC
                """
            else:
                sql = f"""
                SELECT context_item
                FROM TABLE(
                    AI_ML.GET_CLINICAL_CONTEXT_FOR_RAG(
                        '{query}', 
                        NULL, 
                        {max_items}
                    )
                )
                ORDER BY relevance_score DESC
                """
            
            results = self.conn.execute_query(sql)
            
            if results is not None and not results.empty:
                return results['CONTEXT_ITEM'].tolist()
            else:
                return []
                
        except Exception as e:
            logger.error(f"Failed to get clinical context: {e}")
            return []

class CortexAgentRouter:
    """Intelligent router to determine whether to use Cortex Analyst or Cortex Search."""
    
    def __init__(self, analyst_client: CortexAnalystClient, search_client: CortexSearchClient):
        """Initialize the agent router."""
        self.analyst = analyst_client
        self.search = search_client
    
    def route_query(self, question: str, patient_id: Optional[str] = None) -> Dict:
        """
        Route a user question to the appropriate Cortex service.
        
        Args:
            question: User's natural language question
            patient_id: Optional patient ID for context
            
        Returns:
            Dictionary containing the response and routing information
        """
        question_lower = question.lower()
        
        # Structured data queries (Cortex Analyst)
        structured_keywords = [
            'how many', 'count', 'total', 'average', 'percentage', 'rate', 'trend',
            'demographics', 'population', 'volume', 'statistics', 'metrics',
            'revenue', 'charges', 'financial', 'billing', 'cost',
            'department', 'service line', 'performance'
        ]
        
        # Unstructured data queries (Cortex Search)
        unstructured_keywords = [
            'notes', 'report', 'documentation', 'wrote', 'mentioned', 'documented',
            'clinical note', 'radiology', 'pathology', 'discharge summary',
            'progress note', 'what did', 'what was written', 'find in notes',
            'search notes', 'look up', 'review', 'examine'
        ]
        
        # Check for unstructured data queries first
        if any(keyword in question_lower for keyword in unstructured_keywords):
            return self._route_to_search(question, patient_id)
        
        # Check for structured data queries
        elif any(keyword in question_lower for keyword in structured_keywords):
            return self._route_to_analyst(question, patient_id)
        
        # For ambiguous queries, try analyst first, then search if needed
        else:
            return self._route_to_analyst(question, patient_id)
    
    def _route_to_analyst(self, question: str, patient_id: Optional[str] = None) -> Dict:
        """Route query to Cortex Analyst."""
        context = {'patient_id': patient_id} if patient_id else None
        result = self.analyst.query_natural_language(question, context)
        result['routing'] = 'analyst'
        result['service'] = 'Cortex Analyst (Structured Data)'
        return result
    
    def _route_to_search(self, question: str, patient_id: Optional[str] = None) -> Dict:
        """Route query to Cortex Search."""
        # Determine search type based on question content
        question_lower = question.lower()
        
        if any(word in question_lower for word in ['radiology', 'imaging', 'x-ray', 'ct', 'mri', 'ultrasound']):
            search_result = self.search.search_radiology_reports(question, patient_id)
            search_type = "radiology reports"
        else:
            search_result = self.search.search_all_clinical_docs(question, patient_id)
            search_type = "clinical documentation"
        
        # Format the response
        if search_result['success'] and search_result['count'] > 0:
            response = f"I found {search_result['count']} relevant {search_type} for your query:\n\n"
            
            for idx, row in search_result['results'].iterrows():
                response += f"**Document {idx+1}** (Relevance: {row['RELEVANCE_SCORE']:.2f})\n"
                response += f"Type: {row.get('DOCUMENT_TYPE', 'Clinical Note')}\n"
                response += f"Patient: {row.get('PATIENT_CONTEXT', 'N/A')}\n"
                response += f"Preview: {row.get('CONTENT_PREVIEW', 'N/A')}\n\n"
        else:
            response = f"I couldn't find any relevant {search_type} for your query."
        
        return {
            'success': search_result['success'],
            'response': response,
            'routing': 'search',
            'service': f'Cortex Search ({search_type.title()})',
            'search_results': search_result['results'],
            'count': search_result['count']
        }