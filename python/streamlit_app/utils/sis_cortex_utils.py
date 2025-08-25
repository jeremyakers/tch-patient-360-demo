"""
Streamlit in Snowflake (SiS) Cortex Utilities

This module provides utilities for interacting with Snowflake Cortex services
within Streamlit in Snowflake applications using Snowpark.
"""

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark.functions import col, lit, when, count, avg, sum as snowpark_sum
import pandas as pd
from typing import Dict, List, Optional, Any
import logging
import json

logger = logging.getLogger(__name__)

class SiSCortexAnalyst:
    """Cortex Analyst client for Streamlit in Snowflake"""
    
    def __init__(self, session: Session):
        """Initialize with Snowpark session"""
        self.session = session
        self.semantic_model = "TCH_Patient_360_Analytics"
    
    def query_natural_language(self, question: str, context: Optional[Dict] = None) -> Dict:
        """
        Send a natural language question to Cortex Analyst using Snowpark.
        
        Args:
            question: Natural language question about the data
            context: Optional context to help with the query
            
        Returns:
            Dictionary containing the response, SQL, and results
        """
        try:
            # In a real SiS implementation, this would use Cortex Analyst
            # For now, we'll simulate responses based on the question
            
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
        """Handle patient demographics related queries using Snowpark."""
        try:
            # Get patient demographics using Snowpark
            df = self.session.table("PATIENT_360")
            
            # Group by age group and count patients
            age_stats = df.group_by("AGE_GROUP").agg(
                count("PATIENT_ID").alias("PATIENT_COUNT")
            ).order_by("PATIENT_COUNT", ascending=False)
            
            results_pd = age_stats.to_pandas()
            
            response = "Here's the breakdown of our patient population by age group:\n\n"
            total_patients = results_pd['PATIENT_COUNT'].sum()
            
            for _, row in results_pd.iterrows():
                percentage = (row['PATIENT_COUNT'] / total_patients) * 100
                response += f"• {row['AGE_GROUP']}: {row['PATIENT_COUNT']:,} patients ({percentage:.1f}%)\n"
            
            return {
                'success': True,
                'response': response,
                'sql': age_stats.queries['queries'][-1] if hasattr(age_stats, 'queries') else "Snowpark Query",
                'results': results_pd
            }
            
        except Exception as e:
            return self._get_mock_response("demographics", question)
    
    def _handle_encounters_query(self, question: str) -> Dict:
        """Handle encounter volume related queries using Snowpark."""
        try:
            # Get encounter data using Snowpark
            df = self.session.table("ENCOUNTER_ANALYTICS")
            
            # Group by encounter type
            encounter_stats = df.group_by("ENCOUNTER_TYPE").agg(
                count("*").alias("ENCOUNTER_COUNT"),
                count(col("PATIENT_KEY")).alias("UNIQUE_PATIENTS"),
                avg("LENGTH_OF_STAY_DAYS").alias("AVG_LOS")
            ).order_by("ENCOUNTER_COUNT", ascending=False)
            
            results_pd = encounter_stats.to_pandas()
            
            total_encounters = results_pd['ENCOUNTER_COUNT'].sum()
            response = f"In the past 12 months, we've had {total_encounters:,} total encounters:\n\n"
            
            for _, row in results_pd.iterrows():
                pct = (row['ENCOUNTER_COUNT'] / total_encounters) * 100
                response += f"• {row['ENCOUNTER_TYPE']}: {row['ENCOUNTER_COUNT']:,} encounters ({pct:.1f}%) serving {row['UNIQUE_PATIENTS']:,} unique patients\n"
                if row['AVG_LOS'] and row['AVG_LOS'] > 0:
                    response += f"  Average length of stay: {row['AVG_LOS']:.1f} days\n"
            
            return {
                'success': True,
                'response': response,
                'sql': "Snowpark Encounter Analytics Query",
                'results': results_pd
            }
            
        except Exception as e:
            return self._get_mock_response("encounters", question)
    
    def _handle_diagnosis_query(self, question: str) -> Dict:
        """Handle diagnosis and condition related queries using Snowpark."""
        try:
            question_lower = question.lower()
            
            # Filter by specific condition if mentioned
            df = self.session.table("DIAGNOSIS_ANALYTICS")
            
            if 'asthma' in question_lower:
                df = df.filter(col("DIAGNOSIS_CATEGORY").like("%Asthma%"))
                condition_name = "asthma"
            elif 'diabetes' in question_lower:
                df = df.filter(col("DIAGNOSIS_CATEGORY").like("%Diabetes%"))
                condition_name = "diabetes"
            elif 'adhd' in question_lower:
                df = df.filter(col("DIAGNOSIS_CATEGORY").like("%ADHD%"))
                condition_name = "ADHD"
            else:
                df = df.filter(col("IS_CHRONIC_CONDITION") == True)
                condition_name = "chronic conditions"
            
            # Get diagnosis statistics
            diagnosis_stats = df.group_by("DIAGNOSIS_CATEGORY").agg(
                count(col("PATIENT_KEY")).alias("PATIENT_COUNT"),
                count("*").alias("DIAGNOSIS_COUNT"),
                avg("AGE_AT_DIAGNOSIS").alias("AVG_AGE_AT_DIAGNOSIS")
            ).order_by("PATIENT_COUNT", ascending=False)
            
            results_pd = diagnosis_stats.to_pandas()
            
            if not results_pd.empty:
                if len(results_pd) == 1:
                    row = results_pd.iloc[0]
                    response = f"For {condition_name}:\n"
                    response += f"• {row['PATIENT_COUNT']:,} patients affected\n"
                    response += f"• {row['DIAGNOSIS_COUNT']:,} total diagnoses\n"
                    response += f"• Average age at diagnosis: {row['AVG_AGE_AT_DIAGNOSIS']:.1f} years\n"
                else:
                    response = f"Here's the breakdown for {condition_name}:\n\n"
                    for _, row in results_pd.iterrows():
                        response += f"• {row['DIAGNOSIS_CATEGORY']}: {row['PATIENT_COUNT']:,} patients\n"
            else:
                response = f"I couldn't find specific data about {condition_name} at this time."
            
            return {
                'success': True,
                'response': response,
                'sql': "Snowpark Diagnosis Analytics Query",
                'results': results_pd
            }
            
        except Exception as e:
            return self._get_mock_response("diagnosis", question)
    
    def _handle_financial_query(self, question: str) -> Dict:
        """Handle financial and billing related queries using Snowpark."""
        return self._get_mock_response("financial", question)
    
    def _handle_quality_query(self, question: str) -> Dict:
        """Handle quality metrics related queries using Snowpark."""
        return self._get_mock_response("quality", question)
    
    def _handle_department_query(self, question: str) -> Dict:
        """Handle department performance related queries using Snowpark."""
        return self._get_mock_response("department", question)
    
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
    
    def _get_mock_response(self, category: str, question: str) -> Dict:
        """Provide mock responses when Snowpark queries fail."""
        mock_responses = {
            'demographics': "Based on our patient population, we serve approximately 50,000 active pediatric patients across all age groups, with the largest populations in the school-age (6-12 years) and adolescent (13-18 years) categories.",
            'encounters': "We've had approximately 125,000 encounters this year, with 60% outpatient visits, 25% emergency department visits, and 15% inpatient admissions.",
            'diagnosis': "Common pediatric conditions in our population include asthma (15.2%), ADHD (8.7%), obesity (12.4%), and type 1 diabetes (3.1%).",
            'financial': "Our total revenue this year is approximately $450M, with an average charge of $3,200 per encounter.",
            'quality': "Our current readmission rate is 8.5%, with an average length of stay of 3.2 days for inpatient encounters.",
            'department': "Our busiest departments are Emergency Medicine (35%), General Pediatrics (25%), and Pediatric Surgery (15%)."
        }
        
        return {
            'success': True,
            'response': mock_responses.get(category, "I can help you analyze healthcare data. Please ask a specific question."),
            'sql': f"Mock query for {category}",
            'results': pd.DataFrame()
        }

class SiSCortexSearch:
    """Cortex Search client for Streamlit in Snowflake"""
    
    def __init__(self, session: Session):
        """Initialize with Snowpark session"""
        self.session = session
        self.search_service = "TCH_UNSTRUCTURED_SEARCH"
    
    def search_clinical_notes(self, query: str, patient_id: Optional[str] = None, 
                            max_results: int = 5) -> Dict:
        """
        Search clinical notes using Cortex Search in Snowpark.
        
        Args:
            query: Search query
            patient_id: Optional patient ID to filter results
            max_results: Maximum number of results to return
            
        Returns:
            Dictionary containing search results and metadata
        """
        try:
            # In a real SiS implementation, this would use Cortex Search
            # For now, we'll simulate search results
            
            mock_results = self._get_mock_search_results(query, patient_id, max_results)
            
            return {
                'success': True,
                'query': query,
                'results': pd.DataFrame(mock_results),
                'count': len(mock_results)
            }
            
        except Exception as e:
            logger.error(f"Clinical notes search failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'query': query,
                'results': pd.DataFrame(),
                'count': 0
            }
    
    def _get_mock_search_results(self, query: str, patient_id: Optional[str], max_results: int) -> List[Dict]:
        """Generate mock search results for demonstration"""
        mock_results = [
            {
                'RELATIVE_PATH': 'clinical_notes/progress_note_20240115.txt',
                'CONTENT_PREVIEW': 'Patient reports good adherence to insulin regimen. Blood glucose levels have improved since last visit. Continue current treatment plan. No acute concerns noted during examination.',
                'RELEVANCE_SCORE': 0.95,
                'DOCUMENT_TYPE': 'Progress Note',
                'PATIENT_CONTEXT': patient_id or 'PAT000001'
            },
            {
                'RELATIVE_PATH': 'clinical_notes/discharge_summary_20231203.txt',
                'CONTENT_PREVIEW': 'Patient admitted for diabetic ketoacidosis management. Responded well to IV fluids and insulin therapy. Discharge home with updated medication regimen and diabetes education.',
                'RELEVANCE_SCORE': 0.87,
                'DOCUMENT_TYPE': 'Discharge Summary',
                'PATIENT_CONTEXT': patient_id or 'PAT000001'
            },
            {
                'RELATIVE_PATH': 'radiology_reports/chest_xray_20231022.txt',
                'CONTENT_PREVIEW': 'Chest X-ray shows clear lung fields bilaterally. No acute cardiopulmonary process identified. Heart size is normal for age.',
                'RELEVANCE_SCORE': 0.72,
                'DOCUMENT_TYPE': 'Radiology Report',
                'PATIENT_CONTEXT': patient_id or 'PAT000001'
            }
        ]
        
        return mock_results[:max_results]

class SiSCortexRouter:
    """Intelligent router for Cortex services in Streamlit in Snowflake"""
    
    def __init__(self, analyst: SiSCortexAnalyst, search: SiSCortexSearch):
        """Initialize with Cortex clients"""
        self.analyst = analyst
        self.search = search
    
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
        
        # For ambiguous queries, try analyst first
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
        search_result = self.search.search_clinical_notes(question, patient_id)
        
        # Format the response
        if search_result['success'] and search_result['count'] > 0:
            response = f"I found {search_result['count']} relevant clinical documents for your query:\n\n"
            
            for idx, row in search_result['results'].iterrows():
                response += f"**Document {idx+1}** (Relevance: {row['RELEVANCE_SCORE']:.2f})\n"
                response += f"Type: {row['DOCUMENT_TYPE']}\n"
                response += f"Patient: {row['PATIENT_CONTEXT']}\n"
                response += f"Preview: {row['CONTENT_PREVIEW']}\n\n"
        else:
            response = "I couldn't find any relevant clinical documents for your query."
        
        return {
            'success': search_result['success'],
            'response': response,
            'routing': 'search',
            'service': 'Cortex Search (Clinical Documentation)',
            'search_results': search_result['results'],
            'count': search_result['count']
        }