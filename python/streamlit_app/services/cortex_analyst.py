"""
Cortex Analyst Service for TCH Patient 360 PoC

Implements Snowflake Cortex Analyst functionality for natural language queries
against structured healthcare data. Provides intelligent data analysis and 
insights generation for clinical decision support.
"""

import pandas as pd
import streamlit as st
from typing import Dict, List, Optional, Any, Tuple
import logging
import json
from datetime import datetime
import _snowflake

from services.session_manager import SessionManager

logger = logging.getLogger(__name__)

class CortexAnalystService:
    """Cortex Analyst service for natural language data analysis"""
    
    def __init__(self):
        self.session_manager = SessionManager()
        self.analyst_apps = {}
        self._initialize_analyst_apps()
        # REST API configuration
        self.api_endpoint = "/api/v2/cortex/analyst/message"
        # Use same staged semantic model used elsewhere
        self.semantic_model_file = "@TCH_PATIENT_360_POC.AI_ML.SEMANTIC_MODEL_STAGE/semantic_model.yaml"
        
    def _initialize_analyst_apps(self):
        """Initialize Cortex Analyst applications for different data domains"""
        try:
            session = self.session_manager.get_session()
            
            # Initialize with the actual Cortex Analyst semantic model
            # Note: Using the semantic model created in sql/cortex/01_cortex_analyst_setup.sql
            self.semantic_model_name = 'TCH_Patient_360_Analytics'
            self.analyst_apps = {
                'patient_analytics': self.semantic_model_name,
                'clinical_metrics': self.semantic_model_name,
                'population_health': self.semantic_model_name,
                'quality_measures': self.semantic_model_name
            }
            
            logger.info("Cortex Analyst applications initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cortex Analyst apps: {e}")
            # Fallback to structured queries if Cortex Analyst is not available
            self.analyst_apps = {}
    
    def analyze_patient_data(self, question: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Analyze patient data using Cortex AI
        
        Args:
            question: Natural language question about patient data
            context: Optional context (patient_id, date_range, etc.)
            
        Returns:
            Dictionary containing AI-powered analysis results
        """
        try:
            session = self.session_manager.get_session()
            patient_id = context.get('patient_id') if context else None
            
            if not patient_id:
                return {'error': 'Patient ID required for analysis'}
            
            # Use Cortex Complete for intelligent patient analysis
            return self._generate_ai_patient_insights(patient_id, question)
                
        except Exception as e:
            logger.error(f"Patient data analysis failed: {e}")
            return {'error': str(e)}
    
    # Removed legacy SQL-based Cortex Analyst helpers that referenced a non-existent
    # SQL function. All Analyst interactions should use ask_analyst_rest() instead.

    def ask_analyst_rest(self, question: str, stream: bool = False) -> Dict[str, Any]:
        """Call Cortex Analyst via REST API (required in SiS). Returns full JSON response.

        Reference: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/rest-api
        """
        try:
            # Build REST payload
            payload: Dict[str, Any] = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": question or ""}
                        ]
                    }
                ],
                "semantic_model_file": self.semantic_model_file,
                "stream": bool(stream)
            }
            response = _snowflake.send_snow_api_request(
                "POST",
                self.api_endpoint,
                {"Content-Type": "application/json"},
                {},
                payload,
                None,
                45000
            )
            # response.content may already be a dict (SiS behavior); normalize
            content = getattr(response, 'content', response)
            if isinstance(content, str):
                try:
                    return json.loads(content)
                except Exception:
                    return {"error": "Failed to parse Analyst REST response", "raw": content}
            return content if isinstance(content, dict) else {"raw": content}
        except Exception as e:
            logger.error(f"Cortex Analyst REST call failed: {e}")
            return {"error": str(e)}

    @staticmethod
    def extract_sql_from_rest_response(analysis: Dict[str, Any]) -> Optional[str]:
        """Extract SQL from Cortex Analyst REST response structure."""
        try:
            # Some environments return a wrapper {status, content: "{...}"}
            if isinstance(analysis, dict) and 'content' in analysis and isinstance(analysis['content'], str):
                try:
                    import json as _json
                    analysis = _json.loads(analysis['content'])
                except Exception:
                    # If parsing fails, fall back to looking for SQL in raw string
                    return None

            message = analysis.get('message', {}) if isinstance(analysis, dict) else {}
            for item in message.get('content', []) or []:
                if isinstance(item, dict) and item.get('type') == 'sql':
                    stmt = item.get('statement') or item.get('sql')
                    if isinstance(stmt, str) and stmt.strip():
                        return stmt.strip()
                # Analyst sometimes returns a single JSON content object containing keys: sql, text, verified_query_used
                if isinstance(item, dict) and item.get('type') == 'json':
                    js = item.get('json')
                    if isinstance(js, dict):
                        stmt = js.get('sql') or js.get('SQL') or js.get('generated_sql')
                        if isinstance(stmt, str) and stmt.strip():
                            return stmt.strip()
            return None
        except Exception:
            return None
    
    def _generate_ai_patient_insights(self, patient_id: str, question: str) -> Dict[str, Any]:
        """Generate AI-powered patient insights using Cortex Complete - OPTIMIZED VERSION"""
        try:
            session = self.session_manager.get_session()
            
            # Get patient data context once
            patient_data = self._get_patient_context_data(patient_id)
            
            if not patient_data:
                return {'error': 'No patient data found'}
            
            # Generate all three analyses in a single SQL call for better performance
            return self._ai_analyze_all_insights_combined(patient_id, patient_data)
                
        except Exception as e:
            logger.error(f"AI patient analysis failed: {e}")
            return {'error': str(e)}
    
    def _ai_analyze_all_insights_combined(self, patient_id: str, patient_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        OPTIMIZED: Generate all three AI analyses in a single SQL call for better performance
        """
        try:
            session = self.session_manager.get_session()
            
            # Get core patient metrics first
            # Derive age on the fly from DOB when available
            age = patient_data.get('AGE') or (datetime.now().year - pd.to_datetime(patient_data.get('DATE_OF_BIRTH')).year if patient_data.get('DATE_OF_BIRTH') else 0)
            
            # Get encounter metrics
            encounter_metrics = self._get_encounter_metrics(patient_id)
            
            # Get medication metrics  
            med_metrics = self._get_medication_metrics(patient_id)
            
            # Get risk metrics
            risk_metrics = self._get_risk_metrics(patient_id)
            
            # Create comprehensive prompts for all three analyses
            encounter_prompt = f"""
            As a pediatric healthcare AI analyst, analyze this {age}-year-old patient's care utilization pattern:
            
            Encounter Summary:
            - Total Encounters: {encounter_metrics['TOTAL_ENCOUNTERS']}
            - Departments Visited: {encounter_metrics['DEPARTMENTS_VISITED']} ({encounter_metrics['DEPARTMENTS_LIST']})
            - Emergency Visits: {encounter_metrics['EMERGENCY_VISITS']}
            - Inpatient Stays: {encounter_metrics['INPATIENT_STAYS']}
            - Average Length of Stay: {encounter_metrics['AVG_LENGTH_OF_STAY']:.1f} days
            - Chief Complaints: {encounter_metrics['COMPLAINTS_LIST']}
            
            Provide insights on care utilization patterns, coordination opportunities, gaps in care, and pediatric-specific recommendations.
            """.replace("'", "''")
            
            medication_prompt = f"""
            As a pediatric pharmacist AI, analyze this {age}-year-old patient's medication profile:
            
            Medication Summary:
            - Active Medications: {med_metrics['ACTIVE_MEDICATIONS']}
            - Total Medications: {med_metrics['TOTAL_MEDICATIONS']}
            - Medication Classes: {med_metrics['MEDICATION_CLASSES']}
            - Current Medications: {med_metrics['MEDICATION_LIST']}
            - Average Treatment Duration: {med_metrics['AVG_DURATION_DAYS']} days
            
            Analyze medication appropriateness, interactions, adherence considerations, dosing recommendations, and family education needs.
            """.replace("'", "''")
            
            risk_prompt = f"""
            As a pediatric clinical AI, assess this {age}-year-old patient's risk profile:
            
            Risk Summary:
            - Chronic Conditions: {risk_metrics['chronic_conditions']}
            - Encounters Last Year: {risk_metrics['encounters_last_year']}
            - Active Medications: {risk_metrics['active_medications']}
            - Abnormal Labs: {risk_metrics['abnormal_labs']}
            
            Provide risk assessment, complexity factors, monitoring recommendations, and care coordination needs.
            """.replace("'", "''")
            
            # SINGLE SQL CALL with all three AI analyses
            combined_query = f"""
            SELECT 
                SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', '{encounter_prompt}') as encounter_analysis,
                SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', '{medication_prompt}') as medication_analysis,
                SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', '{risk_prompt}') as risk_analysis
            """
            
            result = session.sql(combined_query).to_pandas()
            
            if not result.empty:
                row = result.iloc[0]
                
                # Structure the combined results
                return {
                    'encounter_analysis': {
                        'metrics': encounter_metrics,
                        'insights': self._extract_insights(row['ENCOUNTER_ANALYSIS']),
                        'ai_generated_insights': row['ENCOUNTER_ANALYSIS']
                    },
                    'medication_analysis': {
                        'medication_summary': med_metrics,
                        'insights': self._extract_insights(row['MEDICATION_ANALYSIS']),
                        'ai_generated_insights': row['MEDICATION_ANALYSIS']
                    },
                    'risk_analysis': {
                        'overview': {
                            'risk_level': self._determine_risk_level(risk_metrics),
                            'risk_factors': risk_metrics['chronic_conditions'],
                            'insights': self._extract_insights(row['RISK_ANALYSIS'])
                        },
                        'ai_generated_insights': row['RISK_ANALYSIS'],
                        'patient_metrics': risk_metrics
                    }
                }
            else:
                return {'error': 'No AI analysis generated'}
                
        except Exception as e:
            logger.error(f"Combined AI analysis failed: {e}")
            return {'error': str(e)}
    
    def _get_encounter_metrics(self, patient_id: str) -> Dict[str, Any]:
        """Get encounter metrics for AI analysis"""
        try:
            session = self.session_manager.get_session()
            
            encounter_query = f"""
            SELECT 
                COUNT(*) as total_encounters,
                COUNT(DISTINCT DEPARTMENT_NAME) as departments_visited,
                COALESCE(AVG(LENGTH_OF_STAY_DAYS), 0) as avg_length_of_stay,
                COUNT(CASE WHEN ENCOUNTER_TYPE = 'Emergency' THEN 1 END) as emergency_visits,
                COUNT(CASE WHEN ENCOUNTER_TYPE = 'Inpatient' THEN 1 END) as inpatient_stays,
                COALESCE(LISTAGG(DISTINCT DEPARTMENT_NAME, ', '), 'None') as departments_list,
                COALESCE(LISTAGG(DISTINCT CHIEF_COMPLAINT, '; '), 'None') as complaints_list
            FROM CONFORMED.ENCOUNTER_SUMMARY
            WHERE PATIENT_ID = '{patient_id}'
            """
            
            result = session.sql(encounter_query).to_pandas()
            
            if not result.empty:
                row = result.iloc[0]
                return {
                    'TOTAL_ENCOUNTERS': int(row['TOTAL_ENCOUNTERS']),
                    'DEPARTMENTS_VISITED': int(row['DEPARTMENTS_VISITED']),
                    'AVG_LENGTH_OF_STAY': float(row['AVG_LENGTH_OF_STAY']),
                    'EMERGENCY_VISITS': int(row['EMERGENCY_VISITS']),
                    'INPATIENT_STAYS': int(row['INPATIENT_STAYS']),
                    'DEPARTMENTS_LIST': str(row['DEPARTMENTS_LIST']),
                    'COMPLAINTS_LIST': str(row['COMPLAINTS_LIST'])
                }
            else:
                return {
                    'TOTAL_ENCOUNTERS': 0, 'DEPARTMENTS_VISITED': 0, 'AVG_LENGTH_OF_STAY': 0.0,
                    'EMERGENCY_VISITS': 0, 'INPATIENT_STAYS': 0, 'DEPARTMENTS_LIST': 'None',
                    'COMPLAINTS_LIST': 'None'
                }
                
        except Exception as e:
            logger.error(f"Error getting encounter metrics: {e}")
            return {
                'TOTAL_ENCOUNTERS': 0, 'DEPARTMENTS_VISITED': 0, 'AVG_LENGTH_OF_STAY': 0.0,
                'EMERGENCY_VISITS': 0, 'INPATIENT_STAYS': 0, 'DEPARTMENTS_LIST': 'None',
                'COMPLAINTS_LIST': 'None'
            }
    
    def _get_medication_metrics(self, patient_id: str) -> Dict[str, Any]:
        """Get medication metrics for AI analysis"""
        try:
            session = self.session_manager.get_session()
            
            med_query = f"""
            SELECT 
                COUNT(*) as total_medications,
                COUNT(CASE WHEN END_DATE IS NULL OR END_DATE >= CURRENT_DATE() THEN 1 END) as active_medications,
                COUNT(DISTINCT MEDICATION_CLASS) as medication_classes,
                COALESCE(AVG(DATEDIFF('day', START_DATE, COALESCE(END_DATE, CURRENT_DATE()))), 0) as avg_duration_days,
                COALESCE(LISTAGG(DISTINCT MEDICATION_NAME, ', '), 'None') as medication_list
            FROM CONFORMED.MEDICATION_FACT
            WHERE PATIENT_ID = '{patient_id}'
            """
            
            result = session.sql(med_query).to_pandas()
            
            if not result.empty:
                row = result.iloc[0]
                return {
                    'TOTAL_MEDICATIONS': int(row['TOTAL_MEDICATIONS']),
                    'ACTIVE_MEDICATIONS': int(row['ACTIVE_MEDICATIONS']),
                    'MEDICATION_CLASSES': int(row['MEDICATION_CLASSES']),
                    'AVG_DURATION_DAYS': float(row['AVG_DURATION_DAYS']),
                    'MEDICATION_LIST': str(row['MEDICATION_LIST'])
                }
            else:
                return {
                    'TOTAL_MEDICATIONS': 0, 'ACTIVE_MEDICATIONS': 0, 'MEDICATION_CLASSES': 0,
                    'AVG_DURATION_DAYS': 0.0, 'MEDICATION_LIST': 'None'
                }
                
        except Exception as e:
            logger.error(f"Error getting medication metrics: {e}")
            return {
                'TOTAL_MEDICATIONS': 0, 'ACTIVE_MEDICATIONS': 0, 'MEDICATION_CLASSES': 0,
                'AVG_DURATION_DAYS': 0.0, 'MEDICATION_LIST': 'None'
            }
    
    def _get_risk_metrics(self, patient_id: str) -> Dict[str, Any]:
        """Get risk metrics for AI analysis"""
        try:
            session = self.session_manager.get_session()
            
            # Get basic metrics
            risk_query = f"""
            SELECT 
                COALESCE(CHRONIC_CONDITIONS_COUNT, 0) as chronic_conditions,
                COALESCE(RISK_CATEGORY, 'MODERATE_RISK') as risk_category
            FROM PRESENTATION.PATIENT_360 
            WHERE PATIENT_ID = '{patient_id}'
            """
            
            result = session.sql(risk_query).to_pandas()
            
            if not result.empty:
                row = result.iloc[0]
                chronic_conditions = int(row['CHRONIC_CONDITIONS'])
                risk_category = str(row['RISK_CATEGORY'])
            else:
                chronic_conditions = 0
                risk_category = 'MODERATE_RISK'
            
            # Get additional metrics
            enc_query = f"""
            SELECT COUNT(*) as encounters_last_year
            FROM CONFORMED.ENCOUNTER_SUMMARY 
            WHERE PATIENT_ID = '{patient_id}' 
            AND ENCOUNTER_DATE >= DATEADD('month', -12, CURRENT_DATE())
            """
            enc_result = session.sql(enc_query).to_pandas()
            encounters_last_year = int(enc_result.iloc[0]['ENCOUNTERS_LAST_YEAR']) if not enc_result.empty else 0
            
            med_query = f"""
            SELECT COUNT(*) as active_medications
            FROM CONFORMED.MEDICATION_FACT 
            WHERE PATIENT_ID = '{patient_id}' 
            AND (END_DATE IS NULL OR END_DATE >= CURRENT_DATE())
            """
            med_result = session.sql(med_query).to_pandas()
            active_medications = int(med_result.iloc[0]['ACTIVE_MEDICATIONS']) if not med_result.empty else 0
            
            return {
                'chronic_conditions': chronic_conditions,
                'encounters_last_year': encounters_last_year,
                'active_medications': active_medications,
                'abnormal_labs': 0,  # Placeholder
                'risk_category': risk_category
            }
                
        except Exception as e:
            logger.error(f"Error getting risk metrics: {e}")
            return {
                'chronic_conditions': 0, 'encounters_last_year': 0, 'active_medications': 0,
                'abnormal_labs': 0, 'risk_category': 'MODERATE_RISK'
            }
    
    def _determine_risk_level(self, risk_metrics: Dict[str, Any]) -> str:
        """Determine risk level from metrics"""
        risk_category = risk_metrics.get('risk_category', 'MODERATE_RISK')
        return {
            'HIGH_RISK': 'High',
            'MODERATE_RISK': 'Medium',
            'LOW_RISK': 'Low'
        }.get(risk_category, 'Medium')
    
    def _get_patient_context_data(self, patient_id: str) -> Dict[str, Any]:
        """Get comprehensive patient data for AI analysis"""
        try:
            session = self.session_manager.get_session()
            
            # First, try a simple query to see if patient exists
            simple_query = f"""
            SELECT 
                PATIENT_ID,
                CURRENT_AGE,
                GENDER,
                RISK_CATEGORY,
                TOTAL_ENCOUNTERS,
                CHRONIC_CONDITIONS_COUNT
            FROM CONFORMED.PATIENT_MASTER 
            WHERE PATIENT_ID = '{patient_id}'
            """
            
            result = session.sql(simple_query).to_pandas()
            
            if result.empty:
                return {}
            
            # If patient exists, get the comprehensive data
            patient_base = result.iloc[0].to_dict()
            
            # Add additional context data with simpler queries
            try:
                # Recent encounters
                enc_query = f"""
                SELECT COUNT(*) as encounters_last_year
                FROM CONFORMED.ENCOUNTER_SUMMARY 
                WHERE PATIENT_ID = '{patient_id}' 
                AND ENCOUNTER_DATE >= DATEADD('month', -12, CURRENT_DATE())
                """
                enc_result = session.sql(enc_query).to_pandas()
                patient_base['ENCOUNTERS_LAST_YEAR'] = enc_result.iloc[0]['ENCOUNTERS_LAST_YEAR'] if not enc_result.empty else 0
                
                # Active medications
                med_query = f"""
                SELECT COUNT(*) as active_medications
                FROM CONFORMED.MEDICATION_FACT 
                WHERE PATIENT_ID = '{patient_id}' 
                AND (END_DATE IS NULL OR END_DATE >= CURRENT_DATE())
                """
                med_result = session.sql(med_query).to_pandas()
                patient_base['ACTIVE_MEDICATIONS'] = med_result.iloc[0]['ACTIVE_MEDICATIONS'] if not med_result.empty else 0
                
                # Set default values for other fields
                patient_base['RECENT_DIAGNOSES'] = 'None available'
                patient_base['ABNORMAL_LABS'] = 0
                
            except Exception as e:
                print(f"DEBUG: Error getting additional context: {e}")
                # Set defaults if additional queries fail
                patient_base['ENCOUNTERS_LAST_YEAR'] = 0
                patient_base['ACTIVE_MEDICATIONS'] = 0
                patient_base['RECENT_DIAGNOSES'] = 'None available'
                patient_base['ABNORMAL_LABS'] = 0
            
            print(f"DEBUG: Final patient context data: {patient_base}")
            return patient_base
                
        except Exception as e:
            print(f"DEBUG: Exception in _get_patient_context_data: {e}")
            logger.error(f"Failed to get patient context data: {e}")
            return {}
    
    def _ai_analyze_risk_profile(self, patient_id: str, patient_data: Dict[str, Any]) -> Dict[str, Any]:
        """Use Cortex Complete to analyze patient risk profile"""
        try:
            session = self.session_manager.get_session()
            
            # Prepare patient context for AI analysis
            age = patient_data.get('AGE') or (datetime.now().year - pd.to_datetime(patient_data.get('DATE_OF_BIRTH')).year if patient_data.get('DATE_OF_BIRTH') else 0)
            gender = patient_data.get('GENDER', 'Unknown')
            risk_category = patient_data.get('RISK_CATEGORY', 'Unknown')
            chronic_conditions = patient_data.get('CHRONIC_CONDITIONS_COUNT', 0)
            encounters_last_year = patient_data.get('ENCOUNTERS_LAST_YEAR', 0)
            active_medications = patient_data.get('ACTIVE_MEDICATIONS', 0)
            recent_diagnoses = patient_data.get('RECENT_DIAGNOSES', 'None')
            abnormal_labs = patient_data.get('ABNORMAL_LABS', 0)
            
            # Map database risk category to display format for consistency
            base_risk_level = {
                'HIGH_RISK': 'High',
                'MODERATE_RISK': 'Medium',
                'LOW_RISK': 'Low'
            }.get(risk_category, 'Medium')
            
            # Create AI prompt for risk assessment
            ai_prompt = f"""
            As a pediatric healthcare AI analyst, analyze this patient's risk profile:
            
            Patient Demographics:
            - Age: {age} years
            - Gender: {gender}
            - Current Clinical Risk Category: {base_risk_level} (from {risk_category})
            
            Clinical Data:
            - Chronic Conditions: {chronic_conditions}
            - Healthcare Encounters (Last Year): {encounters_last_year}
            - Active Medications: {active_medications}
            - Recent Diagnoses: {recent_diagnoses}
            - Recent Abnormal Lab Results: {abnormal_labs}
            
            Based on the current clinical risk category of {base_risk_level} and the above clinical data, provide:
            1. Validation of the current {base_risk_level} risk level or suggest adjustments
            2. Key risk factors identified
            3. Specific pediatric care recommendations
            4. Care coordination suggestions
            
            Format response as structured insights focusing on actionable clinical recommendations.
            """
            
            # Use Cortex Complete for AI analysis
            # Escape single quotes in the prompt for SQL
            escaped_prompt = ai_prompt.replace("'", "''")
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mixtral-8x7b',
                '{escaped_prompt}'
            ) as ai_analysis
            """
            
            result = session.sql(cortex_query).to_pandas()
            
            if not result.empty:
                ai_response = result.iloc[0]['AI_ANALYSIS']
                
                # Parse the AI response and structure it
                return {
                    'overview': {
                        'risk_level': base_risk_level,  # Use consistent clinical risk category
                        'risk_factors': chronic_conditions + (1 if abnormal_labs > 3 else 0),
                        'insights': self._extract_insights(ai_response)
                    },
                    'ai_generated_insights': ai_response,
                    'patient_metrics': {
                        'chronic_conditions': chronic_conditions,
                        'encounters_last_year': encounters_last_year,
                        'active_medications': active_medications,
                        'abnormal_labs': abnormal_labs
                    }
                }
            else:
                # If no AI response, still return consistent risk level
                return {
                    'overview': {
                        'risk_level': base_risk_level,
                        'risk_factors': chronic_conditions,
                        'insights': ['Risk assessment based on clinical data']
                    },
                    'error': 'AI analysis temporarily unavailable, showing clinical risk category'
                }
                
        except Exception as e:
            logger.error(f"AI risk analysis failed: {e}")
            # Return consistent risk level even on error
            return {
                'overview': {
                    'risk_level': {
                        'HIGH_RISK': 'High',
                        'MODERATE_RISK': 'Medium', 
                        'LOW_RISK': 'Low'
                    }.get(patient_data.get('RISK_CATEGORY', 'MODERATE_RISK'), 'Medium'),
                    'risk_factors': patient_data.get('CHRONIC_CONDITIONS_COUNT', 0),
                    'insights': ['Risk assessment based on clinical data']
                },
                'error': str(e)
            }
    
    def _ai_analyze_encounters(self, patient_id: str, patient_data: Dict[str, Any]) -> Dict[str, Any]:
        """Use Cortex Complete to analyze encounter patterns"""
        try:
            session = self.session_manager.get_session()
            
            # Get detailed encounter data
            encounter_query = f"""
            SELECT 
                COUNT(*) as total_encounters,
                COUNT(DISTINCT DEPARTMENT_NAME) as departments_visited,
                AVG(LENGTH_OF_STAY_DAYS) as avg_length_of_stay,
                COUNT(CASE WHEN ENCOUNTER_TYPE = 'Emergency' THEN 1 END) as emergency_visits,
                COUNT(CASE WHEN ENCOUNTER_TYPE = 'Inpatient' THEN 1 END) as inpatient_stays,
                LISTAGG(DISTINCT DEPARTMENT_NAME, ', ') as departments_list,
                LISTAGG(DISTINCT CHIEF_COMPLAINT, '; ') as complaints_list
            FROM CONFORMED.ENCOUNTER_SUMMARY
            WHERE PATIENT_ID = '{patient_id}'
            """
            
            encounter_result = session.sql(encounter_query).to_pandas()
            
            if encounter_result.empty:
                return {'error': 'No encounter data found'}
            
            encounter_metrics = encounter_result.iloc[0].to_dict()
            
            # Create AI prompt for encounter analysis
            age = patient_data.get('AGE') or (datetime.now().year - pd.to_datetime(patient_data.get('DATE_OF_BIRTH')).year if patient_data.get('DATE_OF_BIRTH') else 0)
            ai_prompt = f"""
            As a pediatric healthcare AI analyst, analyze this {age}-year-old patient's care utilization pattern:
            
            Encounter Summary:
            - Total Encounters: {encounter_metrics['TOTAL_ENCOUNTERS']}
            - Departments Visited: {encounter_metrics['DEPARTMENTS_VISITED']} ({encounter_metrics['DEPARTMENTS_LIST']})
            - Emergency Visits: {encounter_metrics['EMERGENCY_VISITS']}
            - Inpatient Stays: {encounter_metrics['INPATIENT_STAYS']}
            - Average Length of Stay: {encounter_metrics['AVG_LENGTH_OF_STAY']:.1f} days
            - Chief Complaints: {encounter_metrics['COMPLAINTS_LIST']}
            
            Provide insights on:
            1. Care utilization patterns and appropriateness
            2. Care coordination opportunities
            3. Potential gaps in care
            4. Recommendations for optimal care delivery
            
            Focus on pediatric-specific considerations and family-centered care.
            """
            
            # Use Cortex Complete for AI analysis
            # Escape single quotes in the prompt for SQL
            escaped_prompt = ai_prompt.replace("'", "''")
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mixtral-8x7b',
                '{escaped_prompt}'
            ) as ai_analysis
            """
            
            result = session.sql(cortex_query).to_pandas()
            
            if not result.empty:
                ai_response = result.iloc[0]['AI_ANALYSIS']
                
                return {
                    'metrics': encounter_metrics,
                    'insights': self._extract_insights(ai_response),
                    'ai_generated_insights': ai_response
                }
            else:
                return {'error': 'No AI analysis generated'}
                
        except Exception as e:
            logger.error(f"AI encounter analysis failed: {e}")
            return {'error': str(e)}
    
    def _ai_analyze_medications(self, patient_id: str, patient_data: Dict[str, Any]) -> Dict[str, Any]:
        """Use Cortex Complete to analyze medication patterns"""
        try:
            session = self.session_manager.get_session()
            
            # Get detailed medication data
            med_query = f"""
            SELECT 
                LISTAGG(MEDICATION_NAME, ', ') as medication_list,
                LISTAGG(DISTINCT MEDICATION_CLASS, ', ') as medication_classes,
                COUNT(*) as total_medications,
                COUNT(CASE WHEN END_DATE IS NULL OR END_DATE >= CURRENT_DATE() THEN 1 END) as active_medications,
                AVG(CASE WHEN END_DATE IS NOT NULL THEN DATEDIFF('day', START_DATE, END_DATE) END) as avg_duration_days
            FROM CONFORMED.MEDICATION_FACT
            WHERE PATIENT_ID = '{patient_id}'
            """
            
            med_result = session.sql(med_query).to_pandas()
            
            if med_result.empty:
                return {'error': 'No medication data found'}
            
            med_metrics = med_result.iloc[0].to_dict()
            
            # Create AI prompt for medication analysis
            age = patient_data.get('AGE') or (datetime.now().year - pd.to_datetime(patient_data.get('DATE_OF_BIRTH')).year if patient_data.get('DATE_OF_BIRTH') else 0)
            ai_prompt = f"""
            As a pediatric pharmacist AI, analyze this {age}-year-old patient's medication profile:
            
            Medication Summary:
            - Active Medications: {med_metrics['ACTIVE_MEDICATIONS']}
            - Total Medications: {med_metrics['TOTAL_MEDICATIONS']}
            - Medication Classes: {med_metrics['MEDICATION_CLASSES']}
            - Current Medications: {med_metrics['MEDICATION_LIST']}
            - Average Treatment Duration: {med_metrics['AVG_DURATION_DAYS']} days
            
            Analyze for:
            1. Medication appropriateness for pediatric patient
            2. Potential drug interactions or duplications
            3. Adherence considerations for this age group
            4. Dosing and safety recommendations
            5. Family education needs
            
            Provide actionable clinical pharmacy recommendations.
            """
            
            # Use Cortex Complete for AI analysis
            # Escape single quotes in the prompt for SQL
            escaped_prompt = ai_prompt.replace("'", "''")
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mixtral-8x7b',
                '{escaped_prompt}'
            ) as ai_analysis
            """
            
            result = session.sql(cortex_query).to_pandas()
            
            if not result.empty:
                ai_response = result.iloc[0]['AI_ANALYSIS']
                
                return {
                    'medication_summary': med_metrics,
                    'insights': self._extract_insights(ai_response),
                    'ai_generated_insights': ai_response
                }
            else:
                return {'error': 'No AI analysis generated'}
                
        except Exception as e:
            logger.error(f"AI medication analysis failed: {e}")
            return {'error': str(e)}
    
    def _ai_analyze_overall_insights(self, patient_id: str, patient_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive AI insights about the patient"""
        try:
            session = self.session_manager.get_session()
            
            age = patient_data.get('CURRENT_AGE', 0)
            gender = patient_data.get('GENDER', 'Unknown')
            risk_category = patient_data.get('RISK_CATEGORY', 'Unknown')
            
            # Create comprehensive AI prompt
            ai_prompt = f"""
            As a pediatric AI clinical decision support system, provide comprehensive insights for this patient:
            
            Patient: {age}-year-old {gender}
            Risk Category: {risk_category}
            Total Encounters: {patient_data.get('TOTAL_ENCOUNTERS', 0)}
            Chronic Conditions: {patient_data.get('CHRONIC_CONDITIONS_COUNT', 0)}
            Recent Diagnoses: {patient_data.get('RECENT_DIAGNOSES', 'None')}
            
            Provide:
            1. Overall care summary and patient status
            2. Key areas of concern or opportunity
            3. Care coordination recommendations
            4. Family engagement strategies
            5. Next steps for optimal care
            
            Focus on actionable, evidence-based pediatric care recommendations.
            """
            
            # Use Cortex Complete for AI analysis
            # Escape single quotes in the prompt for SQL
            escaped_prompt = ai_prompt.replace("'", "''")
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mixtral-8x7b',
                '{escaped_prompt}'
            ) as ai_analysis
            """
            
            result = session.sql(cortex_query).to_pandas()
            
            if not result.empty:
                ai_response = result.iloc[0]['AI_ANALYSIS']
                
                return {
                    'comprehensive_insights': ai_response,
                    'patient_summary': patient_data
                }
            else:
                return {'error': 'No AI analysis generated'}
                
        except Exception as e:
            logger.error(f"AI comprehensive analysis failed: {e}")
            return {'error': str(e)}
    
    def _extract_risk_level(self, ai_response: str) -> str:
        """Extract risk level from AI response"""
        response_lower = ai_response.lower()
        if 'high risk' in response_lower or 'high-risk' in response_lower:
            return 'High'
        elif 'medium risk' in response_lower or 'moderate risk' in response_lower:
            return 'Medium'
        else:
            return 'Low'
    
    def _extract_insights(self, ai_response: str) -> List[str]:
        """Extract key insights from AI response"""
        # Simple extraction - in production, this could be more sophisticated
        lines = ai_response.split('\n')
        insights = []
        
        for line in lines:
            line = line.strip()
            if line and (line.startswith('•') or line.startswith('-') or line.startswith('*')):
                insights.append(line.lstrip('•-* '))
            elif line and any(keyword in line.lower() for keyword in ['recommend', 'suggest', 'consider', 'important']):
                insights.append(line)
        
        return insights[:5]  # Return top 5 insights