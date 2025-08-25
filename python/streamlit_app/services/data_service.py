"""
Data Service for TCH Patient 360 PoC

Core data access layer that handles all interactions with Snowflake.
Provides high-level data access methods for patient information, clinical data,
and analytics while leveraging Snowflake's performance optimizations.
"""

import pandas as pd
import streamlit as st
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date, timedelta
import logging

from services.session_manager import SessionManager
from utils.helpers import format_query_params, handle_database_errors
from utils.validators import validate_patient_id, validate_search_criteria

logger = logging.getLogger(__name__)

class DataService:
    """Central data service for Patient 360 application"""
    
    def __init__(self):
        self.session_manager = SessionManager()
        self.cache_timeout = 300  # 5 minutes
        
    def get_session(self):
        """Get active Snowflake session"""
        return self.session_manager.get_session()

    @st.cache_data(ttl=300)
    def get_insurance_options(_self) -> List[str]:
        """Return distinct insurance options from CONFORMED.PATIENT_MASTER."""
        try:
            session = _self.get_session()
            df = session.sql(
                """
                SELECT DISTINCT PRIMARY_INSURANCE
                FROM CONFORMED.PATIENT_MASTER
                WHERE PRIMARY_INSURANCE IS NOT NULL
                ORDER BY PRIMARY_INSURANCE
                """
            ).to_pandas()
            return [str(v) for v in df['PRIMARY_INSURANCE'].tolist()] if not df.empty else []
        except Exception as e:
            logger.warning(f"Failed to load insurance options: {e}")
            return []
    
    @st.cache_data(ttl=300)
    def quick_patient_search(_self, search_term: str) -> pd.DataFrame:
        """
        Quick patient search by MRN or name using only PATIENT_MASTER table

        Args:
            search_term: Search string (MRN or name)

        Returns:
            DataFrame containing matching patients
        """
        try:
            session = _self.get_session()
            
            # Escape search term to prevent SQL injection
            search_pattern = search_term.replace("'", "''").strip()
            
            # Determine search type and build appropriate query
            if search_term.upper().startswith('MRN') or search_term.isdigit():
                # MRN search: support both 'MRN######' and numeric-only inputs
                mrn_raw = search_pattern.strip()
                digits = mrn_raw.upper().replace('MRN', '').strip()
                mrn_with_prefix = f"MRN{digits}" if digits else mrn_raw
                conditions = [f"UPPER(MRN) = UPPER('{mrn_raw}')"]
                if digits:
                    conditions.append(f"MRN = '{digits}'")
                    conditions.append(f"UPPER(MRN) = UPPER('{mrn_with_prefix}')")
                where_sql = " OR ".join(conditions)
                query = f"""
                SELECT 
                    PATIENT_ID,
                    MRN,
                    FIRST_NAME,
                    LAST_NAME,
                    DATE_OF_BIRTH,
                    DATEDIFF('year', DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                    GENDER,
                    PRIMARY_INSURANCE,
                    RISK_CATEGORY,
                    LAST_ENCOUNTER_DATE,
                    TOTAL_ENCOUNTERS
                FROM CONFORMED.PATIENT_MASTER
                WHERE {where_sql}
                ORDER BY LAST_NAME, FIRST_NAME
                """
                
            else:
                # Name search - search first name, last name, or full name
                query = f"""
                SELECT 
                    PATIENT_ID,
                    MRN,
                    FIRST_NAME,
                    LAST_NAME,
                    DATE_OF_BIRTH,
                    DATEDIFF('year', DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                    GENDER,
                    PRIMARY_INSURANCE,
                    RISK_CATEGORY,
                    LAST_ENCOUNTER_DATE,
                    TOTAL_ENCOUNTERS
                FROM CONFORMED.PATIENT_MASTER
                WHERE UPPER(FIRST_NAME) LIKE UPPER('%{search_pattern}%') 
                   OR UPPER(LAST_NAME) LIKE UPPER('%{search_pattern}%')
                   OR UPPER(FULL_NAME) LIKE UPPER('%{search_pattern}%')
                ORDER BY LAST_NAME, FIRST_NAME
                LIMIT 100
                """
            
            # Execute query using Snowpark
            logger.info(f"Searching for: '{search_term}'")
            result = session.sql(query).to_pandas()
            logger.info(f"Found {len(result)} patients")
            return result
            
        except Exception as e:
            logger.error(f"Patient search failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=[
                'PATIENT_ID', 'MRN', 'FIRST_NAME', 'LAST_NAME', 'DATE_OF_BIRTH',
                'AGE', 'GENDER', 'PRIMARY_INSURANCE', 'RISK_CATEGORY', 
                'LAST_ENCOUNTER_DATE', 'TOTAL_ENCOUNTERS'
            ])
    
    @st.cache_data(ttl=300)
    def advanced_patient_search(_self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """
        Advanced patient search with multiple criteria using only PATIENT_MASTER table
        
        Args:
            criteria: Dictionary containing search criteria
            
        Returns:
            DataFrame containing matching patients
        """
        try:
            session = _self.get_session()

            # Decide joins needed
            need_join_dx = bool(criteria.get('diagnosis'))
            need_join_enc = bool(criteria.get('departments') or criteria.get('date_from') or criteria.get('date_to'))

            # Base FROM and optional JOINs
            from_clause = ["CONFORMED.PATIENT_MASTER pm"]
            if need_join_enc:
                from_clause.append("LEFT JOIN PRESENTATION.ENCOUNTER_ANALYTICS ea ON pm.PATIENT_ID = ea.PATIENT_ID")
            if need_join_dx:
                from_clause.append("LEFT JOIN PRESENTATION.DIAGNOSIS_ANALYTICS da ON pm.PATIENT_ID = da.PATIENT_ID")

            where_conditions: List[str] = ["1=1"]

            # Name filters
            if criteria.get('first_name'):
                escaped = criteria['first_name'].replace("'", "''")
                where_conditions.append(f"UPPER(pm.FIRST_NAME) LIKE UPPER('%{escaped}%')")
            if criteria.get('last_name'):
                escaped = criteria['last_name'].replace("'", "''")
                where_conditions.append(f"UPPER(pm.LAST_NAME) LIKE UPPER('%{escaped}%')")
            if criteria.get('mrn'):
                escaped = criteria['mrn'].replace("'", "''")
                where_conditions.append(f"pm.MRN = '{escaped}'")

            # Gender (support M/F and Male/Female variants)
            gender = criteria.get('gender')
            if gender:
                g = gender.strip().lower()
                if g in ('m', 'male'):
                    where_conditions.append("UPPER(pm.GENDER) IN ('M','MALE')")
                elif g in ('f', 'female'):
                    where_conditions.append("UPPER(pm.GENDER) IN ('F','FEMALE')")
                else:
                    where_conditions.append("UPPER(pm.GENDER) NOT IN ('M','MALE','F','FEMALE')")

            # Age range from DOB
            if criteria.get('age_min') is not None:
                where_conditions.append(f"DATEDIFF('year', pm.DATE_OF_BIRTH, CURRENT_DATE()) >= {int(criteria['age_min'])}")
            if criteria.get('age_max') is not None:
                where_conditions.append(f"DATEDIFF('year', pm.DATE_OF_BIRTH, CURRENT_DATE()) <= {int(criteria['age_max'])}")

            # Last encounter date window (use pm aggregate)
            if criteria.get('date_from'):
                where_conditions.append(f"pm.LAST_ENCOUNTER_DATE >= '{criteria['date_from']}'")
            if criteria.get('date_to'):
                where_conditions.append(f"pm.LAST_ENCOUNTER_DATE <= '{criteria['date_to']}'")

            # Insurance
            if criteria.get('insurance_type'):
                escaped = criteria['insurance_type'].replace("'", "''")
                where_conditions.append(f"pm.PRIMARY_INSURANCE = '{escaped}'")

            # Risk level mapping
            risk_level = criteria.get('risk_level')
            if risk_level:
                map_codes = {'high': 'HIGH_RISK', 'medium': 'MODERATE_RISK', 'low': 'LOW_RISK'}
                code = map_codes.get(str(risk_level).lower(), str(risk_level).upper())
                where_conditions.append(f"pm.RISK_CATEGORY = '{code}'")

            # Active status
            active_status = criteria.get('active_status')
            if active_status:
                if str(active_status).lower() == 'active':
                    where_conditions.append("pm.IS_CURRENT = TRUE")
                elif str(active_status).lower() == 'inactive':
                    where_conditions.append("pm.IS_CURRENT = FALSE")

            # Diagnosis ILIKE
            if need_join_dx:
                diag = criteria.get('diagnosis', '').replace("'", "''")
                where_conditions.append(
                    f"(da.diagnosis_description ILIKE '%{diag}%' OR da.diagnosis_code ILIKE '%{diag}%')"
                )

            # Departments
            departments = criteria.get('departments') or []
            if departments:
                dep_vals = ",".join(["'" + str(d).replace("'", "''") + "'" for d in departments])
                where_conditions.append(f"ea.department_name IN ({dep_vals})")

            query = f"""
            SELECT DISTINCT
                pm.PATIENT_ID,
                pm.MRN,
                pm.FIRST_NAME,
                pm.LAST_NAME,
                pm.DATE_OF_BIRTH,
                DATEDIFF('year', pm.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                pm.GENDER,
                pm.PRIMARY_INSURANCE,
                pm.RISK_CATEGORY,
                pm.LAST_ENCOUNTER_DATE,
                pm.TOTAL_ENCOUNTERS
            FROM {' '.join(from_clause)}
            WHERE {' AND '.join(where_conditions)}
            ORDER BY pm.LAST_NAME, pm.FIRST_NAME
            LIMIT 500
            """

            logger.info("Advanced search SQL built")
            result = session.sql(query).to_pandas()
            logger.info(f"Advanced search returned {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Advanced patient search failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return pd.DataFrame(columns=[
                'PATIENT_ID', 'MRN', 'FIRST_NAME', 'LAST_NAME', 'DATE_OF_BIRTH',
                'AGE', 'GENDER', 'PRIMARY_INSURANCE', 'RISK_CATEGORY', 
                'LAST_ENCOUNTER_DATE', 'TOTAL_ENCOUNTERS'
            ])
    
    @st.cache_data(ttl=300)
    def get_patient_overview(_self, patient_id: str) -> Dict[str, Any]:
        """
        Get comprehensive patient overview for Patient 360 view
        
        Args:
            patient_id: Unique patient identifier
            
        Returns:
            Dictionary containing patient overview data
        """
        try:
            session = _self.get_session()
            logger.info(f"Getting patient overview for: {patient_id}")
            
            # Patient demographics - using correct schema and inline parameters
            escaped_patient_id = patient_id.replace("'", "''")
            demographics_query = f"""
            SELECT *
            FROM CONFORMED.PATIENT_MASTER
            WHERE PATIENT_ID = '{escaped_patient_id}'
            """
            
            logger.info(f"Executing query: {demographics_query}")
            demographics = session.sql(demographics_query).to_pandas()
            logger.info(f"Query returned {len(demographics)} rows")
            
            if demographics.empty:
                logger.warning(f"No patient found with ID: {patient_id}")
                return {}
            
            patient_data = demographics.iloc[0].to_dict()
            logger.info(f"Patient data retrieved successfully for: {patient_id}")
            
            # Pull multi-source rollups from presentation view (Oracle ERP + Salesforce)
            try:
                pres_query = f"""
                SELECT 
                    total_lifetime_charges,
                    avg_cost_per_encounter,
                    high_cost_episodes,
                    outstanding_balance,
                    financial_value_category,
                    digital_adoption_level,
                    portal_logins_last_30_days,
                    engagement_score,
                    last_engagement_date
                FROM PRESENTATION.PATIENT_360
                WHERE patient_id = '{escaped_patient_id}'
                """
                pres_df = session.sql(pres_query).to_pandas()
                if not pres_df.empty:
                    pres = pres_df.iloc[0].to_dict()
                    # Store under dedicated keys to avoid clobbering demographics
                    patient_data_financial = {
                        'TOTAL_LIFETIME_CHARGES': pres.get('TOTAL_LIFETIME_CHARGES'),
                        'AVG_COST_PER_ENCOUNTER': pres.get('AVG_COST_PER_ENCOUNTER'),
                        'HIGH_COST_EPISODES': pres.get('HIGH_COST_EPISODES'),
                        'OUTSTANDING_BALANCE': pres.get('OUTSTANDING_BALANCE'),
                        'FINANCIAL_VALUE_CATEGORY': pres.get('FINANCIAL_VALUE_CATEGORY')
                    }
                    patient_data_engagement = {
                        'DIGITAL_ADOPTION_LEVEL': pres.get('DIGITAL_ADOPTION_LEVEL'),
                        'PORTAL_LOGINS_LAST_30_DAYS': pres.get('PORTAL_LOGINS_LAST_30_DAYS'),
                        'ENGAGEMENT_SCORE': pres.get('ENGAGEMENT_SCORE'),
                        'LAST_ENGAGEMENT_DATE': pres.get('LAST_ENGAGEMENT_DATE')
                    }
                else:
                    patient_data_financial = {}
                    patient_data_engagement = {}
            except Exception as e:
                logger.warning(f"Failed to load presentation rollups: {e}")
                patient_data_financial = {}
                patient_data_engagement = {}

            # Query clinical data from CONFORMED schema tables
            logger.info("Querying clinical data tables...")
            
            # Recent encounters (last 12 months)
            recent_encounters_query = f"""
            SELECT 
                ENCOUNTER_ID,
                ENCOUNTER_DATE,
                ENCOUNTER_TYPE,
                DEPARTMENT_NAME,
                CHIEF_COMPLAINT,
                LENGTH_OF_STAY_DAYS,
                ENCOUNTER_STATUS,
                TOTAL_CHARGES
            FROM CONFORMED.ENCOUNTER_SUMMARY
            WHERE PATIENT_ID = '{escaped_patient_id}'
            ORDER BY ENCOUNTER_DATE DESC
            LIMIT 20
            """
            
            # Active diagnoses (recent or chronic conditions)
            active_diagnoses_query = f"""
            SELECT 
                DIAGNOSIS_CODE,
                DIAGNOSIS_DESCRIPTION,
                DIAGNOSIS_TYPE,
                DIAGNOSIS_DATE,
                IS_CHRONIC_CONDITION
            FROM CONFORMED.DIAGNOSIS_FACT
            WHERE PATIENT_ID = '{escaped_patient_id}'
            AND (IS_CHRONIC_CONDITION = TRUE OR DIAGNOSIS_DATE >= DATEADD('year', -2, CURRENT_DATE()))
            ORDER BY DIAGNOSIS_DATE DESC
            LIMIT 10
            """
            
            # Current medications (no end date or future end date)
            current_medications_query = f"""
            SELECT 
                MEDICATION_NAME,
                MEDICATION_CLASS,
                THERAPEUTIC_CATEGORY,
                DOSAGE,
                FREQUENCY,
                START_DATE,
                END_DATE,
                ROUTE
            FROM CONFORMED.MEDICATION_FACT
            WHERE PATIENT_ID = '{escaped_patient_id}'
            AND (END_DATE IS NULL OR END_DATE >= CURRENT_DATE())
            ORDER BY START_DATE DESC
            LIMIT 10
            """
            
            # Recent vital signs (last 30 days)
            recent_vitals_query = f"""
            SELECT 
                MEASUREMENT_DATE,
                TEMPERATURE_FAHRENHEIT,
                HEART_RATE,
                RESPIRATORY_RATE,
                BLOOD_PRESSURE_SYSTOLIC,
                BLOOD_PRESSURE_DIASTOLIC,
                OXYGEN_SATURATION,
                WEIGHT_POUNDS,
                HEIGHT_INCHES
            FROM CONFORMED.VITAL_SIGNS_FACT
            WHERE PATIENT_ID = '{escaped_patient_id}'
            ORDER BY MEASUREMENT_DATE DESC
            LIMIT 20
            """
            
            # Recent lab results (last 90 days)
            recent_labs_query = f"""
            SELECT 
                TEST_NAME,
                TEST_VALUE_NUMERIC,
                TEST_VALUE_TEXT,
                REFERENCE_RANGE_TEXT,
                ABNORMAL_FLAG,
                RESULT_DATE
            FROM CONFORMED.LAB_RESULTS_FACT
            WHERE PATIENT_ID = '{escaped_patient_id}'
            ORDER BY RESULT_DATE DESC
            LIMIT 30
            """
            
            # Execute clinical data queries
            try:
                recent_encounters = session.sql(recent_encounters_query).to_pandas()
                logger.info(f"Retrieved {len(recent_encounters)} recent encounters")
            except Exception as e:
                logger.warning(f"Failed to get recent encounters: {e}")
                recent_encounters = pd.DataFrame()
                
            try:
                active_diagnoses = session.sql(active_diagnoses_query).to_pandas()
                logger.info(f"Retrieved {len(active_diagnoses)} active diagnoses")
            except Exception as e:
                logger.warning(f"Failed to get active diagnoses: {e}")
                active_diagnoses = pd.DataFrame()
                
            try:
                current_medications = session.sql(current_medications_query).to_pandas()
                logger.info(f"Retrieved {len(current_medications)} current medications")
            except Exception as e:
                logger.warning(f"Failed to get current medications: {e}")
                current_medications = pd.DataFrame()
                
            try:
                recent_vitals = session.sql(recent_vitals_query).to_pandas()
                logger.info(f"Retrieved {len(recent_vitals)} recent vitals")
            except Exception as e:
                logger.warning(f"Failed to get recent vitals: {e}")
                recent_vitals = pd.DataFrame()
                
            try:
                recent_labs = session.sql(recent_labs_query).to_pandas()
                logger.info(f"Retrieved {len(recent_labs)} recent lab results")
            except Exception as e:
                logger.warning(f"Failed to get recent lab results: {e}")
                recent_labs = pd.DataFrame()
            
            result = {
                'demographics': patient_data,
                'financial_summary': patient_data_financial,
                'engagement_summary': patient_data_engagement,
                'recent_encounters': recent_encounters,
                'active_diagnoses': active_diagnoses,
                'current_medications': current_medications,
                'recent_vitals': recent_vitals,
                'recent_labs': recent_labs
            }
            logger.info(f"Returning patient overview with keys: {list(result.keys())}")
            return result
            
        except Exception as e:
            logger.error(f"Get patient overview failed for {patient_id}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return {}
    
    @st.cache_data(ttl=600)
    def get_population_metrics(_self) -> Dict[str, Any]:
        """
        Get population-level metrics for dashboard
        
        Returns:
            Dictionary containing population metrics
        """
        try:
            session = _self.get_session()
            
            # Prefer presentation summary table for metrics
            query = """
            SELECT *
            FROM PRESENTATION.POPULATION_HEALTH_SUMMARY
            ORDER BY report_date DESC
            LIMIT 1
            """
            metrics = session.sql(query).to_pandas()
            if metrics.empty:
                return {}

            m: Dict[str, Any] = metrics.iloc[0].to_dict()

            # Derive percentages if not present in the table (backward compatible)
            try:
                total = float(m.get('TOTAL_ACTIVE_PATIENTS') or m.get('TOTAL_ACTIVE_PATIENTS'.lower()) or m.get('total_active_patients') or 0)
                if total > 0:
                    # Pediatric 1-12y = infants (1), toddlers (2-3), preschoolers (4-5), school_age (6-12)
                    ped = 0.0
                    for k in ['INFANTS','TODDLERS','PRESCHOOLERS','SCHOOL_AGE']:
                        ped += float(m.get(k) or m.get(k.lower()) or 0)
                    ped_pct = round(ped * 100.0 / total, 0)
                    m['PEDIATRIC_PERCENTAGE'] = int(ped_pct)

                    # Adolescent/Young Adult 13-21y = adolescents (13-18) + young_adults (19-21)
                    aya = 0.0
                    for k in ['ADOLESCENTS','YOUNG_ADULTS']:
                        aya += float(m.get(k) or m.get(k.lower()) or 0)
                    aya_pct = round(aya * 100.0 / total, 0)
                    m['ADOLESCENT_YOUNG_ADULT_PERCENTAGE'] = int(aya_pct)
                else:
                    m.setdefault('PEDIATRIC_PERCENTAGE', 0)
                    m.setdefault('ADOLESCENT_YOUNG_ADULT_PERCENTAGE', 0)
            except Exception:
                # On any derivation error, fall back to zeros
                m.setdefault('PEDIATRIC_PERCENTAGE', 0)
                m.setdefault('ADOLESCENT_YOUNG_ADULT_PERCENTAGE', 0)

            return m
            
        except Exception as e:
            logger.error(f"Get population metrics failed: {e}")
            return {}
    
    def get_encounter_details(self, encounter_id: str) -> Dict[str, Any]:
        """Get detailed information for a specific encounter"""
        try:
            session = self.get_session()
            
            query = """
            SELECT *
            FROM PRESENTATION.ENCOUNTER_SUMMARY
            WHERE ENCOUNTER_ID = %s
            """
            
            result = session.sql(query).to_pandas() if hasattr(session, 'sql') else pd.read_sql(query, session, params=(encounter_id,))
            
            return result.iloc[0].to_dict() if not result.empty else {}
            
        except Exception as e:
            logger.error(f"Get encounter details failed for {encounter_id}: {e}")
            return {}
    
    def get_clinical_timeline(self, patient_id: str, days_back: int = 365) -> pd.DataFrame:
        """Get chronological clinical timeline for a patient"""
        try:
            session = self.get_session()
            
            cutoff_date = datetime.now() - timedelta(days=days_back)
            escaped_patient_id = patient_id.replace("'", "''")
            
            query = f"""
            SELECT 
                'Encounter' as EVENT_TYPE,
                ENCOUNTER_DATE as EVENT_DATE,
                DEPARTMENT_NAME as LOCATION,
                CHIEF_COMPLAINT as DESCRIPTION,
                ENCOUNTER_ID as REFERENCE_ID
            FROM CONFORMED.ENCOUNTER_SUMMARY
            WHERE PATIENT_ID = '{escaped_patient_id}' AND ENCOUNTER_DATE >= '{cutoff_date.strftime('%Y-%m-%d')}'
            
            UNION ALL
            
            SELECT 
                'Lab Result' as EVENT_TYPE,
                result_date as EVENT_DATE,
                'Laboratory' as LOCATION,
                CONCAT(test_name, ': ', test_value_text, ' ', COALESCE(reference_range_text, '')) as DESCRIPTION,
                lab_result_id as REFERENCE_ID
            FROM CONFORMED.LAB_RESULTS_FACT
            WHERE PATIENT_ID = '{escaped_patient_id}' AND result_date >= '{cutoff_date.strftime('%Y-%m-%d')}'
            
            UNION ALL
            
            SELECT 
                'Medication' as EVENT_TYPE,
                START_DATE as EVENT_DATE,
                'Pharmacy' as LOCATION,
                CONCAT(MEDICATION_NAME, ' - ', DOSAGE, ' ', FREQUENCY) as DESCRIPTION,
                MEDICATION_ID as REFERENCE_ID
            FROM CONFORMED.MEDICATION_FACT
            WHERE PATIENT_ID = '{escaped_patient_id}' AND START_DATE >= '{cutoff_date.strftime('%Y-%m-%d')}'
            
            ORDER BY EVENT_DATE DESC
            """
            
            logger.info(f"Executing clinical timeline query for patient: {patient_id}")
            
            return session.sql(query).to_pandas()
            
        except Exception as e:
            logger.error(f"Get clinical timeline failed for {patient_id}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return pd.DataFrame()

    # -----------------------------
    # Cohort Builder Support
    # -----------------------------
    @st.cache_data(ttl=300)
    def get_cohort_preview(_self, criteria: Dict[str, Any], limit: int = 200) -> pd.DataFrame:
        """Return patient cohort preview based on structured criteria.

        Criteria keys:
          - age_min, age_max (ints)
          - genders (list of 'Male'/'Female')
          - diagnosis_keywords (string)
          - date_start, date_end (YYYY-MM-DD strings)
          - departments (list of department names)
        """
        try:
            session = _self.get_session()

            # Build WHERE conditions
            where = ["pm.is_current = TRUE"]

            # Age
            if criteria.get('age_min') is not None:
                where.append(f"DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) >= {int(criteria['age_min'])}")
            if criteria.get('age_max') is not None:
                where.append(f"DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= {int(criteria['age_max'])}")

            # Gender
            genders = criteria.get('genders') or []
            if genders:
                # Map to single char M/F in conformed/presentation if needed
                gender_map = {"Male": "Male", "Female": "Female"}
                vals = ",".join(["'" + gender_map.get(g, g).replace("'", "''") + "'" for g in genders])
                where.append(f"pm.gender IN ({vals})")

            # Diagnosis keywords
            diag_kw = (criteria.get('diagnosis_keywords') or '').strip()
            diag_cond = None
            if diag_kw:
                escaped = diag_kw.replace("'", "''")
                diag_cond = f"(da.diagnosis_description ILIKE '%{escaped}%' OR da.diagnosis_code ILIKE '%{escaped}%')"
                where.append(diag_cond)

            # Date range on encounters
            ds = criteria.get('date_start')
            de = criteria.get('date_end')
            if ds:
                where.append(f"ea.encounter_date >= '{str(ds)}'")
            if de:
                where.append(f"ea.encounter_date <= '{str(de)}'")

            # Departments
            departments = criteria.get('departments') or []
            if departments:
                vals = ",".join(["'" + str(d).replace("'", "''") + "'" for d in departments])
                where.append(f"ea.department_name IN ({vals})")

            where_sql = " AND ".join(where) if where else "1=1"

            query = f"""
            SELECT DISTINCT
                pm.patient_id,
                pm.mrn,
                pm.full_name,
                DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) AS age,
                pm.gender,
                pm.risk_category,
                pm.total_encounters,
                pm.last_encounter_date
            FROM PRESENTATION.PATIENT_360 pm
            LEFT JOIN PRESENTATION.ENCOUNTER_ANALYTICS ea ON pm.patient_id = ea.patient_id
            LEFT JOIN PRESENTATION.DIAGNOSIS_ANALYTICS da ON pm.patient_id = da.patient_id
            WHERE {where_sql}
            ORDER BY pm.full_name
            LIMIT {int(limit)}
            """

            return session.sql(query).to_pandas()
        except Exception as e:
            logger.error(f"Cohort preview failed: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def get_cohort_summary(_self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """Return summary metrics for a cohort (counts, top diagnoses, departments)."""
        try:
            session = _self.get_session()

            # Reuse same WHERE building logic (inline small helper)
            def build_where() -> str:
                where = ["pm.is_current = TRUE"]
                if criteria.get('age_min') is not None:
                    where.append(f"DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) >= {int(criteria['age_min'])}")
                if criteria.get('age_max') is not None:
                    where.append(f"DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= {int(criteria['age_max'])}")
                genders = criteria.get('genders') or []
                if genders:
                    vals = ",".join(["'" + str(g).replace("'", "''") + "'" for g in genders])
                    where.append(f"pm.gender IN ({vals})")
                diag_kw = (criteria.get('diagnosis_keywords') or '').strip()
                if diag_kw:
                    esc = diag_kw.replace("'", "''")
                    where.append(f"(da.diagnosis_description ILIKE '%{esc}%' OR da.diagnosis_code ILIKE '%{esc}%')")
                ds = criteria.get('date_start')
                de = criteria.get('date_end')
                if ds:
                    where.append(f"ea.encounter_date >= '{str(ds)}'")
                if de:
                    where.append(f"ea.encounter_date <= '{str(de)}'")
                departments = criteria.get('departments') or []
                if departments:
                    vals = ",".join(["'" + str(d).replace("'", "''") + "'" for d in departments])
                    where.append(f"ea.department_name IN ({vals})")
                return " AND ".join(where) if where else "1=1"

            where_sql = build_where()

            # Total patients
            total_q = f"""
            SELECT COUNT(DISTINCT pm.patient_id)
            FROM PRESENTATION.PATIENT_360 pm
            LEFT JOIN PRESENTATION.ENCOUNTER_ANALYTICS ea ON pm.patient_id = ea.patient_id
            LEFT JOIN PRESENTATION.DIAGNOSIS_ANALYTICS da ON pm.patient_id = da.patient_id
            WHERE {where_sql}
            """
            total = session.sql(total_q).to_pandas().iloc[0, 0]

            # Top diagnoses
            top_dx_q = f"""
            SELECT da.diagnosis_description, COUNT(*) AS cnt
            FROM PRESENTATION.PATIENT_360 pm
            LEFT JOIN PRESENTATION.DIAGNOSIS_ANALYTICS da ON pm.patient_id = da.patient_id
            LEFT JOIN PRESENTATION.ENCOUNTER_ANALYTICS ea ON pm.patient_id = ea.patient_id
            WHERE {where_sql}
            GROUP BY da.diagnosis_description
            ORDER BY cnt DESC
            LIMIT 10
            """
            top_dx = session.sql(top_dx_q).to_pandas()

            # Top departments
            top_dept_q = f"""
            SELECT ea.department_name, COUNT(*) AS cnt
            FROM PRESENTATION.PATIENT_360 pm
            LEFT JOIN PRESENTATION.ENCOUNTER_ANALYTICS ea ON pm.patient_id = ea.patient_id
            LEFT JOIN PRESENTATION.DIAGNOSIS_ANALYTICS da ON pm.patient_id = da.patient_id
            WHERE {where_sql}
            GROUP BY ea.department_name
            ORDER BY cnt DESC
            LIMIT 10
            """
            top_dept = session.sql(top_dept_q).to_pandas()

            return {
                'total_patients': int(total or 0),
                'top_diagnoses': top_dx,
                'top_departments': top_dept
            }
        except Exception as e:
            logger.error(f"Cohort summary failed: {e}")
            return {'total_patients': 0, 'top_diagnoses': pd.DataFrame(), 'top_departments': pd.DataFrame()}
    @st.cache_data(ttl=300)
    def get_financial_analytics(_self) -> pd.DataFrame:
        """Load cost analytics by condition from presentation view."""
        try:
            session = _self.get_session()
            query = """
            SELECT 
                diagnosis_description,
                patient_count,
                avg_cost_per_patient,
                total_cost,
                avg_cost_per_day,
                high_cost_cases,
                high_cost_percentage,
                public_payer_percentage,
                commercial_payer_percentage,
                avg_engagement_score
            FROM PRESENTATION.FINANCIAL_ANALYTICS
            ORDER BY total_cost DESC
            """
            return session.sql(query).to_pandas()
        except Exception as e:
            logger.error(f"Failed to load financial analytics: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def get_age_distribution(_self) -> pd.DataFrame:
        """Return ages for the active population for charting."""
        try:
            session = _self.get_session()
            query = """
            SELECT DATEDIFF('year', date_of_birth, CURRENT_DATE()) AS AGE
            FROM CONFORMED.PATIENT_MASTER
            WHERE is_current = TRUE
            """
            return session.sql(query).to_pandas()
        except Exception as e:
            logger.error(f"Failed to load age distribution: {e}")
            return pd.DataFrame()