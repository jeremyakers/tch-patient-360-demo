"""
Patient Search Page for TCH Patient 360 PoC

This page provides sophisticated patient search capabilities including:
- Quick patient lookup by MRN, name, or demographics
- Advanced search with multiple criteria
- AI-powered semantic search using Cortex Search
- Integration with clinical documents and structured data
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from services.data_service import DataService
from services import cortex_search, session_manager, cortex_analyst
from components import search_widgets, patient_cards
from utils import helpers, validators

logger = logging.getLogger(__name__)

# Initialize data service
data_service = DataService()

def render():
    """Entry point called by main.py"""
    render_patient_search()

def render_patient_search():
    """Main entry point for the patient search page"""
    
    st.title("🔍 Patient Search")
    st.markdown("Find patients using advanced search capabilities powered by Snowflake Cortex AI")
    
    # Initialize session state
    if 'search_results' not in st.session_state:
        st.session_state.search_results = None
    if 'selected_patient' not in st.session_state:
        st.session_state.selected_patient = None
    
    # Create search interface
    search_type = st.selectbox(
        "Search Method",
        ["Quick Search", "Advanced Search", "AI Semantic Search"],
        help="Choose your preferred search method"
    )
    
    if search_type == "Quick Search":
        _render_quick_search()
    elif search_type == "Advanced Search":
        _render_advanced_search()
    else:
        _render_semantic_search()
    
    # Display search results
    if st.session_state.search_results is not None:
        _render_search_results()

def _render_quick_search():
    """Render quick search interface for common lookups"""
    
    st.subheader("Quick Patient Lookup")
    
    # Submit-on-Enter form
    with st.form("quick_search_form", enter_to_submit=True, border=False):
        col1, col2 = st.columns([3, 1])
        with col1:
            search_term = st.text_input(
                "Search by MRN or Name",
                placeholder="Enter MRN (e.g., MRN12345678), Last Name, or First Name",
                help="Search by Medical Record Number, last name, or first name"
            )
        with col2:
            st.write("")  # spacing
            search_button = st.form_submit_button("🔍 Search", type="primary")

    if search_button and search_term:
        with st.spinner("Searching patients..."):
            try:
                results = data_service.quick_patient_search(search_term.strip())
                st.session_state.search_results = results
                
                if isinstance(results, pd.DataFrame) and not results.empty:
                    st.success(f"Found {len(results)} patient(s)")
                else:
                    st.warning("No patients found matching your search criteria")
                    
            except Exception as e:
                logger.error(f"Quick search error: {e}")
                st.error("Search failed. Please try again or contact support.")

def _render_advanced_search():
    """Render advanced search with multiple criteria"""
    
    st.subheader("Advanced Patient Search")
    
    with st.form("advanced_search_form"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            first_name = st.text_input("First Name")
            last_name = st.text_input("Last Name") 
            mrn = st.text_input("MRN")
        
        with col2:
            gender = st.selectbox("Gender", ["All", "Male", "Female", "Other"])
            age_min = st.number_input("Min Age", min_value=0, max_value=21, value=0)
            age_max = st.number_input("Max Age", min_value=0, max_value=21, value=21)
        
        with col3:
            date_from = st.date_input("Last Visit From", value=None)
            date_to = st.date_input("Last Visit To", value=None)
            # Load insurance options dynamically from data
            insurance_opts = ["All"] + data_service.get_insurance_options()
            insurance_type = st.selectbox("Insurance", insurance_opts)
        
        # Additional clinical criteria
        st.markdown("**Clinical Criteria** (Optional)")
        col4, col5 = st.columns(2)
        
        with col4:
            diagnosis = st.text_input("Diagnosis/Condition", help="Search by diagnosis or medical condition")
            department = st.multiselect(
                "Department/Unit",
                ["Emergency", "Cardiology", "Oncology", "Neurology", "Surgery", "ICU", "NICU", "General Pediatrics"]
            )
        
        with col5:
            risk_level = st.selectbox("Risk Level", ["All", "High", "Medium", "Low"])
            active_status = st.selectbox("Active Status", ["All", "Active", "Inactive"])
        
        submitted = st.form_submit_button("🔍 Search Patients", type="primary")
        
        if submitted:
            # Build search criteria
            criteria = {
                'first_name': first_name.strip() if first_name else None,
                'last_name': last_name.strip() if last_name else None,
                'mrn': mrn.strip() if mrn else None,
                'gender': gender if gender != "All" else None,
                'age_min': age_min,
                'age_max': age_max,
                'date_from': date_from,
                'date_to': date_to,
                'insurance_type': insurance_type if insurance_type != "All" else None,
                'diagnosis': diagnosis.strip() if diagnosis else None,
                'departments': department if department else None,
                'risk_level': risk_level if risk_level != "All" else None,
                'active_status': active_status if active_status != "All" else None
            }
            
            with st.spinner("Searching with advanced criteria..."):
                try:
                    results = data_service.advanced_patient_search(criteria)
                    st.session_state.search_results = results
                    
                    if isinstance(results, pd.DataFrame) and not results.empty:
                        st.success(f"Found {len(results)} patient(s) matching your criteria")
                    else:
                        st.warning("No patients found. Try broadening your search criteria.")
                        
                except Exception as e:
                    logger.error(f"Advanced search error: {e}")
                    st.error("Search failed. Please try again or contact support.")

def _render_semantic_search():
    """Render AI-powered semantic search interface"""
    
    st.subheader("🤖 AI Semantic Search")
    st.markdown("Use natural language to search across clinical documents and patient data")
    
    search_query = st.text_area(
        "Describe what you're looking for:",
        placeholder="Example: 'Find pediatric patients with asthma who have been to the ER in the last 6 months'",
        height=100,
        help="Use natural language to describe the patients you want to find. The AI will search across structured data and clinical documents."
    )
    
    max_results = st.number_input("Max Results", min_value=10, max_value=500, value=50)
    
    if st.button("🚀 AI Search", type="primary") and search_query:
        with st.spinner("AI is searching across clinical data..."):
            try:
                # Use Cortex Analyst to generate SQL returning MRNs (same guidance as Cohort Builder)
                guidance = (
                    "Return only medical record numbers (MRNs) for patients matching this search: "
                    + search_query
                    + " Your response MUST be pure SQL that returns a single column named MRN. "
                    + "Use presentation tables and prefer structured data: patient_360 (age/demographics), diagnosis_analytics (ICD-10), "
                    + "encounter_analytics (encounter_type/date/department), medication_analytics (is_active), lab_results_analytics (values/dates). "
                    + "Only use AI functions on clinical_documentation when the question explicitly asks to search notes. "
                    + "Do not include prose, JSON, or code fences—output only SQL."
                )
                analysis = cortex_analyst.ask_analyst_rest(guidance, stream=False)
                # Fallback parser mirrors Cohort Builder's tolerant extractor
                def _extract_sql_from_analyst_response_local(analysis_obj: Any) -> Optional[str]:
                    try:
                        if analysis_obj is None:
                            return None
                        if isinstance(analysis_obj, str):
                            text = analysis_obj.strip()
                            if text.upper().startswith("SELECT"):
                                return text
                            try:
                                import json as _json
                                analysis_obj = _json.loads(text)
                            except Exception:
                                return None
                        for key in ['sql','SQL','generated_sql','generatedSql','executableSql','sqlStatement','sql_code']:
                            val = analysis_obj.get(key) if isinstance(analysis_obj, dict) else None
                            if isinstance(val, str) and val.strip().upper().startswith('SELECT'):
                                return val.strip()
                        candidates = []
                        if isinstance(analysis_obj, dict):
                            for k in ['response','result','results','data','analysis','answer']:
                                v = analysis_obj.get(k)
                                if isinstance(v, dict):
                                    candidates.append(v)
                        for obj in candidates:
                            for key in ['sql','SQL','sql_code']:
                                v = obj.get(key)
                                if isinstance(v, str) and v.strip().upper().startswith('SELECT'):
                                    return v.strip()
                        for list_key in ['statements','queries','sqls']:
                            lst = analysis_obj.get(list_key) if isinstance(analysis_obj, dict) else None
                            if isinstance(lst, list):
                                for item in lst:
                                    if isinstance(item, str) and item.strip().upper().startswith('SELECT'):
                                        return item.strip()
                                    if isinstance(item, dict):
                                        for key in ['sql','SQL']:
                                            v = item.get(key)
                                            if isinstance(v, str) and v.strip().upper().startswith('SELECT'):
                                                return v.strip()
                        return None
                    except Exception:
                        return None

                sql_query = (
                    cortex_analyst.extract_sql_from_rest_response(analysis)
                    or _extract_sql_from_analyst_response_local(analysis)
                )
                if not sql_query or not str(sql_query).strip():
                    st.warning("AI could not generate a query. Try rephrasing your request.")
                    st.session_state.search_results = pd.DataFrame()
                    return

                session = session_manager.get_session()
                clean_sql = str(sql_query).strip().rstrip(';')
                try:
                    df_ids = session.sql(clean_sql).to_pandas()
                except Exception as exec_err:
                    st.error(f"AI search failed to execute the generated SQL: {exec_err}")
                    with st.expander("Analyst-generated SQL", expanded=True):
                        st.code(clean_sql, language="sql")
                    st.session_state.search_results = pd.DataFrame()
                    logger.error(f"Analyst SQL execution error: {exec_err}")
                    return
                mrns: list[str] = []
                if not df_ids.empty:
                    cols_upper = {c.upper(): c for c in df_ids.columns}
                    if 'MRN' in cols_upper:
                        mrns = [str(x) for x in df_ids[cols_upper['MRN']].dropna().unique().tolist()]
                    elif df_ids.shape[1] == 1:
                        col = df_ids.columns[0]
                        mrns = [str(x) for x in df_ids[col].dropna().unique().tolist()]

                if not mrns:
                    st.warning("No patients found matching your description.")
                    with st.expander("Analyst-generated SQL", expanded=False):
                        st.code(clean_sql, language="sql")
                    st.session_state.search_results = pd.DataFrame()
                    return

                in_list = ",".join(["'" + m.replace("'","''") + "'" for m in mrns])
                preview_sql = f"""
                SELECT 
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
                FROM CONFORMED.PATIENT_MASTER pm
                WHERE pm.MRN IN ({in_list})
                ORDER BY pm.LAST_NAME, pm.FIRST_NAME
                LIMIT {int(max_results)}
                """
                results_df = session.sql(preview_sql).to_pandas()
                st.session_state.search_results = results_df
                if not results_df.empty:
                    st.success(f"AI found {len(results_df)} relevant patient(s)")
                    with st.expander("Analyst-generated SQL", expanded=False):
                        st.code(clean_sql, language="sql")
                else:
                    st.warning("No patients found after applying MRNs to patient records.")
            except Exception as e:
                logger.error(f"Semantic search error: {e}")
                st.error(f"AI search failed: {e}")

def _render_search_results():
    """Render search results with patient cards and navigation"""
    
    results = st.session_state.search_results
    
    if not isinstance(results, pd.DataFrame) or results.empty:
        return
    
    st.divider()
    st.subheader(f"Search Results ({len(results)} patients)")
    
    # Results controls
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        sort_by = st.selectbox(
            "Sort by",
            ["Last Name", "Last Visit", "Age", "Risk Level", "MRN"],
            help="Choose how to sort the results"
        )
    
    with col2:
        results_per_page = st.selectbox("Per Page", [10, 25, 50, 100], index=1)
    
    with col3:
        if st.button("📊 Analyze Cohort"):
            # Build MRN list for the entire current result set (not just the current page)
            all_results = st.session_state.search_results if isinstance(st.session_state.search_results, pd.DataFrame) else results
            mrns = []
            try:
                if isinstance(all_results, pd.DataFrame) and not all_results.empty:
                    mrns = [str(x) for x in all_results['MRN'].dropna().unique().tolist()]
            except Exception:
                mrns = []

            # Prepare preview DataFrame in the shape expected by Cohort Builder
            try:
                preview_df = pd.DataFrame()
                if isinstance(all_results, pd.DataFrame) and not all_results.empty:
                    preview_df = pd.DataFrame({
                        'patient_id': all_results['PATIENT_ID'],
                        'mrn': all_results['MRN'],
                        'full_name': (all_results['FIRST_NAME'].astype(str) + ' ' + all_results['LAST_NAME'].astype(str)).str.strip(),
                        'current_age': all_results['AGE'],
                        'gender': all_results['GENDER'],
                        'risk_category': all_results['RISK_CATEGORY'],
                        'total_encounters': all_results['TOTAL_ENCOUNTERS'],
                        'last_encounter_date': all_results['LAST_ENCOUNTER_DATE'],
                    })
            except Exception:
                preview_df = pd.DataFrame()

            # Pass to Cohort Builder
            st.session_state['cohort_mrns'] = mrns
            st.session_state['cohort_identifier_is_patient_id'] = False
            st.session_state['cohort_preview_df'] = preview_df
            st.session_state.current_page = "cohort_builder"
            st.rerun()
    
    # Sort results using correct column names
    if sort_by == "Last Name":
        results = results.sort_values('LAST_NAME')
    elif sort_by == "Last Visit":
        results = results.sort_values('LAST_ENCOUNTER_DATE', ascending=False)
    elif sort_by == "Age":
        results = results.sort_values('AGE')
    elif sort_by == "Risk Level":
        risk_order = {'HIGH_RISK': 3, 'MODERATE_RISK': 2, 'LOW_RISK': 1}
        results['risk_sort'] = results['RISK_CATEGORY'].map(risk_order)
        results = results.sort_values('risk_sort', ascending=False)
        results = results.drop('risk_sort', axis=1)
    else:  # MRN
        results = results.sort_values('MRN')
    
    # Pagination
    total_pages = (len(results) - 1) // results_per_page + 1
    if total_pages > 1:
        page = st.selectbox("Page", range(1, total_pages + 1))
        start_idx = (page - 1) * results_per_page
        end_idx = start_idx + results_per_page
        page_results = results.iloc[start_idx:end_idx]
    else:
        page_results = results
    
    # Display patient cards
    for idx, patient in page_results.iterrows():
        patient_cards.render_patient_card(
            patient,
            key=f"patient_card_{patient['PATIENT_ID']}",
            on_select=_on_patient_selected
        )

def _on_patient_selected(patient_id: str, patient_data: pd.Series):
    """Handle patient selection from search results"""
    # Clear cached data if switching to a different patient
    if hasattr(st.session_state, 'selected_patient_id') and st.session_state.selected_patient_id != patient_id:
        # Clear any AI insights cache for previous patient
        cache_keys_to_remove = [key for key in st.session_state.keys() if key.startswith('ai_insights_')]
        for key in cache_keys_to_remove:
            del st.session_state[key]
        
        # Clear tab selection for previous patient
        tab_keys_to_remove = [key for key in st.session_state.keys() if key.startswith('selected_tab_')]
        for key in tab_keys_to_remove:
            del st.session_state[key]
    
    st.session_state.selected_patient_id = patient_id
    
    # Set patient data for the sidebar context directly from search results
    try:
        print(f"DEBUG: Patient data from search: {patient_data.to_dict()}")
        st.session_state.current_patient = {
            'full_name': f"{patient_data.get('FIRST_NAME', '')} {patient_data.get('LAST_NAME', '')}".strip(),
            'mrn': patient_data.get('MRN', 'Unknown'),
            'current_age': patient_data.get('CURRENT_AGE', patient_data.get('AGE', 'Unknown')),
            'gender': patient_data.get('GENDER', 'Unknown'),
            'patient_id': patient_id
        }
        print(f"DEBUG: Current patient set: {st.session_state.current_patient}")
    except Exception as e:
        print(f"Error setting patient context: {e}")
        import traceback
        print(f"DEBUG: Full traceback: {traceback.format_exc()}")
        # Set basic info if data processing fails
        st.session_state.current_patient = {
            'full_name': 'Selected Patient',
            'mrn': 'Unknown',
            'current_age': 'Unknown',
            'gender': 'Unknown',
            'patient_id': patient_id
        }
    
    st.session_state.current_page = "patient_360"
    st.rerun()