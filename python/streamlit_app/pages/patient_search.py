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
from services import cortex_search, session_manager
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
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_term = st.text_input(
            "Search by MRN, Name, or Phone",
            placeholder="Enter MRN (e.g., MRN12345678), Last Name, or Phone Number",
            help="Search for patients using their Medical Record Number, last name, or phone number"
        )
    
    with col2:
        st.write("")  # Add some spacing
        search_button = st.button("🔍 Search", type="primary")
    
    if search_button and search_term:
        with st.spinner("Searching patients..."):
            try:
                results = data_service.quick_patient_search(search_term.strip())
                st.session_state.search_results = results
                
                if not results.empty:
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
            gender = st.selectbox("Gender", ["All", "M", "F", "Other"])
            age_min = st.number_input("Min Age", min_value=0, max_value=21, value=0)
            age_max = st.number_input("Max Age", min_value=0, max_value=21, value=21)
        
        with col3:
            date_from = st.date_input("Last Visit From", value=None)
            date_to = st.date_input("Last Visit To", value=None)
            insurance_type = st.selectbox("Insurance", ["All", "Commercial", "Medicaid", "Medicare", "Self-Pay"])
        
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
                    
                    if not results.empty:
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
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_scope = st.multiselect(
            "Search Scope",
            ["Patient Demographics", "Clinical Notes", "Lab Results", "Procedures", "Medications", "Diagnoses"],
            default=["Patient Demographics", "Clinical Notes"],
            help="Select which data types to include in the search"
        )
    
    with col2:
        max_results = st.number_input("Max Results", min_value=10, max_value=500, value=50)
    
    if st.button("🚀 AI Search", type="primary") and search_query:
        with st.spinner("AI is searching across clinical data..."):
            try:
                # Use Cortex Search for semantic search
                results = cortex_search.semantic_patient_search(
                    query=search_query,
                    scope=search_scope,
                    max_results=max_results
                )
                
                st.session_state.search_results = results
                
                if not results.empty:
                    st.success(f"AI found {len(results)} relevant patient(s)")
                    
                    # Show search insights
                    with st.expander("🧠 AI Search Insights"):
                        insights = cortex_search.get_search_insights(search_query, results)
                        st.markdown(insights)
                else:
                    st.warning("No patients found matching your description. Try rephrasing your query.")
                    
            except Exception as e:
                logger.error(f"Semantic search error: {e}")
                st.error("AI search failed. Please try a different query or use traditional search.")

def _render_search_results():
    """Render search results with patient cards and navigation"""
    
    results = st.session_state.search_results
    
    if results.empty:
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
            st.session_state.cohort_data = results
            st.switch_page("pages/cohort_builder.py")
    
    # Sort results
    if sort_by == "Last Name":
        results = results.sort_values('LAST_NAME')
    elif sort_by == "Last Visit":
        results = results.sort_values('LAST_VISIT_DATE', ascending=False)
    elif sort_by == "Age":
        results = results.sort_values('AGE')
    elif sort_by == "Risk Level":
        risk_order = {'High': 3, 'Medium': 2, 'Low': 1}
        results['risk_sort'] = results['RISK_LEVEL'].map(risk_order)
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

def _on_patient_selected(patient_id: str):
    """Handle patient selection from search results"""
    st.session_state.selected_patient_id = patient_id
    st.switch_page("pages/patient_360.py")