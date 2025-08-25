"""
Patient Cards Component for TCH Patient 360 PoC

Reusable patient information display components including patient cards,
patient lists, and patient selection interfaces.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any, Callable, Optional, List
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

def render_patient_card(patient: pd.Series, key: str, on_select: Callable[[str, pd.Series], None] = None) -> None:
    """
    Render an individual patient card with key information
    
    Args:
        patient: Patient data as pandas Series
        key: Unique key for the component
        on_select: Optional callback function when patient is selected
    """
    try:
        with st.container():
            # Create card layout
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
            
            with col1:
                # Patient name and basic info
                name = f"{patient.get('FIRST_NAME', 'Unknown')} {patient.get('LAST_NAME', 'Unknown')}"
                st.markdown(f"**{name}**")
                st.text(f"MRN: {patient.get('MRN', 'N/A')}")
                
                # Age and gender
                age = patient.get('AGE', 'Unknown')
                gender = patient.get('GENDER', 'Unknown')
                st.text(f"Age: {age} | Gender: {gender}")
                
            with col2:
                # Risk level with color coding - using correct column name
                risk_category = patient.get('RISK_CATEGORY', 'Unknown')
                # Map database values to display values
                risk_display = {
                    'HIGH_RISK': 'High',
                    'MODERATE_RISK': 'Medium', 
                    'LOW_RISK': 'Low'
                }.get(risk_category, 'Unknown')
                
                risk_color = {
                    'High': '🔴',
                    'Medium': '🟡', 
                    'Low': '🟢',
                    'Unknown': '⚪'
                }.get(risk_display, '⚪')
                
                st.markdown(f"**Risk Level**")
                st.markdown(f"{risk_color} {risk_display}")
                
                # Insurance type - using correct column name
                insurance = patient.get('PRIMARY_INSURANCE', 'Unknown')
                st.text(f"Insurance: {insurance}")
                
            with col3:
                # Last visit information - using correct column name
                last_encounter = patient.get('LAST_ENCOUNTER_DATE')
                if last_encounter and pd.notna(last_encounter):
                    if isinstance(last_encounter, str):
                        try:
                            last_visit_date = datetime.strptime(last_encounter, '%Y-%m-%d').date()
                        except:
                            last_visit_date = last_encounter
                    else:
                        last_visit_date = last_encounter
                    
                    st.markdown("**Last Visit**")
                    st.text(f"{last_visit_date}")
                    
                    # Total encounters
                    total_encounters = patient.get('TOTAL_ENCOUNTERS', 0)
                    st.text(f"Total Visits: {total_encounters}")
                else:
                    st.markdown("**Last Visit**")
                    st.text("No recent visits")
                
            with col4:
                # Action buttons
                if st.button("View", key=f"view_{key}", type="primary"):
                    if on_select:
                        on_select(patient.get('PATIENT_ID'), patient)
            
            # Divider between cards
            st.divider()
            
    except Exception as e:
        logger.error(f"Error rendering patient card: {e}")
        st.error(f"❌ Error rendering patient card: {str(e)}")
        st.error(f"🔍 Patient data: {patient}")
        import traceback
        st.error(f"📋 Full traceback: {traceback.format_exc()}")
        # Try to render a minimal fallback card
        st.markdown("**Basic Patient Info (Fallback)**")
        st.text(f"Patient ID: {patient.get('PATIENT_ID', 'Unknown')}")
        st.text(f"Raw patient data keys: {list(patient.keys()) if hasattr(patient, 'keys') else 'Not a dict'}")
        st.text(f"Patient data type: {type(patient)}")

def render_patient_list(patients: pd.DataFrame, on_select: Callable[[str, pd.Series], None] = None, 
                       pagination: bool = True, per_page: int = 10) -> None:
    """
    Render a list of patient cards with optional pagination
    
    Args:
        patients: DataFrame containing patient data
        on_select: Optional callback function when patient is selected
        pagination: Whether to enable pagination
        per_page: Number of patients per page
    """
    try:
        if patients.empty:
            st.info("No patients found matching your criteria.")
            return
        
        total_patients = len(patients)
        
        # Pagination controls
        if pagination and total_patients > per_page:
            total_pages = (total_patients - 1) // per_page + 1
            
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                page = st.selectbox(
                    f"Page (showing {per_page} of {total_patients} patients)",
                    range(1, total_pages + 1),
                    key="patient_list_pagination"
                )
            
            # Calculate page slice
            start_idx = (page - 1) * per_page
            end_idx = min(start_idx + per_page, total_patients)
            page_patients = patients.iloc[start_idx:end_idx]
            
            st.markdown(f"Showing patients {start_idx + 1}-{end_idx} of {total_patients}")
        else:
            page_patients = patients
        
        # Render patient cards
        for idx, patient in page_patients.iterrows():
            render_patient_card(
                patient, 
                key=f"patient_list_{idx}",
                on_select=on_select
            )
            
    except Exception as e:
        logger.error(f"Error rendering patient list: {e}")
        st.error("Error displaying patient list")

def render_patient_summary_card(patient_data: Dict[str, Any], key: str) -> None:
    """
    Render a comprehensive patient summary card with key metrics
    
    Args:
        patient_data: Dictionary containing patient overview data
        key: Unique key for the component
    """
    try:
        demographics = patient_data.get('demographics', {})
        
        # Main patient info header
        col1, col2 = st.columns([2, 1])
        
        with col1:
            name = f"{demographics.get('FIRST_NAME', 'Unknown')} {demographics.get('LAST_NAME', 'Unknown')}"
            st.subheader(name)
            
            info_col1, info_col2 = st.columns(2)
            with info_col1:
                st.text(f"MRN: {demographics.get('MRN', 'N/A')}")
                st.text(f"DOB: {demographics.get('DATE_OF_BIRTH', 'N/A')}")
                st.text(f"Gender: {demographics.get('GENDER', 'N/A')}")
            
            with info_col2:
                age = demographics.get('CURRENT_AGE') or demographics.get('AGE') or (
                    datetime.now().year - demographics.get('DATE_OF_BIRTH', datetime.now()).year
                    if demographics.get('DATE_OF_BIRTH') else 'Unknown'
                )
                st.text(f"Age: {age}")
                st.text(f"Insurance: {demographics.get('PRIMARY_INSURANCE', 'N/A')}")
                
                # Risk level with color - using correct column name
                risk_category = demographics.get('RISK_CATEGORY', 'Unknown')
                # Map database values to display values
                risk_display = {
                    'HIGH_RISK': 'High',
                    'MODERATE_RISK': 'Medium', 
                    'LOW_RISK': 'Low'
                }.get(risk_category, 'Unknown')
                risk_emoji = {'High': '🔴', 'Medium': '🟡', 'Low': '🟢'}.get(risk_display, '⚪')
                st.markdown(f"**Risk Level:** {risk_emoji} {risk_display}")
        
        with col2:
            # Quick metrics
            st.markdown("**Quick Metrics**")
            
            encounters = patient_data.get('recent_encounters', pd.DataFrame())
            medications = patient_data.get('current_medications', pd.DataFrame())
            diagnoses = patient_data.get('active_diagnoses', pd.DataFrame())
            
            st.metric("Recent Encounters", len(encounters))
            st.metric("Active Medications", len(medications))
            st.metric("Active Diagnoses", len(diagnoses))
        
        # Contact information removed (not stored in dataset)
        
        # Emergency contact section removed (not stored in dataset)
        
    except Exception as e:
        logger.error(f"Error rendering patient summary card: {e}")
        st.error("Error displaying patient summary")

def render_patient_selector(patients: pd.DataFrame, key: str, 
                          default_patient_id: str = None) -> Optional[str]:
    """
    Render a patient selector dropdown
    
    Args:
        patients: DataFrame containing patient options
        key: Unique key for the component
        default_patient_id: Optional default selection
        
    Returns:
        Selected patient ID or None
    """
    try:
        if patients.empty:
            st.warning("No patients available for selection")
            return None
        
        # Create display options
        patient_options = {}
        default_index = 0
        
        for idx, patient in patients.iterrows():
            display_name = f"{patient.get('FIRST_NAME', 'Unknown')} {patient.get('LAST_NAME', 'Unknown')} (MRN: {patient.get('MRN', 'N/A')})"
            patient_id = patient.get('PATIENT_ID')
            patient_options[display_name] = patient_id
            
            if default_patient_id and patient_id == default_patient_id:
                default_index = len(patient_options) - 1
        
        # Render selector
        selected_display = st.selectbox(
            "Select Patient",
            options=list(patient_options.keys()),
            index=default_index,
            key=key
        )
        
        return patient_options.get(selected_display)
        
    except Exception as e:
        logger.error(f"Error rendering patient selector: {e}")
        st.error("Error displaying patient selector")
        return None

def render_patient_comparison_cards(patients: List[Dict[str, Any]], key: str) -> None:
    """
    Render multiple patient cards for comparison
    
    Args:
        patients: List of patient data dictionaries
        key: Unique key for the component
    """
    try:
        if not patients:
            st.info("No patients selected for comparison")
            return
        
        # Create columns for each patient
        cols = st.columns(len(patients))
        
        for idx, patient_data in enumerate(patients):
            with cols[idx]:
                st.markdown("---")
                demographics = patient_data.get('demographics', {})
                
                # Patient header
                name = f"{demographics.get('FIRST_NAME', 'Unknown')} {demographics.get('LAST_NAME', 'Unknown')}"
                st.markdown(f"**{name}**")
                st.text(f"MRN: {demographics.get('MRN', 'N/A')}")
                
                # Key metrics
                encounters = len(patient_data.get('recent_encounters', []))
                medications = len(patient_data.get('current_medications', []))
                diagnoses = len(patient_data.get('active_diagnoses', []))
                
                st.metric("Encounters", encounters)
                st.metric("Medications", medications) 
                st.metric("Diagnoses", diagnoses)
                
                # Risk level - using correct column name
                risk_category = demographics.get('RISK_CATEGORY', 'Unknown')
                # Map database values to display values
                risk_display = {
                    'HIGH_RISK': 'High',
                    'MODERATE_RISK': 'Medium', 
                    'LOW_RISK': 'Low'
                }.get(risk_category, 'Unknown')
                risk_emoji = {'High': '🔴', 'Medium': '🟡', 'Low': '🟢'}.get(risk_display, '⚪')
                st.markdown(f"{risk_emoji} {risk_display}")
        
    except Exception as e:
        logger.error(f"Error rendering patient comparison cards: {e}")
        st.error("Error displaying patient comparison")