"""
Clinical Timeline Component for TCH Patient 360 PoC

Interactive timeline visualization for clinical events including encounters,
lab results, medications, procedures, and other medical events.
Provides chronological view of patient's clinical history.
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

def render_timeline(data: pd.DataFrame, show_details: bool = True, key: str = None) -> None:
    """
    Render interactive clinical timeline
    
    Args:
        data: DataFrame with timeline events
        show_details: Whether to show detailed event information
        key: Unique key for the component
    """
    try:
        if data.empty:
            st.info("No timeline events available")
            return
        
        # Prepare data for visualization
        timeline_data = _prepare_timeline_data(data)
        
        # Create timeline visualization
        _render_timeline_chart(timeline_data, key)
        
        # Display detailed events if requested
        if show_details:
            st.divider()
            _render_event_details(timeline_data, key)
            
    except Exception as e:
        logger.error(f"Error rendering timeline: {e}")
        st.error("Error displaying timeline")

def _prepare_timeline_data(data: pd.DataFrame) -> pd.DataFrame:
    """Prepare and clean timeline data for visualization"""
    try:
        timeline_data = data.copy()
        
        # Ensure EVENT_DATE is datetime
        timeline_data['EVENT_DATE'] = pd.to_datetime(timeline_data['EVENT_DATE'])
        
        # Sort by date (most recent first)
        timeline_data = timeline_data.sort_values('EVENT_DATE', ascending=False)
        
        return timeline_data
        
    except Exception as e:
        logger.error(f"Error preparing timeline data: {e}")
        return data

def _render_timeline_chart(data: pd.DataFrame, key: str = None) -> None:
    """Render the main timeline chart visualization using native Streamlit"""
    try:
        st.markdown("**Clinical Timeline**")
        
        # Create event frequency chart by date
        data['DATE_ONLY'] = data['EVENT_DATE'].dt.date
        daily_events = data.groupby('DATE_ONLY').size()
        
        # Display as line chart showing event frequency over time
        st.line_chart(daily_events, height=400)
        
        # Show detailed timeline as interactive table
        st.markdown("**📅 Timeline Events**")
        
        # Prepare timeline data for display
        timeline_display = data[['EVENT_DATE', 'EVENT_TYPE', 'DESCRIPTION', 'LOCATION']].copy()
        timeline_display['EVENT_DATE'] = timeline_display['EVENT_DATE'].dt.strftime('%Y-%m-%d %H:%M')
        timeline_display = timeline_display.sort_values('EVENT_DATE', ascending=False)
        
        # Display as dataframe with formatting (SiS compatible)
        st.dataframe(
            timeline_display,
            use_container_width=True
        )
        
    except Exception as e:
        logger.error(f"Error rendering timeline chart: {e}")
        st.error("Error displaying timeline chart")

def _render_event_details(data: pd.DataFrame, key: str = None) -> None:
    """Render detailed event information in expandable sections"""
    try:
        st.subheader("📋 Event Details")
        
        # Group events by date for better organization
        data['DATE_ONLY'] = data['EVENT_DATE'].dt.date
        grouped_data = data.groupby('DATE_ONLY')
        
        for date, day_events in grouped_data:
            with st.expander(f"📅 {date} ({len(day_events)} events)", expanded=False):
                
                # Sort events by time for the day
                day_events_sorted = day_events.sort_values('EVENT_DATE')
                
                for idx, event in day_events_sorted.iterrows():
                    render_event_details(event, key=f"{key}_event_{idx}")
                    st.divider()
        
    except Exception as e:
        logger.error(f"Error rendering event details: {e}")
        st.error("Error displaying event details")

def render_event_details(event: pd.Series, key: str = None) -> None:
    """
    Render detailed information for a single event
    
    Args:
        event: Series containing event data
        key: Unique key for the component
    """
    try:
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            # Event type and time
            event_type = event.get('EVENT_TYPE', 'Unknown')
            
            # Extract time from EVENT_DATE if available
            event_time = 'Unknown'
            if 'EVENT_DATE' in event and pd.notna(event['EVENT_DATE']):
                try:
                    if isinstance(event['EVENT_DATE'], str):
                        event_time = pd.to_datetime(event['EVENT_DATE']).strftime('%H:%M')
                    else:
                        event_time = event['EVENT_DATE'].strftime('%H:%M')
                except:
                    event_time = 'Unknown'
            
            # Color-coded event type
            event_colors = {
                'Encounter': '🏥',
                'Lab Result': '🧪', 
                'Medication': '💊',
                'Procedure': '⚕️',
                'Diagnosis': '🩺',
                'Vital Signs': '🌡️'
            }
            
            icon = event_colors.get(event_type, '📋')
            st.markdown(f"**{icon} {event_type}**")
            st.text(f"Time: {event_time}")
        
        with col2:
            # Event description and details
            description = event.get('DESCRIPTION', 'No description available')
            st.markdown(f"**Description:** {description}")
            
            location = event.get('LOCATION', 'Unknown')
            if location and location != 'Unknown':
                st.text(f"Location: {location}")
        
        with col3:
            # Reference ID and actions
            reference_id = event.get('REFERENCE_ID')
            if reference_id:
                st.text(f"ID: {reference_id}")
                
                if st.button("📄 View Details", key=f"{key}_details"):
                    st.write("DEBUG: Button clicked!")  # Debug
                    st.session_state[f"show_modal_{key}"] = True
                    st.write(f"DEBUG: Modal state set to True for key: {key}")  # Debug
                    
                # Show modal if button was clicked
                modal_state = st.session_state.get(f"show_modal_{key}", False)
                st.write(f"DEBUG: Modal state for {key}: {modal_state}")  # Debug
                
                if modal_state:
                    try:
                        st.write("DEBUG: About to call _show_event_details_modal")  # Debug
                        _show_event_details_modal(event, key)
                        st.write("DEBUG: _show_event_details_modal completed")  # Debug
                    except Exception as e:
                        st.error(f"Modal error: {e}")
                        import traceback
                        st.code(traceback.format_exc())
                        st.write(f"DEBUG: Exception type: {type(e)}")  # Debug
                        st.write(f"DEBUG: Exception args: {e.args}")  # Debug
        
    except Exception as e:
        logger.error(f"Error rendering event details: {e}")
        st.error("Error displaying event information")

def _show_event_details_modal(event: pd.Series, key: str) -> None:
    """Show detailed event information with real data from related tables"""
    try:
        # Use container instead of expander to avoid nesting
        with st.container():
            
            # Close button at the top
            if st.button("❌ Close", key=f"{key}_close_modal"):
                st.session_state[f"show_modal_{key}"] = False
                st.rerun()
            
            st.divider()
            
            # Get event details
            event_type = event.get('EVENT_TYPE', 'Unknown')
            reference_id = event.get('REFERENCE_ID', '')
            
            # Create detailed view based on event type
            if event_type == 'Lab Result' and reference_id:
                _render_lab_result_details(reference_id, event)
            elif event_type == 'Medication' and reference_id:
                _render_medication_details(reference_id, event)
            elif event_type == 'Encounter' and reference_id:
                _render_encounter_details(reference_id, event)
            else:
                st.info("Additional details not available for this event type")
        
    except Exception as e:
        logger.error(f"Error showing event details modal: {e}")
        st.error("Error displaying detailed event information")
        import traceback
        st.code(traceback.format_exc())

def _render_lab_result_details(lab_result_id: str, event: pd.Series) -> None:
    """Render detailed lab result information"""
    try:
        from services.data_service import DataService
        data_service = DataService()
        
        # Query for detailed lab result
        session = data_service.session_manager.get_session()
        
        lab_query = f"""
        SELECT 
            test_name,
            test_value_text,
            reference_range_text,
            abnormal_flag,
            result_date,
            test_category,
            units,
            COALESCE(comments, 'No comments') as comments
        FROM CONFORMED.LAB_RESULTS_FACT
        WHERE lab_result_id = '{lab_result_id.replace("'", "''")}'
        """
        
        lab_data = session.sql(lab_query).to_pandas()
        
        if not lab_data.empty:
            lab_result = lab_data.iloc[0]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🧪 Lab Test Details")
                st.write(f"**Test Name:** {lab_result.get('TEST_NAME', 'Unknown')}")
                st.write(f"**Result:** {lab_result.get('TEST_VALUE_TEXT', 'N/A')}")
                st.write(f"**Reference Range:** {lab_result.get('REFERENCE_RANGE_TEXT', 'N/A')}")
                st.write(f"**Units:** {lab_result.get('UNITS', 'N/A')}")
                
                # Show abnormal flag with color coding
                abnormal_flag = lab_result.get('ABNORMAL_FLAG', 'Normal')
                if abnormal_flag and abnormal_flag != 'Normal':
                    if abnormal_flag in ['High', 'H', 'Critical High']:
                        st.error(f"🔴 **Status:** {abnormal_flag}")
                    elif abnormal_flag in ['Low', 'L', 'Critical Low']:
                        st.warning(f"🟡 **Status:** {abnormal_flag}")
                    else:
                        st.info(f"**Status:** {abnormal_flag}")
                else:
                    st.success("✅ **Status:** Normal")
            
            with col2:
                st.markdown("### 📊 Additional Information")
                st.write(f"**Test Category:** {lab_result.get('TEST_CATEGORY', 'N/A')}")
                st.write(f"**Result Date:** {lab_result.get('RESULT_DATE', 'N/A')}")
                
                comments = lab_result.get('COMMENTS', 'No comments')
                if comments and comments != 'No comments':
                    st.markdown("**Comments:**")
                    st.text_area("", value=comments, height=100, disabled=True, key=f"lab_comments_{lab_result_id}")
        else:
            st.warning("Detailed lab result information not found")
            
    except Exception as e:
        logger.error(f"Error loading lab result details: {e}")
        st.error("Unable to load detailed lab result information")

def _render_medication_details(medication_reference: str, event: pd.Series) -> None:
    """Render detailed medication information"""
    try:
        from services.data_service import DataService
        data_service = DataService()
        
        # For medications, extract medication name from description
        description = event.get('DESCRIPTION', '')
        medication_name = description.split(' - ')[0] if ' - ' in description else description.split(':')[0]
        
        session = data_service.session_manager.get_session()
        
        # Query for medication details
        med_query = f"""
        SELECT 
            medication_name,
            dosage,
            frequency,
            route,
            start_date,
            end_date,
            prescribing_provider,
            COALESCE(medication_class, 'Unknown') as medication_class,
            COALESCE(therapeutic_category, 'Unknown') as therapeutic_category,
            COALESCE(instructions, 'No special instructions') as instructions
        FROM CONFORMED.MEDICATION_SUMMARY
        WHERE medication_name ILIKE '%{medication_name.replace("'", "''").strip()}%'
        ORDER BY start_date DESC
        LIMIT 1
        """
        
        med_data = session.sql(med_query).to_pandas()
        
        if not med_data.empty:
            medication = med_data.iloc[0]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 💊 Medication Details")
                st.write(f"**Medication:** {medication.get('MEDICATION_NAME', 'Unknown')}")
                st.write(f"**Dosage:** {medication.get('DOSAGE', 'N/A')}")
                st.write(f"**Frequency:** {medication.get('FREQUENCY', 'N/A')}")
                st.write(f"**Route:** {medication.get('ROUTE', 'N/A')}")
                st.write(f"**Prescribing Provider:** {medication.get('PRESCRIBING_PROVIDER', 'N/A')}")
            
            with col2:
                st.markdown("### 📋 Treatment Information")
                st.write(f"**Class:** {medication.get('MEDICATION_CLASS', 'Unknown')}")
                st.write(f"**Category:** {medication.get('THERAPEUTIC_CATEGORY', 'Unknown')}")
                st.write(f"**Start Date:** {medication.get('START_DATE', 'N/A')}")
                
                end_date = medication.get('END_DATE')
                if pd.isna(end_date) or end_date is None:
                    st.success("✅ **Status:** Active")
                else:
                    st.info(f"**End Date:** {end_date}")
                
                instructions = medication.get('INSTRUCTIONS', 'No special instructions')
                if instructions and instructions != 'No special instructions':
                    st.markdown("**Instructions:**")
                    st.text_area("", value=instructions, height=80, disabled=True, key=f"med_instructions_{medication_reference}")
        else:
            st.warning("Detailed medication information not found")
            
    except Exception as e:
        logger.error(f"Error loading medication details: {e}")
        st.error("Unable to load detailed medication information")

def _render_encounter_details(encounter_id: str, event: pd.Series) -> None:
    """Render detailed encounter information"""
    try:
        from services.data_service import DataService
        data_service = DataService()
        
        session = data_service.session_manager.get_session()
        
        # Query for encounter details
        encounter_query = f"""
        SELECT 
            encounter_type,
            encounter_date,
            department_name,
            attending_provider,
            chief_complaint,
            length_of_stay_days,
            encounter_status,
            discharge_datetime,
            service_line,
            encounter_category
        FROM CONFORMED.ENCOUNTER_SUMMARY
        WHERE encounter_id = '{encounter_id.replace("'", "''")}'
        """
        
        encounter_data = session.sql(encounter_query).to_pandas()
        
        if not encounter_data.empty:
            encounter = encounter_data.iloc[0]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🏥 Encounter Details")
                st.write(f"**Type:** {encounter.get('ENCOUNTER_TYPE', 'Unknown')}")
                st.write(f"**Date:** {encounter.get('ENCOUNTER_DATE', 'N/A')}")
                st.write(f"**Department:** {encounter.get('DEPARTMENT_NAME', 'N/A')}")
                st.write(f"**Provider:** {encounter.get('ATTENDING_PROVIDER', 'N/A')}")
                st.write(f"**Chief Complaint:** {encounter.get('CHIEF_COMPLAINT', 'N/A')}")
            
            with col2:
                st.markdown("### 📊 Encounter Information")
                st.write(f"**Length of Stay:** {encounter.get('LENGTH_OF_STAY_DAYS', 'N/A')} days")
                st.write(f"**Status:** {encounter.get('ENCOUNTER_STATUS', 'N/A')}")
                st.write(f"**Discharge:** {encounter.get('DISCHARGE_DATETIME', 'N/A')}")
                st.write(f"**Service Line:** {encounter.get('SERVICE_LINE', 'N/A')}")
                st.write(f"**Category:** {encounter.get('ENCOUNTER_CATEGORY', 'N/A')}")
            
            # Query for related diagnoses
            st.markdown("### 🩺 Related Diagnoses")
            diag_query = f"""
            SELECT 
                diagnosis_code,
                diagnosis_description,
                diagnosis_type
            FROM CONFORMED.DIAGNOSIS_SUMMARY
            WHERE encounter_id = '{encounter_id.replace("'", "''")}'
            ORDER BY diagnosis_type, diagnosis_code
            LIMIT 10
            """
            
            diag_data = session.sql(diag_query).to_pandas()
            
            if not diag_data.empty:
                for idx, diagnosis in diag_data.iterrows():
                    diag_type = diagnosis.get('DIAGNOSIS_TYPE', 'Unknown')
                    diag_code = diagnosis.get('DIAGNOSIS_CODE', 'N/A')
                    diag_desc = diagnosis.get('DIAGNOSIS_DESCRIPTION', 'Unknown')
                    
                    if diag_type == 'Primary':
                        st.success(f"🔴 **Primary:** {diag_code} - {diag_desc}")
                    else:
                        st.info(f"🔵 **{diag_type}:** {diag_code} - {diag_desc}")
            else:
                st.info("No diagnoses found for this encounter")
        else:
            st.warning("Detailed encounter information not found")
            
    except Exception as e:
        logger.error(f"Error loading encounter details: {e}")
        st.error("Unable to load detailed encounter information")

def render_timeline_summary(data: pd.DataFrame, key: str = None) -> None:
    """
    Render timeline summary statistics
    
    Args:
        data: DataFrame with timeline events
        key: Unique key for the component
    """
    try:
        if data.empty:
            st.info("No timeline data available for summary")
            return
        
        st.subheader("📊 Timeline Summary")
        
        # Calculate summary statistics
        total_events = len(data)
        event_types = data['EVENT_TYPE'].value_counts()
        date_range = data['EVENT_DATE'].agg(['min', 'max'])
        
        # Display summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Events", total_events)
        
        with col2:
            if not date_range.isna().any():
                days_span = (date_range['max'] - date_range['min']).days
                st.metric("Timeline Span", f"{days_span} days")
            else:
                st.metric("Timeline Span", "N/A")
        
        with col3:
            st.metric("Event Types", len(event_types))
        
        with col4:
            if total_events > 0:
                avg_per_month = total_events / max(1, (date_range['max'] - date_range['min']).days / 30)
                st.metric("Avg Events/Month", f"{avg_per_month:.1f}")
            else:
                st.metric("Avg Events/Month", "0")
        
        # Event type breakdown
        st.markdown("### Event Type Distribution")
        
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            # Bar chart of event counts
            st.markdown("**Events by Type**")
            st.bar_chart(event_types, height=300)
        
        with col_chart2:
            # Event distribution as bar chart (alternative to pie chart)
            st.markdown("**Event Distribution**")
            st.bar_chart(event_types, height=300)
            st.caption("📊 Showing as bar chart (pie chart alternative)")
            
            # Also show percentage breakdown
            with st.expander("📊 Detailed Breakdown"):
                event_df = pd.DataFrame({
                    'Event Type': event_types.index,
                    'Count': event_types.values,
                    'Percentage': (event_types.values / event_types.sum() * 100).round(1)
                })
                st.dataframe(event_df, use_container_width=True)
        
    except Exception as e:
        logger.error(f"Error rendering timeline summary: {e}")
        st.error("Error displaying timeline summary")

def render_timeline_filters(data: pd.DataFrame, key: str = None) -> Dict[str, Any]:
    """
    Render timeline filter controls
    
    Args:
        data: DataFrame with timeline events
        key: Unique key for the component
        
    Returns:
        Dictionary of selected filter values
    """
    try:
        filters = {}
        
        if data.empty:
            return filters
        
        st.markdown("### 🔧 Timeline Filters")
        
        with st.expander("Filter Options", expanded=False):
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            
            with filter_col1:
                # Event type filter
                event_types = data['EVENT_TYPE'].unique()
                selected_types = st.multiselect(
                    "Event Types",
                    options=sorted(event_types),
                    default=sorted(event_types),
                    key=f"{key}_type_filter"
                )
                filters['event_types'] = selected_types
            
            with filter_col2:
                # Date range filter
                min_date = data['EVENT_DATE'].min().date()
                max_date = data['EVENT_DATE'].max().date()
                
                date_range = st.date_input(
                    "Date Range",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                    key=f"{key}_date_filter"
                )
                
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    filters['date_range'] = date_range
                else:
                    filters['date_range'] = (min_date, max_date)
            
            with filter_col3:
                # Location filter
                locations = data['LOCATION'].dropna().unique()
                if len(locations) > 0:
                    selected_locations = st.multiselect(
                        "Locations",
                        options=sorted(locations),
                        key=f"{key}_location_filter"
                    )
                    if selected_locations:
                        filters['locations'] = selected_locations
        
        return filters
        
    except Exception as e:
        logger.error(f"Error rendering timeline filters: {e}")
        st.error("Error displaying timeline filters")
        return {}

def apply_timeline_filters(data: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    """
    Apply filters to timeline data
    
    Args:
        data: Original timeline data
        filters: Dictionary of filter values
        
    Returns:
        Filtered DataFrame
    """
    try:
        filtered_data = data.copy()
        
        # Apply event type filter
        if 'event_types' in filters and filters['event_types']:
            filtered_data = filtered_data[filtered_data['EVENT_TYPE'].isin(filters['event_types'])]
        
        # Apply date range filter
        if 'date_range' in filters:
            start_date, end_date = filters['date_range']
            filtered_data = filtered_data[
                (filtered_data['EVENT_DATE'].dt.date >= start_date) &
                (filtered_data['EVENT_DATE'].dt.date <= end_date)
            ]
        
        # Apply location filter
        if 'locations' in filters and filters['locations']:
            filtered_data = filtered_data[filtered_data['LOCATION'].isin(filters['locations'])]
        
        return filtered_data
        
    except Exception as e:
        logger.error(f"Error applying timeline filters: {e}")
        return data