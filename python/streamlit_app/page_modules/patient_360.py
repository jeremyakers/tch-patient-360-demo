"""
Patient 360 Page for TCH Patient 360 PoC

This page provides a comprehensive 360-degree view of individual patients including:
- Complete demographic and clinical overview
- Interactive timeline of medical events
- AI-powered insights and risk assessment
- Multi-modal data integration (structured + unstructured)
- Pediatric-specific analytics and recommendations
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta
import logging

from services import data_service, cortex_analyst, session_manager
from components import patient_cards, analytics_widgets, clinical_timeline
from utils import helpers, validators

logger = logging.getLogger(__name__)

def render():
    """Entry point called by main.py"""
    render_patient_360()

def render_patient_360():
    """Main entry point for the Patient 360 page"""
    
    st.title("👤 Patient 360 View")
    
    # Check if patient is selected
    patient_id = st.session_state.get('selected_patient_id')
    
    if not patient_id:
        _render_patient_selection()
        return
    
    # Load patient data
    with st.spinner("Loading patient information..."):
        patient_data = data_service.get_patient_overview(patient_id)
    
    if not patient_data:
        st.error("Patient information could not be loaded. Please try again.")
        if st.button("🔙 Back to Search"):
            st.session_state.selected_patient_id = None
            st.session_state.current_patient = None
            st.session_state.current_page = "patient_search"
            st.rerun()
        return
    
    # Update patient context for sidebar
    if 'demographics' in patient_data:
        demographics = patient_data['demographics']
        st.session_state.current_patient = {
            'full_name': f"{demographics.get('FIRST_NAME', '')} {demographics.get('LAST_NAME', '')}".strip(),
            'mrn': demographics.get('MRN', 'Unknown'),
            'current_age': demographics.get('CURRENT_AGE', demographics.get('AGE', 'Unknown')),
            'gender': demographics.get('GENDER', 'Unknown'),
            'patient_id': patient_id
        }
    
    # Render patient overview
    _render_patient_header(patient_data)
    
    # Create tabs for different views with conditional rendering
    tab_names = [
        "📋 Clinical Overview", 
        "📈 Analytics & Trends", 
        "🤖 AI Insights", 
        "📝 Clinical Timeline",
        "🔍 Document Search"
    ]
    
    # Initialize selected tab in session state
    if f'selected_tab_{patient_id}' not in st.session_state:
        st.session_state[f'selected_tab_{patient_id}'] = 0
    
    # Create tab container but handle selection manually to avoid processing all tabs
    selected_tab = st.radio(
        "Select View:",
        options=range(len(tab_names)),
        format_func=lambda x: tab_names[x],
        index=st.session_state[f'selected_tab_{patient_id}'],
        horizontal=True,
        key=f"tab_selector_{patient_id}"
    )
    
    # Update session state
    st.session_state[f'selected_tab_{patient_id}'] = selected_tab
    
    st.divider()
    
    # Only render the selected tab content
    if selected_tab == 0:
        _render_clinical_overview(patient_data, patient_id)
        _render_financial_and_engagement(patient_data)
    elif selected_tab == 1:
        _render_analytics_dashboard(patient_data, patient_id)
    elif selected_tab == 2:
        _render_ai_insights(patient_data, patient_id)
    elif selected_tab == 3:
        _render_clinical_timeline(patient_data, patient_id)
    elif selected_tab == 4:
        _render_document_search(patient_data, patient_id)

def _render_patient_selection():
    """Render patient selection interface if no patient is selected"""
    
    st.info("Please select a patient to view their 360-degree profile.")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("🔍 Search Patients", type="primary"):
            st.session_state.current_page = "patient_search"
            st.rerun()
    
    with col2:
        if st.button("📊 Population Health"):
            st.session_state.current_page = "population_health"
            st.rerun()
    
    # Quick patient lookup
    st.subheader("Quick Patient Lookup")
    quick_search = st.text_input(
        "Enter MRN or Patient Name",
        placeholder="e.g., MRN12345678 or Smith, John"
    )
    
    if quick_search and st.button("🔍 Search"):
        with st.spinner("Searching..."):
            results = data_service.quick_patient_search(quick_search)
            
            if not results.empty:
                if len(results) == 1:
                    # Single result - go directly to patient 360
                    st.session_state.selected_patient_id = results.iloc[0]['PATIENT_ID']
                    st.rerun()
                else:
                    # Multiple results - show selection
                    st.subheader("Select Patient:")
                    for idx, patient in results.iterrows():
                        if st.button(
                            f"{patient['FIRST_NAME']} {patient['LAST_NAME']} (MRN: {patient['MRN']})",
                            key=f"quick_select_{idx}"
                        ):
                            st.session_state.selected_patient_id = patient['PATIENT_ID']
                            st.rerun()
            else:
                st.warning("No patients found matching your search.")

def _render_patient_header(patient_data: Dict[str, Any]):
    """Render the patient header with key information and navigation"""
    
    # Navigation bar
    col1, col2, col3 = st.columns([1, 4, 1])
    
    with col1:
        if st.button("🔙 Back to Search"):
            st.session_state.selected_patient_id = None
            st.session_state.current_page = "patient_search"
            st.rerun()
    
    # Quick Actions button removed (no-op)
    
    # Patient summary card
    patient_cards.render_patient_summary_card(
        patient_data, 
        key="patient_360_header"
    )
    
    # Quick actions modal removed
    
    st.divider()

def _render_clinical_overview(patient_data: Dict[str, Any], patient_id: str):
    """Render comprehensive clinical overview"""
    
    demographics = patient_data.get('demographics', {})
    recent_encounters = patient_data.get('recent_encounters', pd.DataFrame())
    active_diagnoses = patient_data.get('active_diagnoses', pd.DataFrame())
    current_medications = patient_data.get('current_medications', pd.DataFrame())
    recent_vitals = patient_data.get('recent_vitals', pd.DataFrame())
    recent_labs = patient_data.get('recent_labs', pd.DataFrame())
    
    # Clinical summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        analytics_widgets.render_metric_card(
            "Recent Encounters",
            len(recent_encounters),
            help_text="Encounters in the last 12 months"
        )
    
    with col2:
        analytics_widgets.render_metric_card(
            "Active Diagnoses", 
            len(active_diagnoses),
            help_text="Currently active medical conditions"
        )
    
    with col3:
        analytics_widgets.render_metric_card(
            "Current Medications",
            len(current_medications), 
            help_text="Active medication prescriptions"
        )
    
    with col4:
        # Calculate days since last visit
        if not recent_encounters.empty:
            last_visit = pd.to_datetime(recent_encounters['ENCOUNTER_DATE'].max())
            days_since = (datetime.now() - last_visit).days
            analytics_widgets.render_metric_card(
                "Days Since Last Visit",
                days_since,
                help_text="Days since most recent encounter"
            )
        else:
            analytics_widgets.render_metric_card(
                "Days Since Last Visit", 
                "N/A",
                help_text="No recent encounters found"
            )
    
    st.divider()
    
    # Detailed sections
    detail_col1, detail_col2 = st.columns([1, 1])
    
    with detail_col1:
        # Recent encounters
        st.subheader("🏥 Recent Encounters")
        if not recent_encounters.empty:
            for idx, encounter in recent_encounters.head(5).iterrows():
                with st.expander(
                    f"{encounter['ENCOUNTER_DATE']} - {encounter['DEPARTMENT_NAME']}",
                    expanded=False
                ):
                    st.write(f"**Type:** {encounter.get('ENCOUNTER_TYPE', 'N/A')}")
                    st.write(f"**Chief Complaint:** {encounter.get('CHIEF_COMPLAINT', 'N/A')}")
                    st.write(f"**Length of Stay:** {encounter.get('LENGTH_OF_STAY_DAYS', 'N/A')} days")
                    st.write(f"**Status:** {encounter.get('ENCOUNTER_STATUS', 'N/A')}")
        else:
            st.info("No recent encounters found")
        
        # Active diagnoses
        st.subheader("🩺 Active Diagnoses")
        if not active_diagnoses.empty:
            for idx, diagnosis in active_diagnoses.head(5).iterrows():
                st.write(f"• **{diagnosis.get('DIAGNOSIS_DESCRIPTION', 'Unknown')}** "
                        f"({diagnosis.get('DIAGNOSIS_CODE', 'N/A')}) - "
                        f"Since {diagnosis.get('DIAGNOSIS_DATE', 'Unknown')}")
        else:
            st.info("No active diagnoses found")
    
    with detail_col2:
        # Current medications
        st.subheader("💊 Current Medications")
        if not current_medications.empty:
            for idx, med in current_medications.head(5).iterrows():
                with st.expander(
                    f"{med.get('MEDICATION_NAME', 'Unknown')}",
                    expanded=False
                ):
                    st.write(f"**Dosage:** {med.get('DOSAGE', 'N/A')}")
                    st.write(f"**Frequency:** {med.get('FREQUENCY', 'N/A')}")
                    st.write(f"**Route:** {med.get('ROUTE', 'N/A')}")
                    st.write(f"**Start Date:** {med.get('START_DATE', 'N/A')}")
                    # Determine status based on end date
                    end_date = med.get('END_DATE')
                    if pd.isna(end_date) or end_date is None:
                        status = "Active"
                    else:
                        status = f"Ended {end_date}"
                    st.write(f"**Status:** {status}")
        else:
            st.info("No current medications found")
        
        # Recent vitals
        st.subheader("🌡️ Recent Vital Signs")
        if not recent_vitals.empty:
            # Show most recent vitals
            latest_vitals = recent_vitals.iloc[0]  # Most recent row
            st.write(f"**Date:** {latest_vitals.get('MEASUREMENT_DATE', 'Unknown')}")
            
            # Display each vital sign if it exists
            if pd.notna(latest_vitals.get('TEMPERATURE_FAHRENHEIT')):
                st.write(f"• **Temperature:** {latest_vitals['TEMPERATURE_FAHRENHEIT']}°F")
            if pd.notna(latest_vitals.get('HEART_RATE')):
                st.write(f"• **Heart Rate:** {latest_vitals['HEART_RATE']} bpm")
            if pd.notna(latest_vitals.get('RESPIRATORY_RATE')):
                st.write(f"• **Respiratory Rate:** {latest_vitals['RESPIRATORY_RATE']} /min")
            if pd.notna(latest_vitals.get('BLOOD_PRESSURE_SYSTOLIC')) and pd.notna(latest_vitals.get('BLOOD_PRESSURE_DIASTOLIC')):
                st.write(f"• **Blood Pressure:** {latest_vitals['BLOOD_PRESSURE_SYSTOLIC']}/{latest_vitals['BLOOD_PRESSURE_DIASTOLIC']} mmHg")
            if pd.notna(latest_vitals.get('OXYGEN_SATURATION')):
                st.write(f"• **Oxygen Saturation:** {latest_vitals['OXYGEN_SATURATION']}%")
            if pd.notna(latest_vitals.get('WEIGHT_POUNDS')):
                st.write(f"• **Weight:** {latest_vitals['WEIGHT_POUNDS']} lbs")
            if pd.notna(latest_vitals.get('HEIGHT_INCHES')):
                st.write(f"• **Height:** {latest_vitals['HEIGHT_INCHES']} inches")
        else:
            st.info("No recent vital signs found")

def _render_financial_and_engagement(patient_data: Dict[str, Any]):
    """Render Oracle ERP financial metrics and Salesforce engagement metrics."""
    fin = patient_data.get('financial_summary', {}) or {}
    eng = patient_data.get('engagement_summary', {}) or {}

    if not fin and not eng:
        return

    st.divider()
    st.subheader("💰 Financial & 📱 Engagement Overview")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        analytics_widgets.render_metric_card(
            "Total Lifetime Charges",
            f"${fin.get('TOTAL_LIFETIME_CHARGES', 0):,.2f}",
            help_text="Oracle ERP"
        )
    with col2:
        analytics_widgets.render_metric_card(
            "Outstanding Balance",
            f"${fin.get('OUTSTANDING_BALANCE', 0):,.2f}",
            delta=fin.get('FINANCIAL_VALUE_CATEGORY'),
            help_text="Oracle ERP"
        )
    with col3:
        analytics_widgets.render_metric_card(
            "Avg Cost / Encounter",
            f"${fin.get('AVG_COST_PER_ENCOUNTER', 0):,.2f}",
            help_text="Oracle ERP"
        )
    with col4:
        analytics_widgets.render_metric_card(
            "High-Cost Episodes",
            fin.get('HIGH_COST_EPISODES', 0),
            help_text="Oracle ERP"
        )

    col5, col6, col7 = st.columns(3)
    with col5:
        analytics_widgets.render_metric_card(
            "Engagement Score",
            eng.get('ENGAGEMENT_SCORE', 0),
            delta=eng.get('DIGITAL_ADOPTION_LEVEL'),
            help_text="Salesforce"
        )
    with col6:
        analytics_widgets.render_metric_card(
            "Portal Logins (30d)",
            eng.get('PORTAL_LOGINS_LAST_30_DAYS', 0),
            help_text="Salesforce"
        )
    with col7:
        # Format last engagement date safely for display
        last_eng = eng.get('LAST_ENGAGEMENT_DATE')
        try:
            if hasattr(last_eng, 'strftime'):
                last_eng_str = last_eng.strftime('%Y-%m-%d')
            else:
                last_eng_str = str(last_eng) if last_eng is not None else 'N/A'
        except Exception:
            last_eng_str = 'N/A'

        analytics_widgets.render_metric_card(
            "Last Engagement",
            last_eng_str,
            help_text="Salesforce"
        )

def _render_analytics_dashboard(patient_data: Dict[str, Any], patient_id: str):
    """Render analytics and trends dashboard"""
    
    st.subheader("📊 Clinical Analytics & Trends")
    
    # Get additional data for analytics
    timeline_data = data_service.get_clinical_timeline(patient_id, days_back=365)
    
    if not timeline_data.empty:
        # Event trends over time
        analytics_widgets.render_encounter_trends(timeline_data, key="patient_timeline")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Event type distribution
            event_counts = timeline_data['EVENT_TYPE'].value_counts()
            analytics_widgets.render_chart_widget(
                pd.DataFrame({'Event_Type': event_counts.index, 'Count': event_counts.values}),
                chart_type='pie',
                title="Clinical Events Distribution",
                x_col='Event_Type',
                y_col='Count',
                key="event_distribution"
            )
        
        with col2:
            # Monthly activity
            timeline_data['EVENT_DATE'] = pd.to_datetime(timeline_data['EVENT_DATE'])
            timeline_data['Month'] = timeline_data['EVENT_DATE'].dt.to_period('M').astype(str)
            monthly_activity = timeline_data.groupby('Month').size().reset_index(name='Events')
            
            analytics_widgets.render_chart_widget(
                monthly_activity,
                chart_type='line',
                title="Monthly Clinical Activity",
                x_col='Month',
                y_col='Events',
                key="monthly_activity"
            )
    
    # Lab trends if available
    recent_labs = patient_data.get('recent_labs', pd.DataFrame())
    if not recent_labs.empty:
        st.subheader("🧪 Laboratory Trends")
        analytics_widgets.render_lab_results_summary(recent_labs, key="patient_labs")
    
    # Medication analysis
    current_medications = patient_data.get('current_medications', pd.DataFrame())
    if not current_medications.empty:
        st.subheader("💊 Medication Analysis")
        analytics_widgets.render_medication_analysis(current_medications, key="patient_meds")

def _render_ai_insights(patient_data: Dict[str, Any], patient_id: str):
    """Render AI-powered insights and recommendations"""
    
    st.subheader("🤖 AI-Powered Clinical Insights")
    
    # Add debug logging
    logger.info(f"🔍 DEBUG: Starting AI Insights for patient_id: {patient_id}")
    
    # Cache key for AI insights
    insights_cache_key = f"ai_insights_{patient_id}"
    logger.info(f"🔍 DEBUG: Cache key: {insights_cache_key}")
    
    # Check if insights are already cached for this patient
    if insights_cache_key not in st.session_state:
        logger.info(f"🔍 DEBUG: No cached insights found, generating new ones...")
        # Generate AI insights using Cortex Analyst - OPTIMIZED VERSION
        with st.spinner("Generating AI insights..."):
            try:
                # Use the optimized single-call approach
                logger.info(f"🔍 DEBUG: Calling optimized AI analysis...")
                start_time = datetime.now()
                
                combined_analysis = cortex_analyst.analyze_patient_data(
                    "Generate comprehensive patient insights", 
                    {'patient_id': patient_id}
                )
                
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                logger.info(f"🔍 DEBUG: Analysis completed in {execution_time:.2f} seconds")
                logger.info(f"🔍 DEBUG: Analysis result type: {type(combined_analysis)}")
                logger.info(f"🔍 DEBUG: Analysis result keys: {list(combined_analysis.keys()) if isinstance(combined_analysis, dict) else 'Not a dict'}")
                
                # Check if we got the optimized format
                if 'encounter_analysis' in combined_analysis and 'medication_analysis' in combined_analysis and 'risk_analysis' in combined_analysis:
                    logger.info(f"🔍 DEBUG: ✅ Got optimized combined analysis format")
                    encounter_analysis = combined_analysis['encounter_analysis']
                    medication_analysis = combined_analysis['medication_analysis']
                    risk_analysis = combined_analysis['risk_analysis']
                else:
                    logger.warning(f"🔍 DEBUG: ⚠️ Got single analysis format, using as encounter_analysis")
                    encounter_analysis = combined_analysis
                    medication_analysis = {'error': 'Single analysis format - no medication analysis'}
                    risk_analysis = {'error': 'Single analysis format - no risk analysis'}
                
                logger.info(f"🔍 DEBUG: Final encounter_analysis type: {type(encounter_analysis)}")
                logger.info(f"🔍 DEBUG: Final medication_analysis type: {type(medication_analysis)}")
                logger.info(f"🔍 DEBUG: Final risk_analysis type: {type(risk_analysis)}")
                        
                # Cache the results
                st.session_state[insights_cache_key] = {
                    'encounter_analysis': encounter_analysis,
                    'medication_analysis': medication_analysis,
                    'risk_analysis': risk_analysis,
                    'timestamp': datetime.now(),
                    'execution_time': execution_time
                }
                
                logger.info(f"🔍 DEBUG: ✅ Results cached successfully")
                        
            except Exception as e:
                logger.error(f"🔍 DEBUG: ❌ Error generating AI insights: {e}")
                import traceback
                logger.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
                st.error(f"Unable to generate AI insights: {str(e)}")
                return
    else:
        logger.info(f"🔍 DEBUG: ✅ Using cached insights")
        # Use cached results
        cached_data = st.session_state[insights_cache_key]
        encounter_analysis = cached_data['encounter_analysis']
        medication_analysis = cached_data['medication_analysis']
        risk_analysis = cached_data['risk_analysis']
        
        # Show cache timestamp
        cache_time = cached_data['timestamp']
        execution_time = cached_data.get('execution_time', 'Unknown')
        st.caption(f"💾 Cached insights from {cache_time.strftime('%I:%M %p')} (Generated in {execution_time:.1f}s)")
    
    # Add refresh button - MOVED OUTSIDE THE CACHE CHECK
    if st.button("🔄 Refresh AI Insights", key="refresh_ai_insights"):
        # Clear cache and rerun
        logger.info(f"🔍 DEBUG: 🔄 Refreshing AI insights - clearing cache")
        del st.session_state[insights_cache_key]
        st.rerun()
    
    # DISPLAY INSIGHTS - MOVED OUTSIDE THE REFRESH BUTTON
    logger.info(f"🔍 DEBUG: 🎨 Starting to display insights...")
    
    # Display insights in organized sections
    insight_col1, insight_col2 = st.columns(2)
    
    with insight_col1:
        st.markdown("### 🚨 Risk Assessment")
        logger.info(f"🔍 DEBUG: Displaying risk analysis...")
        if 'error' not in risk_analysis:
            logger.info(f"🔍 DEBUG: ✅ Risk analysis has no errors")
            overview = risk_analysis.get('overview', {})
            logger.info(f"🔍 DEBUG: Overview keys: {list(overview.keys()) if overview else 'No overview'}")
            
            # Risk level indicator
            risk_level = overview.get('risk_level', 'Unknown')
            risk_color = {'High': '🔴', 'Medium': '🟡', 'Low': '🟢'}.get(risk_level, '⚪')
            st.markdown(f"**Current Risk Level:** {risk_color} {risk_level}")
            
            # Risk factors
            risk_factors = overview.get('risk_factors', 0)
            st.metric("Risk Factors Identified", risk_factors)
            
            # Key insights
            insights = overview.get('insights', [])
            if insights:
                st.markdown("**Key Risk Factors:**")
                for insight in insights[:3]:
                    st.write(f"• {insight}")
            
            # Show AI generated insights for debugging
            ai_insights = risk_analysis.get('ai_generated_insights', '')
            if ai_insights:
                with st.expander("🧠 AI Risk Analysis (Full)"):
                    st.text_area("AI Response", ai_insights, height=200, disabled=True)
        else:
            error_msg = risk_analysis.get('error', 'Unknown error')
            logger.error(f"🔍 DEBUG: ❌ Risk analysis error: {error_msg}")
            st.warning(f"Risk analysis temporarily unavailable: {error_msg}")
            st.json(risk_analysis)  # Debug: show the full response
        
        st.markdown("### 💊 Medication Insights")
        logger.info(f"🔍 DEBUG: Displaying medication analysis...")
        if 'error' not in medication_analysis:
            logger.info(f"🔍 DEBUG: ✅ Medication analysis has no errors")
            med_metrics = medication_analysis.get('medication_summary', {})
            med_insights = medication_analysis.get('insights', [])
            
            if med_metrics:
                st.metric("Active Medications", med_metrics.get('ACTIVE_MEDICATIONS', 0))
                st.metric("Medication Classes", med_metrics.get('MEDICATION_CLASSES', 0))
            
            if med_insights:
                st.markdown("**Medication Recommendations:**")
                for insight in med_insights[:3]:
                    st.write(f"• {insight}")
            
            # Show AI generated insights for debugging
            ai_insights = medication_analysis.get('ai_generated_insights', '')
            if ai_insights:
                with st.expander("🧠 AI Medication Analysis (Full)"):
                    st.text_area("AI Response", ai_insights, height=200, disabled=True)
        else:
            error_msg = medication_analysis.get('error', 'Unknown error')
            logger.error(f"🔍 DEBUG: ❌ Medication analysis error: {error_msg}")
            st.warning(f"Medication analysis temporarily unavailable: {error_msg}")
            st.json(medication_analysis)  # Debug: show the full response
    
    with insight_col2:
        st.markdown("### 🏥 Care Utilization Insights")
        logger.info(f"🔍 DEBUG: Displaying encounter analysis...")
        if 'error' not in encounter_analysis:
            logger.info(f"🔍 DEBUG: ✅ Encounter analysis has no errors")
            enc_metrics = encounter_analysis.get('metrics', {})
            enc_insights = encounter_analysis.get('insights', [])
            logger.info(f"🔍 DEBUG: Enc metrics keys: {list(enc_metrics.keys()) if enc_metrics else 'No metrics'}")
            
            if enc_metrics:
                col_a, col_b = st.columns(2)
                with col_a:
                    st.metric("Total Encounters", enc_metrics.get('TOTAL_ENCOUNTERS', 0))
                    st.metric("Emergency Visits", enc_metrics.get('EMERGENCY_VISITS', 0))
                with col_b:
                    st.metric("Departments Visited", enc_metrics.get('DEPARTMENTS_VISITED', 0))
                if enc_metrics.get('AVG_LENGTH_OF_STAY'):
                    st.metric("Avg Length of Stay", f"{enc_metrics['AVG_LENGTH_OF_STAY']:.1f} days")
            
            if enc_insights:
                st.markdown("**Care Coordination Insights:**")
                for insight in enc_insights[:3]:
                    st.write(f"• {insight}")
            
            # Show AI generated insights for debugging
            ai_insights = encounter_analysis.get('ai_generated_insights', '')
            if ai_insights:
                with st.expander("🧠 AI Encounter Analysis (Full)"):
                    st.text_area("AI Response", ai_insights, height=200, disabled=True)
        else:
            error_msg = encounter_analysis.get('error', 'Unknown error')
            logger.error(f"🔍 DEBUG: ❌ Encounter analysis error: {error_msg}")
            st.warning(f"Encounter analysis temporarily unavailable: {error_msg}")
            st.json(encounter_analysis)  # Debug: show the full response
        
        # Pediatric-specific insights
        demographics = patient_data.get('demographics', {})
        age = demographics.get('AGE')
        if age is not None:
            st.markdown("### 👶 Pediatric Care Insights")
            
            if age < 2:
                st.info("**Infant Care:** Focus on developmental milestones and immunization schedule")
            elif age < 6:
                st.info("**Early Childhood:** Monitor growth patterns and behavioral development")
            elif age < 13:
                st.info("**School Age:** Emphasize preventive care and academic health")
            elif age < 18:
                st.info("**Adolescent:** Address mental health, risk behaviors, and transition planning")
            else:
                st.info("**Young Adult:** Focus on independence, health maintenance, and transition to adult care")

def _render_clinical_timeline(patient_data: Dict[str, Any], patient_id: str):
    """Render interactive clinical timeline"""
    
    st.subheader("📝 Clinical Timeline")
    
    # Timeline controls
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        days_back = st.selectbox(
            "Timeline Period",
            [30, 90, 180, 365, 730],
            index=2,
            format_func=lambda x: f"Last {x} days" if x < 365 else f"Last {x//365} year{'s' if x > 365 else ''}"
        )
    
    with col2:
        event_types = st.multiselect(
            "Event Types",
            ["Encounter", "Lab Result", "Medication"],
            default=["Encounter", "Lab Result", "Medication"]
        )
    
    with col3:
        show_details = st.checkbox("Show Details", value=True)
    
    # Load timeline data
    with st.spinner("Loading clinical timeline..."):
        timeline_data = data_service.get_clinical_timeline(patient_id, days_back=days_back)
    
    if not timeline_data.empty:
        # Filter by selected event types
        if event_types:
            timeline_data = timeline_data[timeline_data['EVENT_TYPE'].isin(event_types)]
        
        if not timeline_data.empty:
            # Render timeline using clinical_timeline component
            clinical_timeline.render_timeline(
                timeline_data,
                show_details=show_details,
                key="patient_timeline"
            )
        else:
            st.info("No events found for the selected criteria.")
    else:
        st.info("No clinical timeline data available for this patient.")

def _render_document_search(patient_data: Dict[str, Any], patient_id: str):
    """Render clinical document search interface"""
    
    st.subheader("🔍 Clinical Document Search")
    

    
    # Search interface
    search_query = st.text_area(
        "Search clinical documents (optional):",
        placeholder="Enter keywords to search across clinical notes, reports, and documentation... Or leave empty to see all documents for this patient.",
        height=100
    )
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        document_types = st.multiselect(
            "Document Types",
            ["Clinical Notes", "Discharge Summaries", "Radiology Reports", "Lab Reports", "Pathology Reports"],
            default=["Clinical Notes"]
        )
    
    with col2:
        max_results = st.number_input("Max Results", min_value=5, max_value=50, value=10)
    
    # Cache key for this patient's document search
    doc_search_cache_key = f"doc_search_results_{patient_id}"
    
    # Check if search button was clicked
    search_clicked = st.button("🔍 Search Documents", type="primary")
    
    # Perform search if button clicked or show cached results
    if search_clicked:
        # If button clicked, perform new search and cache results
        with st.spinner("Searching clinical documents using Cortex Agents..."):
            try:
                # Import and initialize Cortex Agents
                from services import cortex_agents, cortex_search
                
                # Handle empty search query
                if not search_query or search_query.strip() == "":
                    search_query = "*"  # Use wildcard for all documents
                    st.info("🔍 Searching for all documents (no specific query provided)")
                
                # Use Cortex Agents for document search
                current_mrn = patient_data.get('demographics', {}).get('MRN', 'Unknown')
                st.info(f"🔍 Searching for documents related to '{search_query}' for MRN {current_mrn}")
                
                try:
                    agent_response, citations = cortex_agents.search_documents_for_patient(
                        mrn=current_mrn,
                        search_query=search_query,
                        document_types=document_types,
                        max_results=max_results
                    )
                    
                    st.info(f"📋 Agent response received: {len(agent_response) if agent_response else 0} characters")
                    st.info(f"📄 Citations received: {len(citations) if citations else 0} items")
                    
                    # Display the agent's natural language response
                    if agent_response and not agent_response.startswith("Error:"):
                        st.markdown("### 🤖 AI Analysis")
                        st.markdown(agent_response)
                        st.markdown("---")
                        # Persist AI response so it remains visible across reruns (e.g., when expanding a document)
                        st.session_state[f"doc_search_response_{patient_id}"] = agent_response
                    elif agent_response and agent_response.startswith("Error:"):
                        st.error("**Document Search Error:**")
                        st.error(agent_response)
                        
                        # Show debugging info in expander
                        with st.expander("🔧 Debug Information", expanded=False):
                            st.write(f"**Patient ID:** {patient_id}")
                            st.write(f"**Search Query:** {search_query}")
                            st.write(f"**Document Types:** {document_types}")
                            st.write(f"**Max Results:** {max_results}")
                            st.write("**Check Snowflake logs for detailed error information**")
                        
                        document_info = []
                        st.session_state[doc_search_cache_key] = document_info
                        return  # Exit early on error
                    
                    # Process citations into document_info format for UI compatibility
                    document_info = []
                    if citations:
                        st.success(f"Found {len(citations)} relevant documents")
                        
                        for idx, citation in enumerate(citations, start=1):
                            # Extract document info from citation
                            doc_type = citation.get('document_type', 'Document')
                            doc_date = citation.get('document_date', 'Unknown Date')
                            doc_id = citation.get('file_path') or citation.get('doc_id', 'N/A')
                            excerpt = citation.get('text', 'No preview available')
                            author = citation.get('author', 'N/A')
                            department = citation.get('department', 'N/A')
                            
                            # Store document info in the same format as before
                            doc_info = {
                                'idx': idx,
                                'doc_type': doc_type,
                                'doc_date': doc_date,
                                'doc_id': doc_id,
                                'excerpt': excerpt,
                                'author': author,
                                'department': department
                            }
                            document_info.append(doc_info)
                        
                        # Cache the search results for this patient
                        st.session_state[doc_search_cache_key] = document_info
                    else:
                        st.warning("No documents found matching your search criteria.")
                        st.info("Try adjusting your search terms or document type filters.")
                        document_info = []
                        st.session_state[doc_search_cache_key] = document_info
                        
                except Exception as search_error:
                    st.error("**Critical Document Search Error:**")
                    st.error(f"Exception: {str(search_error)}")
                    
                    with st.expander("🔧 Debug Information", expanded=True):
                        st.write(f"**Patient ID:** {patient_id}")
                        st.write(f"**Search Query:** {search_query}")
                        st.write(f"**Document Types:** {document_types}")
                        st.write(f"**Max Results:** {max_results}")
                        st.write(f"**Exception Type:** {type(search_error).__name__}")
                        st.write(f"**Exception Details:** {str(search_error)}")
                        
                        # Show traceback
                        import traceback
                        st.code(traceback.format_exc())
                    
                    document_info = []
                    st.session_state[doc_search_cache_key] = document_info
                    return  # Exit early on critical error
                
                # Enrich author/department via EXTRACT_ANSWER (batch) before showing
                try:
                    if document_info:
                        logger.info(f"Attempting to enrich metadata for {len(document_info)} documents")
                        from services.cortex_search import CortexSearchService
                        from services import session_manager
                        
                        # Create a new instance of CortexSearchService
                        cortex_search_svc = CortexSearchService(session_manager)
                        
                        ids = [d['doc_id'] for d in document_info]
                        types = [d['doc_type'] for d in document_info]
                        logger.info(f"Document IDs to enrich: {ids}")
                        extracted = cortex_search_svc.batch_extract_document_metadata(ids, types)
                        logger.info(f"Extracted metadata for {len(extracted)} documents")
                        for d in document_info:
                            meta = extracted.get(d['doc_id']) or {}
                            if (not d.get('author')) or d.get('author') == 'N/A':
                                if meta.get('author'):
                                    d['author'] = meta['author']
                                    logger.info(f"Updated author for {d['doc_id']}: {meta['author']}")
                            if (not d.get('department')) or d.get('department') == 'N/A':
                                if meta.get('department'):
                                    d['department'] = meta['department']
                                    logger.info(f"Updated department for {d['doc_id']}: {meta['department']}")
                        # Update cache with enriched values
                        st.session_state[doc_search_cache_key] = document_info
                except Exception as _meta_err:
                    logger.error(f"Metadata enrichment failed: {_meta_err}", exc_info=True)
                
                # Display search results
                if document_info:
                    for doc_info in document_info:
                        idx = doc_info['idx']
                        doc_type = doc_info['doc_type']
                        doc_date = doc_info['doc_date']
                        doc_id = doc_info['doc_id']
                        excerpt = doc_info['excerpt']
                        author = doc_info['author']
                        department = doc_info['department']
                        
                        # Check if this document is being viewed to keep expander open
                        btn_key = f"btn_{doc_id}"
                        is_viewing_document = st.session_state.get(btn_key, False) or st.session_state.get('expand_target') == doc_id
                        
                        with st.expander(
                            f"📄 [{idx}] {doc_type} - {doc_date}",
                            expanded=is_viewing_document
                        ):
                            st.write(f"**Author:** {author}")
                            st.write(f"**Department:** {department}")
                            st.write(f"**File Path:** {doc_id}")
                            
                            # Document excerpt from search results
                            if excerpt and excerpt != 'No preview available':
                                st.markdown("**Relevant Content:**")
                                # Limit excerpt length for display
                                display_excerpt = excerpt[:500] + "..." if len(excerpt) > 500 else excerpt
                                st.markdown(f">{display_excerpt}")
                            
                            # View Full Document button
                            btn_key = f"btn_{doc_id}"
                            # Toggle behavior to avoid page reload clearing AI response
                            if st.button("📄 View Full Document", key=f"open_{btn_key}"):
                                st.session_state[btn_key] = True
                                st.session_state['expand_target'] = doc_id
                                st.rerun()
                            
                            # Display content based on button state (sticky)
                            if st.session_state.get(btn_key):
                                try:
                                    with st.spinner("Loading full document..."):
                                        # Create cortex search service instance if needed
                                        from services.cortex_search import CortexSearchService
                                        from services import session_manager
                                        cortex_search_svc = CortexSearchService(session_manager)
                                        full_content = cortex_search_svc.get_full_document_content(doc_id, doc_type)
                                    
                                    if full_content and full_content.strip():
                                        st.markdown("---")
                                        st.markdown("### 📄 **Full Document Content**")
                                        # Display in a scrollable text area
                                        st.text_area(
                                            "Document Text",
                                            value=full_content,
                                            height=400,
                                            disabled=True,
                                            label_visibility="collapsed",
                                            key=f"doc_content_{doc_id}"
                                        )
                                        
                                        # Hide button to close document (without rerun)
                                        if st.button(f"🔽 Hide Document", key=f"hide_{doc_id}"):
                                            st.session_state[btn_key] = False
                                    else:
                                        st.warning("Could not retrieve full document content.")
                                        st.info("The document may no longer be available.")
                                        
                                except Exception as e:
                                    st.error(f"Error loading document: {e}")
                                    import traceback
                                    st.code(traceback.format_exc())
            
            except Exception as e:
                st.error(f"Document search failed: {e}")
                import traceback
                st.code(traceback.format_exc())
    
    # Display cached search results if they exist (even if no search was performed)
    elif doc_search_cache_key in st.session_state:
        # Use cached search results
        st.info("📋 Displaying cached search results (click 'Search Documents' to refresh)")
        # Show cached AI response if available
        cached_resp_key = f"doc_search_response_{patient_id}"
        cached_response = st.session_state.get(cached_resp_key)
        if cached_response:
            st.markdown("### 🤖 AI Analysis")
            st.markdown(cached_response)
            st.markdown("---")
        document_info = st.session_state[doc_search_cache_key]
        
        # Enrich missing metadata once for cached results
        try:
            if document_info and any((not d.get('author') or d.get('author') == 'N/A' or not d.get('department') or d.get('department') == 'N/A') for d in document_info):
                logger.info(f"Enriching metadata for cached results: {len(document_info)} documents")
                from services.cortex_search import CortexSearchService
                from services import session_manager
                
                # Create a new instance of CortexSearchService
                cortex_search_svc = CortexSearchService(session_manager)
                
                ids = [d['doc_id'] for d in document_info]
                types = [d['doc_type'] for d in document_info]
                logger.info(f"Cached document IDs to enrich: {ids}")
                extracted = cortex_search_svc.batch_extract_document_metadata(ids, types)
                logger.info(f"Extracted cached metadata for {len(extracted)} documents")
                for d in document_info:
                    meta = extracted.get(d['doc_id']) or {}
                    if (not d.get('author')) or d.get('author') == 'N/A':
                        if meta.get('author'):
                            d['author'] = meta['author']
                            logger.info(f"Updated cached author for {d['doc_id']}: {meta['author']}")
                    if (not d.get('department')) or d.get('department') == 'N/A':
                        if meta.get('department'):
                            d['department'] = meta['department']
                            logger.info(f"Updated cached department for {d['doc_id']}: {meta['department']}")
                st.session_state[doc_search_cache_key] = document_info
        except Exception as _meta_err:
            logger.error(f"Cached metadata enrichment failed: {_meta_err}", exc_info=True)
        
        # Display cached results with same format
        if document_info:
            st.success(f"Found {len(document_info)} relevant documents (cached)")
            
            # Import cortex_search for the view document functionality
            from services.cortex_search import CortexSearchService
            from services import session_manager
            cortex_search_svc = CortexSearchService(session_manager)
            
            # Display cached search results
            for i, doc_info in enumerate(document_info, start=1):
                doc_type = doc_info['doc_type']
                doc_date = doc_info['doc_date']
                doc_id = doc_info['doc_id']
                excerpt = doc_info['excerpt']
                author = doc_info['author']
                department = doc_info['department']
                
                # Check if this document is being viewed to keep expander open
                btn_key = f"btn_{doc_id}"
                is_viewing_document = st.session_state.get(btn_key, False) or st.session_state.get('expand_target') == doc_id
                
                with st.expander(
                    f"📄 [{i}] {doc_type} - {doc_date}",
                    expanded=is_viewing_document
                ):
                    st.write(f"**Author:** {author}")
                    st.write(f"**Department:** {department}")
                    st.write(f"**File Path:** {doc_id}")
                    
                    # Document excerpt from search results
                    if excerpt and excerpt != 'No preview available':
                        st.markdown("**Relevant Content:**")
                        # Limit excerpt length for display
                        display_excerpt = excerpt[:500] + "..." if len(excerpt) > 500 else excerpt
                        st.markdown(f">{display_excerpt}")
                    
                    # View Full Document button
                    if st.button("📄 View Full Document", key=f"open_{btn_key}"):
                        st.session_state[btn_key] = True
                        st.session_state['expand_target'] = doc_id
                        st.rerun()
                    
                    # Display content based on button state
                    if st.session_state.get(btn_key, False):
                        try:
                            with st.spinner("Loading full document..."):
                                full_content = cortex_search_svc.get_full_document_content(doc_id, doc_type)
                            
                            if full_content and full_content.strip():
                                st.markdown("---")
                                st.markdown("### 📄 **Full Document Content**")
                                # Display in a scrollable text area
                                st.text_area(
                                    "Document Text",
                                    value=full_content,
                                    height=400,
                                    disabled=True,
                                    label_visibility="collapsed",
                                    key=f"doc_content_{doc_id}"
                                )
                                
                                # Hide button to close document
                                if st.button(f"🔽 Hide Document", key=f"hide_{doc_id}"):
                                    st.session_state[btn_key] = False
                            else:
                                st.warning("Could not retrieve full document content.")
                                st.info("The document may no longer be available.")
                        except Exception as e:
                            st.error(f"Error loading document: {e}")
                            import traceback
                            st.code(traceback.format_exc())
        else:
            st.warning("No cached search results found.")

    
    # Recent documents section removed (no backing data source)


