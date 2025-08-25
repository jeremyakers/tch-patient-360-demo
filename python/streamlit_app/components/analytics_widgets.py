"""
Analytics Widgets Component for TCH Patient 360 PoC

Reusable data visualization and analytics components using Streamlit's native
charting capabilities for optimal compatibility with Streamlit in Snowflake.
"""

import streamlit as st
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, date, timedelta
import logging

logger = logging.getLogger(__name__)

def render_metric_card(title: str, value: Union[int, float, str], 
                      delta: Optional[Union[int, float]] = None, 
                      delta_color: str = "normal", 
                      help_text: str = None,
                      key: str = None) -> None:
    """
    Render a metric card with title, value, and optional delta
    
    Args:
        title: Metric title
        value: Primary metric value
        delta: Optional delta/change value
        delta_color: Color for delta ("normal", "inverse", "off")
        help_text: Optional help text
        key: Unique key for the component
    """
    try:
        with st.container():
            if help_text:
                st.metric(
                    label=title,
                    value=value,
                    delta=delta,
                    delta_color=delta_color,
                    help=help_text
                )
            else:
                st.metric(
                    label=title,
                    value=value,
                    delta=delta,
                    delta_color=delta_color
                )
                
    except Exception as e:
        logger.error(f"Error rendering metric card: {e}")
        st.error("Error displaying metric")

def render_chart_widget(data: pd.DataFrame, chart_type: str, 
                       title: str, x_col: str = None, y_col: str = None,
                       color_col: str = None, size_col: str = None,
                       height: int = 400, key: str = None) -> None:
    """
    Render various chart types using Streamlit's native charting capabilities
    
    Args:
        data: DataFrame containing chart data
        chart_type: Type of chart ('bar', 'line', 'scatter', 'pie', 'histogram')
        title: Chart title
        x_col: Column for x-axis
        y_col: Column for y-axis  
        color_col: Optional column for color coding
        size_col: Optional column for size (scatter plots)
        height: Chart height in pixels
        key: Unique key for the component
    """
    try:
        if data.empty:
            st.warning(f"No data available for {title}")
            return
        
        # Display chart title
        st.markdown(f"**{title}**")
        
        if chart_type == 'bar':
            # Prepare data for bar chart
            if x_col and y_col:
                chart_data = data.set_index(x_col)[y_col] if x_col in data.columns else data[y_col]
                st.bar_chart(chart_data, height=height)
            else:
                st.bar_chart(data, height=height)
                
        elif chart_type == 'line':
            # Prepare data for line chart
            if x_col and y_col:
                chart_data = data.set_index(x_col)[y_col] if x_col in data.columns else data[y_col]
                st.line_chart(chart_data, height=height)
            else:
                st.line_chart(data, height=height)
                
        elif chart_type == 'scatter':
            # Use scatter chart if available, otherwise fallback to line chart
            if hasattr(st, 'scatter_chart') and x_col and y_col:
                chart_data = data[[x_col, y_col]].set_index(x_col)
                st.scatter_chart(chart_data, height=height)
            else:
                # Fallback to displaying data as table
                st.dataframe(data[[x_col, y_col]] if x_col and y_col else data)
                
        elif chart_type == 'pie':
            # Streamlit doesn't have native pie chart, use bar chart as alternative
            if x_col and y_col:
                chart_data = data.set_index(x_col)[y_col]
                st.bar_chart(chart_data, height=height)
                st.caption("📊 Displaying as bar chart (pie chart alternative)")
            else:
                st.error("Pie charts require both x_col and y_col parameters")
                
        elif chart_type == 'histogram':
            # Use bar chart for histogram representation
            if y_col:
                chart_data = data[y_col].value_counts().sort_index()
                st.bar_chart(chart_data, height=height)
            else:
                st.bar_chart(data, height=height)
                
        elif chart_type == 'area':
            # Use area chart
            if x_col and y_col:
                chart_data = data.set_index(x_col)[y_col] if x_col in data.columns else data[y_col]
                st.area_chart(chart_data, height=height)
            else:
                st.area_chart(data, height=height)
                
        else:
            st.error(f"Unsupported chart type: {chart_type}")
            st.info("Supported types: bar, line, scatter, pie, histogram, area")
            return
            
    except Exception as e:
        logger.error(f"Error rendering chart widget: {e}")
        st.error("Error displaying chart")

def render_pediatric_age_distribution(data: pd.DataFrame, key: str = None) -> None:
    """
    Render pediatric-specific age distribution chart
    
    Args:
        data: DataFrame with age data
        key: Unique key for the component
    """
    try:
        if 'AGE' not in data.columns:
            st.warning("Age data not available")
            return
        
        # Create pediatric age groups
        age_bins = [0, 1, 5, 12, 17, 21]
        age_labels = ['Infants (0-1)', 'Toddlers (1-5)', 'Children (6-12)', 
                     'Adolescents (13-17)', 'Young Adults (18-21)']
        
        data_copy = data.copy()
        data_copy['Age_Group'] = pd.cut(data_copy['AGE'], bins=age_bins, 
                                       labels=age_labels, include_lowest=True)
        
        age_counts = data_copy['Age_Group'].value_counts().sort_index()
        
        # Display age distribution using native Streamlit charts
        st.markdown("**Pediatric Age Distribution**")
        
        # Use bar chart for age distribution
        st.bar_chart(age_counts, height=400)
        
        # Also show as dataframe for exact values
        with st.expander("📊 Detailed Age Distribution"):
            age_df = pd.DataFrame({
                'Age Group': age_counts.index,
                'Count': age_counts.values,
                'Percentage': (age_counts.values / age_counts.sum() * 100).round(1)
            })
            st.dataframe(age_df, use_container_width=True)
        
    except Exception as e:
        logger.error(f"Error rendering pediatric age distribution: {e}")
        st.error("Error displaying age distribution")

def render_risk_level_dashboard(data: pd.DataFrame, key: str = None) -> None:
    """
    Render risk level dashboard with multiple visualizations
    
    Args:
        data: DataFrame with risk level data
        key: Unique key for the component
    """
    try:
        if 'RISK_LEVEL' not in data.columns:
            st.warning("Risk level data not available")
            return
        
        # Risk level distribution
        risk_counts = data['RISK_LEVEL'].value_counts()
        total_patients = len(data)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "High Risk Patients",
                risk_counts.get('High', 0),
                delta=f"{(risk_counts.get('High', 0) / total_patients * 100):.1f}%"
            )
        
        with col2:
            st.metric(
                "Medium Risk Patients", 
                risk_counts.get('Medium', 0),
                delta=f"{(risk_counts.get('Medium', 0) / total_patients * 100):.1f}%"
            )
        
        with col3:
            st.metric(
                "Low Risk Patients",
                risk_counts.get('Low', 0), 
                delta=f"{(risk_counts.get('Low', 0) / total_patients * 100):.1f}%"
            )
        
        # Risk level distribution chart
        st.markdown("**Risk Level Distribution**")
        st.bar_chart(risk_counts, height=300)
        
        # Show detailed breakdown
        with st.expander("📊 Risk Level Details"):
            risk_df = pd.DataFrame({
                'Risk Level': risk_counts.index,
                'Count': risk_counts.values,
                'Percentage': (risk_counts.values / risk_counts.sum() * 100).round(1)
            })
            st.dataframe(risk_df, use_container_width=True)
        
    except Exception as e:
        logger.error(f"Error rendering risk level dashboard: {e}")
        st.error("Error displaying risk dashboard")

def render_encounter_trends(data: pd.DataFrame, key: str = None) -> None:
    """
    Render encounter trends over time
    
    Args:
        data: DataFrame with encounter data
        key: Unique key for the component
    """
    try:
        # Check for either ENCOUNTER_DATE or EVENT_DATE columns
        date_col = None
        if 'ENCOUNTER_DATE' in data.columns:
            date_col = 'ENCOUNTER_DATE'
        elif 'EVENT_DATE' in data.columns:
            date_col = 'EVENT_DATE'
        else:
            st.warning("Encounter date data not available")
            return
        
        # Convert date column
        data_copy = data.copy()
        data_copy['DATE'] = pd.to_datetime(data_copy[date_col])
        data_copy['Month'] = data_copy['DATE'].dt.to_period('M')
        
        # Group by month and event/encounter type
        event_type_col = 'ENCOUNTER_TYPE' if 'ENCOUNTER_TYPE' in data_copy.columns else 'EVENT_TYPE'
        monthly_encounters = data_copy.groupby(['Month', event_type_col]).size().reset_index(name='Count')
        monthly_encounters['Month'] = monthly_encounters['Month'].astype(str)
        
        # Create encounter trends chart
        st.markdown("**Encounter Trends by Type**")
        
        # Pivot data for line chart
        pivot_data = monthly_encounters.pivot(index='Month', columns=event_type_col, values='Count').fillna(0)
        
        # Use native line chart
        st.line_chart(pivot_data, height=400)
        
        # Show summary statistics
        with st.expander("📊 Encounter Trends Summary"):
            st.dataframe(pivot_data, use_container_width=True)
        
    except Exception as e:
        logger.error(f"Error rendering encounter trends: {e}")
        st.error("Error displaying encounter trends")

def render_department_utilization(data: pd.DataFrame, key: str = None) -> None:
    """
    Render department utilization chart
    
    Args:
        data: DataFrame with department data
        key: Unique key for the component
    """
    try:
        if 'DEPARTMENT' not in data.columns:
            st.warning("Department data not available") 
            return
        
        dept_counts = data['DEPARTMENT'].value_counts().head(10)
        
        # Display department utilization chart
        st.markdown("**Top 10 Departments by Utilization**")
        st.bar_chart(dept_counts, height=400)
        
        # Show detailed table
        with st.expander("📊 Department Utilization Details"):
            dept_df = pd.DataFrame({
                'Department': dept_counts.index,
                'Encounters': dept_counts.values,
                'Percentage': (dept_counts.values / dept_counts.sum() * 100).round(1)
            })
            st.dataframe(dept_df, use_container_width=True)
        
    except Exception as e:
        logger.error(f"Error rendering department utilization: {e}")
        st.error("Error displaying department utilization")

def render_lab_results_summary(data: pd.DataFrame, key: str = None) -> None:
    """
    Render lab results summary with abnormal flags
    
    Args:
        data: DataFrame with lab results
        key: Unique key for the component
    """
    try:
        if data.empty or 'ABNORMAL_FLAG' not in data.columns:
            st.warning("Lab results data not available")
            return
        
        # Summary metrics
        total_labs = len(data)
        abnormal_count = data['ABNORMAL_FLAG'].notna().sum()
        abnormal_pct = (abnormal_count / total_labs * 100) if total_labs > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Lab Orders", total_labs)
        
        with col2:
            st.metric("Abnormal Results", abnormal_count)
        
        with col3:
            st.metric("Abnormal Rate", f"{abnormal_pct:.1f}%")
        
        # Abnormal flag distribution
        flag_counts = data['ABNORMAL_FLAG'].value_counts()
        normal_count = total_labs - abnormal_count
        
        # Create stacked bar chart
        categories = ['Normal', 'High', 'Low', 'Critical']
        values = [
            normal_count,
            flag_counts.get('High', 0) + flag_counts.get('H', 0),
            flag_counts.get('Low', 0) + flag_counts.get('L', 0),
            flag_counts.get('Critical', 0) + flag_counts.get('C', 0)
        ]
        
        # Create lab results distribution chart
        st.markdown("**Lab Results Distribution**")
        
        lab_results_df = pd.DataFrame({
            'Category': categories,
            'Count': values
        }).set_index('Category')
        
        st.bar_chart(lab_results_df, height=300)
        
        # Show detailed breakdown
        with st.expander("📊 Lab Results Breakdown"):
            results_detail = pd.DataFrame({
                'Category': categories,
                'Count': values,
                'Percentage': [(v / sum(values) * 100) if sum(values) > 0 else 0 for v in values]
            })
            results_detail['Percentage'] = results_detail['Percentage'].round(1)
            st.dataframe(results_detail, use_container_width=True)
        
    except Exception as e:
        logger.error(f"Error rendering lab results summary: {e}")
        st.error("Error displaying lab results")

def render_medication_analysis(data: pd.DataFrame, key: str = None) -> None:
    """
    Render medication analysis charts
    
    Args:
        data: DataFrame with medication data
        key: Unique key for the component
    """
    try:
        if data.empty or 'MEDICATION_CLASS' not in data.columns:
            st.warning("Medication data not available")
            return
        
        # Top medication classes
        med_class_counts = data['MEDICATION_CLASS'].value_counts().head(8)
        
        # Display medication classes chart
        st.markdown("**Top Medication Classes**")
        st.bar_chart(med_class_counts, height=400)
        
        # Show detailed medication breakdown
        with st.expander("💊 Medication Classes Details"):
            med_df = pd.DataFrame({
                'Medication Class': med_class_counts.index,
                'Prescriptions': med_class_counts.values,
                'Percentage': (med_class_counts.values / med_class_counts.sum() * 100).round(1)
            })
            st.dataframe(med_df, use_container_width=True)
        
    except Exception as e:
        logger.error(f"Error rendering medication analysis: {e}")
        st.error("Error displaying medication analysis")

def render_kpi_dashboard(metrics: Dict[str, Any], key: str = None) -> None:
    """
    Render a comprehensive KPI dashboard
    
    Args:
        metrics: Dictionary containing various metrics
        key: Unique key for the component
    """
    try:
        st.subheader("📊 Key Performance Indicators")
        
        # Primary metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            render_metric_card(
                "Total Patients",
                metrics.get('total_patients', 0),
                help_text="Total number of patients in the system"
            )
        
        with col2:
            render_metric_card(
                "Active Encounters",
                metrics.get('active_encounters', 0),
                delta=metrics.get('encounter_delta'),
                help_text="Current active patient encounters"
            )
        
        with col3:
            render_metric_card(
                "Avg Length of Stay",
                f"{metrics.get('avg_los', 0):.1f} days",
                delta=metrics.get('los_delta'),
                help_text="Average length of stay across all admissions"
            )
        
        with col4:
            render_metric_card(
                "Quality Score",
                f"{metrics.get('quality_score', 0):.1f}%",
                delta=metrics.get('quality_delta'),
                help_text="Overall quality performance score"
            )
        
        # Secondary metrics row
        col5, col6, col7, col8 = st.columns(4)
        
        with col5:
            render_metric_card(
                "Readmission Rate",
                f"{metrics.get('readmission_rate', 0):.1f}%",
                delta=metrics.get('readmission_delta'),
                delta_color="inverse",
                help_text="30-day readmission rate"
            )
        
        with col6:
            render_metric_card(
                "Patient Satisfaction",
                f"{metrics.get('satisfaction_score', 0):.1f}/5",
                delta=metrics.get('satisfaction_delta'),
                help_text="Average patient satisfaction score"
            )
        
        with col7:
            render_metric_card(
                "Emergency Visits",
                metrics.get('emergency_visits', 0),
                delta=metrics.get('emergency_delta'),
                help_text="Total emergency department visits"
            )
        
        with col8:
            render_metric_card(
                "High Risk Patients",
                metrics.get('high_risk_patients', 0),
                delta=metrics.get('risk_delta'),
                delta_color="inverse",
                help_text="Patients classified as high risk"
            )
        
    except Exception as e:
        logger.error(f"Error rendering KPI dashboard: {e}")
        st.error("Error displaying KPI dashboard")

def render_interactive_filter_widget(data: pd.DataFrame, 
                                   filter_columns: List[str],
                                   key: str = None) -> Dict[str, Any]:
    """
    Render interactive filter widgets for data analysis
    
    Args:
        data: DataFrame to filter
        filter_columns: List of columns to create filters for
        key: Unique key for the component
        
    Returns:
        Dictionary of selected filter values
    """
    try:
        filters = {}
        
        st.subheader("🔧 Analysis Filters")
        
        with st.expander("Configure Filters", expanded=True):
            cols = st.columns(min(len(filter_columns), 3))
            
            for idx, column in enumerate(filter_columns):
                col_idx = idx % len(cols)
                
                with cols[col_idx]:
                    if column in data.columns:
                        unique_values = data[column].dropna().unique()
                        
                        if data[column].dtype in ['object', 'category']:
                            # Categorical filter
                            selected = st.multiselect(
                                f"Filter by {column}",
                                options=sorted(unique_values),
                                key=f"{key}_{column}_filter"
                            )
                            if selected:
                                filters[column] = selected
                                
                        elif data[column].dtype in ['int64', 'float64']:
                            # Numeric range filter
                            min_val = float(data[column].min())
                            max_val = float(data[column].max())
                            
                            range_vals = st.slider(
                                f"{column} Range",
                                min_value=min_val,
                                max_value=max_val,
                                value=(min_val, max_val),
                                key=f"{key}_{column}_range"
                            )
                            
                            if range_vals != (min_val, max_val):
                                filters[column] = range_vals
        
        return filters
        
    except Exception as e:
        logger.error(f"Error rendering filter widget: {e}")
        st.error("Error displaying filters")
        return {}