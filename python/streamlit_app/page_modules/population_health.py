"""
Population Health Page for TCH Patient 360 PoC

This page provides population-level analytics and insights including:
- Demographics and trends analysis
- Disease prevalence tracking
- Risk stratification analytics
- Quality measures and outcomes
- Public health reporting capabilities
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta
import logging

from services import data_service, cortex_analyst, session_manager
from components import analytics_widgets
from utils import helpers, config

logger = logging.getLogger(__name__)

def render():
    """Entry point called by main.py"""
    render_population_health()

def render_population_health():
    """Main entry point for the population health page"""
    
    st.title("📊 Population Health Analytics")
    st.markdown("Analyze population-level health trends and outcomes for pediatric care optimization")

    # Top KPI row (live from PRESENTATION.POPULATION_HEALTH_SUMMARY)
    metrics = data_service.get_population_metrics()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        analytics_widgets.render_metric_card("Total Active Patients", metrics.get('TOTAL_ACTIVE_PATIENTS', 0))
    with col2:
        analytics_widgets.render_metric_card("Pediatric % (1-12y)", f"{metrics.get('PEDIATRIC_PERCENTAGE', 0)}%")
    with col3:
        analytics_widgets.render_metric_card("Adolescent/YA %", f"{metrics.get('ADOLESCENT_YOUNG_ADULT_PERCENTAGE', 0)}%")
    with col4:
        analytics_widgets.render_metric_card("High Risk Patients", metrics.get('HIGH_RISK_PATIENTS', 0))

    st.divider()

    # Demographics & Trends
    st.subheader("📈 Demographics & Trends")
    ages_df = data_service.get_age_distribution()
    if not ages_df.empty:
        analytics_widgets.render_pediatric_age_distribution(ages_df, key="age_dist")
    else:
        st.info("No age data available")

    st.divider()

    # Financial analytics by condition (multi-source)
    st.subheader("💰 Cost by Condition (Oracle ERP + Clinical + Engagement)")
    fin_df = data_service.get_financial_analytics()
    if not fin_df.empty:
        # Show top conditions by total cost
        st.dataframe(fin_df.head(20), use_container_width=True)
        # Quick charts
        top_cost = fin_df[['DIAGNOSIS_DESCRIPTION','TOTAL_COST']].head(10)
        analytics_widgets.render_chart_widget(
            top_cost.rename(columns={'DIAGNOSIS_DESCRIPTION':'Condition','TOTAL_COST':'Total Cost'}),
            chart_type='bar',
            title='Top 10 Conditions by Total Cost',
            x_col='Condition',
            y_col='Total Cost',
            key='top_cost_bar'
        )
    else:
        st.info("Financial analytics view is empty.")

    st.divider()

    # Quality metrics quick view (from QUALITY_METRICS_DASHBOARD)
    st.subheader("✅ Quality Metrics (Last 12 Months)")
    try:
        session = session_manager.get_session()
        q_df = session.sql("SELECT * FROM PRESENTATION.QUALITY_METRICS_DASHBOARD").to_pandas()
        if not q_df.empty:
            row = q_df.iloc[0]
            qc1, qc2, qc3, qc4 = st.columns(4)
            with qc1:
                analytics_widgets.render_metric_card("Total Encounters", int(row.get('TOTAL_ENCOUNTERS', 0)))
            with qc2:
                analytics_widgets.render_metric_card("Readmission Rate", f"{row.get('READMISSION_RATE_PERCENT', 0)}%")
            with qc3:
                analytics_widgets.render_metric_card("Avg LOS (days)", f"{row.get('AVG_LENGTH_OF_STAY_DAYS', 0):.1f}")
            with qc4:
                analytics_widgets.render_metric_card("ED Throughput <4h", f"{row.get('ED_THROUGHPUT_RATE_PERCENT', 0)}%")
        else:
            st.info("No quality metrics available.")
    except Exception as e:
        st.warning(f"Quality metrics unavailable: {e}")