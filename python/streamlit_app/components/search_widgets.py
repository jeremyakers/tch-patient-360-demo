"""
Search Widgets Component for TCH Patient 360 PoC

Reusable search interface components including search filters,
results controls, and advanced search functionality.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, date, timedelta
import logging

logger = logging.getLogger(__name__)

def render_search_filters(filter_config: Dict[str, Any] = None, key: str = None) -> Dict[str, Any]:
    """
    Render advanced search filters for patient data
    
    Args:
        filter_config: Configuration for available filters
        key: Unique key for the component
        
    Returns:
        Dictionary containing selected filter values
    """
    try:
        filters = {}
        
        with st.expander("🔍 Advanced Search Filters", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                # Demographics filters
                st.markdown("**Demographics**")
                
                age_range = st.slider(
                    "Age Range",
                    min_value=0,
                    max_value=21,
                    value=(0, 21),
                    key=f"age_filter_{key}" if key else "age_filter"
                )
                filters['age_min'] = age_range[0]
                filters['age_max'] = age_range[1]
                
                gender = st.multiselect(
                    "Gender",
                    options=["Male", "Female"],
                    default=["Male", "Female"],
                    key=f"gender_filter_{key}" if key else "gender_filter"
                )
                filters['gender'] = gender
                
                insurance = st.multiselect(
                    "Insurance Type",
                    options=["Commercial", "Medicaid", "Medicare", "Self-Pay", "Other"],
                    key=f"insurance_filter_{key}" if key else "insurance_filter"
                )
                filters['insurance'] = insurance
                
            with col2:
                # Clinical filters
                st.markdown("**Clinical Criteria**")
                
                risk_level = st.multiselect(
                    "Risk Level",
                    options=["High", "Medium", "Low"],
                    key=f"risk_filter_{key}" if key else "risk_filter"
                )
                filters['risk_level'] = risk_level
                
                departments = st.multiselect(
                    "Departments",
                    options=[
                        "Emergency", "Cardiology", "Pulmonology", 
                        "Endocrinology", "Neurology", "Oncology",
                        "General Pediatrics", "Surgery"
                    ],
                    key=f"dept_filter_{key}" if key else "dept_filter"
                )
                filters['departments'] = departments
                
                # Date range filter
                encounter_range = st.date_input(
                    "Encounter Date Range",
                    value=(date.today() - timedelta(days=365), date.today()),
                    max_value=date.today(),
                    key=f"date_filter_{key}" if key else "date_filter"
                )
                if isinstance(encounter_range, tuple) and len(encounter_range) == 2:
                    filters['encounter_start'] = encounter_range[0]
                    filters['encounter_end'] = encounter_range[1]
                else:
                    filters['encounter_start'] = None
                    filters['encounter_end'] = None
            
            # Geographic filters
            st.markdown("**Geographic**")
            col3, col4 = st.columns(2)
            
            with col3:
                zip_codes = st.text_input(
                    "ZIP Codes (comma separated)",
                    placeholder="77001, 77002, 77003",
                    key=f"zip_filter_{key}" if key else "zip_filter"
                )
                if zip_codes:
                    filters['zip_codes'] = [z.strip() for z in zip_codes.split(',')]
                else:
                    filters['zip_codes'] = []
                    
            with col4:
                distance = st.number_input(
                    "Max Distance (miles)",
                    min_value=0,
                    max_value=100,
                    value=25,
                    key=f"distance_filter_{key}" if key else "distance_filter"
                )
                filters['max_distance'] = distance
        
        return filters
        
    except Exception as e:
        logger.error(f"Error rendering search filters: {e}")
        st.error("Error displaying search filters")
        return {}

def render_search_results_controls(results_count: int = 0, 
                                 total_count: int = 0,
                                 page_size: int = 25,
                                 current_page: int = 1,
                                 key: str = None) -> Dict[str, Any]:
    """
    Render search results controls including pagination, sorting, and export
    
    Args:
        results_count: Number of results shown
        total_count: Total number of results available
        page_size: Number of results per page
        current_page: Current page number
        key: Unique key for the component
        
    Returns:
        Dictionary containing control settings
    """
    try:
        controls = {}
        
        # Results summary and controls
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        
        with col1:
            if total_count > 0:
                start_idx = (current_page - 1) * page_size + 1
                end_idx = min(current_page * page_size, total_count)
                st.markdown(f"**Showing {start_idx}-{end_idx} of {total_count:,} patients**")
            else:
                st.markdown("**No patients found**")
        
        with col2:
            # Sort options
            sort_by = st.selectbox(
                "Sort by",
                options=["Last Name", "Age", "Last Visit", "Risk Level"],
                key=f"sort_filter_{key}" if key else "sort_filter"
            )
            controls['sort_by'] = sort_by
            
        with col3:
            # Sort direction
            sort_order = st.selectbox(
                "Order",
                options=["Ascending", "Descending"],
                index=0,
                key=f"order_filter_{key}" if key else "order_filter"
            )
            controls['sort_order'] = sort_order
            
        with col4:
            # Page size
            page_size_new = st.selectbox(
                "Per Page",
                options=[10, 25, 50, 100],
                index=1,  # Default to 25
                key=f"pagesize_filter_{key}" if key else "pagesize_filter"
            )
            controls['page_size'] = page_size_new
        
        # Pagination controls
        if total_count > page_size:
            total_pages = (total_count - 1) // page_size + 1
            
            st.divider()
            
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                if st.button("⬅️ Previous", disabled=(current_page <= 1)):
                    controls['page_action'] = 'previous'
                    
            with col2:
                # Page selector
                page = st.number_input(
                    f"Page (1-{total_pages})",
                    min_value=1,
                    max_value=total_pages,
                    value=current_page,
                    key=f"page_selector_{key}" if key else "page_selector"
                )
                controls['page'] = page
                
            with col3:
                if st.button("Next ➡️", disabled=(current_page >= total_pages)):
                    controls['page_action'] = 'next'
        
        # Export options
        if total_count > 0:
            st.divider()
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📊 Export to Excel"):
                    controls['export'] = 'excel'
                    
            with col2:
                if st.button("📄 Export to CSV"):
                    controls['export'] = 'csv'
                    
            with col3:
                if st.button("🔗 Share Results"):
                    controls['export'] = 'share'
        
        return controls
        
    except Exception as e:
        logger.error(f"Error rendering search results controls: {e}")
        st.error("Error displaying search controls")
        return {}

def render_search_suggestions(query: str, suggestions: List[str] = None) -> Optional[str]:
    """
    Render search suggestions and auto-complete
    
    Args:
        query: Current search query
        suggestions: List of suggested search terms
        
    Returns:
        Selected suggestion or None
    """
    try:
        if not suggestions:
            # Default suggestions for healthcare search
            suggestions = [
                "Asthma patients aged 5-12",
                "High-risk cardiac patients",
                "Emergency department visits last 30 days",
                "Diabetes patients overdue for HbA1c",
                "Patients with multiple chronic conditions",
                "Recent surgical patients",
                "Patients missing vaccinations"
            ]
        
        if query and len(query) >= 2:
            # Filter suggestions based on query
            filtered_suggestions = [s for s in suggestions if query.lower() in s.lower()]
            
            if filtered_suggestions:
                st.markdown("**Suggestions:**")
                for suggestion in filtered_suggestions[:5]:  # Show top 5
                    if st.button(f"🔍 {suggestion}", key=f"suggestion_{suggestion}"):
                        return suggestion
        
        return None
        
    except Exception as e:
        logger.error(f"Error rendering search suggestions: {e}")
        return None