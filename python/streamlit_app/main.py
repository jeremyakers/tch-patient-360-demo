"""
Texas Children's Hospital Patient 360 PoC - Main Application
Enterprise-grade Streamlit application showcasing Snowflake's AI capabilities for healthcare analytics

This is the main entry point for the Patient 360 PoC, designed to demonstrate:
- Cortex Analyst for structured data queries  
- Cortex Search for unstructured clinical document search
- Cortex Agents for intelligent routing between data sources
- Multi-modal healthcare data integration
- Pediatric-focused analytics and insights
"""

import streamlit as st
import os
from datetime import datetime
from typing import Dict, Any

# Import page components
from page_modules import patient_search, patient_360, population_health, chat_interface, cohort_builder
from services import session_manager, data_service
from utils import config, helpers

# Configure the Streamlit page
st.set_page_config(
    page_title="TCH Patient 360 PoC",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://docs.snowflake.com/en/user-guide/ui-snowsight-cortex-analyst',
        'Report a bug': None,
        'About': "Texas Children's Hospital Patient 360 PoC - Powered by Snowflake Cortex AI"
    }
)

def initialize_session_state():
    """Initialize session state variables for the application"""
    if 'initialized' not in st.session_state:
        # Core application state
        st.session_state.initialized = True
        st.session_state.current_patient = None
        st.session_state.search_results = {}
        st.session_state.chat_history = []
        st.session_state.cohort_results = None
        
        # User preferences
        st.session_state.user_role = "physician"  # physician, nurse, admin, researcher
        st.session_state.department = "general_pediatrics"
        
        # Performance tracking
        st.session_state.query_cache = {}
        st.session_state.last_refresh = datetime.now()

def render_header():
    """Render the main application header with TCH branding"""
    col1, col2, col3 = st.columns([2, 4, 2])
    
    with col1:
        st.markdown("### 🏥 Texas Children's")
        st.caption("Largest Children's Hospital in the US")
    
    with col2:
        st.markdown("""
        <div style='text-align: center;'>
            <h1 style='color: #1f77b4; margin-bottom: 0;'>Patient 360 Analytics Platform</h1>
            <p style='color: #666; margin-top: 0; font-size: 16px;'>
                Powered by Snowflake Cortex AI • Healthcare PoC
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div style='text-align: right; padding-top: 20px;'>
            <div style='color: #888; font-size: 14px;'>
                🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}
            </div>
            <div style='color: #888; font-size: 12px;'>
                Role: {st.session_state.get('user_role', 'physician').title()}
            </div>
        </div>
        """, unsafe_allow_html=True)

def render_sidebar_navigation():
    """Render the sidebar navigation menu"""
    st.sidebar.markdown("## 🧭 Navigation")
    
    # Main navigation options
    page_options = {
        "🔍 Patient Search": "patient_search",
        "👤 Patient 360 View": "patient_360", 
        "📊 Population Health": "population_health",
        "🤖 AI Chat Interface": "chat_interface",
        "👥 Cohort Builder": "cohort_builder"
    }
    
    # Current page selection
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "patient_search"
    
    selected_page = st.sidebar.radio(
        "Select Page",
        options=list(page_options.keys()),
        index=list(page_options.values()).index(st.session_state.current_page)
    )
    
    st.session_state.current_page = page_options[selected_page]
    
    # Patient context sidebar
    render_patient_context_sidebar()

def render_patient_context_sidebar():
    """Show current patient context in sidebar"""
    st.sidebar.markdown("---")
    st.sidebar.markdown("## 👤 Patient Context")
    
    if st.session_state.current_patient:
        patient = st.session_state.current_patient
        st.sidebar.markdown(f"""
        **Current Patient:**
        - **Name:** {patient.get('full_name', 'Unknown')}
        - **MRN:** {patient.get('mrn', 'Unknown')}
        - **Age:** {patient.get('current_age', 'Unknown')} years
        - **Gender:** {patient.get('gender', 'Unknown')}
        """)
        
        if st.sidebar.button("Clear Patient Selection"):
            st.session_state.current_patient = None
            st.session_state.selected_patient_id = None
            st.session_state.current_page = "patient_search"
            st.rerun()
    else:
        st.sidebar.info("No patient currently selected")

    # Removed legacy "System Status" sidebar and related checks per request

def render_main_content():
    """Route to the appropriate page based on navigation selection"""
    page = st.session_state.current_page
    
    try:
        if page == "patient_search":
            patient_search.render()
        elif page == "patient_360":
            patient_360.render()
        elif page == "population_health":
            population_health.render()
        elif page == "chat_interface":
            chat_interface.render()
        elif page == "cohort_builder":
            cohort_builder.render()
        else:
            st.error(f"Unknown page: {page}")
            
    except Exception as e:
        st.error(f"Error rendering page '{page}': {str(e)}")
        st.markdown("Please try refreshing the page or contact support.")
        
        # Show error details in expander for debugging
        with st.expander("Error Details (for debugging)"):
            st.exception(e)

def render_footer():
    """Render the application footer"""
    st.markdown("---")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style='text-align: center; color: #666; font-size: 12px;'>
            <p>Texas Children's Hospital Patient 360 PoC</p>
            <p>Powered by <strong>Snowflake Cortex AI</strong> | 
               Built with <strong>Streamlit</strong> | 
               Designed for <strong>Pediatric Healthcare Excellence</strong></p>
        </div>
        """, unsafe_allow_html=True)

def main():
    """Main application entry point"""
    
    # Initialize application
    initialize_session_state()
    
    # Load configuration
    config.load_app_config()
    
    # Initialize data services
    session_manager.initialize_services()
    
    # Render application structure
    render_header()
    
    # Create main layout
    with st.container():
        # Sidebar navigation
        render_sidebar_navigation()
        
        # Main content area
        render_main_content()
        
        # Footer
        render_footer()

if __name__ == "__main__":
    main()