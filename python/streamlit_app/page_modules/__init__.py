"""
Pages Module for TCH Patient 360 PoC

This module contains the main page components for the Patient 360 application.
Each page is a self-contained module that handles a specific aspect of the user experience.

Pages:
- patient_search: Patient lookup and search functionality
- patient_360: Comprehensive patient dashboard and 360-degree view
- population_health: Population-level analytics and insights
- chat_interface: Natural language interface powered by Cortex Agents
- cohort_builder: Advanced cohort creation and analysis tools
"""

from .patient_search import render_patient_search
from .patient_360 import render_patient_360
from .population_health import render_population_health
from .chat_interface import render_chat_interface
from .cohort_builder import render_cohort_builder

__all__ = [
    'render_patient_search',
    'render_patient_360', 
    'render_population_health',
    'render_chat_interface',
    'render_cohort_builder'
]