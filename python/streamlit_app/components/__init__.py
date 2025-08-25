"""
Components Module for TCH Patient 360 PoC

This module contains reusable UI components that can be used across different pages.
Components are designed to be modular, configurable, and follow consistent design patterns.

Components:
- patient_cards: Patient information display cards
- search_widgets: Search interface components 
- analytics_widgets: Data visualization and analytics components
- clinical_timeline: Timeline visualization for clinical events
"""

from .patient_cards import render_patient_card, render_patient_list
from .search_widgets import render_search_filters, render_search_results_controls
from .analytics_widgets import render_metric_card, render_chart_widget
from .clinical_timeline import render_timeline, render_event_details

__all__ = [
    'render_patient_card',
    'render_patient_list', 
    'render_search_filters',
    'render_search_results_controls',
    'render_metric_card',
    'render_chart_widget',
    'render_timeline',
    'render_event_details'
]