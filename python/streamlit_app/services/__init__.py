"""
Services Module for TCH Patient 360 PoC

This module contains the core business logic and data services for the application.
Services handle data access, business logic, and integration with Snowflake Cortex AI.
"""

from .session_manager import SessionManager
from .data_service import DataService  
from .cortex_analyst import CortexAnalystService
from .cortex_search import CortexSearchService
from .cortex_agents import CortexAgentsService

# Initialize service instances
session_manager = SessionManager()
data_service = DataService()
cortex_analyst = CortexAnalystService()
cortex_search = CortexSearchService()
cortex_agents = CortexAgentsService()

__all__ = [
    'session_manager',
    'data_service', 
    'cortex_analyst',
    'cortex_search',
    'cortex_agents',
    'SessionManager',
    'DataService',
    'CortexAnalystService', 
    'CortexSearchService',
    'CortexAgentsService'
]