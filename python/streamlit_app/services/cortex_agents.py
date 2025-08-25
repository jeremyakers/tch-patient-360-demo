"""
Cortex Agents Service for TCH Patient 360 PoC

This service provides intelligent query routing using Snowflake Cortex Agents.
Agents can automatically determine whether to use:
- Cortex Analyst for structured data queries (SQL generation)  
- Cortex Search for unstructured data searches (clinical documents)

Based on: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents
Example: https://raw.githubusercontent.com/Snowflake-Labs/sfguide-getting-started-with-cortex-agents/refs/heads/main/streamlit.py
"""

import json
import logging
from typing import Dict, List, Tuple, Optional, Any
import _snowflake
from snowflake.snowpark.context import get_active_session
try:
    import streamlit as st
except Exception:
    st = None

# Set up logging
logger = logging.getLogger(__name__)

class CortexAgentsService:
    """Service for interacting with Snowflake Cortex Agents via REST API."""
    
    def __init__(self):
        """Initialize the Cortex Agents service."""
        self.session = get_active_session()
        # Build full API endpoint URL
        # Note: In SiS, we use the relative path, the base URL is handled by _snowflake module
        self.api_endpoint = "/api/v2/cortex/agent:run"
        self.api_timeout = 50000  # milliseconds
        self.model = "claude-3-5-sonnet"
        
        # Healthcare-specific configuration  
        # Use the existing semantic model YAML file
        self.semantic_model_file = "@TCH_PATIENT_360_POC.AI_ML.SEMANTIC_MODEL_STAGE/semantic_model.yaml"
        # Chat-specific semantic model (identical except AI_FILTER verified query removed)
        self.semantic_model_file_chat = "@TCH_PATIENT_360_POC.AI_ML.SEMANTIC_MODEL_STAGE/semantic_model_chat.yaml"
        self.search_services = {
            "clinical_notes": "TCH_PATIENT_360_POC.AI_ML.CLINICAL_NOTES_SEARCH",
            "radiology": "TCH_PATIENT_360_POC.AI_ML.RADIOLOGY_REPORTS_SEARCH", 
            "clinical_docs": "TCH_PATIENT_360_POC.AI_ML.CLINICAL_DOCUMENTATION_SEARCH"
        }
        
        # Healthcare system prompt
        self.system_prompt = self._get_healthcare_system_prompt()
        
        logger.info("CortexAgentsService initialized")
    
    def _get_healthcare_system_prompt(self) -> str:
        """Get the healthcare-specific system prompt for the agent."""
        return """You are an AI assistant specialized in pediatric healthcare data analysis for Texas Children's Hospital.

CAPABILITIES:
- Analyze structured patient data (demographics, encounters, lab results, medications)
- Search clinical documents (notes, radiology reports, discharge summaries)
- Provide evidence-based insights for pediatric care (ages 0-21)
- Support clinical decision-making with data-driven recommendations

GUIDELINES:
- Always prioritize patient safety and privacy
- Provide clear, actionable insights for healthcare providers
- Reference specific data sources and timeframes in your responses
- Highlight any limitations or data quality concerns
- Use pediatric-appropriate medical terminology and reference ranges
- When showing patient data, always respect HIPAA guidelines

TOOL ROUTING RULES (CRITICAL):
- Use cortex_analyst_text_to_sql ONLY for questions that can be answered from structured tables via SQL (e.g., counts, joins, aggregates on conformed/presentation tables).
- Use cortex_search for questions that require scanning or citing UNSTRUCTURED clinical text (notes, reports, documents, mentions, excerpts, who/what/when in a document).
- If the user asks about “notes”, “mentioned in notes”, “report text”, “discharge summary”, “radiology report”, or similar document-centric phrasing, favor cortex_search over SQL.
- DO NOT generate SQL that applies AI_FILTER over large unstructured text corpora when a search is requested. Running AI_FILTER over all notes is slow; prefer cortex_search which is optimized and returns citations.
- When the user intent is ambiguous between structured and unstructured, prefer cortex_search first to gather relevant documents and citations; include SQL only if clearly needed for structured roll-ups.
- When using cortex_search, include concise explanatory text and return citations; when using cortex_analyst_text_to_sql, include the SQL you generated.

STRICT AVOIDANCE FOR UNSTRUCTURED QUERIES:
- For questions about unstructured clinical text, DO NOT select from or reference SQL tables/views such as clinical_documentation, clinical_documentation_search_base, or any table exposing full_searchable_text. Use cortex_search instead and answer with citations.

WHEN TO USE SQL VS SEARCH (EXPLICIT EXAMPLES):
- SQL (cortex_analyst_text_to_sql): questions about structured fields like chief_complaint_category, diagnosis codes, encounter counts, lab values, medication activity, provider/department rollups.
- Cortex Search: questions asking what is “mentioned in notes,” “document says…,” “what does the report state,” or to find excerpts in documents.

DATA SOURCES AVAILABLE:
- Patient demographics and insurance information
- Encounter history and department visits
- Laboratory results and vital signs
- Medication orders and administration records
- Clinical notes and provider documentation
- Radiology reports and imaging studies
- Diagnosis codes and chronic conditions

EXAMPLE QUERIES YOU CAN HANDLE:
- "Show me asthma patients aged 5-12 with recent ER visits"
- "Find diabetes patients overdue for HbA1c testing"
- "What are the most common diagnoses in our catchment area?"
- "Search for clinical notes mentioning medication allergies"
- "Analyze readmission patterns for heart conditions"
- "Find patients with elevated BMI who need nutrition counseling"

Always provide context about the data timeframe and any limitations of your analysis."""
        
        # NOTE TO THE AGENT (Operational directives)
        # - When you choose the cortex_search tool, ALWAYS include 'MRN' and 'file_path' in the input 'columns' array.
        # - Prefer 'MRN' (uppercase) for patient identification. If both are available, return 'MRN'.
        # - Respect the provided limit; do not exceed it.

    def _build_agent_payload(self, user_message: str, conversation_history: List[Dict]) -> Dict:
        """Build the payload for the Cortex Agent API call."""
        
        # Build conversation messages
        messages = []
        
        # Add system prompt
        messages.append({
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": self.system_prompt
                }
            ]
        })
        
        # Add conversation history
        for msg in conversation_history:
            messages.append(msg)
        
        # Add current user message
        messages.append({
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": user_message
                }
            ]
        })
        
        # Build payload with tools configuration
        payload = {
            "model": self.model,
            "messages": messages,
            "tools": [
                {
                    "tool_spec": {
                        "type": "cortex_analyst_text_to_sql",
                        "name": "healthcare_analyst"
                    }
                },
                {
                    "tool_spec": {
                        "type": "cortex_search",
                        "name": "clinical_document_search"
                    }
                }
            ],
            "tool_resources": {
                "healthcare_analyst": {
                    "semantic_model_file": getattr(self, 'semantic_model_file_chat', self.semantic_model_file)
                },
                "clinical_document_search": {
                    "name": self.search_services["clinical_docs"],
                    "max_results": int(st.session_state.get('cortex_search_max_results', 50)) if 'st' in globals() else 50,
                    "id_column": "file_path",
                    "title_column": "MRN"
                }
            }
        }
        
        return payload
    
    def send_message(self, user_message: str, conversation_history: List[Dict] = None) -> Optional[Dict]:
        """Send a message to the Cortex Agent and get response with enhanced debugging."""
        
        if conversation_history is None:
            conversation_history = []
        
        try:
            logger.info(f"=== CORTEX AGENTS DEBUG START ===")
            logger.info(f"User message: {user_message}")
            logger.info(f"Conversation history length: {len(conversation_history)}")
            
            # Build the API payload
            payload = self._build_agent_payload(user_message, conversation_history)
            logger.info(f"Request payload structure: {json.dumps(payload, indent=2)}")
            # Expose payload for Streamlit UI debugging
            try:
                if st is not None:
                    import copy as _copy
                    st.session_state['last_agents_request_payload'] = _copy.deepcopy(payload)
            except Exception:
                pass
            
            # Log configuration details
            logger.info(f"API endpoint: {self.api_endpoint}")
            logger.info(f"Model: {self.model}")
            logger.info(f"Semantic model file: {self.semantic_model_file}")
            logger.info(f"Search services: {self.search_services}")
            
            # Make the API call using positional arguments
            logger.info("Making API call to Cortex Agents...")
            # Ensure cortex_search tool input includes MRN and file_path columns
            # No unsupported columns injection; rely on id_column per docs

            response = _snowflake.send_snow_api_request(
                "POST",                              # method
                self.api_endpoint,                   # endpoint 
                {"Content-Type": "application/json"}, # headers
                {},                                  # params
                payload,                             # body - pass as dict, not JSON string
                None,                                # request_guid
                30000                                # timeout_ms
            )
            
            logger.info(f"Raw response type: {type(response)}")
            logger.info(f"Raw response attributes: {dir(response)}")
            
            # Enhanced response handling with detailed debugging
            if hasattr(response, 'status'):
                logger.info(f"Response status: {response.status}")
                if response.status != 200:
                    error_reason = getattr(response, 'reason', 'Unknown reason')
                    error_content = getattr(response, 'content', 'No content')
                    logger.error(f"API Error - Status: {response.status}, Reason: {error_reason}")
                    logger.error(f"Error content: {error_content}")
                    return {
                        "error": f"HTTP Error: {response.status} - {error_reason}",
                        "status_code": response.status,
                        "error_content": str(error_content),
                        "debug_info": {
                            "endpoint": self.api_endpoint,
                            "payload_keys": list(payload.keys()),
                            "semantic_model": self.semantic_model_file
                        }
                    }
            
            # Parse response content
            if hasattr(response, 'content'):
                logger.info(f"Response content type: {type(response.content)}")
                logger.info(f"Response content (first 500 chars): {str(response.content)[:500]}")
                
                try:
                    if isinstance(response.content, str):
                        response_content = json.loads(response.content)
                    else:
                        response_content = response.content
                    
                    logger.info(f"Parsed response structure: {json.dumps(response_content, indent=2) if isinstance(response_content, dict) else str(response_content)}")
                    
                    # Check for error events in the response
                    if isinstance(response_content, list):
                        for item in response_content:
                            if isinstance(item, dict) and item.get('event') == 'error':
                                error_data = item.get('data', {})
                                error_message = error_data.get('message', 'Unknown error')
                                error_code = error_data.get('code', 'Unknown code')
                                logger.error(f"Cortex Error Detected - Code: {error_code}, Message: {error_message}")
                                logger.error(f"Full error data: {json.dumps(error_data, indent=2)}")
                                
                                # Return structured error response
                                return {
                                    "error": f"Cortex Analyst Error [{error_code}]: {error_message}",
                                    "error_code": error_code,
                                    "request_id": error_data.get('request_id', 'unknown'),
                                    "full_traceback": json.dumps(error_data, indent=2),
                                    "debug_info": {
                                        "error_event": item,
                                        "full_response": response_content,
                                        "semantic_model": self.semantic_model_file
                                    }
                                }
                    
                    logger.info("=== CORTEX AGENTS DEBUG END ===")
                    return response_content
                    
                except (json.JSONDecodeError, AttributeError) as e:
                    logger.error(f"JSON parsing failed: {e}")
                    logger.error(f"Raw content that failed to parse: {response.content}")
                    return {
                        "error": "Failed to parse API response as JSON",
                        "json_error": str(e),
                        "raw_response": str(response.content),
                        "debug_info": {
                            "content_type": type(response.content).__name__,
                            "content_length": len(str(response.content))
                        }
                    }
            else:
                # Handle case where response doesn't have content attribute
                logger.info(f"Response object (no content attribute): {response}")
                logger.info("=== CORTEX AGENTS DEBUG END ===")
                return response
                
        except Exception as e:
            logger.error(f"=== CORTEX AGENTS ERROR ===")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Exception message: {str(e)}")
            logger.error(f"Exception details: {repr(e)}")
            
            # Import traceback for full stack trace
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            return {
                "error": f"API call failed: {str(e)}",
                "exception_type": type(e).__name__,
                "debug_info": {
                    "endpoint": self.api_endpoint,
                    "user_message": user_message[:100] + "..." if len(user_message) > 100 else user_message,
                    "semantic_model": self.semantic_model_file,
                    "search_services": list(self.search_services.keys())
                },
                "full_traceback": traceback.format_exc()
            }
    
    def process_agent_response(self, response: Dict) -> Tuple[str, Optional[str], List[Dict]]:
        """
        Process the agent response and extract text, SQL, and citations.
        
        Returns:
            Tuple of (response_text, sql_query, citations)
        """
        
        response_text = ""
        sql_query = None
        citations = []
        
        if not response or "error" in response:
            error_msg = response.get("error", "Unknown error") if response else "No response received"
            return f"Error: {error_msg}", None, []
        
        try:
            # Process streaming response format
            if isinstance(response, dict) and "choices" in response:
                # Handle standard response format
                choice = response["choices"][0] if response["choices"] else {}
                message = choice.get("message", {})
                content = message.get("content", [])
                
                for content_item in content:
                    content_type = content_item.get("type", "")
                    
                    if content_type == "text":
                        response_text += content_item.get("text", "")
                    
                    elif content_type == "tool_results":
                        tool_results = content_item.get("tool_results", {})
                        if "content" in tool_results:
                            for result in tool_results["content"]:
                                if result.get("type") == "json":
                                    json_data = result.get("json", {})
                                    
                                    # Extract text response
                                    response_text += json_data.get("text", "")
                                    
                                    # Extract SQL query
                                    if "sql" in json_data:
                                        sql_query = json_data["sql"]
                                    
                                    # Extract search results/citations
                                    search_results = json_data.get("searchResults", [])
                                    for search_result in search_results:
                                        sc = search_result.get("score", 0) or 0
                                        try:
                                            sc = float(sc)
                                        except Exception:
                                            sc = 0.0
                                        citations.append({
                                            "source_id": search_result.get("source_id", ""),
                                            "doc_id": search_result.get("doc_id", ""),
                                            "document_type": search_result.get("document_type", ""),
                                            "relevance_score": sc
                                        })
            
            # Process raw JSON string response (from debug output format)
            elif isinstance(response, dict) and "content" in response:
                # Parse the JSON string content
                try:
                    content_str = response["content"]
                    if isinstance(content_str, str):
                        events = json.loads(content_str)
                    else:
                        events = content_str
                    
                    # Process the list of events
                    if isinstance(events, list):
                        for event in events:
                            if event.get("event") == "message.delta":
                                data = event.get("data", {})
                                delta = data.get("delta", {})
                                
                                for content_item in delta.get("content", []):
                                    content_type = content_item.get("type")
                                    
                                    if content_type == "tool_results":
                                        tool_results = content_item.get("tool_results", {})
                                        if "content" in tool_results:
                                            for result in tool_results["content"]:
                                                if result.get("type") == "json":
                                                    json_data = result.get("json", {})
                                                    response_text += json_data.get("text", "")
                                                    search_results = json_data.get("searchResults", [])
                                                    for search_result in search_results:
                                                        # Extract the file_path from doc_id since id_column maps file_path to doc_id
                                                        doc_id_value = search_result.get("doc_id", "")
                                                        sc = search_result.get("score", 0) or 0
                                                        try:
                                                            sc = float(sc)
                                                        except Exception:
                                                            sc = 0.0
                                                        citations.append({
                                                            "source_id": search_result.get("source_id", ""),
                                                            "doc_id": doc_id_value,
                                                            "document_id": search_result.get("document_id", ""),
                                                            "file_id": search_result.get("file_id", ""),
                                                            "note_id": search_result.get("note_id", ""),
                                                            "file_path": doc_id_value,  # Use doc_id as file_path since id_column maps file_path to doc_id
                                                            "mrn": search_result.get("mrn", "") or search_result.get("MRN", ""),
                                                            "patient_name": search_result.get("patient_name", ""),
                                                            "document_type": search_result.get("document_type", ""),
                                                            "document_date": search_result.get("document_date", ""),
                                                            "author": search_result.get("author", ""),
                                                            "department": search_result.get("department", ""),
                                                            "source_system": search_result.get("source_system", ""),
                                                            "text": search_result.get("text", "")[:200] + "..." if len(search_result.get("text", "")) > 200 else search_result.get("text", ""),
                                                            "relevance_score": sc
                                                        })
                                                    
                                                    if "sql" in json_data:
                                                        sql_query = json_data["sql"]
                                    
                                    elif content_type == "text":
                                        text_content = content_item.get("text", "")
                                        response_text += text_content
                                        logger.debug(f"Added text content: {text_content[:50]}...")
                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(f"Failed to parse events from content: {e}")
                    
            # Process event-based streaming format (if applicable)
            elif isinstance(response, list):
                for event in response:
                    if event.get("event") == "message.delta":
                        data = event.get("data", {})
                        delta = data.get("delta", {})
                        
                        for content_item in delta.get("content", []):
                            content_type = content_item.get("type")
                            
                            if content_type == "tool_results":
                                tool_results = content_item.get("tool_results", {})
                                if "content" in tool_results:
                                    for result in tool_results["content"]:
                                        if result.get("type") == "json":
                                            json_data = result.get("json", {})
                                            response_text += json_data.get("text", "")
                                            
                                            search_results = json_data.get("searchResults", [])
                                            for search_result in search_results:
                                                sc = search_result.get("score", 0) or 0
                                                try:
                                                    sc = float(sc)
                                                except Exception:
                                                    sc = 0.0
                                                citations.append({
                                                    "source_id": search_result.get("source_id", ""),
                                                    "doc_id": search_result.get("doc_id", ""),
                                                    "document_id": search_result.get("document_id", ""),
                                                    "file_id": search_result.get("file_id", ""),
                                                    "note_id": search_result.get("note_id", ""),
                                                    "file_path": search_result.get("file_path", ""),  # This is the key field!
                                                    "mrn": search_result.get("mrn", "") or search_result.get("MRN", ""),
                                                    "patient_name": search_result.get("patient_name", ""),
                                                    "document_type": search_result.get("document_type", ""),
                                                    "document_date": search_result.get("document_date", ""),
                                                    "author": search_result.get("author", ""),
                                                    "department": search_result.get("department", ""),
                                                    "source_system": search_result.get("source_system", ""),
                                                    "text": search_result.get("text", ""),
                                                    "relevance_score": sc
                                                })
                                            
                                            if "sql" in json_data:
                                                sql_query = json_data["sql"]
                            
                            elif content_type == "text":
                                text_content = content_item.get("text", "")
                                response_text += text_content
                                logger.debug(f"Added text content: {text_content[:50]}...")
            
        except Exception as e:
            logger.error(f"Error processing agent response: {e}")
            return f"Error processing response: {str(e)}", None, []
        
        # Clean up response text
        if response_text:
            # Clean up citation markers
            response_text = response_text.replace("【†", "[")
            response_text = response_text.replace("†】", "]")
        
        return response_text, sql_query, citations
    
    def execute_sql_query(self, sql_query: str) -> Optional[Any]:
        """Execute a SQL query and return the results."""
        
        if not sql_query or not sql_query.strip():
            return None
        
        try:
            logger.info(f"Executing SQL query: {sql_query[:100]}...")
            
            # Remove semicolons and execute
            clean_query = sql_query.replace(';', '')
            result = self.session.sql(clean_query)
            
            logger.info("SQL query executed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error executing SQL query: {e}")
            return None
    
    def search_documents_for_patient(self, mrn: str, search_query: str = "*", document_types: List[str] = None, max_results: int = 10) -> Tuple[str, List[Dict]]:
        """
        Search clinical documents for a specific patient using Cortex Agents.
        
        Args:
            patient_id: The patient ID to search for
            search_query: Natural language search query
            document_types: List of document types to search (optional)
            max_results: Maximum number of results to return
            
        Returns:
            Tuple of (agent_response_text, citations_list)
        """
        
        # Build a focused search message for document search
        doc_types_str = ", ".join(document_types) if document_types else "all clinical documents"
        subject_label = f"MRN {mrn}"
        
        if not search_query or search_query.strip() == "" or search_query == "*":
            search_message = f"Find all {doc_types_str} for {subject_label}. Provide a summary of the available documents and their key content."
        else:
            search_message = f"Search {doc_types_str} for {subject_label} related to: {search_query}. Provide relevant excerpts and explain what was found."
        
        # Build payload specifically for document search (Cortex Search only)
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system", 
                    "content": [
                        {
                            "type": "text",
                            "text": f"""You are a clinical document search assistant. Your task is to search through clinical documents and provide relevant excerpts with citations.

SEARCH INSTRUCTIONS:
- Search for documents for the specified patient MRN
- Focus on the query: \"{search_query}\"
- Provide relevant excerpts from matching documents
- Always include citations with document details
- Explain what you found and why it's relevant

RESPONSE FORMAT:
- Start with a brief summary of what you found
- Include relevant excerpts from documents
- Provide clear citations for each piece of information (e.g., 【†1†】)
- Explain the clinical relevance when appropriate"""
                        }
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text", 
                            "text": search_message
                        }
                    ]
                }
            ],
            "tools": [
                {
                    "tool_spec": {
                        "type": "cortex_search",
                        "name": "clinical_document_search"
                    }
                }
            ],
            "tool_resources": {
                "clinical_document_search": {
                    "name": self.search_services["clinical_docs"],
                    "max_results": max_results,
                    "id_column": "file_path",
                    "title_column": "MRN",
                    "filter": {"@eq": {"MRN": mrn}}
                }
            }
        }
        
        try:
            logger.info(f"Searching documents for {subject_label} with query: {search_query}")
            logger.info(f"Document search payload: {json.dumps(payload, indent=2)}")
            
            # Make the API call
            response = _snowflake.send_snow_api_request(
                "POST",
                self.api_endpoint,
                {"Content-Type": "application/json"},
                {},
                payload,
                None,
                30000
            )
            
            logger.info(f"Document search response type: {type(response)}")
            try:
                logger.info(f"Document search response attributes: {dir(response)}")
            except Exception:
                pass

            # Normalize status handling for dict or object responses
            if isinstance(response, dict) and 'status' in response:
                status_code = response.get('status')
                logger.info(f"Document search response status: {status_code}")
                if status_code != 200:
                    error_reason = response.get('reason', 'Unknown reason')
                    error_content = response.get('content', 'No content')
                    logger.error(f"Document search API error - Status: {status_code}, Reason: {error_reason}")
                    logger.error(f"Error content: {str(error_content)[:500]}")
                    return f"Error: HTTP {status_code} - {error_reason}. Content: {str(error_content)[:200]}", []
            elif hasattr(response, 'status'):
                logger.info(f"Document search response status: {getattr(response, 'status', None)}")
                if getattr(response, 'status', None) != 200:
                    error_reason = getattr(response, 'reason', 'Unknown reason')
                    error_content = getattr(response, 'content', 'No content')
                    logger.error(f"Document search API error - Status: {response.status}, Reason: {error_reason}")
                    logger.error(f"Error content: {error_content}")
                    return f"Error: HTTP {response.status} - {error_reason}. Content: {str(error_content)[:200]}", []

            # Parse response content (support dict key or object attribute)
            if isinstance(response, dict) and 'content' in response:
                content_value = response.get('content')
                logger.info(f"Document search response content type: {type(content_value)}")
                logger.info(f"Document search response content (first 500 chars): {str(content_value)[:500]}")
                response_payload = {"content": content_value}
            elif hasattr(response, 'content'):
                content_value = getattr(response, 'content')
                logger.info(f"Document search response content type: {type(content_value)}")
                logger.info(f"Document search response content (first 500 chars): {str(content_value)[:500]}")
                response_payload = {"content": content_value}
            else:
                logger.error("Document search response missing content (neither attribute nor key)")
                logger.error(f"Response object: {response}")
                return f"Error: No response content. Response: {str(response)}", []

            try:
                logger.info("Processing streaming response content...")
                response_text, _, citations = self.process_agent_response(response_payload)
                logger.info(f"Extracted response_text length: {len(response_text) if response_text else 0}")
                logger.info(f"Extracted citations count: {len(citations) if citations else 0}")

                if not response_text and not citations:
                    return "Error: No meaningful response extracted from agent", []

                return response_text, citations
            except Exception as e:
                logger.error(f"Failed to process document search response: {e}")
                logger.error(f"Raw content provided: {str(content_value)[:500]}")
                return f"Error: Failed to process search results - {str(e)}", []
            
        except Exception as e:
            logger.error(f"Document search failed with exception: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return f"Error: Document search failed - {str(e)}. Check logs for details.", []

    def get_citation_content(self, citation: Dict) -> Optional[str]:
        """Retrieve the full content for a citation."""
        
        doc_id = citation.get("doc_id", "")
        if not doc_id:
            return None
        
        try:
            # Try to get content from different possible tables
            queries = [
                f"SELECT raw_content FROM RAW_DATA.CLINICAL_NOTES_RAW WHERE file_id = '{doc_id}' OR filename LIKE '%{doc_id}%'",
                f"SELECT raw_content FROM RAW_DATA.RADIOLOGY_REPORTS_RAW WHERE file_id = '{doc_id}' OR filename LIKE '%{doc_id}%'"
            ]
            
            for query in queries:
                try:
                    result = self.session.sql(query)
                    result_df = result.to_pandas()
                    
                    if not result_df.empty:
                        return result_df.iloc[0, 0]
                        
                except Exception:
                    continue
            
            logger.warning(f"No content found for citation doc_id: {doc_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving citation content: {e}")
            return None