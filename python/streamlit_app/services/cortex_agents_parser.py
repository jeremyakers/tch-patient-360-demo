"""
Parser for Cortex Agents v2 API responses.
Handles the complex nested structure of streaming responses.
"""

import json
import logging
import re
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

def parse_v2_agent_response(response: Dict) -> Tuple[str, Optional[str], List[Dict], List[str]]:
    """
    Parse the v2 agent response with its complex nested structure.
    
    Returns:
        Tuple of (response_text, sql_query, citations, thinking_steps)
    """
    response_text = ""
    sql_query = None
    citations = []
    thinking_steps = []
    
    try:
        # Get the content string
        content_str = response.get("content", "")
        if not content_str:
            return "", None, [], []
            
        # Parse the JSON events array
        events = []
        if isinstance(content_str, str):
            try:
                events = json.loads(content_str)
            except json.JSONDecodeError:
                logger.error("Failed to parse content as JSON")
                return "", None, [], []
        elif isinstance(content_str, list):
            events = content_str
            
        # Process each event
        for event in events:
            event_type = event.get("event")
            
            # Main response event contains all the content
            if event_type == "response":
                data = event.get("data", {})
                content_items = data.get("content", [])
                
                # Process each content item
                for item in content_items:
                    item_type = item.get("type")
                    
                    # Extract thinking steps
                    if item_type == "thinking":
                        thinking_obj = item.get("thinking", {})
                        if isinstance(thinking_obj, dict):
                            thinking_text = thinking_obj.get("text", "")
                        else:
                            thinking_text = item.get("text", "")
                        
                        if thinking_text:
                            thinking_steps.append(thinking_text)
                            logger.debug(f"Found thinking step: {thinking_text[:100]}...")
                    
                    # Extract tool use (for tracking what tools were called)
                    elif item_type == "tool_use":
                        tool_use = item.get("tool_use", {})
                        tool_type = tool_use.get("type", "")
                        tool_name = tool_use.get("name", "")
                        logger.debug(f"Tool used: {tool_name} ({tool_type})")
                    
                    # Extract tool results (contains SQL and search results)
                    elif item_type == "tool_result":
                        tool_result = item.get("tool_result", {})
                        tool_content = tool_result.get("content", [])
                        
                        for content_item in tool_content:
                            if content_item.get("type") == "json":
                                json_data = content_item.get("json", {})
                                
                                # Extract SQL query
                                if "sql" in json_data and not sql_query:  # Take first SQL
                                    sql_query = json_data["sql"]
                                    logger.debug(f"Found SQL: {sql_query[:200]}...")
                                
                                # Extract search results
                                search_results = json_data.get("search_results", [])
                                if not search_results:
                                    search_results = json_data.get("searchResults", [])
                                
                                for result in search_results[:5]:  # Limit to first 5
                                    citations.append({
                                        "text": result.get("text", "")[:200] + "...",
                                        "doc_id": result.get("id", result.get("doc_id", "")),
                                        "score": float(result.get("score", 0))
                                    })
                    
                    # Extract final text response (last text item is the response)
                    elif item_type == "text":
                        text = item.get("text", "")
                        if text and ("I attempted" in text or "Based on" in text or len(text) > 100):
                            response_text = text  # Replace, don't append
                            logger.debug(f"Found response text: {text[:200]}...")
            
            # Handle error events
            elif event_type == "error":
                error_data = event.get("data", {})
                error_msg = error_data.get("message", "Unknown error")
                logger.error(f"Agent error: {error_msg}")
                if not response_text:
                    response_text = f"Error: {error_msg}"
    
    except Exception as e:
        logger.error(f"Error parsing v2 response: {e}")
        import traceback
        logger.debug(traceback.format_exc())
    
    # Log what we extracted
    logger.info(f"Parsed response - Text: {len(response_text)} chars, SQL: {bool(sql_query)}, "
                f"Thinking steps: {len(thinking_steps)}, Citations: {len(citations)}")
    
    return response_text, sql_query, citations, thinking_steps
