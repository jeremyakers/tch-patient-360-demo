"""
Parser for Cortex Agents v2 API responses.
Handles the complex nested structure of streaming responses.
"""

import json
import logging
import re
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

def parse_sse_content(sse_str: str) -> List[Dict]:
    """
    Parse SSE (Server-Sent Events) format content into a list of events.
    
    SSE format for Cortex Agents looks like:
    event: message.delta
    data: {"id":"msg_001","object":"message.delta","delta":{"content":[{"index":0,"type":"text","text":"fragment"}]}}
    
    The API sends many small text fragments that need to be accumulated.
    
    Args:
        sse_str: SSE formatted string
        
    Returns:
        List of parsed event dictionaries
    """
    # For document search, we just need to acknowledge that we got a response
    # The actual content is incomplete streaming data that we can't fully parse
    
    # Check if this looks like SSE streaming data
    if "event:" in sse_str and ("message.delta" in sse_str or "response.text.annotation" in sse_str):
        logger.info("Detected SSE streaming response for document search")
        
        # Extract any complete text fragments and annotations
        accumulated_text = ""
        citations = []
        lines = sse_str.replace("\\n", "\n").split("\n")
        
        current_event = None
        
        for line in lines:
            # Parse event type
            if line.startswith("event:"):
                current_event = line[6:].strip()
                logger.debug(f"Found SSE event: {current_event}")
            elif line.startswith("data:"):
                data_str = line[5:].strip()
                try:
                    # Try to parse each data line
                    data_obj = json.loads(data_str)
                    
                    # Handle text deltas
                    if current_event == "message.delta" and "delta" in data_obj and "content" in data_obj["delta"]:
                        for content_item in data_obj["delta"]["content"]:
                            if content_item.get("type") == "text":
                                text_fragment = content_item.get("text", "")
                                accumulated_text += text_fragment
                    
                    # Handle annotations (citations) according to Snowflake docs
                    elif current_event == "response.text.annotation":
                        # Extract citation information from annotation
                        logger.info(f"Found annotation data: {data_obj}")
                        citations.append(data_obj)
                    
                except json.JSONDecodeError:
                    # Skip incomplete JSON fragments
                    pass
        
        if accumulated_text:
            # Clean up the accumulated text - remove weird encoding artifacts
            # These appear to be UTF-8 encoding issues in the SSE stream
            
            # Fix quotation marks
            accumulated_text = accumulated_text.replace("€™", "'")  # Smart apostrophe
            accumulated_text = accumulated_text.replace("€œ", '"')  # Left smart quote
            accumulated_text = accumulated_text.replace("€\x9d", '"')  # Right smart quote
            accumulated_text = accumulated_text.replace("€", '"')  # Generic quote cleanup
            
            # Fix citation markers
            accumulated_text = accumulated_text.replace("ã\x80\x80", "【")
            accumulated_text = accumulated_text.replace("â\x80", "†")  
            accumulated_text = accumulated_text.replace("ã\x80\x91", "】")
            
            # Clean up other common artifacts
            accumulated_text = accumulated_text.replace("ã", "")
            accumulated_text = accumulated_text.replace("â", "")
            
            logger.info(f"Accumulated {len(accumulated_text)} chars of text from SSE stream")
            logger.info(f"Found {len(citations)} citations from annotations")
            # Return as a simple text response with citations
            return [{
                "event": "response", 
                "data": {
                    "content": [{
                        "type": "text",
                        "text": accumulated_text
                    }],
                    "citations": citations  # Include parsed citations
                }
            }]
    
    # If not SSE or no text found, return empty
    logger.warning("Could not parse SSE content into meaningful response")
    return []

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
            
        # Parse the content - could be JSON or SSE format
        events = []
        if isinstance(content_str, str):
            # Check if it's SSE format (event: ... \ndata: ...)
            # SSE format is typically used for document search responses
            if (content_str.startswith("event:") or "\\nevent:" in content_str) and ("message.delta" in content_str or "response.text.annotation" in content_str):
                logger.info("Parsing SSE format response (likely document search)")
                # Parse SSE format into events
                sse_events = parse_sse_content(content_str)
                # For SSE parsed content, return the accumulated text and citations
                if sse_events and len(sse_events) > 0:
                    event = sse_events[0]
                    if "data" in event:
                        text = ""
                        citations = event["data"].get("citations", [])
                        
                        # Extract text content
                        if "content" in event["data"]:
                            for item in event["data"]["content"]:
                                if item.get("type") == "text":
                                    text = item.get("text", "")
                        
                        # Return text with citations, empty SQL and thinking
                        return text, None, citations, []
                return "", None, [], []
            else:
                # Try to parse as JSON (for regular AI Chat responses)
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
                        
                        # For document search, we might get a simple acknowledgment
                        if tool_name == "cortex_search" or tool_type == "cortex_search":
                            # This is just the tool being invoked, actual results come in tool_result
                            response_text = "Searching clinical documents..."
                    
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
                        if text:
                            # Always capture text responses, not just ones matching patterns
                            response_text = text  # Replace with latest text
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
