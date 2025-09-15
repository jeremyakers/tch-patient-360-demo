"""
Server-Sent Events (SSE) handler for Cortex Agents v2 API.
Processes streaming responses in real-time for UI updates.
"""

import json
import logging
from typing import Dict, Generator, Optional, Any
import _snowflake

logger = logging.getLogger(__name__)

class SSEProcessor:
    """Process Server-Sent Events from Cortex Agents API."""
    
    def __init__(self):
        self.current_thinking = []
        self.current_sql = None
        self.current_text = ""
        self.tool_calls = []
        self.search_results = []
        
    def process_sse_stream(self, response) -> Generator[Dict, None, None]:
        """
        Process SSE stream and yield events as they arrive.
        
        Yields:
            Dict with event type and data for real-time UI updates
        """
        try:
            # Log what type of response we got
            logger.debug(f"Processing response type: {type(response)}")
            
            # Check if response has content
            if hasattr(response, 'content'):
                content = response.content
                logger.debug(f"Response has content attribute, type: {type(content)}")
                
                # Parse the content as JSON
                if isinstance(content, str):
                    try:
                        events = json.loads(content) if content else []
                        logger.debug(f"Parsed {len(events)} events from JSON")
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON: {e}")
                        logger.debug(f"Content preview: {content[:500]}")
                        yield {
                            "type": "error",
                            "message": f"Failed to parse response: {e}"
                        }
                        return
                else:
                    events = content if isinstance(content, list) else []
                    
                # Process each event
                for i, event in enumerate(events):
                    event_type = event.get("event")
                    logger.debug(f"Processing event {i}: {event_type}")
                    
                    if event_type == "response":
                        # Process response event with all content
                        yield from self._process_response_event(event)
                    
                    elif event_type == "error":
                        # Handle error events
                        error_data = event.get("data", {})
                        yield {
                            "type": "error",
                            "message": error_data.get("message", "Unknown error"),
                            "code": error_data.get("code", ""),
                            "request_id": error_data.get("request_id", "")
                        }
                    
                    elif event_type == "done":
                        # Signal completion
                        yield {
                            "type": "done",
                            "final_text": self.current_text,
                            "sql": self.current_sql,
                            "thinking_steps": self.current_thinking,
                            "search_results": self.search_results
                        }
            else:
                # Response doesn't have content attribute
                logger.error(f"Response has no content attribute: {response}")
                yield {
                    "type": "error",
                    "message": "Invalid response format"
                }
                        
        except Exception as e:
            logger.error(f"SSE processing error: {e}")
            yield {
                "type": "error",
                "message": str(e)
            }
    
    def _process_response_event(self, event: Dict) -> Generator[Dict, None, None]:
        """Process a response event and yield individual content items."""
        data = event.get("data", {})
        content_items = data.get("content", [])
        
        for item in content_items:
            item_type = item.get("type")
            
            if item_type == "thinking":
                # Extract and yield thinking step
                thinking_obj = item.get("thinking", {})
                thinking_text = thinking_obj.get("text", "") if isinstance(thinking_obj, dict) else item.get("text", "")
                
                if thinking_text:
                    self.current_thinking.append(thinking_text)
                    yield {
                        "type": "thinking",
                        "text": thinking_text,
                        "step_number": len(self.current_thinking)
                    }
            
            elif item_type == "tool_use":
                # Track tool usage
                tool_use = item.get("tool_use", {})
                tool_type = tool_use.get("type", "")
                tool_name = tool_use.get("name", "")
                tool_input = tool_use.get("input", {})
                
                self.tool_calls.append({
                    "type": tool_type,
                    "name": tool_name,
                    "input": tool_input
                })
                
                yield {
                    "type": "tool_use",
                    "tool_type": tool_type,
                    "tool_name": tool_name,
                    "query": tool_input.get("query", "") if "query" in tool_input else None
                }
            
            elif item_type == "tool_result":
                # Process tool results
                tool_result = item.get("tool_result", {})
                tool_content = tool_result.get("content", [])
                
                for content_item in tool_content:
                    if content_item.get("type") == "json":
                        json_data = content_item.get("json", {})
                        
                        # Extract SQL if present
                        if "sql" in json_data:
                            self.current_sql = json_data["sql"]
                            yield {
                                "type": "sql",
                                "query": self.current_sql,
                                "verified": json_data.get("verified_query_used", False)
                            }
                        
                        # Extract search results
                        search_results = json_data.get("search_results", json_data.get("searchResults", []))
                        if search_results:
                            self.search_results.extend(search_results[:5])  # Limit to 5
                            yield {
                                "type": "search_results",
                                "count": len(search_results),
                                "results": search_results[:3]  # Show first 3 in UI
                            }
            
            elif item_type == "text":
                # Final response text
                text = item.get("text", "")
                if text:
                    self.current_text = text
                    yield {
                        "type": "response_text",
                        "text": text
                    }


def send_message_with_streaming(
    api_endpoint: str,
    payload: Dict,
    timeout: int = 60000
) -> Generator[Dict, None, None]:
    """
    Send message to Cortex Agents API and yield SSE events.
    
    Args:
        api_endpoint: The API endpoint URL
        payload: The request payload
        timeout: Request timeout in milliseconds
        
    Yields:
        Dict events for real-time UI updates
    """
    processor = SSEProcessor()
    
    try:
        # Make the API call
        logger.info(f"SSE: Sending request to: {api_endpoint}")
        logger.debug(f"SSE: Payload keys: {list(payload.keys())}")
        
        response = _snowflake.send_snow_api_request(
            "POST",
            api_endpoint,
            {"Content-Type": "application/json"},
            {},
            payload,
            None,
            timeout
        )
        
        logger.info(f"SSE: Received response, type: {type(response)}")
        
        # Process the SSE stream
        event_count = 0
        for event in processor.process_sse_stream(response):
            event_count += 1
            logger.debug(f"SSE: Yielding event {event_count}: {event.get('type')}")
            yield event
        
        logger.info(f"SSE: Stream completed with {event_count} events")
        
    except Exception as e:
        logger.error(f"SSE: Streaming API call failed: {e}")
        import traceback
        logger.error(f"SSE: Traceback: {traceback.format_exc()}")
        yield {
            "type": "error",
            "message": str(e)
        }
