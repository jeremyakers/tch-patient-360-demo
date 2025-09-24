"""
Server-Sent Events (SSE) handler for Cortex Agents v2 API.
Processes streaming responses in real-time for UI updates.
"""

import json
import logging
from typing import Dict, Generator, Optional, Any, List
from utils.snowflake_api import send_snow_api_request
try:
    import requests
    import sseclient
    SSE_AVAILABLE = True
except ImportError:
    SSE_AVAILABLE = False
    logging.warning("requests/sseclient not available - falling back to non-streaming mode")

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
            logger.info(f"SSEProcessor: Processing response type: {type(response)}")
            logger.debug(f"SSEProcessor: Response sample: {str(response)[:200]}")
            
            # Check if response has content (either as attribute or dict key)
            content = None
            if hasattr(response, 'content'):
                content = response.content
                logger.debug(f"Response has content attribute, type: {type(content)}")
            elif isinstance(response, dict) and 'content' in response:
                content = response['content']
                logger.debug(f"Response is dict with content key, type: {type(content)}")
            
            if content is not None:
                events = []
                
                # Check if content is SSE format (text with data: lines) or JSON
                if isinstance(content, str):
                    # First check if it looks like a Python dict/list string representation
                    if content.startswith(("{'content':", "[{", "[\"")) or "'content':" in content[:100]:
                        # This is a string representation of a Python dict/list, try to parse it
                        try:
                            import ast
                            parsed_content = ast.literal_eval(content)
                            if isinstance(parsed_content, dict) and 'content' in parsed_content:
                                # Extract the actual content
                                actual_content = parsed_content['content']
                                if isinstance(actual_content, str):
                                    events = json.loads(actual_content) if actual_content else []
                                else:
                                    events = actual_content if isinstance(actual_content, list) else []
                            elif isinstance(parsed_content, list):
                                events = parsed_content
                            else:
                                events = []
                            logger.debug(f"Parsed {len(events)} events from Python dict string")
                        except (ValueError, SyntaxError) as e:
                            logger.warning(f"Failed to parse as Python dict: {e}")
                            # Fall back to JSON parsing
                            try:
                                events = json.loads(content) if content else []
                                logger.debug(f"Parsed {len(events)} events from JSON")
                            except json.JSONDecodeError as je:
                                logger.error(f"Failed to parse JSON: {je}")
                                logger.debug(f"Content preview: {content[:500]}")
                                yield {
                                    "type": "error",
                                    "message": f"Failed to parse response: {je}"
                                }
                                return
                    # Check if it's SSE format
                    elif 'data:' in content or 'event:' in content:
                        # Parse SSE format
                        logger.debug("Parsing SSE format response")
                        events = self._parse_sse_text(content)
                    else:
                        # Try to parse as JSON
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
    
    def _parse_sse_text(self, sse_text: str) -> List[Dict]:
        """Parse SSE formatted text into events.
        
        SSE format:
        event: <event_type>
        data: <json_data>
        
        data: <json_data>
        
        Args:
            sse_text: Raw SSE formatted text
            
        Returns:
            List of parsed events
        """
        events = []
        current_event = {}
        current_data = []
        
        for line in sse_text.split('\n'):
            line = line.strip()
            
            if not line:
                # Empty line signals end of event
                if current_data:
                    # Combine data lines and parse JSON
                    data_str = '\n'.join(current_data)
                    try:
                        data = json.loads(data_str)
                        if current_event:
                            current_event['data'] = data
                            events.append(current_event)
                        else:
                            # Data-only event (no event type)
                            events.append({'event': 'message', 'data': data})
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse SSE data: {e}")
                        logger.debug(f"Data string: {data_str[:200]}")
                    
                    current_event = {}
                    current_data = []
            
            elif line.startswith('event:'):
                # Event type line
                event_type = line[6:].strip()
                current_event['event'] = event_type
                
            elif line.startswith('data:'):
                # Data line
                data_line = line[5:].strip()
                if data_line == '[DONE]':
                    # Special done marker
                    events.append({'event': 'done', 'data': {}})
                else:
                    current_data.append(data_line)
            
            elif line.startswith(':'):
                # Comment line, ignore
                pass
        
        # Handle any remaining data
        if current_data:
            data_str = '\n'.join(current_data)
            try:
                data = json.loads(data_str)
                if current_event:
                    current_event['data'] = data
                    events.append(current_event)
                else:
                    events.append({'event': 'message', 'data': data})
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse final SSE data: {e}")
        
        logger.debug(f"Parsed {len(events)} SSE events")
        return events
    
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
    Send message to Cortex Agents API and yield SSE events in real-time.
    
    Uses requests library for true SSE streaming when available,
    falls back to _snowflake.send_snow_api_request otherwise.
    
    Args:
        api_endpoint: The API endpoint URL
        payload: The request payload
        timeout: Request timeout in milliseconds
        
    Yields:
        Dict events for real-time UI updates
    """
    processor = SSEProcessor()
    
    # Try to use real SSE streaming if available
    logger.info(f"SSE: SSE_AVAILABLE = {SSE_AVAILABLE}")
    if SSE_AVAILABLE:
        logger.info("SSE: Attempting real SSE streaming")
        sse_generator = _try_sse_streaming(api_endpoint, payload, processor, timeout)
        if sse_generator is not None:
            logger.info("SSE: Real streaming successful, yielding events")
            yield from sse_generator
            return
        else:
            logger.warning("SSE: Real streaming failed, falling back to non-streaming")
    
    # Fallback to non-streaming mode
    try:
        logger.info(f"SSE: Using fallback mode (non-streaming)")
        logger.info(f"SSE: Sending request to: {api_endpoint}")
        logger.debug(f"SSE: Payload has stream={payload.get('stream', False)}")
        
        response = send_snow_api_request(
            "POST",
            api_endpoint,
            {"Content-Type": "application/json"},
            {},
            payload,
            None,
            timeout
        )
        
        logger.info(f"SSE: Received response, type: {type(response)}")
        
        # Process the complete response and yield events
        event_count = 0
        logger.info("SSE: Starting to process response and yield events")
        
        for event in processor.process_sse_stream(response):
            event_count += 1
            event_type = event.get('type')
            logger.info(f"SSE: Yielding event {event_count}: {event_type}")
            logger.debug(f"SSE: Event data: {event}")
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


def _try_sse_streaming(
    api_endpoint: str,
    payload: Dict,
    processor: SSEProcessor,
    timeout: int
) -> Optional[Generator[Dict, None, None]]:
    """
    Attempt to use real SSE streaming with requests and sseclient libraries.
    
    Returns:
        Generator if successful, None if should fallback
    """
    if not SSE_AVAILABLE:
        return None
    
    try:
        # Use the built-in OAuth token and environment variables provided by Snowflake in SPCS
        import os
        
        # Read OAuth token from the file provided by Snowflake
        token_path = "/snowflake/session/token"
        try:
            with open(token_path, 'r') as token_file:
                oauth_token = token_file.read().strip()
            logger.info("SSE: Successfully read OAuth token from Snowflake")
        except Exception as e:
            raise RuntimeError(f"Cannot read Snowflake OAuth token from {token_path}: {e}")
        
        # Get host information from environment variable
        snowflake_host = os.getenv('SNOWFLAKE_HOST')
        if not snowflake_host:
            raise RuntimeError("SNOWFLAKE_HOST environment variable not set by Snowflake")
        
        # Build full URL using the Snowflake-provided host
        full_url = f"https://{snowflake_host}{api_endpoint}"
        
        logger.info(f"SSE: Attempting real streaming to: {full_url}")
        
        # Make streaming request with OAuth authentication
        headers = {
            "Authorization": f"Bearer {oauth_token}",
            "X-Snowflake-Authorization-Token-Type": "OAUTH",
            "Content-Type": "application/json",
            "Accept": "text/event-stream"
        }
        
        response = requests.post(
            full_url,
            json=payload,
            headers=headers,
            stream=True,
            timeout=timeout/1000  # Convert ms to seconds
        )
        
        if response.status_code != 200:
            logger.error(f"SSE streaming failed: {response.status_code} - {response.text}")
            return None
        
        # Create generator function for SSE events
        def sse_event_generator():
            """Process SSE stream using sseclient library."""
            client = sseclient.SSEClient(response)
            event_count = 0
            
            logger.info("SSE: Starting to process SSE events from client")
            
            for event in client.events():
                event_count += 1
                logger.info(f"SSE: Raw event {event_count} - type: {event.event}, data length: {len(event.data) if event.data else 0}")
                logger.debug(f"SSE: Raw event data: {event.data[:200]}..." if event.data and len(event.data) > 200 else f"SSE: Raw event data: {event.data}")
                
                if event.data == "[DONE]":
                    logger.info(f"SSE: Stream done signal received")
                    yield {
                        "type": "done",
                        "final_text": processor.current_text,
                        "sql": processor.current_sql,
                        "thinking_steps": processor.current_thinking,
                        "search_results": processor.search_results
                    }
                    break
                
                try:
                    data = json.loads(event.data)
                    event_type = event.event or 'message'
                    logger.info(f"SSE: Processing event {event_count}: {event_type}")
                    
                    # Process based on actual Cortex Agents v2 event types
                    if event_type == "response.thinking.delta":
                        # Real-time thinking steps
                        thinking_text = data.get("text", "")
                        if thinking_text:
                            processor.current_thinking.append(thinking_text)
                            yield {
                                "type": "thinking",
                                "text": thinking_text,
                                "step_number": len(processor.current_thinking)
                            }
                    
                    elif event_type == "response.tool_use":
                        # Tool usage events
                        tool_name = data.get("name", "Unknown Tool")
                        tool_input = data.get("input", {})
                        yield {
                            "type": "tool_use",
                            "tool_name": tool_name,
                            "query": tool_input.get("query", "")
                        }
                    
                    elif event_type == "response.tool_result":
                        # Tool result events (may contain SQL)
                        content_items = data.get("content", [])
                        for content_item in content_items:
                            if isinstance(content_item, dict) and content_item.get("type") == "json":
                                json_data = content_item.get("json", {})
                                if "sql" in json_data:
                                    processor.current_sql = json_data["sql"]
                                    yield {
                                        "type": "sql",
                                        "query": json_data["sql"]
                                    }
                                if "search_results" in json_data:
                                    search_results = json_data["search_results"]
                                    processor.search_results.extend(search_results)
                                    yield {
                                        "type": "search_results",
                                        "count": len(search_results),
                                        "results": search_results[:3]
                                    }
                    
                    elif event_type == "response.text.delta":
                        # Streaming text response
                        text_delta = data.get("text", "")
                        if text_delta:
                            processor.current_text += text_delta
                    
                    elif event_type == "response.done":
                        # Stream completion
                        yield {
                            "type": "response_text",
                            "text": processor.current_text
                        }
                        yield {
                            "type": "done",
                            "final_text": processor.current_text,
                            "sql": processor.current_sql,
                            "thinking_steps": processor.current_thinking,
                            "search_results": processor.search_results
                        }
                    
                    elif event_type == "error":
                        # Error events
                        error_message = data.get("message", "Unknown error")
                        error_code = data.get("code", "")
                        yield {
                            "type": "error",
                            "message": error_message,
                            "code": error_code
                        }
                    
                    else:
                        # Log unknown event types for debugging
                        logger.debug(f"SSE: Unknown event type: {event_type}, data: {data}")
                            
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse SSE event data: {e}")
                    logger.debug(f"Data was: {event.data}")
                    continue
            
            logger.info(f"SSE: Real streaming completed with {event_count} events")
        
        return sse_event_generator()
        
    except Exception as e:
        logger.error(f"SSE streaming error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None
