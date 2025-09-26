"""
Enhanced Chat Interface with Content Persistence
Based on Cortex Agents example pattern
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from services import cortex_agents, data_service, session_manager
from services.cortex_agents_sse import send_message_with_streaming
from utils import helpers
from models.content_items import (
    ChatMessage, 
    TextContentItem, 
    TableContentItem, 
    ChartContentItem, 
    MessageContentItem
)

logger = logging.getLogger(__name__)

def render():
    """Entry point called by main.py"""
    render_chat_interface()

def render_chat_interface():
    """Main entry point for the AI chat interface page with content persistence"""
    
    st.title("🤖 AI Healthcare Assistant")
    st.markdown("Ask questions about patients, cohorts, or clinical data using natural language")
    
    # Initialize session state
    if 'chat_messages' not in st.session_state:
        st.session_state.chat_messages = []
    
    if 'conversation_history' not in st.session_state:
        st.session_state.conversation_history = []
    
    # Display chat messages using content persistence pattern
    for idx, message in enumerate(st.session_state.chat_messages):
        # Handle both dict (for streaming placeholder) and ChatMessage objects
        if isinstance(message, dict):
            # This is a streaming placeholder
            if message.get('streaming', False):
                # Skip - will be handled by live streaming UI
                continue
            else:
                # Legacy dict format
                with st.chat_message(message['role']):
                    st.markdown(message.get('content', ''))
        
        elif isinstance(message, ChatMessage):
            # Enhanced message with content persistence
            with st.chat_message(message.role):
                if message.role == 'user':
                    st.markdown(message.content)
                else:
                    # Display processed content if available
                    if message.is_processed and message.processed_content:
                        for content_item in message.processed_content:
                            if hasattr(content_item, 'actual_instance'):
                                instance = content_item.actual_instance
                                
                                if isinstance(instance, TextContentItem):
                                    st.markdown(instance.text, unsafe_allow_html=True)
                                
                                elif isinstance(instance, TableContentItem):
                                    if instance.title:
                                        st.markdown(instance.title)
                                    if instance.data and instance.columns:
                                        df = pd.DataFrame(instance.data, columns=instance.columns)
                                        st.dataframe(df, use_container_width=True)
                                
                                elif isinstance(instance, ChartContentItem):
                                    if instance.title:
                                        st.markdown(instance.title)
                                    if instance.spec:
                                        st.vega_lite_chart(instance.spec, use_container_width=True)
                    else:
                        # Fallback for messages without processed content
                        if message.content:
                            st.markdown(message.content)
    
    # Chat input
    if query := st.chat_input("Ask about patients, conditions, or search clinical documents..."):
        process_user_query(query)

def process_user_query(query: str):
    """Process user query with content persistence"""
    
    # Add user message as ChatMessage
    user_message = ChatMessage(role="user", content=query)
    st.session_state.chat_messages.append(user_message)
    
    # Add streaming placeholder
    streaming_placeholder = {"role": "assistant", "content": "", "streaming": True}
    st.session_state.chat_messages.append(streaming_placeholder)
    message_index = len(st.session_state.chat_messages) - 1
    
    # Stream the response
    stream_and_store_response(query, message_index)

def stream_and_store_response(query: str, message_index: int):
    """Stream response and store with content persistence"""
    
    # Create live streaming UI
    with st.chat_message("assistant"):
        thinking_expander = st.expander("🧠 Agent Thinking Process (Live)", expanded=True)
        with thinking_expander:
            thinking_container = st.container(height=200)
            thinking_placeholder = thinking_container.empty()
        
        response_placeholder = st.empty()
    
    # Initialize collectors
    thinking_steps = []
    sql_query = None
    final_response = ""
    tables_data = []
    charts_data = []
    
    # Stream the response
    try:
        # Build payload
        payload = cortex_agents._build_agent_payload(
            query,
            st.session_state.conversation_history,
            thread_id=None
        )
        
        # Process streaming events
        thinking_buffer = ""
        
        for event in send_message_with_streaming(
            payload,
            cortex_agents.api_endpoint,
            cortex_agents.model
        ):
            event_type = event.get("type", "")
            
            if event_type == "thinking":
                # Accumulate thinking text
                thinking_text = event.get("text", "")
                thinking_buffer += thinking_text
                thinking_steps.append(thinking_text)
                
                # Update thinking display
                with thinking_placeholder:
                    st.markdown(thinking_buffer)
            
            elif event_type == "sql":
                # Store SQL query
                sql_query = event.get("query", "")
                
                # Update display
                with response_placeholder.container():
                    st.markdown("### 🔍 Generated SQL Query")
                    st.code(sql_query, language="sql")
            
            elif event_type == "table":
                # Process table data
                table_data = event.get("data", {})
                if "data" in table_data and "columns" in table_data:
                    tables_data.append({
                        'data': table_data['data'],
                        'columns': table_data['columns'],
                        'title': "### 📊 Query Results"
                    })
                    
                    # Update display
                    with response_placeholder.container():
                        if sql_query:
                            st.markdown("### 🔍 Generated SQL Query")
                            st.code(sql_query, language="sql")
                        
                        for table in tables_data:
                            st.markdown(table['title'])
                            df = pd.DataFrame(table['data'], columns=table['columns'])
                            st.dataframe(df, use_container_width=True)
                        
                        if final_response:
                            st.markdown("### 💬 Response")
                            st.markdown(final_response)
            
            elif event_type == "chart":
                # Process chart data
                chart_data = event.get("data", {})
                if "spec" in chart_data:
                    charts_data.append({
                        'spec': chart_data['spec'],
                        'title': "### 📈 Data Visualization"
                    })
                    
                    # Update display with all content
                    with response_placeholder.container():
                        if sql_query:
                            st.markdown("### 🔍 Generated SQL Query")
                            st.code(sql_query, language="sql")
                        
                        for table in tables_data:
                            st.markdown(table['title'])
                            df = pd.DataFrame(table['data'], columns=table['columns'])
                            st.dataframe(df, use_container_width=True)
                        
                        if final_response:
                            st.markdown("### 💬 Response")
                            st.markdown(final_response)
                        
                        for chart in charts_data:
                            st.markdown(chart['title'])
                            st.vega_lite_chart(chart['spec'], use_container_width=True)
            
            elif event_type == "response_text":
                # Accumulate response text
                text_delta = event.get("text", "")
                final_response += text_delta
                
                # Update display
                with response_placeholder.container():
                    if sql_query:
                        st.markdown("### 🔍 Generated SQL Query")
                        st.code(sql_query, language="sql")
                    
                    for table in tables_data:
                        st.markdown(table['title'])
                        df = pd.DataFrame(table['data'], columns=table['columns'])
                        st.dataframe(df, use_container_width=True)
                    
                    if final_response:
                        st.markdown("### 💬 Response")
                        st.markdown(final_response)
                    
                    for chart in charts_data:
                        st.markdown(chart['title'])
                        st.vega_lite_chart(chart['spec'], use_container_width=True)
        
        # After streaming completes, store processed content
        if final_response or sql_query or tables_data or charts_data:
            # Create ChatMessage with all content
            assistant_message = ChatMessage(
                role="assistant",
                content=final_response or "",
                sql=sql_query,
                thinking_steps=thinking_steps
            )
            
            # Store processed content for persistence
            assistant_message.store_processed_content(
                processed_text=final_response or "",
                sql_query=sql_query,
                tables=tables_data,
                charts=charts_data
            )
            
            # Replace streaming placeholder with processed message
            st.session_state.chat_messages[message_index] = assistant_message
            
            # Update conversation history
            if final_response:
                st.session_state.conversation_history.extend([
                    {"role": "user", "content": [{"type": "text", "text": query}]},
                    {"role": "assistant", "content": [{"type": "text", "text": final_response}]}
                ])
                
                # Limit history
                if len(st.session_state.conversation_history) > 20:
                    st.session_state.conversation_history = st.session_state.conversation_history[-20:]
        
        # Collapse thinking expander
        thinking_expander.expanded = False
        
    except Exception as e:
        logger.error(f"Error in streaming: {e}")
        error_message = ChatMessage(
            role="assistant",
            content=f"❌ Error: {str(e)}"
        )
        st.session_state.chat_messages[message_index] = error_message
