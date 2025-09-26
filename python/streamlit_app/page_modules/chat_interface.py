"""
Chat Interface Page for TCH Patient 360 PoC

This page provides a natural language interface powered by Cortex Agents including:
- Intelligent routing between structured and unstructured data
- Natural language queries for patient data
- Clinical decision support conversations
- Multi-modal data exploration through chat
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import requests
import os
import re

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

def test_oauth_api_call():
    """Test OAuth token authentication with a simple Snowflake REST API call"""
    try:
        # Read OAuth token from the file provided by Snowflake
        token_path = "/snowflake/session/token"
        with open(token_path, 'r') as token_file:
            oauth_token = token_file.read().strip()
        
        # Get host information from environment variable
        snowflake_host = os.getenv('SNOWFLAKE_HOST')
        if not snowflake_host:
            st.error("SNOWFLAKE_HOST environment variable not set")
            return
        
        # Test with a simple endpoint - list databases
        test_endpoint = "/api/v2/databases"
        full_url = f"https://{snowflake_host}{test_endpoint}"
        
        # Set up headers as per documentation
        headers = {
            "Authorization": f"Bearer {oauth_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Log the request details
        logger.info(f"TEST OAuth API Call:")
        logger.info(f"  URL: {full_url}")
        logger.info(f"  Headers: Authorization=Bearer <token>, Content-Type=application/json")
        logger.info(f"  Token length: {len(oauth_token)}")
        logger.info(f"  Token preview: {oauth_token[:20]}...")
        
        # Make the request
        response = requests.get(full_url, headers=headers, timeout=10)
        
        # Log the response
        logger.info(f"TEST Response:")
        logger.info(f"  Status Code: {response.status_code}")
        logger.info(f"  Headers: {dict(response.headers)}")
        logger.info(f"  Response Text (first 500 chars): {response.text[:500]}")
        
        # Display results in UI
        st.success(f"✅ OAuth Test: Status {response.status_code}")
        if response.status_code == 200:
            st.json(response.json())
        else:
            st.error(f"Response: {response.text}")
            
        return response.status_code == 200
        
    except Exception as e:
        logger.error(f"TEST OAuth API call failed: {e}")
        st.error(f"❌ OAuth Test Failed: {e}")
        return False

def render_chat_interface():
    """Main entry point for the AI chat interface page"""
    
    st.title("🤖 AI Healthcare Assistant")
    st.markdown("Ask questions about patients, cohorts, or clinical data using natural language")
    
    # Sidebar for testing, conversation and examples
    with st.sidebar:
        # Testing section
        st.subheader("🧪 Testing")
        if st.button("Test OAuth API Call"):
            test_oauth_api_call()
        
        st.markdown("---")
        
        # Conversation section
        st.subheader("💬 Conversation")
        
        if st.button("🔄 New Conversation", key="new_chat"):
            try:
                # Delete existing Cortex thread if present
                if 'cortex_thread_id' in st.session_state and st.session_state.cortex_thread_id:
                    cortex_agents.delete_thread(st.session_state.cortex_thread_id)
            except Exception:
                pass
            st.session_state.cortex_thread_id = None
            st.session_state.chat_messages = []
            st.session_state.conversation_history = []
            st.rerun()
        
        st.markdown("---")
        
        st.subheader("📝 Example Queries")
        
        # Updated examples to showcase v2 multi-step reasoning capabilities
        example_queries = [
            "Show me the top 5 diagnoses for patients aged 10-15 and create a chart showing their distribution",
            "Find patients with asthma who had recent ER visits and analyze their medication patterns",
            "Show me all asthma patients aged 5-12 with recent ER visits",
            "Find patients with diabetes who haven't had HbA1c in 6 months",
            "What are the most common diagnoses for patients from ZIP code 77001?",
            "Search for clinical notes mentioning medication allergies",
            "Analyze readmission patterns for heart conditions and identify high-risk patients",
            "Find patients with elevated BMI, show their recent vitals, and identify those needing nutrition counseling"
        ]
        
        for i, example in enumerate(example_queries):
            if st.button(f"📌 {example[:40]}...", key=f"example_{i}"):
                st.session_state.example_query = example
                st.rerun()

        st.markdown("---")
        st.subheader("⚙️ Search Settings")
        # Control for Cortex Search max_results used by Agents in chat
        try:
            current_default = int(st.session_state.get('cortex_search_max_results', 50))
        except Exception:
            current_default = 50
        max_docs = st.number_input(
            label="Max document results",
            min_value=1,
            max_value=1000,
            value=current_default,
            step=10,
            help="Controls how many documents Cortex Search can return in AI Chat (via Cortex Agents)."
        )
        st.session_state['cortex_search_max_results'] = int(max_docs)
        
        # Debug panel for troubleshooting
        st.markdown("---")
        st.subheader("🔍 Debug Panel")
        
        # Show agent configuration
        try:
            from services.cortex_agents import CortexAgentsService
            agent_service = CortexAgentsService()
            st.info(f"🤖 Agent: {agent_service.agent_name}")
            st.info(f"🔗 Endpoint: {agent_service.api_endpoint}")
            st.info(f"📊 Model: {agent_service.model}")
        except Exception as e:
            st.warning(f"⚠️ Agent info unavailable: {e}")
            
        # Show thread information
        thread_id = st.session_state.get('cortex_thread_id')
        if thread_id:
            st.success(f"🧵 Thread: {thread_id[:8]}...")
        else:
            st.info("🧵 No active thread (disabled for debugging)")
            
        st.info("📜 All debug details are logged - check application logs for full trace")
        
        # Show logging status
        import logging
        logger = logging.getLogger(__name__)
        st.info(f"📝 Logger level: {logger.level}")
    
    # Initialize session state for chat
    if 'chat_messages' not in st.session_state:
        st.session_state.chat_messages = []
    
    if 'conversation_history' not in st.session_state:
        st.session_state.conversation_history = []

    # Initialize Cortex thread once per session
    if 'cortex_thread_id' not in st.session_state or not st.session_state.cortex_thread_id:
        try:
            from services import cortex_agents as _agents_service
            st.session_state.cortex_thread_id = _agents_service.create_thread()
        except Exception:
            st.session_state.cortex_thread_id = None
    
    # Create a container for chat messages with fixed height to keep input at bottom
    chat_container = st.container()
    
    with chat_container:
        # Display welcome message for new conversations
        if not st.session_state.chat_messages:
            with st.chat_message("assistant"):
                _render_welcome_message()
        
        # Display chat messages with content persistence support
        for idx, message in enumerate(st.session_state.chat_messages):
            # Handle both dict format and ChatMessage objects
            if isinstance(message, dict):
                with st.chat_message(message['role']):
                    if message['role'] == 'user':
                        st.markdown(message['content'])
                    else:
                        # Display assistant response
                        st.markdown(message['content'])
                    
                    # Note: Thinking steps are now shown in the live streaming box during conversation
                    # No need to duplicate the reasoning process in saved messages
                    
                    # Display SQL if present
                    if 'sql' in message and message['sql']:
                        st.markdown("### 🔍 Generated SQL Query")
                        st.code(message['sql'], language="sql")
                        
                        # Display results if present
                        if 'results' in message and message['results'] is not None:
                            st.markdown("### 📊 Query Results")
                            try:
                                df = message['results'].to_pandas()
                                if not df.empty:
                                    # Get chart type selection
                                    chart_type = st.selectbox(
                                        "📊 Chart Type",
                                        ["table", "bar", "line", "area", "scatter"],
                                        index=0,
                                        key=f"chart_type_{idx}_{hash(message.get('sql', ''))}"
                                    )
                                    
                                    # Display visualization based on selection
                                    if chart_type == "table":
                                        st.dataframe(df, use_container_width=True)
                                    else:
                                        # Add column selectors for chart types
                                        if len(df.columns) >= 2:
                                            col1, col2, col3 = st.columns(3)
                                            with col1:
                                                x_column = st.selectbox(
                                                    "X-axis Column",
                                                    df.columns,
                                                    index=0,
                                                    key=f"x_col_{idx}_{chart_type}_{hash(message.get('sql', ''))}"
                                                )
                                            with col2:
                                                if chart_type == "scatter":
                                                    y_column = st.selectbox(
                                                        "Y-axis Column",
                                                        [col for col in df.columns if col != x_column],
                                                        index=0,
                                                        key=f"y_col_{idx}_{chart_type}_{hash(message.get('sql', ''))}"
                                                    )
                                                else:
                                                    y_columns = st.multiselect(
                                                        "Value Columns",
                                                        [col for col in df.columns if col != x_column],
                                                        default=[col for col in df.columns if col != x_column][:1],  # Default to first 1
                                                        key=f"y_cols_{idx}_{chart_type}_{hash(message.get('sql', ''))}"
                                                    )
                                            with col3:
                                                if chart_type != "scatter" and len(df.columns) >= 3:
                                                    remaining_columns = [col for col in df.columns if col not in [x_column] + (y_columns if chart_type != "scatter" else [])]
                                                    if remaining_columns:
                                                        series_column = st.selectbox(
                                                            "Series/Group Column",
                                                            ["None"] + remaining_columns,
                                                            index=0,
                                                            key=f"series_col_{idx}_{chart_type}_{hash(message.get('sql', ''))}"
                                                        )
                                                    else:
                                                        series_column = "None"
                                                else:
                                                    series_column = "None"
                                            
                                            # Generate chart based on selections
                                            if chart_type == "bar":
                                                if y_columns:
                                                    if series_column != "None":
                                                        # Create pivot table for multi-series bar chart
                                                        pivot_df = df.pivot_table(
                                                            index=x_column, 
                                                            columns=series_column, 
                                                            values=y_columns[0],  # Use first value column
                                                            aggfunc='sum',
                                                            fill_value=0
                                                        )
                                                        st.bar_chart(pivot_df)
                                                    else:
                                                        # Single series bar chart
                                                        chart_df = df[[x_column] + y_columns].set_index(x_column)
                                                        st.bar_chart(chart_df)
                                                else:
                                                    st.warning("Please select at least one value column")
                                            elif chart_type == "line":
                                                if y_columns:
                                                    if series_column != "None":
                                                        # Create pivot table for multi-series line chart
                                                        pivot_df = df.pivot_table(
                                                            index=x_column, 
                                                            columns=series_column, 
                                                            values=y_columns[0],  # Use first value column
                                                            aggfunc='sum',
                                                            fill_value=0
                                                        )
                                                        st.line_chart(pivot_df)
                                                    else:
                                                        # Single series line chart
                                                        chart_df = df[[x_column] + y_columns].set_index(x_column)
                                                        st.line_chart(chart_df)
                                                else:
                                                    st.warning("Please select at least one value column")
                                            elif chart_type == "area":
                                                if y_columns:
                                                    if series_column != "None":
                                                        # Create pivot table for multi-series area chart
                                                        pivot_df = df.pivot_table(
                                                            index=x_column, 
                                                            columns=series_column, 
                                                            values=y_columns[0],  # Use first value column
                                                            aggfunc='sum',
                                                            fill_value=0
                                                        )
                                                        st.area_chart(pivot_df)
                                                    else:
                                                        # Single series area chart
                                                        chart_df = df[[x_column] + y_columns].set_index(x_column)
                                                        st.area_chart(chart_df)
                                                else:
                                                    st.warning("Please select at least one value column")
                                            elif chart_type == "scatter":
                                                st.scatter_chart(df, x=x_column, y=y_column)
                                        else:
                                            # Fallback for single column data
                                            if chart_type == "bar":
                                                st.bar_chart(df)
                                            elif chart_type == "line":
                                                st.line_chart(df)
                                            elif chart_type == "area":
                                                st.area_chart(df)
                                            elif chart_type == "scatter":
                                                st.warning("Scatter plot requires at least 2 columns")
                                else:
                                    st.info("Query returned no results")
                            except Exception as e:
                                st.error(f"Error displaying results: {e}")
                    
                    # Display citations if present with enhanced document viewer
                    if 'citations' in message and message['citations']:
                        st.markdown("### 📄 Clinical Document Sources")
                        
                        # Import cortex_search for the full document functionality
                        try:
                            import importlib
                            cortex_search_module = importlib.import_module('services.cortex_search')
                            CortexSearchService = cortex_search_module.CortexSearchService
                            cortex_search = CortexSearchService()
                        except ImportError:
                            cortex_search = None
                            st.warning("Document search service not available")
                        
                        # Track documents that need metadata extraction
                        documents_needing_metadata = []
                        citation_info = []
                        
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.info(f"Chat Interface: Processing {len(message['citations'])} citations")
                        
                        for i, citation in enumerate(message['citations']):
                            source_id = citation.get('source_id', f'Source {i+1}')
                            file_path = citation.get('file_path', '')
                            doc_type = citation.get('document_type', 'Clinical Note')
                            relevance = citation.get('relevance_score', 0)
                            excerpt = citation.get('text', '')
                            mrn = citation.get('mrn', '')
                            patient_name = citation.get('patient_name', '')
                            author = citation.get('author', 'N/A')
                            department = citation.get('department', 'N/A')
                            document_date = citation.get('document_date', '')
                            
                            logger.info(f"Citation {i}: file_path='{file_path}', author='{author}', department='{department}', doc_type='{doc_type}'")
                            
                            # Since Cortex Agents doesn't return file_path, try to get it from citation attributes
                            # Use file_path from citation if available, otherwise skip document retrieval
                            doc_id = file_path if file_path else ""
                            
                            # If no file_path available, document retrieval won't work
                            if not doc_id:
                                logger.warning(f"No file_path available for source {source_id}, document retrieval not possible")
                            
                            # Store citation info for processing
                            citation_info.append({
                                'index': i,
                                'source_id': source_id,
                                'file_path': file_path,
                                'doc_type': doc_type,
                                'relevance': relevance,
                                'excerpt': excerpt,
                                'mrn': mrn,
                                'patient_name': patient_name,
                                'author': author,
                                'department': department,
                                'document_date': document_date,
                                'doc_id': doc_id
                            })
                            
                            # Always extract metadata for chat interface (Cortex Agents doesn't provide author/department)
                            if doc_id not in ['N/A', '', None]:
                                documents_needing_metadata.append((doc_id, doc_type))
                                logger.info(f"Citation {i} needs metadata extraction: doc_id='{doc_id}', doc_type='{doc_type}'")
                        
                        # Batch extract metadata for documents that need it (same as Patient 360!)
                        extracted_metadata = {}
                        logger.info(f"Documents needing metadata: {len(documents_needing_metadata)}")
                        logger.info(f"Cortex search available: {cortex_search is not None}")
                        
                        if documents_needing_metadata and cortex_search:
                            try:
                                logger.info(f"Starting metadata extraction for {len(documents_needing_metadata)} documents...")
                                with st.spinner(f"Extracting metadata for {len(documents_needing_metadata)} documents..."):
                                    doc_ids = [item[0] for item in documents_needing_metadata]
                                    doc_types = [item[1] for item in documents_needing_metadata]
                                    logger.info(f"Calling batch_extract_document_metadata with doc_ids: {doc_ids}")
                                    logger.info(f"Doc types: {doc_types}")
                                    extracted_metadata = cortex_search.batch_extract_document_metadata(doc_ids, doc_types)
                                    logger.info(f"Metadata extraction completed. Results: {extracted_metadata}")
                            except Exception as e:
                                logger.error(f"Chat metadata extraction failed: {e}")
                                import traceback
                                logger.error(f"Full traceback: {traceback.format_exc()}")
                        else:
                            if not documents_needing_metadata:
                                logger.info("No documents need metadata extraction")
                            if not cortex_search:
                                logger.warning("Cortex search service not available for metadata extraction")
                        
                        # Display documents with updated metadata
                        for cit_info in citation_info:
                            i = cit_info['index']
                            source_id = cit_info['source_id']
                            file_path = cit_info['file_path']
                            doc_type = cit_info['doc_type']
                            relevance = cit_info['relevance']
                            excerpt = cit_info['excerpt']
                            mrn = cit_info['mrn']
                            patient_name = cit_info['patient_name']
                            author = cit_info['author']
                            department = cit_info['department']
                            document_date = cit_info['document_date']
                            doc_id = cit_info['doc_id']
                            
                            # Update with extracted metadata if available
                            if doc_id in extracted_metadata:
                                extracted = extracted_metadata[doc_id]
                                if author in ['N/A', '', None] and extracted.get('author'):
                                    author = extracted['author']
                                if department in ['N/A', '', None] and extracted.get('department'):
                                    department = extracted['department']
                            
                            # Generate unique key for this message and citation using stable hash
                            stable_hash = hash(f"{idx}_{doc_id}_{i}_{excerpt[:50] if excerpt else ''}")
                            btn_key = f"chat_btn_{stable_hash}"
                            is_viewing_document = st.session_state.get(btn_key, False)
                            
                            with st.expander(
                                f"📄 {doc_type} - Source {source_id}",
                                expanded=is_viewing_document
                            ):
                                st.write(f"**Author:** {author}")
                                st.write(f"**Department:** {department}")
                                if file_path:
                                    st.write(f"**File Path:** {file_path}")
                                else:
                                    st.write(f"**File Path:** Not available from Cortex Agents")
                                if relevance > 0:
                                    st.write(f"**Relevance Score:** {relevance:.2f}")
                                if patient_name:
                                    st.write(f"**Patient:** {patient_name}")
                                if document_date:
                                    st.write(f"**Date:** {document_date}")
                                
                                # Display excerpt from search results
                                if excerpt and excerpt.strip():
                                    st.markdown("**Relevant Content:**")
                                    display_excerpt = excerpt[:500] + "..." if len(excerpt) > 500 else excerpt
                                    st.markdown(f">{display_excerpt}")
                                
                                # View Full Document button (same pattern as Patient 360)
                                show_document = st.button(f"📄 View Full Document", key=f"view_{stable_hash}")
                                
                                # Display content based on button state (same pattern as Patient 360)
                                if st.session_state.get(btn_key, False):
                                    try:
                                        with st.spinner("Loading full document..."):
                                            # Use doc_id for document retrieval (same as Patient 360)
                                            full_content = cortex_search.get_full_document_content(doc_id, doc_type, mrn)
                                        
                                        if full_content and full_content.strip():
                                            st.markdown("---")
                                            st.markdown("### 📄 **Full Document Content**")
                                            st.text_area(
                                                "Document Text",
                                                value=full_content,
                                                height=400,
                                                disabled=True,
                                                label_visibility="collapsed",
                                                key=f"chat_doc_content_{stable_hash}"
                                            )
                                            
                                            # Hide button to close document
                                            hide_btn_key = f"hide_{stable_hash}"
                                            if st.button(f"🔽 Hide Document", key=hide_btn_key):
                                                st.session_state[btn_key] = False
                                                st.rerun()
                                        else:
                                            st.warning("Could not retrieve full document content.")
                                            st.info("The document may no longer be available.")
                                            
                                    except Exception as e:
                                        st.error(f"Error loading document: {e}")
                                        import traceback
                                        st.code(traceback.format_exc())
                                
                                # Handle button click to toggle document view state
                                if show_document:
                                    st.session_state[btn_key] = True
                                    st.rerun()
            
            elif isinstance(message, ChatMessage):
                # Enhanced message format with content persistence
                with st.chat_message(message.role):
                    if message.role == 'user':
                        st.markdown(message.content)
                    else:
                        # Display processed content if available
                        if message.is_processed and message.processed_content:
                            logger.debug(f"Displaying {len(message.processed_content)} content items")
                            for i, content_item in enumerate(message.processed_content):
                                if hasattr(content_item, 'actual_instance'):
                                    instance = content_item.actual_instance
                                    logger.debug(f"Content item {i}: {type(instance).__name__}")
                                    
                                    if isinstance(instance, TextContentItem):
                                        st.markdown(instance.text, unsafe_allow_html=True)
                                    
                                    elif isinstance(instance, TableContentItem):
                                        logger.info(f"Displaying table with {len(instance.data)} rows, {len(instance.columns)} columns")
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
    
    
    # Chat input at the bottom (outside of chat_container)
    st.markdown("---")
    
    # Handle example query injection
    default_value = ""
    if 'example_query' in st.session_state:
        default_value = st.session_state.example_query
        del st.session_state.example_query
    
    # Process any pending query first
    if default_value:
        _process_user_query(default_value)
    
    # Chat input always at the very bottom
    if query := st.chat_input("Ask about patients, conditions, or search clinical documents...", key="chat_input"):
        # Process the query
        _process_user_query(query)

def _process_user_query(query: str):
    """Process a user query through Cortex Agents."""
    
    # Display user message in chat format
    with st.chat_message("user"):
        st.markdown(query)
    
    # Add user message using ChatMessage for consistency
    user_message = ChatMessage(role="user", content=query)
    st.session_state.chat_messages.append(user_message)
    
    # Create unique containers for this conversation turn to preserve previous responses
    import time
    turn_id = int(time.time() * 1000)  # Unique timestamp ID
    
    # Initialize response components
    thinking_steps = []
    sql_query = None
    search_results = []
    final_response = ""
    error_occurred = False
    
    # Track content for persistence
    tables_data = []
    charts_data = []
    
    # Create containers for this specific turn
    thinking_container = st.container()
    response_container = st.container()
    response_placeholder = response_container.empty()
    
    with thinking_container:
        thinking_expander = st.expander("🧠 Agent Thinking Process (Live)", expanded=True)
        # Use a container with chat messages for built-in auto-scroll behavior
        with thinking_expander:
            # Add CSS to reduce chat message padding for compact display
            st.html("""
            <style>
            .stChatMessage {
                padding-top: 0.25rem;
                padding-bottom: 0.25rem;
                margin-bottom: 0.25rem;
            }
            </style>
            """)
            thinking_chat_container = st.container(height=200)
            thinking_placeholder = thinking_chat_container.empty()
        
    with response_container:
        with st.spinner("🤖 Processing your request with AI agents..."):
            try:
                # Build the payload
                logger.info(f"DEBUG: Building payload for query: {query}")
                payload = cortex_agents._build_agent_payload(
                    query,
                    st.session_state.conversation_history,
                    thread_id=None  # Temporarily disable threads
                )
                
                logger.info(f"DEBUG: Sending to endpoint: {cortex_agents.api_endpoint}")
                
                # Stream the response
                event_count = 0
                logger.info("DEBUG: Starting SSE streaming loop")
                
                # Single buffer for accumulating ALL thinking text as one continuous paragraph
                thinking_buffer = ""
                tool_status = ""
                
                # Track what content we have for the response
                has_sql = False
                has_table = False
                has_chart = False
                chart_spec = None
                table_df = None
                
                def update_response_display():
                    """Update the complete response display with all accumulated content"""
                    with response_placeholder:
                        with st.chat_message("assistant"):
                            # Display SQL query if we have it
                            if has_sql and sql_query:
                                st.markdown("### 🔍 Generated SQL Query")
                                st.code(sql_query, language="sql")
                            
                            # Display table results if we have them
                            if has_table and table_df is not None:
                                st.markdown("### 📊 Query Results")
                                st.dataframe(table_df, use_container_width=True)
                            
                            # Display response text BEFORE chart (agent expects chart "below" the text)
                            if final_response:
                                st.markdown("### 💬 Response")
                                st.markdown(final_response)
                            
                            # Display chart AFTER response text (agent says "chart below")
                            if has_chart and chart_spec is not None:
                                st.markdown("### 📈 Data Visualization")
                                st.vega_lite_chart(chart_spec, use_container_width=True)
                
                for event in send_message_with_streaming(
                    cortex_agents.api_endpoint,
                    payload,
                    timeout=60000
                ):
                    event_count += 1
                    event_type = event.get("type")
                    logger.info(f"CHAT: Received SSE event {event_count}: {event_type}")
                    logger.debug(f"CHAT: Event data: {event}")
                    
                    if event_type == "thinking":
                        # Just append thinking text to the buffer
                        thinking_text = event.get("text", "")
                        
                        # Store original for history
                        thinking_steps.append(thinking_text)
                        
                        # Accumulate text as-is - the agent sends proper text
                        thinking_buffer += thinking_text
                        
                        # Update display using chat_message for built-in auto-scroll
                        thinking_placeholder.empty()
                        with thinking_placeholder:
                            with st.chat_message("assistant", avatar="🧠"):
                                st.markdown(thinking_buffer)
                    
                    elif event_type == "tool_use":
                        # Show tool status once, append to thinking buffer instead of separate status
                        tool_name = event.get('tool_name', 'Unknown')
                        tool_message = f"\n\n🔧 Using tool: {tool_name}\n\n"
                        thinking_buffer += tool_message
                        
                        # Update display
                        thinking_placeholder.empty()
                        with thinking_placeholder:
                            with st.chat_message("assistant", avatar="🧠"):
                                st.markdown(thinking_buffer)
                    
                    elif event_type == "sql":
                        # Capture SQL query and append status to thinking buffer
                        sql_query = event["query"]
                        has_sql = True
                        sql_message = f"\n\n✅ Generated SQL query\n\n"
                        thinking_buffer += sql_message
                        
                        # Update thinking display
                        thinking_placeholder.empty()
                        with thinking_placeholder:
                            with st.chat_message("assistant", avatar="🧠"):
                                st.markdown(thinking_buffer)
                        
                        # Update unified response display
                        update_response_display()
                    
                    elif event_type == "search_results":
                        # Capture search results and append status to thinking buffer
                        search_results = event.get("results", [])
                        count = event.get('count', len(search_results))
                        search_message = f"\n\n✅ Found {count} search results\n\n"
                        thinking_buffer += search_message
                        
                        # Update display
                        thinking_placeholder.empty()
                        with thinking_placeholder:
                            with st.chat_message("assistant", avatar="🧠"):
                                st.markdown(thinking_buffer)
                    
                    elif event_type == "text_delta":
                        # Stream the actual response text in real-time!
                        text_delta = event.get("text", "")
                        accumulated_text = event.get("accumulated", "")
                        
                        # Update the final response with accumulated text
                        final_response = accumulated_text
                        
                        # Update unified response display with all content
                        update_response_display()
                        
                        logger.debug(f"Text delta received: {len(text_delta)} chars, total: {len(accumulated_text)} chars")
                    
                    elif event_type == "table":
                        # Process and store table data
                        table_data = event.get("data", {})
                        logger.info("Received table event for SQL results")
                        
                        try:
                            # Parse table data from Cortex response format
                            if 'result_set' in table_data:
                                result_set = table_data['result_set']
                                if 'data' in result_set and 'result_set_meta_data' in result_set:
                                    import pandas as pd
                                    import numpy as np
                                    
                                    # Extract data and column names
                                    data_array = np.array(result_set['data'])
                                    metadata = result_set['result_set_meta_data']
                                    
                                    # Get column names
                                    if 'row_type' in metadata:
                                        column_names = [col.get('name', f'col_{i}') for i, col in enumerate(metadata['row_type'])]
                                    else:
                                        column_names = [f'col_{i}' for i in range(len(data_array[0]) if len(data_array) > 0 else 0)]
                                    
                                    # Store the DataFrame
                                    table_df = pd.DataFrame(data_array, columns=column_names)
                                    has_table = True
                                    
                                    # Store for persistence
                                    tables_data.append({
                                        'data': data_array.tolist(),
                                        'columns': column_names,
                                        'title': "### 📊 Query Results"
                                    })
                                    
                        except Exception as e:
                            logger.error(f"Table processing error: {e}")
                        
                        # Update unified response display
                        update_response_display()
                    
                    elif event_type == "chart":
                        # Process and store chart data
                        chart_data = event.get("data", {})
                        logger.info("Received chart event for visualization")
                        
                        try:
                            # Parse chart specification
                            if 'chart_spec' in chart_data:
                                import json
                                chart_spec_raw = chart_data['chart_spec']
                                if isinstance(chart_spec_raw, str):
                                    spec = json.loads(chart_spec_raw)
                                else:
                                    spec = chart_spec_raw
                                
                                # Handle nested chart structure
                                if isinstance(spec, dict) and "charts" in spec:
                                    charts_array = spec["charts"]
                                    if isinstance(charts_array, list) and len(charts_array) > 0:
                                        first_chart = charts_array[0]
                                        if isinstance(first_chart, str):
                                            spec = json.loads(first_chart)
                                        else:
                                            spec = first_chart
                                
                                # Store the chart spec
                                chart_spec = spec
                                has_chart = True
                                
                                # Store for persistence
                                charts_data.append({
                                    'spec': spec,
                                    'title': "### 📈 Data Visualization"
                                })
                                        
                        except Exception as e:
                            logger.error(f"Chart processing error: {e}")
                        
                        # Update unified response display
                        update_response_display()
                    
                    elif event_type == "response_text":
                        # Final complete response
                        final_response = event["text"]
                        logger.info(f"DEBUG: Received final response text")
                    
                    elif event_type == "done":
                        # Finalize the response
                        logger.info("DEBUG: Stream completed")
                        # Make sure we have the final text
                        if "final_text" in event and event["final_text"]:
                            final_response = event["final_text"]
                            logger.info(f"DEBUG: Got final_text from done event: {len(final_response)} chars")
                        else:
                            logger.warning(f"DEBUG: No final_text in done event. Event keys: {event.keys()}")
                        break
                    
                    elif event_type == "error":
                        error_msg = event.get('message', 'Unknown error')
                        st.error(f"Error: {error_msg}")
                        logger.error(f"CHAT ERROR: {error_msg}")
                        error_occurred = True
                        break
                
                # Collapse thinking expander after completion but keep it visible for review
                thinking_expander.expanded = False
                
                # Log what we have
                logger.info(f"DEBUG: After streaming - final_response length: {len(final_response) if final_response else 0}")
                logger.info(f"DEBUG: After streaming - sql_query: {bool(sql_query)}")
                logger.info(f"DEBUG: After streaming - thinking_steps count: {len(thinking_steps)}")
                
                # Create response dict for compatibility
                response = {
                    "content": final_response,
                    "sql": sql_query,
                    "thinking_steps": thinking_steps,
                    "citations": search_results
                } if final_response else None
            
            except Exception as e:
                logger.error(f"Error during streaming: {e}")
                st.error(f"Error: {str(e)}")
                error_occurred = True
                response = None
            
            if error_occurred:
                return
                
            if not response or not final_response:
                error_msg = "No response received from the agent"
                
                # Enhanced error logging
                logger.error(f"CHAT ERROR: {error_msg}")
                logger.error(f"CHAT ERROR - Full response object: {response}")
                
                # Log debug information if available
                if response and isinstance(response, dict):
                    if 'debug_info' in response:
                        debug_info = response['debug_info']
                        logger.error(f"CHAT ERROR - Debug Info: {debug_info}")
                        logger.error(f"CHAT ERROR - Endpoint: {debug_info.get('endpoint', 'Unknown')}")
                        logger.error(f"CHAT ERROR - Error Type: {response.get('exception_type', 'Unknown')}")
                        
                    if 'full_traceback' in response:
                        logger.error(f"CHAT ERROR - Full Traceback: {response['full_traceback']}")
                        
                    if 'error_code' in response:
                        logger.error(f"CHAT ERROR - Error Code: {response['error_code']}")
                        
                    if 'status_code' in response:
                        logger.error(f"CHAT ERROR - Status Code: {response['status_code']}")
                
                # Simple error message for user (no debug clutter)
                error_display = f"❌ I encountered an error: {error_msg}"
                
                # Add error to chat history
                st.session_state.chat_messages.append({
                    "role": "assistant",
                    "content": error_display,
                    "error_details": response if response else None
                })
                st.rerun()
                return
            
            # Use the streamed response directly
            response_text = final_response
            citations = search_results
            
            if not response_text:
                response_text = "I received your query but couldn't generate a meaningful response. Please try rephrasing your question."
                logger.warning("CHAT WARNING: Empty response_text from streaming")
            
            # Execute SQL if present
            results = None
            if sql_query:
                with st.spinner("Executing SQL query..."):
                    try:
                        results = cortex_agents.execute_sql_query(sql_query)
                        # If we got results and no table was streamed, add the results as a table
                        if results is not None and not tables_data:
                            try:
                                df = results.to_pandas()
                                if not df.empty:
                                    tables_data.append({
                                        'data': df.values.tolist(),
                                        'columns': df.columns.tolist(),
                                        'title': "### 📊 Query Results"
                                    })
                                    logger.info(f"Added SQL results to tables_data: {len(df)} rows")
                            except Exception as e:
                                logger.error(f"Error converting results to table: {e}")
                    except Exception as e:
                        logger.error(f"Error executing SQL: {e}")
                        results = None
            
            # Create ChatMessage with content persistence
            assistant_message = ChatMessage(
                role="assistant",
                content=response_text,
                sql=sql_query,
                thinking_steps=thinking_steps,
                citations=citations,
                results=results
            )
            
            # Store processed content for persistence
            logger.info(f"DEBUG: Storing content - tables: {len(tables_data)}, charts: {len(charts_data)}")
            if tables_data:
                logger.info(f"DEBUG: First table has {len(tables_data[0]['data'])} rows, {len(tables_data[0]['columns'])} columns")
            
            assistant_message.store_processed_content(
                processed_text=response_text,
                sql_query=sql_query,
                tables=tables_data,
                charts=charts_data
            )
            
            st.session_state.chat_messages.append(assistant_message)
            
            # Don't display response here - it's already being displayed by the SSE streaming
            # The streaming handles the real-time response display
            
            # Update conversation history for context
            st.session_state.conversation_history.extend([
                {
                    "role": "user",
                    "content": [{"type": "text", "text": query}]
                },
                {
                    "role": "assistant", 
                    "content": [{"type": "text", "text": response_text}]
                }
            ])
                
            # Limit conversation history to last 10 exchanges
            if len(st.session_state.conversation_history) > 20:
                st.session_state.conversation_history = st.session_state.conversation_history[-20:]
    
    # Don't rerun - let the response display naturally to preserve the thinking box
    # st.rerun()  # Removed to keep thinking box visible

def _render_welcome_message():
    """Render a welcome message with capabilities."""
    
    st.markdown("""
    ### 👋 Welcome to the AI Healthcare Assistant!
    
    **🚀 Powered by Cortex Agents v2** with advanced multi-step reasoning and tool orchestration.
    
    I can help you explore patient data and clinical documents using natural language. Here's what I can do:
    
    **🔍 Query Patient Data:**
    - Find patients by demographics, conditions, or visit patterns
    - Analyze lab results, medications, and vital signs
    - Generate population health insights
    
    **📋 Search Clinical Documents:**
    - Find relevant clinical notes and reports
    - Search by symptoms, treatments, or medical terms
    - Access radiology reports and discharge summaries
    
    **🧠 Advanced v2 Capabilities:**
    - **Multi-step reasoning**: I break down complex questions into logical steps
    - **Tool orchestration**: I automatically combine multiple data sources
    - **Conversation threads**: I maintain full context across our conversation
    - **Execution traces**: View my reasoning process in Snowsight
    
    **Try asking complex questions that require multi-step analysis:**
    - "Show me the top 5 diagnoses for patients aged 10-15 and create a chart showing their distribution"
    - "Find patients with asthma who had recent ER visits and analyze their medication patterns"
    - "Analyze readmission patterns for heart conditions and identify high-risk patients"
    
    **Or simpler queries:**
    - "Show me pediatric asthma patients with recent ER visits"
    - "Find notes mentioning drug allergies"
    - "What are the top diagnoses this month?"
    """)

# Welcome message will be displayed in render_chat_interface() when needed