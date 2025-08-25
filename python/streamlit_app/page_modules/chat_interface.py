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

from services import cortex_agents, data_service, session_manager
from utils import helpers

logger = logging.getLogger(__name__)

def render():
    """Entry point called by main.py"""
    render_chat_interface()

def render_chat_interface():
    """Main entry point for the AI chat interface page"""
    
    st.title("🤖 AI Healthcare Assistant")
    st.markdown("Ask questions about patients, cohorts, or clinical data using natural language")
    
    # Sidebar for new conversation and examples
    with st.sidebar:
        st.subheader("💬 Conversation")
        
        if st.button("🔄 New Conversation", key="new_chat"):
            st.session_state.chat_messages = []
            st.session_state.conversation_history = []
            st.rerun()
        
        st.markdown("---")
        
        st.subheader("📝 Example Queries")
        
        example_queries = [
            "Show me all asthma patients aged 5-12 with recent ER visits",
            "Find patients with diabetes who haven't had HbA1c in 6 months",
            "What are the most common diagnoses for patients from ZIP code 77001?",
            "Search for clinical notes mentioning medication allergies",
            "Analyze readmission patterns for heart conditions",
            "Find patients with elevated BMI who need nutrition counseling"
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
    
    # Initialize session state for chat
    if 'chat_messages' not in st.session_state:
        st.session_state.chat_messages = []
    
    if 'conversation_history' not in st.session_state:
        st.session_state.conversation_history = []
    
    # Create a container for chat messages with fixed height to keep input at bottom
    chat_container = st.container()
    
    with chat_container:
        # Display welcome message for new conversations
        if not st.session_state.chat_messages:
            with st.chat_message("assistant"):
                _render_welcome_message()
        
        # Display chat messages
        for idx, message in enumerate(st.session_state.chat_messages):
            with st.chat_message(message['role']):
                if message['role'] == 'user':
                    st.markdown(message['content'])
                else:
                    # Display assistant response
                    st.markdown(message['content'])
                    
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
    
    # Chat input at the bottom (outside of chat_container)
    st.markdown("---")
    
    # Handle example query injection
    default_value = ""
    if 'example_query' in st.session_state:
        default_value = st.session_state.example_query
        del st.session_state.example_query
    
    # Chat input always at the bottom
    if query := st.chat_input("Ask about patients, conditions, or search clinical documents...", key="chat_input"):
        # Process the query
        _process_user_query(query)
    
    # If we have a default value, auto-submit it
    elif default_value:
        _process_user_query(default_value)

def _process_user_query(query: str):
    """Process a user query through Cortex Agents."""
    
    # Add user message to chat
    st.session_state.chat_messages.append({
        "role": "user", 
        "content": query
    })
    
    # Get response from Cortex Agents
    with st.spinner("🤖 Processing your request with AI agents..."):
        try:
            # Send to Cortex Agents
            response = cortex_agents.send_message(query, st.session_state.conversation_history)
            
            if not response or "error" in response:
                error_msg = response.get("error", "Unknown error") if response else "No response received"
                
                # Add error to chat history
                st.session_state.chat_messages.append({
                    "role": "assistant",
                    "content": f"❌ I encountered an error: {error_msg}",
                    "error_details": response if response else None
                })
                st.rerun()
                return
            
            # Process the response
            response_text, sql_query, citations = cortex_agents.process_agent_response(response)
            
            if not response_text:
                response_text = "I received your query but couldn't generate a meaningful response. Please try rephrasing your question."
            
            # Execute SQL if present
            results = None
            if sql_query:
                with st.spinner("Executing SQL query..."):
                    try:
                        results = cortex_agents.execute_sql_query(sql_query)
                    except Exception as e:
                        logger.error(f"Error executing SQL: {e}")
                        results = None
            
            # Add response to chat history (citations will be displayed from history)
            assistant_message = {
                "role": "assistant",
                "content": response_text,
                "sql": sql_query,
                "citations": citations,
                "results": results
            }
            
            st.session_state.chat_messages.append(assistant_message)
            
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
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            
            # Add error to chat history
            st.session_state.chat_messages.append({
                "role": "assistant",
                "content": f"❌ An unexpected error occurred: {str(e)}"
            })
    
    # Trigger rerun to display the new messages
    st.rerun()

def _render_welcome_message():
    """Render a welcome message with capabilities."""
    
    st.markdown("""
    ### 👋 Welcome to the AI Healthcare Assistant!
    
    I can help you explore patient data and clinical documents using natural language. Here's what I can do:
    
    **🔍 Query Patient Data:**
    - Find patients by demographics, conditions, or visit patterns
    - Analyze lab results, medications, and vital signs
    - Generate population health insights
    
    **📋 Search Clinical Documents:**
    - Find relevant clinical notes and reports
    - Search by symptoms, treatments, or medical terms
    - Access radiology reports and discharge summaries
    
    **🤖 Intelligent Routing:**
    - I automatically determine whether to query databases or search documents
    - I can combine structured and unstructured data in responses
    - I maintain context across our conversation
    
    **Try asking questions like:**
    - "Show me pediatric asthma patients with recent ER visits"
    - "Find notes mentioning drug allergies"
    - "What are the top diagnoses this month?"
    """)

# Welcome message will be displayed in render_chat_interface() when needed