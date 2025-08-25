"""
Cortex Search Service for TCH Patient 360 PoC

Implements Snowflake Cortex Search functionality for semantic search across
clinical documents, patient records, and unstructured healthcare data.
Provides AI-powered search capabilities to find relevant information
using natural language queries.
"""

import pandas as pd
import streamlit as st
from typing import Dict, List, Optional, Any, Tuple
import logging
import json
from datetime import datetime

from services.session_manager import SessionManager
from snowflake.core import Root

logger = logging.getLogger(__name__)

class CortexSearchService:
    """Cortex Search service for semantic search capabilities"""
    
    def __init__(self, session_manager: Optional[SessionManager] = None):
        self.session_manager = session_manager or SessionManager()
        self.search_services = {}
        self._initialize_search_services()
        
    def _initialize_search_services(self):
        """Initialize Cortex Search services for different data types"""
        try:
            session = self.session_manager.get_session()
            
            # Initialize search services to match actual deployed services
            # Reference: sql/cortex/02_cortex_search_setup.sql
            self.search_services = {
                'clinical_notes': 'CLINICAL_NOTES_SEARCH',
                'radiology_reports': 'RADIOLOGY_REPORTS_SEARCH',
                'clinical_documentation': 'CLINICAL_DOCUMENTATION_SEARCH'
            }
            
            logger.info("Cortex Search services initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cortex Search services: {e}")
            # Fallback to structured search if Cortex Search is not available
            self.search_services = {}
    
    def _get_mrn_from_patient_id(self, patient_id: str) -> Optional[str]:
        """Get MRN from patient_id for semantic search filtering"""
        try:
            session = self.session_manager.get_session()
            query = f"SELECT mrn FROM CONFORMED.PATIENT_MASTER WHERE patient_id = '{patient_id}'"
            result = session.sql(query).to_pandas()
            
            if not result.empty:
                mrn = result.iloc[0]['MRN']
                logger.info(f"Found MRN {mrn} for patient {patient_id}")
                return mrn
            else:
                logger.warning(f"No MRN found for patient {patient_id}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get MRN for patient {patient_id}: {e}")
            return None
    
    def _parse_extract_answer_response(self, response: str) -> Optional[str]:
        """Parse EXTRACT_ANSWER JSON response to get the actual answer"""
        try:
            if not response:
                return None
                
            # Handle string responses that might be JSON or plain text
            if isinstance(response, str):
                text = response.strip()
                # If it starts with '[', it's likely JSON array format
                if text.startswith('['):
                    import json
                    parsed = json.loads(text)
                    if isinstance(parsed, list) and len(parsed) > 0:
                        if isinstance(parsed[0], dict) and 'answer' in parsed[0]:
                            return str(parsed[0]['answer']).strip()
                    return None
                else:
                    # Plain text response
                    return text
            
            return str(response)
            
        except Exception as e:
            logger.error(f"Failed to parse EXTRACT_ANSWER response: {e}")
            return None
    
    def _clean_author_text(self, text: Optional[str]) -> str:
        """Normalize author/provider text to a short, display-friendly value (no re-casing)."""
        if not text:
            return 'N/A'
        cleaned = ' '.join(str(text).strip().strip(':').split())
        # Guard against obviously wrong matches
        if any(bad in cleaned.lower() for bad in ['mrn', 'dob', 'subjective', 'objective', 'assessment', 'plan']):
            return 'N/A'
        # Trim extremely long answers
        return cleaned if len(cleaned) <= 80 else cleaned[:77] + '…'
    
    def _clean_department_text(self, text: Optional[str]) -> str:
        """Normalize department text by mapping to known departments when possible (no re-casing)."""
        if not text:
            return 'N/A'
        raw = ' '.join(str(text).strip().strip(':').split())
        # If the answer looks like a paragraph or contains unrelated headers, ignore
        if len(raw) > 80 or any(term in raw.lower() for term in ['mrn', 'dob', 'subjective', 'objective', 'assessment', 'plan', 'patient', 'author:', 'department:']):
            # Try to salvage by matching known departments from the dataset
            known_departments = [
                'Emergency Department', 'Pediatric ICU', 'NICU', 'General Pediatrics', 'Cardiology', 'Neurology',
                'Oncology', 'Orthopedics', 'Pulmonology', 'Gastroenterology', 'Endocrinology', 'Nephrology',
                'Radiology', 'Laboratory', 'Pharmacy'
            ]
            lower = raw.lower()
            for dept in known_departments:
                if dept.lower() in lower:
                    return dept
            return 'N/A'
        return raw
    
    def _extract_document_metadata(self, content: str) -> Dict[str, str]:
        """Extract metadata from document content using Cortex EXTRACT_ANSWER"""
        try:
            session = self.session_manager.get_session()
            
            # Escape single quotes in content for SQL (no truncation to avoid losing trailing metadata)
            clean_content = content.replace("'", "''")
            
            # Extract author and department using AI_EXTRACT in a single call
            ai_query = f"""
            SELECT SNOWFLAKE.CORTEX.AI_EXTRACT(
                '{clean_content}',
                {{
                    'author': 'What nurse, consultant, care provider, doctor (Dr or MD), or attending physician was mentioned or credited as authoring or signing this document? Do not return the patient name.', 
                    'department': 'What medical department, medical service, or medical specialty (Such as: Nursing, Cardiology, Dermatology, ENT (Ear Nose Throat), Emergency, Endocrinology, Gastroenterology, Neurology, etc) is mentioned in or the main focus of this document?'
                }}
            ) as ai_response
            """
            ai_result = session.sql(ai_query).to_pandas()
            if ai_result.empty:
                return {'author': 'N/A', 'department': 'N/A'}
            raw_resp = ai_result.iloc[0]['AI_RESPONSE']
            try:
                if isinstance(raw_resp, str):
                    import json as _json
                    parsed = _json.loads(raw_resp)
                else:
                    parsed = raw_resp
            except Exception:
                parsed = None
            if isinstance(parsed, dict) and 'response' in parsed and isinstance(parsed['response'], dict):
                data = parsed['response']
            elif isinstance(parsed, dict):
                data = parsed
            else:
                data = {}
            author = self._clean_author_text(data.get('author'))
            department = self._clean_department_text(data.get('department'))
            return {'author': author, 'department': department}
            
        except Exception as e:
            logger.error(f"Failed to extract document metadata: {e}")
            return {'author': 'N/A', 'department': 'N/A'}
    
    def batch_extract_document_metadata(self, document_ids: List[str], document_types: List[str]) -> Dict[str, Dict[str, str]]:
        """
        Batch extract metadata for multiple documents using a single SQL query.
        Much more efficient than individual EXTRACT_ANSWER calls.
        
        Args:
            document_ids: List of document IDs to process
            document_types: List of document types (for determining which table to query)
            
        Returns:
            Dictionary mapping document_id to extracted metadata
        """
        try:
            if not document_ids:
                return {}
            
            session = self.session_manager.get_session()
            
            # Group documents by type to query the appropriate raw tables
            clinical_notes_ids = []
            radiology_ids = []
            
            for doc_id, doc_type in zip(document_ids, document_types):
                if 'clinical note' in doc_type.lower() or 'note' in doc_type.lower():
                    clinical_notes_ids.append(doc_id)
                elif 'radiology' in doc_type.lower() or 'imaging' in doc_type.lower():
                    radiology_ids.append(doc_id)
                else:
                    # Default to clinical notes for unknown types
                    clinical_notes_ids.append(doc_id)
            
            results = {}
            
            # Batch query for clinical notes
            if clinical_notes_ids:
                notes_results = self._batch_extract_from_table(
                    session,
                    'RAW_DATA.CLINICAL_NOTES_RAW',
                    clinical_notes_ids,
                    'file_path'
                )
                results.update(notes_results)
            
            # Batch query for radiology reports
            if radiology_ids:
                radiology_results = self._batch_extract_from_table(
                    session,
                    'RAW_DATA.RADIOLOGY_REPORTS_RAW', 
                    radiology_ids,
                    'file_path'
                )
                results.update(radiology_results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error in batch metadata extraction: {e}")
            # Return empty metadata for all documents
            return {doc_id: {'author': 'N/A', 'department': 'N/A'} for doc_id in document_ids}
    
    def _batch_extract_from_table(self, session, table_name: str, document_ids: List[str], id_column: str) -> Dict[str, Dict[str, str]]:
        """
        Execute batch EXTRACT_ANSWER query on a specific table.
        
        Args:
            session: Snowpark session
            table_name: Name of the raw data table
            document_ids: List of document IDs
            id_column: Name of the ID column in the table
            
        Returns:
            Dictionary mapping document_id to extracted metadata
        """
        try:
            if not document_ids:
                return {}
            
            # Create comma-separated list for IN clause
            ids_list = "'" + "','".join(document_ids) + "'"
            
            query = f"""
            SELECT 
                {id_column} as document_id,
                SNOWFLAKE.CORTEX.AI_EXTRACT(
                    raw_content,
                    {{
                        'author': 'What nurse, consultant, care provider, doctor (Dr or MD), or attending physician was mentioned or credited as authoring or signing this document? Do not return the patient name.', 
                        'department': 'What medical department, medical service, or medical specialty (Such as: Nursing, Cardiology, Dermatology, ENT (Ear Nose Throat), Emergency, Endocrinology, Gastroenterology, Neurology, etc) is mentioned in or the main focus of this document?'
                    }}
                ) as ai_response
            FROM {table_name}
            WHERE {id_column} IN ({ids_list})
            AND raw_content IS NOT NULL
            AND LENGTH(TRIM(raw_content)) > 50
            """
            
            result = session.sql(query).to_pandas()
            
            extracted_data = {}
            for _, row in result.iterrows():
                doc_id = row['DOCUMENT_ID']
                raw_resp = row['AI_RESPONSE']
                try:
                    if isinstance(raw_resp, str):
                        import json as _json
                        parsed = _json.loads(raw_resp)
                    else:
                        parsed = raw_resp
                except Exception:
                    parsed = None
                if isinstance(parsed, dict) and 'response' in parsed and isinstance(parsed['response'], dict):
                    data = parsed['response']
                elif isinstance(parsed, dict):
                    data = parsed
                else:
                    data = {}
                # Parse and clean responses
                author = self._clean_author_text(data.get('author'))
                department = self._clean_department_text(data.get('department'))
                
                extracted_data[doc_id] = {
                    'author': author,
                    'department': department
                }
            
            # Ensure every requested id has a result entry
            for doc_id in document_ids:
                extracted_data.setdefault(doc_id, {'author': 'N/A', 'department': 'N/A'})
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error executing batch query on {table_name}: {e}")
            return {doc_id: {'author': 'N/A', 'department': 'N/A'} for doc_id in document_ids}
    
    def _extract_metadata_by_mrn(self, mrn: str, doc_type: str) -> Dict[str, str]:
        """
        Extract metadata for a single document using MRN-based lookup.
        Used by chat interface where we only have MRN, not file_id.
        
        Args:
            mrn: Patient MRN (e.g., "MRN35435139")
            doc_type: Document type for table selection
            
        Returns:
            Dictionary with author and department metadata
        """
        try:
            session = self.session_manager.get_session()
            
            # Determine table based on document type
            if 'clinical note' in doc_type.lower() or 'note' in doc_type.lower():
                table_name = 'RAW_DATA.CLINICAL_NOTES_RAW'
            elif 'radiology' in doc_type.lower() or 'imaging' in doc_type.lower():
                table_name = 'RAW_DATA.RADIOLOGY_REPORTS_RAW'
            else:
                table_name = 'RAW_DATA.CLINICAL_NOTES_RAW'  # Default
            
            # Query using MRN to find a recent document for this patient
            query = f"""
            SELECT 
                SNOWFLAKE.CORTEX.AI_EXTRACT(
                    raw_content,
                    {{
                        'author': 'What nurse, consultant, care provider, doctor (Dr or MD), or attending physician was mentioned or credited as authoring or signing this document? Do not return the patient name.', 
                        'department': 'What medical department, medical service, or medical specialty (Such as: Nursing, Cardiology, Dermatology, ENT (Ear Nose Throat), Emergency, Endocrinology, Gastroenterology, Neurology, etc) is mentioned in or the main focus of this document?'
                    }}
                ) as ai_response
            FROM {table_name}
            WHERE MRN = '{mrn}'
            AND raw_content IS NOT NULL
            AND LENGTH(TRIM(raw_content)) > 50
            ORDER BY file_last_modified DESC
            LIMIT 1
            """
            
            result = session.sql(query).to_pandas()
            
            if not result.empty:
                row = result.iloc[0]
                raw_resp = row['AI_RESPONSE']
                try:
                    if isinstance(raw_resp, str):
                        import json as _json
                        parsed = _json.loads(raw_resp)
                    else:
                        parsed = raw_resp
                except Exception:
                    parsed = None
                if isinstance(parsed, dict) and 'response' in parsed and isinstance(parsed['response'], dict):
                    data = parsed['response']
                elif isinstance(parsed, dict):
                    data = parsed
                else:
                    data = {}
                author = self._clean_author_text(data.get('author'))
                department = self._clean_department_text(data.get('department'))
                return {
                    'author': author,
                    'department': department
                }
            
            return {'author': 'N/A', 'department': 'N/A'}
            
        except Exception as e:
            logger.error(f"Error extracting metadata by MRN {mrn}: {e}")
            return {'author': 'N/A', 'department': 'N/A'}
    
    def get_full_document_content(self, document_id: str, document_type: str = None, mrn: str = None) -> Optional[str]:
        """Retrieve the full content of a document from raw data tables"""
        try:
            session = self.session_manager.get_session()
            
            # Debug logging
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"get_full_document_content called with document_id: '{document_id}', document_type: '{document_type}', mrn: '{mrn}'")
            
            # Handle different types of document identifiers
            if document_id and "/" in document_id:
                # This is a file_path, use it directly
                file_path = document_id
                
                # Determine which table to query based on file_path
                if "clinical_notes" in file_path.lower():
                    table_name = "RAW_DATA.CLINICAL_NOTES_RAW"
                elif "radiology" in file_path.lower():
                    table_name = "RAW_DATA.RADIOLOGY_REPORTS_RAW"
                else:
                    # Default to clinical notes
                    table_name = "RAW_DATA.CLINICAL_NOTES_RAW"
                
                # Use exact file_path match
                query = f"""
                SELECT raw_content::VARCHAR as content 
                FROM {table_name} 
                WHERE file_path = '{file_path}'
                LIMIT 1
                """
                
                logger.info(f"Executing file_path query: {query}")
                
                result = session.sql(query).to_pandas()
                if not result.empty:
                    logger.info(f"Found document using file_path match")
                    content = result.iloc[0]['CONTENT']
                    return content
                
                # Fallback: try filename match
                filename = file_path.split("/")[-1] if "/" in file_path else file_path
                fallback_query = f"""
                SELECT raw_content::VARCHAR as content 
                FROM {table_name} 
                WHERE filename = '{filename}'
                LIMIT 1
                """
                
                logger.info(f"Executing filename fallback query: {fallback_query}")
                
                fallback_result = session.sql(fallback_query).to_pandas()
                if not fallback_result.empty:
                    logger.info(f"Found document using filename fallback")
                    content = fallback_result.iloc[0]['CONTENT']
                    return content

            else:
                # Legacy logic for document IDs
                # Use MRN for efficient lookup if available
                if mrn:
                    # Try clinical notes first using MRN
                    try:
                        clinical_query = f"""
                        SELECT raw_content::VARCHAR as content 
                        FROM RAW_DATA.CLINICAL_NOTES_RAW 
                        WHERE MRN = '{mrn}' AND (file_id = '{document_id}' OR filename = '{document_id}')
                        LIMIT 1
                        """
                        logger.info(f"Executing clinical query with MRN: {clinical_query}")
                        clinical_result = session.sql(clinical_query).to_pandas()
                        if not clinical_result.empty:
                            logger.info(f"Found document using clinical notes with MRN")
                            content = clinical_result.iloc[0]['CONTENT']
                            return content
                    except Exception as e:
                        logger.debug(f"Clinical notes query with MRN failed: {e}")
                    
                    # Try radiology reports using MRN
                    try:
                        radiology_query = f"""
                        SELECT raw_content::VARCHAR as content 
                        FROM RAW_DATA.RADIOLOGY_REPORTS_RAW 
                        WHERE MRN = '{mrn}' AND (file_id = '{document_id}' OR filename = '{document_id}')
                        LIMIT 1
                        """
                        logger.info(f"Executing radiology query with MRN: {radiology_query}")
                        radiology_result = session.sql(radiology_query).to_pandas()
                        if not radiology_result.empty:
                            logger.info(f"Found document using radiology reports with MRN")
                            content = radiology_result.iloc[0]['CONTENT']
                            return content
                    except Exception as e:
                        logger.debug(f"Radiology reports query with MRN failed: {e}")
                
                # Fallback: try without MRN (less efficient but more thorough)
                try:
                    clinical_query = f"""
                    SELECT raw_content::VARCHAR as content 
                    FROM RAW_DATA.CLINICAL_NOTES_RAW 
                    WHERE file_id = '{document_id}' OR filename = '{document_id}'
                    LIMIT 1
                    """
                    logger.info(f"Executing clinical fallback query: {clinical_query}")
                    clinical_result = session.sql(clinical_query).to_pandas()
                    if not clinical_result.empty:
                        logger.info(f"Found document using clinical notes fallback")
                        content = clinical_result.iloc[0]['CONTENT']
                        return content
                except Exception as e:
                    logger.debug(f"Clinical notes fallback query failed: {e}")
                
                try:
                    radiology_query = f"""
                    SELECT raw_content::VARCHAR as content 
                    FROM RAW_DATA.RADIOLOGY_REPORTS_RAW 
                    WHERE file_id = '{document_id}' OR filename = '{document_id}'
                    LIMIT 1
                    """
                    logger.info(f"Executing radiology fallback query: {radiology_query}")
                    radiology_result = session.sql(radiology_query).to_pandas()
                    if not radiology_result.empty:
                        logger.info(f"Found document using radiology reports fallback")
                        content = radiology_result.iloc[0]['CONTENT']
                        return content
                except Exception as e:
                    logger.debug(f"Radiology reports fallback query failed: {e}")
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve full document content for {document_id}: {e}")
            return None
    
    def get_full_document_by_filepath(self, file_path: str, mrn: str = None) -> Optional[str]:
        """Retrieve the full content of a document using the file_path from Cortex Search"""
        try:
            session = self.session_manager.get_session()
            
            if not file_path:
                logger.warning("No file_path provided for document retrieval")
                return None
            
            # Determine which table to query based on file_path
            if "clinical_notes" in file_path.lower():
                table_name = "RAW_DATA.CLINICAL_NOTES_RAW"
            elif "radiology" in file_path.lower():
                table_name = "RAW_DATA.RADIOLOGY_REPORTS_RAW"
            else:
                # Default to clinical notes
                table_name = "RAW_DATA.CLINICAL_NOTES_RAW"
            
            # Use exact file_path match for efficient lookup
            query_conditions = []
            if mrn:
                query_conditions.append(f"MRN = '{mrn}'")
            
            # Try exact file_path match
            query_conditions.append(f"file_path = '{file_path}'")
            
            where_clause = " AND ".join(query_conditions)
            
            query = f"""
            SELECT raw_content::VARCHAR as content 
            FROM {table_name} 
            WHERE {where_clause}
            LIMIT 1
            """
            
            if st.session_state.get('debug_mode', False):
                logger.info(f"Document retrieval query: {query}")
                st.code(f"Executing query: {query}", language="sql")
            
            result = session.sql(query).to_pandas()
            if not result.empty:
                return result.iloc[0]['CONTENT']
            
            # Fallback: try filename match if exact file_path fails
            if "filename" in file_path:
                filename = file_path.split("/")[-1] if "/" in file_path else file_path
                fallback_conditions = []
                if mrn:
                    fallback_conditions.append(f"MRN = '{mrn}'")
                fallback_conditions.append(f"filename = '{filename}'")
                
                fallback_where = " AND ".join(fallback_conditions)
                fallback_query = f"""
                SELECT raw_content::VARCHAR as content 
                FROM {table_name} 
                WHERE {fallback_where}
                LIMIT 1
                """
                
                fallback_result = session.sql(fallback_query).to_pandas()
                if not fallback_result.empty:
                    return fallback_result.iloc[0]['CONTENT']
            
            logger.warning(f"No document found for file_path: {file_path}, MRN: {mrn}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve document by file_path {file_path}: {e}")
            return None

    
    
    def search_clinical_notes(self, query: str, filters: Dict[str, Any] = None, limit: int = 50) -> List[Dict]:
        """
        Search clinical notes using Cortex Search Python API with MRN-based filtering
        
        Args:
            query: Search query string
            filters: Dictionary of filters (e.g., patient_id, date_range)
            limit: Maximum number of results
            
        Returns:
            List of dictionaries containing search results
        """
        try:
            session = self.session_manager.get_session()
            
            if 'clinical_notes' not in self.search_services:
                logger.warning("Clinical notes search service not available")
                return []
            
            # Use Python API for Cortex Search
            root = Root(session)
            
            # Get the search service
            search_service = (root
                .databases["TCH_PATIENT_360_POC"]
                .schemas["AI_ML"]
                .cortex_search_services[self.search_services['clinical_notes']]
            )
            
            # Use exact MRN filtering with Cortex Search @eq operator
            filter_config = None
            if filters and 'patient_id' in filters:
                patient_mrn = self._get_mrn_from_patient_id(filters['patient_id'])
                if patient_mrn:
                    filter_config = {"@eq": {"MRN": patient_mrn}}
                    logger.info(f"Clinical notes search using exact MRN filter: {patient_mrn}")
            
            # Perform search using Python API with exact filtering
            search_results = search_service.search(
                query=query or "*",  # Use wildcard for empty queries
                filter=filter_config,  # Exact MRN matching using EXTRACT_ANSWER-derived field
                columns=["note_id", "document_id", "document_type", "source_system", "file_path", "document_date", "MRN"],
                limit=limit
            )
            
            # Convert results to list of dictionaries
            results = []
            for result in search_results.results:
                # Since patient_id is not in the raw files, extract from content or use filter patient_id
                patient_id = filters.get('patient_id', '') if filters else ''
                
                # Try multiple possible field names for content from Cortex Search
                content = (result.get('chunk') or 
                          result.get('content') or 
                          result.get('text') or 
                          result.get('raw_content') or
                          result.get('document_content') or
                          '')
                
                results.append({
                    'content': content,
                    'document_id': result.get('note_id', '') or result.get('document_id', ''),
                    'patient_id': patient_id,
                    'document_type': result.get('document_type', 'Clinical Note'),
                    'document_date': result.get('document_date', ''),
                    'author': result.get('author', ''),
                    'department': result.get('department', ''),
                    'patient_name': result.get('patient_name', ''),
                    'mrn': result.get('mrn', ''),
                    'source_system': result.get('source_system', 'Clinical Notes'),
                    'file_path': result.get('file_path', ''),
                    'relevance_score': result.get('score', 0.0)
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Clinical notes search failed: {e}")
            return []
    
    def search_radiology_reports(self, query: str, filters: Dict[str, Any] = None, limit: int = 50) -> List[Dict]:
        """
        Search radiology reports using Cortex Search Python API with precise filtering
        to prevent semantic fallback when no relevant documents exist
        
        Args:
            query: Search query string
            filters: Dictionary of filters (e.g., patient_id, date_range)
            limit: Maximum number of results
            
        Returns:
            List of dictionaries containing search results (empty if no exact matches)
        """
        try:
            session = self.session_manager.get_session()
            
            if 'radiology_reports' not in self.search_services:
                logger.warning("Radiology reports search service not available")
                return []
            
            # Use Python API for Cortex Search
            root = Root(session)
            
            # Get the search service
            search_service = (root
                .databases["TCH_PATIENT_360_POC"]
                .schemas["AI_ML"]
                .cortex_search_services[self.search_services['radiology_reports']]
            )
            
            # Use exact MRN filtering with Cortex Search @eq operator
            filter_config = None
            if filters and 'patient_id' in filters:
                patient_mrn = self._get_mrn_from_patient_id(filters['patient_id'])
                if patient_mrn:
                    filter_config = {"@eq": {"MRN": patient_mrn}}
                    logger.info(f"Radiology reports search using exact MRN filter: {patient_mrn}")
            
            # Perform search using Python API with exact filtering
            search_results = search_service.search(
                query=query or "*",  # Use wildcard for empty queries
                filter=filter_config,  # Exact MRN matching using EXTRACT_ANSWER-derived field
                columns=["note_id", "document_id", "document_type", "source_system", "file_path", "document_date", "MRN"],
                limit=limit
            )
            
            # Convert results - no manual validation needed with exact filtering
            results = []
            for result in search_results.results:
                patient_id = filters.get('patient_id', '') if filters else ''
                
                # Try multiple possible field names for content from Cortex Search
                content = (result.get('chunk') or 
                          result.get('content') or 
                          result.get('text') or 
                          result.get('raw_content') or
                          result.get('document_content') or
                          '')
                
                results.append({
                    'content': content,
                    'document_id': result.get('note_id', '') or result.get('document_id', ''),
                    'patient_id': patient_id,
                    'document_type': result.get('document_type', 'Radiology Report'),
                    'document_date': result.get('document_date', ''),
                    'author': result.get('author', ''),
                    'department': result.get('department', ''),
                    'study_type': result.get('study_type', ''),
                    'patient_name': result.get('patient_name', ''),
                    'mrn': result.get('MRN', ''),  # Use the extracted MRN from EXTRACT_ANSWER
                    'source_system': result.get('source_system', 'Radiology'),
                    'file_path': result.get('file_path', ''),
                    'relevance_score': result.get('score', 0.0)
                })
            
            # Log the filtering results for transparency
            if filter_config:
                patient_mrn = filter_config["@eq"]["MRN"] 
                logger.info(f"Radiology search for MRN {patient_mrn}: "
                          f"found {len(results)} exact matches")
                if len(results) == 0:
                    logger.warning(f"No radiology reports found for patient MRN {patient_mrn}. "
                                 f"This is expected in real-world healthcare data where not all patients have all document types.")
            
            return results
            
        except Exception as e:
            logger.error(f"Radiology reports search failed: {e}")
            return []
    
    def search_clinical_documentation(self, query: str, filters: Dict[str, Any] = None, limit: int = 50) -> List[Dict]:
        """
        Search all clinical documentation using combined Cortex Search Python API with MRN-based filtering
        
        Args:
            query: Search query string
            filters: Dictionary of filters (e.g., patient_id, date_range)
            limit: Maximum number of results
            
        Returns:
            List of dictionaries containing search results
        """
        try:
            session = self.session_manager.get_session()
            
            if 'clinical_documentation' not in self.search_services:
                logger.warning("Clinical documentation search service not available")
                return []
            
            # Use Python API for Cortex Search
            root = Root(session)
            
            # Get the search service
            search_service = (root
                .databases["TCH_PATIENT_360_POC"]
                .schemas["AI_ML"]
                .cortex_search_services[self.search_services['clinical_documentation']]
            )
            
            # Use exact MRN filtering with Cortex Search @eq operator
            filter_config = None
            if filters and 'patient_id' in filters:
                patient_mrn = self._get_mrn_from_patient_id(filters['patient_id'])
                if patient_mrn:
                    filter_config = {"@eq": {"MRN": patient_mrn}}
                    logger.info(f"Clinical documentation search using exact MRN filter: {patient_mrn}")
            
            # Perform search using Python API with exact filtering
            search_results = search_service.search(
                query=query or "*",  # Use wildcard for empty queries
                filter=filter_config,  # Exact MRN matching using EXTRACT_ANSWER-derived field
                columns=["document_type", "document_id", "document_subtype", "source_system", "file_path", "document_date", "MRN"],
                limit=limit
            )
            
            # Convert results to list of dictionaries
            results = []
            for result in search_results.results:
                # Since patient_id is not in the raw files, extract from content or use filter patient_id
                patient_id = filters.get('patient_id', '') if filters else ''
                
                results.append({
                    'content': result.get('chunk', ''),
                    'document_id': result.get('document_id', ''),
                    'patient_id': patient_id,
                    'document_type': result.get('document_type', 'Clinical Document'),
                    'document_date': result.get('document_date', ''),
                    'author': result.get('author', ''),
                    'department': result.get('department', ''),
                    'document_subtype': result.get('document_subtype', ''),
                    'patient_name': result.get('patient_name', ''),
                    'mrn': result.get('MRN', ''),  # Use the extracted MRN from EXTRACT_ANSWER
                    'source_system': result.get('source_system', 'Clinical Documentation'),
                    'file_path': result.get('file_path', ''),
                    'relevance_score': result.get('score', 0.0)
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Clinical documentation search failed: {e}")
            return []

    def semantic_patient_search(self, query: str, scope: List[str], max_results: int = 50) -> pd.DataFrame:
        """
        Perform semantic search across patient data using natural language
        
        Args:
            query: Natural language search query
            scope: List of data types to search across
            max_results: Maximum number of results to return
            
        Returns:
            DataFrame containing relevant patients
        """
        try:
            session = self.session_manager.get_session()
            
            # If Cortex Search is available, use it
            if self.search_services:
                return self._cortex_semantic_search(query, scope, max_results)
            else:
                # Fallback to enhanced structured search
                return self._fallback_semantic_search(query, scope, max_results)
                
        except Exception as e:
            logger.error(f"Semantic patient search failed: {e}")
            return pd.DataFrame()
    
    def _cortex_semantic_search(self, query: str, scope: List[str], max_results: int) -> pd.DataFrame:
        """Perform semantic search using Cortex Search"""
        try:
            session = self.session_manager.get_session()
            
            # Build search queries for each scope
            search_results = []
            
            for data_type in scope:
                if data_type == "Clinical Notes":
                    results = self._search_clinical_notes(query, max_results // len(scope))
                elif data_type == "Radiology Reports":
                    results = self._search_radiology_reports(query, max_results // len(scope))
                elif data_type == "Clinical Documentation":
                    results = self._search_clinical_documentation(query, max_results // len(scope))
                elif data_type == "Patient Demographics":
                    # Fallback to keyword search for demographics
                    results = self._keyword_patient_search(query, max_results // len(scope))
                else:
                    # For other data types, use the comprehensive clinical documentation search
                    results = self._search_clinical_documentation(query, max_results // len(scope))
                    
                if not results.empty:
                    search_results.append(results)
            
            if search_results:
                # Combine and deduplicate results
                combined_results = pd.concat(search_results, ignore_index=True)
                combined_results = combined_results.drop_duplicates(subset=['PATIENT_ID'])
                return combined_results.head(max_results)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Cortex semantic search failed: {e}")
            return pd.DataFrame()
    
    def _search_patient_demographics(self, query: str, limit: int) -> pd.DataFrame:
        """Search patient demographics using Cortex Search"""
        try:
            session = self.session_manager.get_session()
            
            # Use Cortex Search if available
            if 'patient_demographics' in self.search_services:
                search_sql = f"""
                SELECT 
                    p.PATIENT_ID,
                    p.MRN,
                    p.FIRST_NAME,
                    p.LAST_NAME,
                    p.DATE_OF_BIRTH,
                    p.GENDER,
                    p.PHONE_NUMBER,
                    p.ADDRESS,
                    p.INSURANCE_TYPE,
                    p.RISK_LEVEL,
                    DATEDIFF('year', p.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                    e.LAST_VISIT_DATE,
                    e.LAST_DEPARTMENT,
                    search.score AS relevance_score
                FROM TABLE(
                    SNOWFLAKE.CORTEX.SEARCH(
                        '{self.search_services['patient_demographics']}',
                        %s,
                        {{'limit': {limit}, 'filter': {{'status': 'active'}}}}
                    )
                ) AS search
                JOIN PRESENTATION.PATIENT_MASTER p ON p.PATIENT_ID = search.patient_id
                LEFT JOIN (
                    SELECT 
                        PATIENT_ID,
                        MAX(ENCOUNTER_DATE) AS LAST_VISIT_DATE,
                        LAST_VALUE(DEPARTMENT) OVER (
                            PARTITION BY PATIENT_ID 
                            ORDER BY ENCOUNTER_DATE 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS LAST_DEPARTMENT
                    FROM PRESENTATION.ENCOUNTER_SUMMARY
                    GROUP BY PATIENT_ID
                ) e ON p.PATIENT_ID = e.PATIENT_ID
                ORDER BY search.score DESC
                """
                
                return session.sql(search_sql).to_pandas() if hasattr(session, 'sql') else pd.read_sql(search_sql, session, params=(query,))
            else:
                # Fallback to keyword search
                return self._keyword_patient_search(query, limit)
                
        except Exception as e:
            logger.error(f"Patient demographics search failed: {e}")
            return pd.DataFrame()
    
    def _search_clinical_notes(self, query: str, limit: int) -> pd.DataFrame:
        """Search clinical notes via Cortex Search Python API and hydrate patients."""
        try:
            session = self.session_manager.get_session()

            # Use Python API to search notes
            root = Root(session)
            if 'clinical_notes' not in self.search_services:
                return pd.DataFrame()
            svc = (root
                .databases["TCH_PATIENT_360_POC"]
                .schemas["AI_ML"]
                .cortex_search_services[self.search_services['clinical_notes']]
            )
            sr = svc.search(query=query or "*", limit=limit)
            mrns = [r.get('MRN') or r.get('mrn') for r in sr.results]
            mrns = [str(m) for m in mrns if m]
            if not mrns:
                return pd.DataFrame()

            in_list = ",".join(["'" + m.replace("'","''") + "'" for m in mrns])
            sql = f"""
            SELECT 
                pm.PATIENT_ID,
                pm.MRN,
                pm.FIRST_NAME,
                pm.LAST_NAME,
                pm.DATE_OF_BIRTH,
                DATEDIFF('year', pm.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                pm.GENDER,
                pm.PRIMARY_INSURANCE,
                pm.RISK_CATEGORY,
                pm.LAST_ENCOUNTER_DATE,
                pm.TOTAL_ENCOUNTERS
            FROM CONFORMED.PATIENT_MASTER pm
            WHERE pm.MRN IN ({in_list})
            LIMIT {int(max(1, limit))}
            """
            return session.sql(sql).to_pandas()
        except Exception as e:
            logger.error(f"Clinical notes search failed: {e}")
            return pd.DataFrame()
    
    def _search_lab_results(self, query: str, limit: int) -> pd.DataFrame:
        """Search lab results using semantic understanding"""
        try:
            session = self.session_manager.get_session()
            
            # For lab results, we can search based on test names, abnormal values, etc.
            search_sql = """
            SELECT DISTINCT
                p.PATIENT_ID,
                p.MRN,
                p.FIRST_NAME,
                p.LAST_NAME,
                p.DATE_OF_BIRTH,
                p.GENDER,
                p.PHONE_NUMBER,
                p.ADDRESS,
                p.INSURANCE_TYPE,
                p.RISK_LEVEL,
                DATEDIFF('year', p.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                e.LAST_VISIT_DATE,
                e.LAST_DEPARTMENT
            FROM PRESENTATION.LAB_RESULTS_SUMMARY lr
            JOIN PRESENTATION.PATIENT_MASTER p ON p.PATIENT_ID = lr.PATIENT_ID
            LEFT JOIN (
                SELECT 
                    PATIENT_ID,
                    MAX(ENCOUNTER_DATE) AS LAST_VISIT_DATE,
                    LAST_VALUE(DEPARTMENT) OVER (
                        PARTITION BY PATIENT_ID 
                        ORDER BY ENCOUNTER_DATE 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS LAST_DEPARTMENT
                FROM PRESENTATION.ENCOUNTER_SUMMARY
                GROUP BY PATIENT_ID
            ) e ON p.PATIENT_ID = e.PATIENT_ID
            WHERE UPPER(lr.TEST_NAME) LIKE UPPER(%s)
               OR UPPER(lr.ABNORMAL_FLAG) LIKE UPPER(%s)
               OR (UPPER(%s) LIKE '%ABNORMAL%' AND lr.ABNORMAL_FLAG IS NOT NULL)
               OR (UPPER(%s) LIKE '%HIGH%' AND UPPER(lr.ABNORMAL_FLAG) LIKE '%HIGH%')
               OR (UPPER(%s) LIKE '%LOW%' AND UPPER(lr.ABNORMAL_FLAG) LIKE '%LOW%')
            ORDER BY lr.ORDER_DATE DESC
            LIMIT %s
            """
            
            search_pattern = f'%{query}%'
            params = (search_pattern, search_pattern, query, query, query, limit)
            
            return session.sql(search_sql).to_pandas() if hasattr(session, 'sql') else pd.read_sql(search_sql, session, params=params)
            
        except Exception as e:
            logger.error(f"Lab results search failed: {e}")
            return pd.DataFrame()
    
    def _search_diagnoses(self, query: str, limit: int) -> pd.DataFrame:
        """Search diagnoses and conditions"""
        try:
            session = self.session_manager.get_session()
            
            search_sql = """
            SELECT DISTINCT
                p.PATIENT_ID,
                p.MRN,
                p.FIRST_NAME,
                p.LAST_NAME,
                p.DATE_OF_BIRTH,
                p.GENDER,
                p.PHONE_NUMBER,
                p.ADDRESS,
                p.INSURANCE_TYPE,
                p.RISK_LEVEL,
                DATEDIFF('year', p.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                e.LAST_VISIT_DATE,
                e.LAST_DEPARTMENT
            FROM PRESENTATION.DIAGNOSIS_SUMMARY d
            JOIN PRESENTATION.PATIENT_MASTER p ON p.PATIENT_ID = d.PATIENT_ID
            LEFT JOIN (
                SELECT 
                    PATIENT_ID,
                    MAX(ENCOUNTER_DATE) AS LAST_VISIT_DATE,
                    LAST_VALUE(DEPARTMENT) OVER (
                        PARTITION BY PATIENT_ID 
                        ORDER BY ENCOUNTER_DATE 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS LAST_DEPARTMENT
                FROM PRESENTATION.ENCOUNTER_SUMMARY
                GROUP BY PATIENT_ID
            ) e ON p.PATIENT_ID = e.PATIENT_ID
            WHERE UPPER(d.DIAGNOSIS_DESCRIPTION) LIKE UPPER(%s)
               OR UPPER(d.DIAGNOSIS_CODE) LIKE UPPER(%s)
            ORDER BY d.DIAGNOSIS_DATE DESC
            LIMIT %s
            """
            
            search_pattern = f'%{query}%'
            params = (search_pattern, search_pattern, limit)
            
            return session.sql(search_sql).to_pandas() if hasattr(session, 'sql') else pd.read_sql(search_sql, session, params=params)
            
        except Exception as e:
            logger.error(f"Diagnosis search failed: {e}")
            return pd.DataFrame()
    
    def _search_medications(self, query: str, limit: int) -> pd.DataFrame:
        """Search medications"""
        try:
            session = self.session_manager.get_session()
            
            search_sql = """
            SELECT DISTINCT
                p.PATIENT_ID,
                p.MRN,
                p.FIRST_NAME,
                p.LAST_NAME,
                p.DATE_OF_BIRTH,
                p.GENDER,
                p.PHONE_NUMBER,
                p.ADDRESS,
                p.INSURANCE_TYPE,
                p.RISK_LEVEL,
                DATEDIFF('year', p.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                e.LAST_VISIT_DATE,
                e.LAST_DEPARTMENT
            FROM PRESENTATION.MEDICATION_SUMMARY m
            JOIN PRESENTATION.PATIENT_MASTER p ON p.PATIENT_ID = m.PATIENT_ID
            LEFT JOIN (
                SELECT 
                    PATIENT_ID,
                    MAX(ENCOUNTER_DATE) AS LAST_VISIT_DATE,
                    LAST_VALUE(DEPARTMENT) OVER (
                        PARTITION BY PATIENT_ID 
                        ORDER BY ENCOUNTER_DATE 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS LAST_DEPARTMENT
                FROM PRESENTATION.ENCOUNTER_SUMMARY
                GROUP BY PATIENT_ID
            ) e ON p.PATIENT_ID = e.PATIENT_ID
            WHERE UPPER(m.MEDICATION_NAME) LIKE UPPER(%s)
               OR UPPER(m.MEDICATION_CLASS) LIKE UPPER(%s)
            ORDER BY m.START_DATE DESC
            LIMIT %s
            """
            
            search_pattern = f'%{query}%'
            params = (search_pattern, search_pattern, limit)
            
            return session.sql(search_sql).to_pandas() if hasattr(session, 'sql') else pd.read_sql(search_sql, session, params=params)
            
        except Exception as e:
            logger.error(f"Medication search failed: {e}")
            return pd.DataFrame()
    
    def _search_procedures(self, query: str, limit: int) -> pd.DataFrame:
        """Search procedures"""
        try:
            session = self.session_manager.get_session()
            
            search_sql = """
            SELECT DISTINCT
                p.PATIENT_ID,
                p.MRN,
                p.FIRST_NAME,
                p.LAST_NAME,
                p.DATE_OF_BIRTH,
                p.GENDER,
                p.PHONE_NUMBER,
                p.ADDRESS,
                p.INSURANCE_TYPE,
                p.RISK_LEVEL,
                DATEDIFF('year', p.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                e.LAST_VISIT_DATE,
                e.LAST_DEPARTMENT
            FROM PRESENTATION.PROCEDURE_SUMMARY pr
            JOIN PRESENTATION.PATIENT_MASTER p ON p.PATIENT_ID = pr.PATIENT_ID
            LEFT JOIN (
                SELECT 
                    PATIENT_ID,
                    MAX(ENCOUNTER_DATE) AS LAST_VISIT_DATE,
                    LAST_VALUE(DEPARTMENT) OVER (
                        PARTITION BY PATIENT_ID 
                        ORDER BY ENCOUNTER_DATE 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS LAST_DEPARTMENT
                FROM PRESENTATION.ENCOUNTER_SUMMARY
                GROUP BY PATIENT_ID
            ) e ON p.PATIENT_ID = e.PATIENT_ID
            WHERE UPPER(pr.PROCEDURE_DESCRIPTION) LIKE UPPER(%s)
               OR UPPER(pr.PROCEDURE_CODE) LIKE UPPER(%s)
            ORDER BY pr.PROCEDURE_DATE DESC
            LIMIT %s
            """
            
            search_pattern = f'%{query}%'
            params = (search_pattern, search_pattern, limit)
            
            return session.sql(search_sql).to_pandas() if hasattr(session, 'sql') else pd.read_sql(search_sql, session, params=params)
            
        except Exception as e:
            logger.error(f"Procedure search failed: {e}")
            return pd.DataFrame()
    
    def _fallback_semantic_search(self, query: str, scope: List[str], max_results: int) -> pd.DataFrame:
        """Fallback semantic search using enhanced keyword matching"""
        try:
            # Parse natural language query to extract keywords and intent
            keywords = self._extract_keywords(query)
            
            # Build enhanced search based on extracted keywords
            from services.data_service import DataService
            data_service = DataService()
            
            # Convert natural language to search criteria
            criteria = self._parse_query_to_criteria(query, keywords)
            
            return data_service.advanced_patient_search(criteria)
            
        except Exception as e:
            logger.error(f"Fallback semantic search failed: {e}")
            return pd.DataFrame()
    
    def _extract_keywords(self, query: str) -> List[str]:
        """Extract meaningful keywords from natural language query"""
        import re
        
        # Common medical terms and their variations
        medical_terms = {
            'asthma': ['asthma', 'wheezing', 'bronchospasm'],
            'diabetes': ['diabetes', 'diabetic', 'blood sugar', 'glucose'],
            'emergency': ['emergency', 'er', 'urgent', 'acute'],
            'surgery': ['surgery', 'surgical', 'operation', 'procedure'],
            'cardiac': ['cardiac', 'heart', 'cardiovascular'],
            'oncology': ['cancer', 'oncology', 'tumor', 'malignancy'],
            'neurology': ['neuro', 'neurological', 'brain', 'seizure']
        }
        
        # Extract age-related terms
        age_pattern = r'(\d+)\s*(year|month|day)s?\s*(old|age)'
        age_matches = re.findall(age_pattern, query.lower())
        
        # Extract department mentions
        departments = ['emergency', 'cardiology', 'oncology', 'neurology', 'surgery', 'icu', 'nicu']
        
        keywords = []
        query_lower = query.lower()
        
        # Add medical terms
        for term, variations in medical_terms.items():
            if any(var in query_lower for var in variations):
                keywords.append(term)
        
        # Add departments
        for dept in departments:
            if dept in query_lower:
                keywords.append(dept)
        
        # Add time-related keywords
        if any(word in query_lower for word in ['recent', 'last', 'past', 'within']):
            keywords.append('recent_visits')
        
        return keywords
    
    def _parse_query_to_criteria(self, query: str, keywords: List[str]) -> Dict[str, Any]:
        """Parse natural language query to search criteria"""
        import re
        
        criteria = {}
        query_lower = query.lower()
        
        # Extract age ranges
        age_pattern = r'(\d+)\s*to\s*(\d+)\s*year'
        age_range = re.search(age_pattern, query_lower)
        if age_range:
            criteria['age_min'] = int(age_range.group(1))
            criteria['age_max'] = int(age_range.group(2))
        
        # Extract single age
        single_age_pattern = r'(\d+)\s*year.*old'
        single_age = re.search(single_age_pattern, query_lower)
        if single_age:
            age = int(single_age.group(1))
            criteria['age_min'] = max(0, age - 1)
            criteria['age_max'] = min(21, age + 1)
        
        # Extract gender
        if 'male' in query_lower and 'female' not in query_lower:
            criteria['gender'] = 'M'
        elif 'female' in query_lower and 'male' not in query_lower:
            criteria['gender'] = 'F'
        
        # Extract conditions/diagnoses
        medical_conditions = []
        for keyword in keywords:
            if keyword in ['asthma', 'diabetes', 'cardiac']:
                medical_conditions.append(keyword)
        
        if medical_conditions:
            criteria['diagnosis'] = ' OR '.join(medical_conditions)
        
        # Extract departments
        departments = []
        for keyword in keywords:
            if keyword in ['emergency', 'cardiology', 'oncology', 'neurology', 'surgery']:
                departments.append(keyword.title())
        
        if departments:
            criteria['departments'] = departments
        
        # Extract time constraints
        if 'recent_visits' in keywords or any(word in query_lower for word in ['recent', 'last', 'past']):
            from datetime import datetime, timedelta
            if 'month' in query_lower:
                months = re.search(r'(\d+)\s*month', query_lower)
                if months:
                    months_back = int(months.group(1))
                    criteria['date_from'] = datetime.now() - timedelta(days=months_back * 30)
            elif 'year' in query_lower:
                years = re.search(r'(\d+)\s*year', query_lower)
                if years:
                    years_back = int(years.group(1))
                    criteria['date_from'] = datetime.now() - timedelta(days=years_back * 365)
        
        return criteria
    
    def get_search_insights(self, query: str, results: pd.DataFrame) -> str:
        """Generate insights about the search results"""
        if results.empty:
            return "No insights available - no results found."
        
        insights = []
        
        # Demographics insights
        if 'AGE' in results.columns:
            avg_age = results['AGE'].mean()
            insights.append(f"Average age of patients: {avg_age:.1f} years")
        
        if 'GENDER' in results.columns:
            gender_dist = results['GENDER'].value_counts()
            if len(gender_dist) > 1:
                insights.append(f"Gender distribution: {dict(gender_dist)}")
        
        if 'RISK_LEVEL' in results.columns:
            risk_dist = results['RISK_LEVEL'].value_counts()
            if len(risk_dist) > 1:
                insights.append(f"Risk level distribution: {dict(risk_dist)}")
        
        # Department insights
        if 'LAST_DEPARTMENT' in results.columns:
            dept_dist = results['LAST_DEPARTMENT'].value_counts().head(3)
            insights.append(f"Top departments: {dict(dept_dist)}")
        
        # Keywords found
        keywords = self._extract_keywords(query)
        if keywords:
            insights.append(f"Search targeted: {', '.join(keywords)}")
        
        return "\\n\\n".join([f"• {insight}" for insight in insights])
    
    def _keyword_patient_search(self, query: str, limit: int) -> pd.DataFrame:
        """Basic keyword search as fallback"""
        try:
            session = self.session_manager.get_session()
            
            search_sql = """
            SELECT 
                p.PATIENT_ID,
                p.MRN,
                p.FIRST_NAME,
                p.LAST_NAME,
                p.DATE_OF_BIRTH,
                p.GENDER,
                p.PHONE_NUMBER,
                p.ADDRESS,
                p.INSURANCE_TYPE,
                p.RISK_LEVEL,
                DATEDIFF('year', p.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                e.LAST_VISIT_DATE,
                e.LAST_DEPARTMENT
            FROM PRESENTATION.PATIENT_MASTER p
            LEFT JOIN (
                SELECT 
                    PATIENT_ID,
                    MAX(ENCOUNTER_DATE) AS LAST_VISIT_DATE,
                    LAST_VALUE(DEPARTMENT) OVER (
                        PARTITION BY PATIENT_ID 
                        ORDER BY ENCOUNTER_DATE 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS LAST_DEPARTMENT
                FROM PRESENTATION.ENCOUNTER_SUMMARY
                GROUP BY PATIENT_ID
            ) e ON p.PATIENT_ID = e.PATIENT_ID
            WHERE UPPER(p.FIRST_NAME) LIKE UPPER('{escaped_query}')
               OR UPPER(p.LAST_NAME) LIKE UPPER('{escaped_query}')
               OR UPPER(e.LAST_DEPARTMENT) LIKE UPPER('{escaped_query}')
            ORDER BY p.LAST_NAME, p.FIRST_NAME
            LIMIT {limit}
            """
            
            clean_query = query.replace("'", "''")
            escaped_query = f"%{clean_query}%"
            
            return session.sql(search_sql).to_pandas()
            
        except Exception as e:
            logger.error(f"Keyword patient search failed: {e}")
            return pd.DataFrame()
    
    def _search_radiology_reports(self, query: str, limit: int) -> pd.DataFrame:
        """Search radiology reports via Cortex Search Python API and hydrate patients."""
        try:
            session = self.session_manager.get_session()
            root = Root(session)
            if 'radiology_reports' not in self.search_services:
                return pd.DataFrame()
            svc = (root
                .databases["TCH_PATIENT_360_POC"]
                .schemas["AI_ML"]
                .cortex_search_services[self.search_services['radiology_reports']]
            )
            sr = svc.search(query=query or "*", limit=limit)
            mrns = [r.get('MRN') or r.get('mrn') for r in sr.results]
            mrns = [str(m) for m in mrns if m]
            if not mrns:
                return pd.DataFrame()

            in_list = ",".join(["'" + m.replace("'","''") + "'" for m in mrns])
            sql = f"""
            SELECT 
                pm.PATIENT_ID,
                pm.MRN,
                pm.FIRST_NAME,
                pm.LAST_NAME,
                pm.DATE_OF_BIRTH,
                DATEDIFF('year', pm.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                pm.GENDER,
                pm.PRIMARY_INSURANCE,
                pm.RISK_CATEGORY,
                pm.LAST_ENCOUNTER_DATE,
                pm.TOTAL_ENCOUNTERS
            FROM CONFORMED.PATIENT_MASTER pm
            WHERE pm.MRN IN ({in_list})
            LIMIT {int(max(1, limit))}
            """
            return session.sql(sql).to_pandas()
        except Exception as e:
            logger.error(f"Radiology reports search failed: {e}")
            return pd.DataFrame()
    
    def _search_clinical_documentation(self, query: str, limit: int) -> pd.DataFrame:
        """Search clinical documentation via Cortex Search Python API and hydrate patients."""
        try:
            session = self.session_manager.get_session()
            root = Root(session)
            if 'clinical_documentation' not in self.search_services:
                return pd.DataFrame()
            svc = (root
                .databases["TCH_PATIENT_360_POC"]
                .schemas["AI_ML"]
                .cortex_search_services[self.search_services['clinical_documentation']]
            )
            sr = svc.search(query=query or "*", limit=limit)
            mrns = [r.get('MRN') or r.get('mrn') for r in sr.results]
            mrns = [str(m) for m in mrns if m]
            if not mrns:
                return pd.DataFrame()

            in_list = ",".join(["'" + m.replace("'","''") + "'" for m in mrns])
            sql = f"""
            SELECT 
                pm.PATIENT_ID,
                pm.MRN,
                pm.FIRST_NAME,
                pm.LAST_NAME,
                pm.DATE_OF_BIRTH,
                DATEDIFF('year', pm.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                pm.GENDER,
                pm.PRIMARY_INSURANCE,
                pm.RISK_CATEGORY,
                pm.LAST_ENCOUNTER_DATE,
                pm.TOTAL_ENCOUNTERS
            FROM CONFORMED.PATIENT_MASTER pm
            WHERE pm.MRN IN ({in_list})
            LIMIT {int(max(1, limit))}
            """
            return session.sql(sql).to_pandas()
        except Exception as e:
            logger.error(f"Clinical documentation search failed: {e}")
            return pd.DataFrame()