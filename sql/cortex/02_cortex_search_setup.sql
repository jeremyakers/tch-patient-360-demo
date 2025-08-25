-- Texas Children's Hospital Patient 360 PoC - Cortex Search Setup (Simplified)
-- Creates Cortex Search Services for unstructured clinical data
-- Uses direct raw content without pre-extraction for better accuracy

USE DATABASE TCH_PATIENT_360_POC;
USE SCHEMA AI_ML;
USE WAREHOUSE TCH_AI_ML_WH;

-- =============================================================================
-- CLINICAL NOTES SEARCH BASE (SIMPLIFIED)
-- =============================================================================

-- Create search-optimized view for clinical notes using direct raw content
-- Cortex Search will handle semantic search and extraction at query time
CREATE OR REPLACE VIEW CLINICAL_NOTES_SEARCH_BASE AS
WITH raw AS (
    SELECT 
        filename,
        file_path,
        file_last_modified,
        file_size,
        raw_content,
        mrn,
        REGEXP_SUBSTR(filename, '(PROG|NURS|DISC|CONS)-[0-9A-F]{8}') AS parsed_note_id,
    FROM RAW_DATA.CLINICAL_NOTES_RAW
    WHERE raw_content IS NOT NULL AND LENGTH(raw_content) > 0
)
SELECT 
    r.parsed_note_id AS note_id,
    r.filename AS document_id,
    r.file_path,
    CASE 
        WHEN r.filename ILIKE '%NOTE-%' THEN 'Progress Note'
        WHEN r.filename ILIKE '%NURS-%' THEN 'Nursing Note'
        WHEN r.filename ILIKE '%CONS-%' THEN 'Consultation Note'
        WHEN r.filename ILIKE '%DISC-%' THEN 'Discharge Summary'
        ELSE 'Clinical Note'
    END AS document_type,
    'Clinical Notes' AS source_system,
    r.file_last_modified AS document_date,
    r.file_size,
    r.raw_content AS full_searchable_text,
    p.mrn AS MRN,
    p.patient_id AS patient_id
FROM raw r
JOIN EPIC.PATIENTS p ON p.mrn = r.mrn;

-- =============================================================================
-- RADIOLOGY REPORTS SEARCH BASE (SIMPLIFIED)
-- =============================================================================

-- Create Radiology Reports Search Base view using direct raw content
-- Cortex Search will handle semantic search and extraction at query time
CREATE OR REPLACE VIEW RADIOLOGY_REPORTS_SEARCH_BASE AS
WITH raw AS (
    SELECT 
        filename,
        file_path,
        file_last_modified,
        file_size,
        raw_content,
        mrn,
        REGEXP_SUBSTR(filename, 'RAD-[0-9A-F]{8}') AS parsed_note_id,
    FROM RAW_DATA.RADIOLOGY_REPORTS_RAW
    WHERE raw_content IS NOT NULL AND LENGTH(raw_content) > 0
)
SELECT 
    r.parsed_note_id AS note_id,
    r.filename AS document_id,
    r.file_path,
    'Radiology Report' AS document_type,
    'Radiology Reports' AS source_system,
    r.file_last_modified AS document_date,
    r.file_size,
    r.raw_content AS full_searchable_text,
    p.mrn AS MRN,
    p.patient_id AS patient_id
FROM raw r
JOIN EPIC.PATIENTS p ON p.mrn = r.mrn;

-- =============================================================================
-- COMBINED CLINICAL DOCUMENTATION SEARCH BASE (SIMPLIFIED)
-- =============================================================================

-- Create unified search base for all clinical documentation
-- This combines both clinical notes and radiology reports for comprehensive search
CREATE OR REPLACE VIEW CLINICAL_DOCUMENTATION_SEARCH_BASE AS
SELECT 
    document_type,
    note_id AS document_id,
    file_path,
    document_type AS document_subtype,
    document_date,
    source_system,
    file_size,
    full_searchable_text,
    LENGTH(full_searchable_text) AS content_length,
    MRN,
    patient_id
FROM CLINICAL_NOTES_SEARCH_BASE

UNION ALL

SELECT 
    document_type,
    note_id AS document_id,
    file_path,
    document_type AS document_subtype,
    document_date,
    source_system,
    file_size,
    full_searchable_text,
    LENGTH(full_searchable_text) AS content_length,
    MRN,
    patient_id
FROM RADIOLOGY_REPORTS_SEARCH_BASE;

-- =============================================================================
-- CORTEX SEARCH SERVICES (SIMPLIFIED)
-- =============================================================================

-- Clinical Notes Search Service
-- Searches clinical notes using semantic search capabilities
CREATE OR REPLACE CORTEX SEARCH SERVICE CLINICAL_NOTES_SEARCH
ON full_searchable_text
ATTRIBUTES note_id, document_id, document_type, source_system, file_path, document_date, MRN, patient_id
WAREHOUSE = TCH_AI_ML_WH
TARGET_LAG = '1 hour'
AS (
    SELECT 
        full_searchable_text,
        note_id,
        document_id,
        document_type,
        source_system,
        file_path,
        document_date,
        MRN,
        patient_id
    FROM CLINICAL_NOTES_SEARCH_BASE
);

-- Radiology Reports Search Service
-- Searches radiology reports using semantic search capabilities
CREATE OR REPLACE CORTEX SEARCH SERVICE RADIOLOGY_REPORTS_SEARCH
ON full_searchable_text
ATTRIBUTES note_id, document_id, document_type, source_system, file_path, document_date, MRN, patient_id
WAREHOUSE = TCH_AI_ML_WH
TARGET_LAG = '1 hour'
AS (
    SELECT 
        full_searchable_text,
        note_id,
        document_id,
        document_type,
        source_system,
        file_path,
        document_date,
        MRN,
        patient_id
    FROM RADIOLOGY_REPORTS_SEARCH_BASE
);

-- Combined Clinical Documentation Search Service
-- Searches all clinical documentation types using semantic search
CREATE OR REPLACE CORTEX SEARCH SERVICE CLINICAL_DOCUMENTATION_SEARCH
ON full_searchable_text
ATTRIBUTES document_type, document_id, document_subtype, source_system, file_path, document_date, MRN, patient_id
WAREHOUSE = TCH_AI_ML_WH
TARGET_LAG = '1 hour'
AS (
    SELECT 
        full_searchable_text,
        document_type,
        document_id,
        document_subtype,
        source_system,
        file_path,
        document_date,
        MRN,
        patient_id
    FROM CLINICAL_DOCUMENTATION_SEARCH_BASE
);

-- =============================================================================
-- MONITORING AND TEST VIEWS
-- =============================================================================

-- Monitoring view for Cortex Search services
CREATE OR REPLACE VIEW CORTEX_SEARCH_MONITORING AS
SELECT 
    'CLINICAL_NOTES_SEARCH' AS search_service_name,
    COUNT(*) AS total_documents,
    AVG(LENGTH(full_searchable_text)) AS avg_content_length,
    MAX(document_date) AS latest_document_date,
    MIN(document_date) AS earliest_document_date
FROM CLINICAL_NOTES_SEARCH_BASE

UNION ALL

SELECT 
    'RADIOLOGY_REPORTS_SEARCH' AS search_service_name,
    COUNT(*) AS total_documents,
    AVG(LENGTH(full_searchable_text)) AS avg_content_length,
    MAX(document_date) AS latest_document_date,
    MIN(document_date) AS earliest_document_date
FROM RADIOLOGY_REPORTS_SEARCH_BASE

UNION ALL

SELECT 
    'CLINICAL_DOCUMENTATION_SEARCH' AS search_service_name,
    COUNT(*) AS total_documents,
    AVG(content_length) AS avg_content_length,
    MAX(document_date) AS latest_document_date,
    MIN(document_date) AS earliest_document_date
FROM CLINICAL_DOCUMENTATION_SEARCH_BASE;

-- Test queries for Cortex Search
CREATE OR REPLACE VIEW CORTEX_SEARCH_TEST_QUERIES AS
SELECT * FROM VALUES
    ('asthma treatment pediatric', 'Test search for asthma-related clinical documentation'),
    ('chest x-ray pneumonia', 'Test search for chest imaging and pneumonia'),
    ('fever infant newborn', 'Test search for fever in young children'),
    ('diabetes management child', 'Test search for pediatric diabetes care'),
    ('ADHD medication methylphenidate', 'Test search for ADHD treatment'),
    ('growth development concern', 'Test search for growth and development issues'),
    ('emergency department visit', 'Test search for emergency department encounters'),
    ('vaccination immunization schedule', 'Test search for immunization records'),
    ('allergy food reaction', 'Test search for allergy-related documentation'),
    ('surgery procedure pediatric', 'Test search for surgical procedures')
AS search_tests(query, description);

SELECT 'Cortex Search services created successfully. RAG-enabled clinical documentation search is now available.' AS cortex_search_status;