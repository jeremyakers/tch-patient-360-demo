-- Texas Children's Hospital Patient 360 PoC - Unstructured Data Loading
-- Loads unstructured files from stages into raw tables for performance

USE DATABASE TCH_PATIENT_360_POC;
USE WAREHOUSE TCH_COMPUTE_WH;

-- =============================================================================
-- LOAD UNSTRUCTURED DATA FROM STAGES TO RAW TABLES
-- =============================================================================

-- Ensure we're in the correct context
USE DATABASE TCH_PATIENT_360_POC;

-- Clear existing raw unstructured data to ensure clean load
TRUNCATE TABLE RAW_DATA.CLINICAL_NOTES_RAW;
TRUNCATE TABLE RAW_DATA.RADIOLOGY_REPORTS_RAW;

-- =============================================================================
-- LOAD CLINICAL NOTES
-- =============================================================================

INSERT INTO RAW_DATA.CLINICAL_NOTES_RAW (
    file_id,
    filename,
    file_path,
    file_size,
    file_last_modified,
    content_type,
    raw_content,
    mrn
)
SELECT 
    REPLACE(REPLACE(METADATA$FILENAME, 'note_', ''), '.txt', '') AS file_id,
    METADATA$FILENAME AS filename,
    METADATA$FILENAME AS file_path,  -- METADATA$FILENAME already contains the full path
    LENGTH($1) AS file_size,  -- Calculate size from content length
    METADATA$FILE_LAST_MODIFIED AS file_last_modified,
    'text/plain' AS content_type,
    $1 AS raw_content,
    UPPER(PARSE_JSON(SNOWFLAKE.CORTEX.EXTRACT_ANSWER($1, 'MRN = "Medical Record Number", DOB = "Date of Birth": What is the MRN value, which starts with MRN followed by 8 digits and contains no whitespace?')[0]::VARCHAR):answer::VARCHAR) AS mrn
FROM @RAW_DATA.UNSTRUCTURED_DATA_STAGE/clinical_notes/
WHERE $1 IS NOT NULL 
AND LENGTH($1) > 0;

-- =============================================================================
-- LOAD RADIOLOGY REPORTS
-- =============================================================================

INSERT INTO RAW_DATA.RADIOLOGY_REPORTS_RAW (
    file_id,
    filename,
    file_path,
    file_size,
    file_last_modified,
    content_type,
    raw_content,
    mrn
)
SELECT 
    REPLACE(REPLACE(METADATA$FILENAME, 'note_', ''), '.txt', '') AS file_id,
    METADATA$FILENAME AS filename,
    METADATA$FILENAME AS file_path,  -- METADATA$FILENAME already contains the full path
    LENGTH($1) AS file_size,  -- Calculate size from content length
    METADATA$FILE_LAST_MODIFIED AS file_last_modified,
    'text/plain' AS content_type,
    $1 AS raw_content,
    UPPER(PARSE_JSON(SNOWFLAKE.CORTEX.EXTRACT_ANSWER($1, 'MRN = "Medical Record Number", DOB = "Date of Birth": What is the MRN value, which starts with MRN followed by 8 digits and contains no whitespace?')[0]::VARCHAR):answer::VARCHAR) AS mrn
FROM @RAW_DATA.UNSTRUCTURED_DATA_STAGE/radiology_reports/
WHERE $1 IS NOT NULL 
AND LENGTH($1) > 0;

-- =============================================================================
-- VALIDATION AND SUMMARY
-- =============================================================================

-- Create summary view for monitoring
CREATE OR REPLACE VIEW RAW_DATA.UNSTRUCTURED_DATA_LOAD_SUMMARY AS
SELECT 
    'Clinical Notes' AS data_type,
    COUNT(*) AS file_count,
    AVG(LENGTH(raw_content)) AS avg_content_length,
    MIN(file_last_modified) AS earliest_file,
    MAX(file_last_modified) AS latest_file,
    MIN(loaded_timestamp) AS load_start_time,
    MAX(loaded_timestamp) AS load_end_time
FROM RAW_DATA.CLINICAL_NOTES_RAW

UNION ALL

SELECT 
    'Radiology Reports' AS data_type,
    COUNT(*) AS file_count,
    AVG(LENGTH(raw_content)) AS avg_content_length,
    MIN(file_last_modified) AS earliest_file,
    MAX(file_last_modified) AS latest_file,
    MIN(loaded_timestamp) AS load_start_time,
    MAX(loaded_timestamp) AS load_end_time
FROM RAW_DATA.RADIOLOGY_REPORTS_RAW

ORDER BY data_type;

-- Display loading results
SELECT 'Unstructured data loading complete. Summary:' AS status;
SELECT * FROM RAW_DATA.UNSTRUCTURED_DATA_LOAD_SUMMARY;

-- Show sample of loaded data for verification
SELECT 'Sample clinical notes (first 200 characters):' AS sample_type;
SELECT 
    file_id,
    filename,
    LEFT(raw_content, 200) AS content_preview,
    LENGTH(raw_content) AS content_length
FROM RAW_DATA.CLINICAL_NOTES_RAW 
LIMIT 3;

SELECT 'Sample radiology reports (first 200 characters):' AS sample_type;
SELECT 
    file_id,
    filename,
    LEFT(raw_content, 200) AS content_preview,
    LENGTH(raw_content) AS content_length
FROM RAW_DATA.RADIOLOGY_REPORTS_RAW 
LIMIT 3;