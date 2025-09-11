-- Cortex Agents setup: grants and validations for persisted Agent usage
-- Purpose: Ensure privileges and resources required by a persisted Cortex Agent
-- Notes:
--   - Persisted Agent objects are managed via the Agents REST API. This script
--     prepares privileges and validates resources the Agent will reference.
--   - The Streamlit app will auto-create the Agent named TCH_P360_AGENT at startup
--     if it does not already exist.

-------------------------------------------------------------------------------
-- Context
-------------------------------------------------------------------------------
USE ROLE TCH_PATIENT_360_ROLE;
USE DATABASE TCH_PATIENT_360_POC;

-- Ensure AI_ML schema exists (should already be created earlier)
CREATE SCHEMA IF NOT EXISTS AI_ML;

-------------------------------------------------------------------------------
-- Required privileges for Cortex Agents usage
-------------------------------------------------------------------------------
-- Grant the database role needed to call Cortex Agents REST API
-- Note: Must be executed by ACCOUNTADMIN (handled during initial setup). If this
-- role grant fails due to privileges, run under ACCOUNTADMIN separately.
BEGIN
    EXECUTE IMMEDIATE $$
        GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE TCH_PATIENT_360_ROLE;
    $$;
EXCEPTION WHEN OTHER THEN
    -- Ignore if already granted or insufficient privileges in this session
    SELECT 'INFO: Grant of CORTEX_AGENT_USER may already exist or require ACCOUNTADMIN' AS note;
END;

-------------------------------------------------------------------------------
-- Validate resources the Agent will reference
-------------------------------------------------------------------------------
-- 1) Semantic model YAMLs stage
CREATE STAGE IF NOT EXISTS AI_ML.SEMANTIC_MODEL_STAGE;

-- 2) Cortex Search services (must exist from 02_cortex_search_setup.sql)
-- Validate presence; WARN if missing
WITH services AS (
    SELECT name, database_name, schema_name
    FROM TABLE(INFORMATION_SCHEMA.CORTEX_SEARCH_SERVICES())
    WHERE database_name = CURRENT_DATABASE()
      AND schema_name = 'AI_ML'
)
SELECT
    'AI_ML search services detected' AS check_name,
    ARRAY_AGG(name) AS services
FROM services;

-------------------------------------------------------------------------------
-- Optional: record desired Agent metadata for transparency (name/model)
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_ML.CORTEX_AGENT_CONFIG (
    agent_name STRING,
    default_model STRING,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO AI_ML.CORTEX_AGENT_CONFIG t
USING (
    SELECT 'TCH_P360_AGENT' AS agent_name, 'claude-3-7-sonnet' AS default_model
) s
ON t.agent_name = s.agent_name
WHEN MATCHED THEN UPDATE SET default_model = s.default_model, last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (agent_name, default_model) VALUES (s.agent_name, s.default_model);

-------------------------------------------------------------------------------
-- Verification notes
-------------------------------------------------------------------------------
-- The Streamlit app will:
--  - Check if Agent 'TCH_P360_AGENT' exists via REST
--  - Create/update the Agent with tools (Analyst semantic model & Cortex Search)
--  - Manage threads per conversation
-- Use TCH_AI_ML_WH for AI workloads where applicable.


