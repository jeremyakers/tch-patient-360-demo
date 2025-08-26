-- Texas Children's Hospital Patient 360 PoC - Workspace Orchestrator
-- Purpose: Single-entry "Run All" script for Snowsight Workspaces
-- Execution model:
--   - SQL executes from the Snowsight Workspace (snow://workspace) to honor user edits
--   - Notebook and Streamlit are created from the Git repository object for reproducibility
--
-- Notes:
-- - Streamlit/Notebook creation points at the Git repository object so no manual staging is required.

-------------------------------------------------------------------------------
-- Context & parameters
-------------------------------------------------------------------------------

-- Required role/warehouse for deployment
USE ROLE TCH_PATIENT_360_ROLE;
USE WAREHOUSE TCH_COMPUTE_WH;

-- Adjustable parameters
SET data_size = 'medium';              -- small|medium|large (affects Notebook generation)

-- Workspace settings (preferred execution source)
-- Snowsight Workspace location (DB and schema are fixed as USER$.PUBLIC)
SET workspace_name = 'tch-patient-360-demo';
SET workspace_root = 'snow://workspace/USER$.PUBLIC."' || $workspace_name || '"/versions/live';
SET git_db = 'TCH_PATIENT_360_POC';    -- database of the Git repository object
SET git_schema = 'RAW_DATA';           -- schema of the Git repository object
SET git_repo_name = 'TCH_P360_REPO';   -- name of the Git repository object
SET git_ref_type = 'branches';         -- 'branches' or 'tags'
SET git_ref_name = 'main';             -- branch or tag to pin

-- Derived repo path (used for Notebook and Streamlit sources)
SET repo_path = '@' || $git_db || '.' || $git_schema || '.' || $git_repo_name
                 || '/' || $git_ref_type || '/' || $git_ref_name;

-------------------------------------------------------------------------------
-- Database & schemas (safe re-run)
-------------------------------------------------------------------------------

-- Database and core schemas
USE DATABASE TCH_PATIENT_360_POC;
CREATE SCHEMA IF NOT EXISTS RAW_DATA;
CREATE SCHEMA IF NOT EXISTS CONFORMED;
CREATE SCHEMA IF NOT EXISTS PRESENTATION;
CREATE SCHEMA IF NOT EXISTS AI_ML;

-- Git repository clone (placed in RAW_DATA). Requires API INTEGRATION GIT and USAGE granted to your role.
USE SCHEMA RAW_DATA;
CREATE OR REPLACE GIT REPOSITORY IDENTIFIER($git_repo_name)
  ORIGIN = 'https://github.com/jeremyakers/tch-patient-360-demo'
  API_INTEGRATION = GIT;
ALTER GIT REPOSITORY IDENTIFIER($git_repo_name) FETCH;

-------------------------------------------------------------------------------
-- Execute step scripts from Workspace (preferred for customization)
-------------------------------------------------------------------------------

SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/setup/01_database_setup.sql''';
EXECUTE IMMEDIATE $stmt;
SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/setup/02_raw_tables.sql''';
EXECUTE IMMEDIATE $stmt;
SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/setup/03_conformed_tables.sql''';
EXECUTE IMMEDIATE $stmt;

-------------------------------------------------------------------------------
-- Generate staged data via Snowflake Notebook (run before data load)
-------------------------------------------------------------------------------

-- Optional: fetch latest commit for the configured ref
-- ALTER GIT REPOSITORY IDENTIFIER($git_db || '.' || $git_schema || '.' || $git_repo_name) FETCH;

SET nb_stmt = 'CREATE OR REPLACE NOTEBOOK AI_ML.TCH_DATA_GENERATOR FROM ' ||
              $repo_path || '/python/notebooks/ ' ||
              'MAIN_FILE = ''tch_data_generator.ipynb'' ' ||
              'QUERY_WAREHOUSE = ''TCH_AI_ML_WH'' ' ||
              'RUNTIME_NAME = ''SYSTEM$BASIC_RUNTIME'' ' ||
              'COMPUTE_POOL = ''TCH_PATIENT_360_POOL'' ' ||
              'EXTERNAL_ACCESS_INTEGRATIONS = (pypi_access_integration)';
EXECUTE IMMEDIATE $nb_stmt;

-- Use AI/ML warehouse for the EXECUTE NOTEBOOK caller session
USE WAREHOUSE TCH_AI_ML_WH;

EXECUTE NOTEBOOK AI_ML.TCH_DATA_GENERATOR( 'data_size=' || $data_size, 'parallel=true' );

-- Restore compute warehouse for subsequent SQL work
USE WAREHOUSE TCH_COMPUTE_WH;

-------------------------------------------------------------------------------
-- Data load (structured + unstructured) after notebook generates files
-------------------------------------------------------------------------------

SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/data_load/01_load_raw_data.sql''';
EXECUTE IMMEDIATE $stmt;
SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/data_load/02_load_unstructured_data.sql''';
EXECUTE IMMEDIATE $stmt;

SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/dynamic_tables/01_patient_dynamic_tables.sql''';
EXECUTE IMMEDIATE $stmt;
SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/dynamic_tables/02_clinical_dynamic_tables.sql''';
EXECUTE IMMEDIATE $stmt;

-- Presentation layer after dynamic tables are created
SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/setup/04_presentation_tables.sql''';
EXECUTE IMMEDIATE $stmt;

SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/cortex/01_cortex_analyst_setup.sql''';
EXECUTE IMMEDIATE $stmt;
SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/cortex/02_cortex_search_setup.sql''';
EXECUTE IMMEDIATE $stmt;

-------------------------------------------------------------------------------
-- Streamlit app creation directly from Git repo object (no manual staging)
-------------------------------------------------------------------------------

SET app_stmt = 'CREATE OR REPLACE STREAMLIT PRESENTATION.TCH_PATIENT_360_APP ' ||
               'ROOT_LOCATION = ' || $repo_path || '/python/streamlit_app/ ' ||
               'MAIN_FILE = ''main.py'' ' ||
               'QUERY_WAREHOUSE = ''TCH_ANALYTICS_WH'' ' ||
               'TITLE = ''TCH Patient 360''';
EXECUTE IMMEDIATE $app_stmt;

-------------------------------------------------------------------------------
-- Verification
-------------------------------------------------------------------------------

SET stmt = 'EXECUTE IMMEDIATE FROM ''' || $workspace_root || '/sql/99_verification.sql''';
EXECUTE IMMEDIATE $stmt;

SELECT 'Orchestration completed.' AS status,
       $data_size AS data_size;


