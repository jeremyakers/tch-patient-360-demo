-- Texas Children's Hospital Patient 360 PoC - Workspace Orchestrator
-- Purpose: Single-entry "Run All" script for Snowsight Workspaces
-- Modes supported:
--   1) Workspace-inline: Run this file after editing as needed (simplest)
--   2) Git-orchestrated: Execute step files directly from a Snowflake Git repository object
--
-- Notes:
-- - To execute committed Git sources, set the variables in the "Git settings" section and
--   ensure the repo object is fetched to the latest commit/tag.
-- - To execute the Workspace edits you just made, keep everything inline here and click Run All.
-- - Streamlit/Notebook creation can point at the Git repository object so no manual staging is required.

-------------------------------------------------------------------------------
-- Context & parameters
-------------------------------------------------------------------------------

-- Required role/warehouse for deployment
USE ROLE TCH_PATIENT_360_ROLE;
USE WAREHOUSE TCH_COMPUTE_WH;

-- Adjustable parameters
SET data_size = 'medium';              -- small|medium|large (affects Notebook generation)
SET enable_git_mode = false;           -- true to execute from a Git repo object paths
SET git_db = 'TCH_PATIENT_360_POC';    -- database of the Git repository object
SET git_schema = 'RAW_DATA';           -- schema of the Git repository object
SET git_repo_name = 'TCH_P360_REPO';   -- name of the Git repository object
SET git_ref_type = 'branches';         -- 'branches' or 'tags'
SET git_ref_name = 'main';             -- branch or tag to pin

-- Derived repo path (only used if enable_git_mode=true)
SET repo_path = '@' || IDENTIFIER($git_db) || '.' || IDENTIFIER($git_schema) || '.' || IDENTIFIER($git_repo_name)
                 || '/' || $git_ref_type || '/' || QUOTE_IDENT($git_ref_name);

-------------------------------------------------------------------------------
-- Database & schemas (safe re-run)
-------------------------------------------------------------------------------

-- Database and core schemas
EXECUTE IMMEDIATE $$
    USE DATABASE TCH_PATIENT_360_POC;
    CREATE SCHEMA IF NOT EXISTS RAW_DATA;
    CREATE SCHEMA IF NOT EXISTS CONFORMED;
    CREATE SCHEMA IF NOT EXISTS PRESENTATION;
    CREATE SCHEMA IF NOT EXISTS AI_ML;
$$;

-------------------------------------------------------------------------------
-- Option A: Run existing step scripts from Git repo object (committed source)
-------------------------------------------------------------------------------

BEGIN
    IF ($enable_git_mode) THEN
        -- Optional: fetch latest commit for the configured ref (requires object to exist)
        --
        -- Example (uncomment and adjust if desired):
        -- ALTER GIT REPOSITORY IDENTIFIER($git_db || '.' || $git_schema || '.' || $git_repo_name) FETCH;

        -- Setup & structure
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/setup/01_database_setup.sql' );
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/setup/02_raw_tables.sql' );
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/setup/03_conformed_tables.sql' );
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/setup/04_presentation_tables.sql' );

        -- Data load (structured + unstructured)
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/data_load/01_load_raw_data.sql' );
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/data_load/02_load_unstructured_data.sql' );

        -- Dynamic tables
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/dynamic_tables/01_patient_dynamic_tables.sql' );
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/dynamic_tables/02_clinical_dynamic_tables.sql' );

        -- Cortex services (Analyst + Search)
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/cortex/01_cortex_analyst_setup.sql' );
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/cortex/02_cortex_search_setup.sql' );
    END IF;
END;

-------------------------------------------------------------------------------
-- Option B: Workspace-inline execution (edit here, then Run All)
-------------------------------------------------------------------------------

BEGIN
    IF (NOT $enable_git_mode) THEN
        -- Minimal inline bootstrap (safe if setup files already cover this)
        EXECUTE IMMEDIATE $$ USE DATABASE TCH_PATIENT_360_POC; $$;
        EXECUTE IMMEDIATE $$ USE SCHEMA RAW_DATA; $$;

        -- If preferred, copy key statements from the step scripts here for a fully inline flow.
        -- Keeping this lightweight to avoid duplication; by default we rely on the existing
        -- step scripts being executed in Git mode or run manually from the Workspace.
    END IF;
END;

-------------------------------------------------------------------------------
-- Data generation via Snowflake Notebook (already in-account Python execution)
-------------------------------------------------------------------------------

-- Preferred: create the Notebook object from the Git repo object, then execute it
BEGIN
    IF ($enable_git_mode) THEN
        -- Create Notebook from Git (if supported in the account)
        -- Adjust MAIN_FILE if you place the notebook elsewhere
        CREATE OR REPLACE NOTEBOOK AI_ML.TCH_DATA_GENERATOR
            FROM ( $repo_path || '/python/notebooks/' )
            MAIN_FILE = 'tch_data_generator.ipynb';

        -- Execute Notebook with parameters controlling data volume
        EXECUTE NOTEBOOK AI_ML.TCH_DATA_GENERATOR( 'data_size=' || $data_size, 'parallel=true' );
    END IF;
END;

-------------------------------------------------------------------------------
-- Streamlit app creation directly from Git repo object (no manual staging)
-------------------------------------------------------------------------------

BEGIN
    IF ($enable_git_mode) THEN
        CREATE OR REPLACE STREAMLIT PRESENTATION.TCH_PATIENT_360_APP
            ROOT_LOCATION = ( $repo_path || '/python/streamlit_app/' )
            MAIN_FILE     = 'main.py'
            QUERY_WAREHOUSE = 'TCH_ANALYTICS_WH'
            TITLE = 'TCH Patient 360';
    END IF;
END;

-------------------------------------------------------------------------------
-- Verification
-------------------------------------------------------------------------------

BEGIN
    IF ($enable_git_mode) THEN
        EXECUTE IMMEDIATE FROM ( $repo_path || '/sql/99_verification.sql' );
    ELSE
        -- Inline lightweight checks
        SELECT 'DYNAMIC_TABLE_COUNT' AS metric,
               COUNT(*) AS value
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'DYNAMIC TABLE'
          AND TABLE_SCHEMA = 'CONFORMED';
    END IF;
END;

SELECT 'Orchestration completed.' AS status,
       $enable_git_mode AS used_git_mode,
       $data_size AS data_size;


