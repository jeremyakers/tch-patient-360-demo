-- =====================================================
-- TCH Patient 360 PoC - Migrate Streamlit to SPCS
-- =====================================================
-- This script migrates the existing Streamlit app from
-- warehouse runtime to SPCS container runtime to enable
-- SSE streaming functionality
-- 
-- Prerequisites:
-- - 05_spcs_streamlit_setup.sql must be executed first
-- - Streamlit app must already exist
-- - requirements.txt and pyproject.toml must be updated
-- =====================================================

USE ROLE TCH_PATIENT_360_ROLE;
USE DATABASE TCH_PATIENT_360_POC;
USE SCHEMA PRESENTATION;

-- =====================================================
-- Step 1: Check Current Streamlit App Status
-- =====================================================
SHOW STREAMLITS;

-- Describe current app configuration
DESCRIBE STREAMLIT PRESENTATION.QJO_FP08XRIQWG4B;

-- =====================================================
-- Step 2: Migrate to SPCS Container Runtime
-- =====================================================
-- This ALTER command migrates the app to container runtime
-- The app will take a couple minutes to reboot and build the container

ALTER STREAMLIT PRESENTATION.QJO_FP08XRIQWG4B
  SET RUNTIME_NAME = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'
      COMPUTE_POOL = tch_streamlit_compute_pool
      EXTERNAL_ACCESS_INTEGRATIONS = (pypi_access_integration);

-- =====================================================
-- Step 3: Verify Migration
-- =====================================================
-- Check the updated configuration
DESCRIBE STREAMLIT PRESENTATION.QJO_FP08XRIQWG4B;

-- Show compute pool status
SHOW COMPUTE POOLS;

-- Check integration status
SHOW INTEGRATIONS LIKE 'pypi_access_integration';

-- =====================================================
-- Step 4: Display Migration Status
-- =====================================================
SELECT 'Migration Complete' AS status,
       'Streamlit app now running on SPCS container runtime' AS message,
       'SSE streaming should now be supported' AS capability;

-- =====================================================
-- TROUBLESHOOTING NOTES
-- =====================================================
-- If migration fails, you can revert using:
-- ALTER STREAMLIT PRESENTATION.QJO_FP08XRIQWG4B SET RUNTIME_NAME = 'SYSTEM$WAREHOUSE_RUNTIME';
--
-- Common issues:
-- 1. Insufficient privileges - check grants in step 4 of setup script
-- 2. Compute pool not ready - wait for pool to be active
-- 3. Package compatibility - check requirements.txt format
-- 4. App created with legacy ROOT_LOCATION - requires CREATE OR REPLACE
-- =====================================================
