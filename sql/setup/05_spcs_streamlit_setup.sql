-- =====================================================
-- TCH Patient 360 PoC - SPCS Streamlit Setup
-- =====================================================
-- This script sets up Snowpark Container Services (SPCS) 
-- runtime for Streamlit to enable SSE streaming functionality
-- 
-- Prerequisites: 
-- - Account must have SPCS enabled (Private Preview)
-- - Must be run by ACCOUNTADMIN or role with MANAGE COMPUTE privilege
-- =====================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TCH_PATIENT_360_POC;

-- =====================================================
-- Step 1: Create Compute Pool for Streamlit
-- =====================================================
-- This compute pool will run the containerized Streamlit app
-- MIN_NODES = 1 ensures fast app startup
-- MAX_NODES = 2 allows for scaling during high usage

CREATE OR REPLACE COMPUTE POOL tch_streamlit_compute_pool
  MIN_NODES = 1
  MAX_NODES = 2  
  INSTANCE_FAMILY = CPU_X64_XS
  COMMENT = 'Compute pool for TCH Patient 360 Streamlit app on SPCS';

-- =====================================================
-- Step 2: Create Network Rule for PyPI Access
-- =====================================================
-- This allows the container to download Python packages
-- from PyPI and related hosting services

CREATE OR REPLACE NETWORK RULE pypi_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'pypi.org',
    'pypi.python.org', 
    'pythonhosted.org',
    'files.pythonhosted.org'
  )
  COMMENT = 'Network rule to allow PyPI package downloads';

-- =====================================================
-- Step 3: Create External Access Integration
-- =====================================================
-- This integration manages external network access
-- for the Streamlit container

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION pypi_access_integration
  ALLOWED_NETWORK_RULES = (pypi_network_rule)
  ENABLED = true
  COMMENT = 'External access integration for PyPI package installation';

-- =====================================================
-- Step 4: Grant Privileges to TCH Role
-- =====================================================
-- Grant necessary privileges for SPCS runtime

-- Compute pool usage
GRANT USAGE ON COMPUTE POOL tch_streamlit_compute_pool TO ROLE TCH_PATIENT_360_ROLE;

-- External access integration usage  
GRANT USAGE ON INTEGRATION pypi_access_integration TO ROLE TCH_PATIENT_360_ROLE;

-- Verify existing privileges (should already exist from previous setup)
SHOW GRANTS TO ROLE TCH_PATIENT_360_ROLE;

-- =====================================================
-- Step 5: Display Migration Information
-- =====================================================
SELECT 'SPCS Setup Complete' AS status,
       'Ready to migrate Streamlit app to container runtime' AS next_step;

-- Show compute pool status
DESCRIBE COMPUTE POOL tch_streamlit_compute_pool;

-- Show integration status
DESCRIBE INTEGRATION pypi_access_integration;
