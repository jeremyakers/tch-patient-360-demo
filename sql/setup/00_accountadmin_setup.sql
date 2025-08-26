-- Texas Children's Hospital Patient 360 PoC - ACCOUNTADMIN Setup
-- This script must be run by ACCOUNTADMIN before deploying the PoC
-- It creates the dedicated role and grants minimal necessary privileges

-- Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create the dedicated PoC role
CREATE ROLE IF NOT EXISTS TCH_PATIENT_360_ROLE
    COMMENT = 'Dedicated role for Texas Children\'s Hospital Patient 360 PoC resources';

-- Create warehouses (requires ACCOUNTADMIN or CREATE WAREHOUSE privilege)
CREATE OR REPLACE WAREHOUSE TCH_COMPUTE_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'General compute warehouse for data processing and transformations';

CREATE OR REPLACE WAREHOUSE TCH_ANALYTICS_WH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Analytics warehouse for heavy analytical workloads and reporting';

CREATE OR REPLACE WAREHOUSE TCH_AI_ML_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 180
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for AI/ML workloads including Cortex functions';

-- Grant warehouse usage and ownership to the PoC role
GRANT OWNERSHIP ON WAREHOUSE TCH_COMPUTE_WH TO ROLE TCH_PATIENT_360_ROLE;
GRANT OWNERSHIP ON WAREHOUSE TCH_ANALYTICS_WH TO ROLE TCH_PATIENT_360_ROLE;
GRANT OWNERSHIP ON WAREHOUSE TCH_AI_ML_WH TO ROLE TCH_PATIENT_360_ROLE;

-- Create Container Services compute pool for Notebooks on Container
CREATE COMPUTE POOL IF NOT EXISTS TCH_PATIENT_360_POOL
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = STANDARD_1;

-- Grant pool usage to PoC role
GRANT USAGE ON COMPUTE POOL TCH_PATIENT_360_POOL TO ROLE TCH_PATIENT_360_ROLE;

-- External access integration for PyPI (Notebook package installs)
CREATE OR REPLACE NETWORK RULE PIPY_RULE
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('pypi.org:443', 'files.pythonhosted.org:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION pypi_access_integration
    ALLOWED_NETWORK_RULES = (PIPY_RULE)
    ENABLED = TRUE;

GRANT USAGE ON INTEGRATION pypi_access_integration TO ROLE TCH_PATIENT_360_ROLE;

-- Grant essential privileges to create and manage databases
GRANT CREATE DATABASE ON ACCOUNT TO ROLE TCH_PATIENT_360_ROLE;

-- Additional privileges for Cortex AI and Streamlit features
-- Most Cortex functions work with database ownership and don't require additional account privileges
-- Streamlit in Snowflake apps can be created within owned databases
-- If specific Cortex or SiS features require additional privileges, they can be granted individually:
--
-- For advanced Cortex features (if needed):
-- GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE TCH_PATIENT_360_ROLE;
-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE TCH_PATIENT_360_ROLE;
--
-- For Streamlit apps with external access (if needed):
-- GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE TCH_PATIENT_360_ROLE;

-- Grant the role to users who will deploy/manage the PoC
-- Replace 'deployment_user' with the actual username(s)
-- GRANT ROLE TCH_PATIENT_360_ROLE TO USER deployment_user;

-- Optional: Grant to SYSADMIN for administrative oversight
GRANT ROLE TCH_PATIENT_360_ROLE TO ROLE SYSADMIN;

-- Display setup completion message
SELECT 'ACCOUNTADMIN setup complete. TCH_PATIENT_360_ROLE created with minimal necessary privileges.' AS setup_status,
       'Next step: Grant TCH_PATIENT_360_ROLE to deployment users and run the main deployment script.' AS next_steps;