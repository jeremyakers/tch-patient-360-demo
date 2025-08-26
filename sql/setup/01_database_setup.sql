-- Texas Children's Hospital Patient 360 PoC - Database Setup
-- Creates the main database and schemas for the PoC demonstration
-- Prerequisites: Run 00_accountadmin_setup.sql first as ACCOUNTADMIN

-- Set context to use the dedicated PoC role
USE ROLE TCH_PATIENT_360_ROLE;

-- Create database and take ownership
CREATE DATABASE IF NOT EXISTS TCH_PATIENT_360_POC
    COMMENT = 'Texas Children\'s Hospital Patient 360 Proof of Concept Database';

USE DATABASE TCH_PATIENT_360_POC;

-- Create schemas for data layers
CREATE SCHEMA IF NOT EXISTS RAW_DATA
    COMMENT = 'Raw data from source systems (Epic, Workday, Salesforce, etc.)';

CREATE SCHEMA IF NOT EXISTS CONFORMED
    COMMENT = 'Conformed/standardized data layer with business rules applied';

CREATE SCHEMA IF NOT EXISTS PRESENTATION
    COMMENT = 'Presentation layer optimized for analytics and visualization';

CREATE SCHEMA IF NOT EXISTS AI_ML
    COMMENT = 'AI/ML models, semantic definitions, and Cortex configurations';

-- Create schemas for source systems
CREATE SCHEMA IF NOT EXISTS EPIC
    COMMENT = 'Epic EHR raw data tables';

CREATE SCHEMA IF NOT EXISTS WORKDAY
    COMMENT = 'Workday HCM raw data tables';

CREATE SCHEMA IF NOT EXISTS ORACLE_ERP
    COMMENT = 'Oracle ERP raw data tables';

CREATE SCHEMA IF NOT EXISTS SALESFORCE
    COMMENT = 'Salesforce CRM raw data tables';

-- Create file formats for data loading in RAW_DATA schema
CREATE OR REPLACE FILE FORMAT RAW_DATA.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\134'
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    NULL_IF = ('NULL', 'null', '', 'N/A');

-- Create stages for data loading
CREATE STAGE IF NOT EXISTS RAW_DATA.PATIENT_DATA_STAGE
    COMMENT = 'Stage for patient demographic and clinical data files'
    DIRECTORY = ( ENABLE = TRUE );
ALTER STAGE RAW_DATA.PATIENT_DATA_STAGE SET FILE_FORMAT = RAW_DATA.CSV_FORMAT;

CREATE STAGE IF NOT EXISTS RAW_DATA.UNSTRUCTURED_DATA_STAGE
    COMMENT = 'Stage for unstructured clinical documents and notes'
    DIRECTORY = ( ENABLE = TRUE );
ALTER STAGE RAW_DATA.UNSTRUCTURED_DATA_STAGE SET FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = 'NONE' RECORD_DELIMITER = 'NONE');

-- Set default warehouse (created by ACCOUNTADMIN setup)
USE WAREHOUSE TCH_COMPUTE_WH;

-- Grant additional usage permissions if needed for demo/development
-- Note: TCH_PATIENT_360_ROLE already owns all warehouses and the database
-- Uncomment the lines below if you need to grant access to other roles

-- GRANT USAGE ON DATABASE TCH_PATIENT_360_POC TO ROLE <OTHER_ROLE>;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE TCH_PATIENT_360_POC TO ROLE <OTHER_ROLE>;
-- GRANT SELECT ON ALL TABLES IN DATABASE TCH_PATIENT_360_POC TO ROLE <OTHER_ROLE>;
-- GRANT SELECT ON ALL VIEWS IN DATABASE TCH_PATIENT_360_POC TO ROLE <OTHER_ROLE>;

-- Display setup completion message
SELECT 'Database setup complete. Created TCH_PATIENT_360_POC database with schemas: RAW_DATA, CONFORMED, PRESENTATION, AI_ML' AS setup_status;