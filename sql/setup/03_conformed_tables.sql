-- Texas Children's Hospital Patient 360 PoC - Conformed Data Layer Tables
-- Creates reference and lookup tables in the CONFORMED schema
-- Note: Main conformed tables are created by Dynamic Tables in the data pipeline phase

USE DATABASE TCH_PATIENT_360_POC;
USE SCHEMA CONFORMED;
USE WAREHOUSE TCH_COMPUTE_WH;

-- =============================================================================
-- REFERENCE DATA TABLES
-- =============================================================================
-- These tables contain static reference data and do not conflict with Dynamic Tables

-- ICD-10 Reference Data
CREATE OR REPLACE TABLE ICD10_REFERENCE (
    icd10_code VARCHAR(10) NOT NULL,
    diagnosis_description VARCHAR(500),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    is_chronic BOOLEAN DEFAULT FALSE,
    is_pediatric_relevant BOOLEAN DEFAULT TRUE,
    severity_level VARCHAR(20),
    created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (icd10_code)
)
COMMENT = 'ICD-10 diagnosis code reference for standardization';

-- Age Group Reference Data
CREATE OR REPLACE TABLE AGE_GROUP_REFERENCE (
    age_group_id INTEGER NOT NULL,
    age_group_name VARCHAR(50),
    min_age INTEGER,
    max_age INTEGER,
    age_description VARCHAR(200),
    is_pediatric BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (age_group_id)
)
COMMENT = 'Age group classifications for pediatric care';

-- Insert ICD-10 reference data
INSERT OVERWRITE INTO ICD10_REFERENCE (icd10_code, diagnosis_description, category, subcategory, is_chronic, is_pediatric_relevant, severity_level) VALUES
('J45.9', 'Asthma, unspecified', 'Respiratory', 'Asthma', TRUE, TRUE, 'Moderate'),
('F90.9', 'ADHD, unspecified', 'Mental Health', 'ADHD', TRUE, TRUE, 'Mild'),
('E10.9', 'Type 1 diabetes, unspecified', 'Endocrine', 'Diabetes', TRUE, TRUE, 'High'),
('E11.9', 'Type 2 diabetes, unspecified', 'Endocrine', 'Diabetes', TRUE, TRUE, 'High'),
('F84.0', 'Autistic disorder', 'Mental Health', 'Autism', TRUE, TRUE, 'Moderate'),
('G40.909', 'Epilepsy, unspecified', 'Neurological', 'Epilepsy', TRUE, TRUE, 'High'),
('K21.9', 'GERD, unspecified', 'Digestive', 'GERD', TRUE, TRUE, 'Mild'),
('E66.9', 'Obesity, unspecified', 'Endocrine', 'Obesity', TRUE, TRUE, 'Moderate');

-- Insert age group reference data
INSERT OVERWRITE INTO AGE_GROUP_REFERENCE (age_group_id, age_group_name, min_age, max_age, age_description, is_pediatric) VALUES
(1, 'Neonate', 0, 0, 'Birth to 28 days', TRUE),
(2, 'Infant', 0, 1, '1 month to 1 year', TRUE),
(3, 'Toddler', 1, 3, '1 to 3 years', TRUE),
(4, 'Preschooler', 3, 5, '3 to 5 years', TRUE),
(5, 'School Age', 5, 12, '5 to 12 years', TRUE),
(6, 'Adolescent', 12, 18, '12 to 18 years', TRUE),
(7, 'Young Adult', 18, 21, '18 to 21 years', TRUE);

-- =============================================================================
-- DEPLOYMENT STATUS
-- =============================================================================

SELECT 'Conformed layer reference tables created successfully. Dynamic Tables will create operational tables.' AS deployment_status;

-- Summary of what will be created by Dynamic Tables:
SELECT 'Dynamic Tables will create the following conformed tables:' AS info_message;
SELECT 'PATIENT_MASTER, ENCOUNTER_SUMMARY, DIAGNOSIS_FACT,' AS dynamic_tables_part1;
SELECT 'LAB_RESULTS_FACT, MEDICATION_FACT, VITAL_SIGNS_FACT,' AS dynamic_tables_part2;  
SELECT 'IMAGING_STUDY_FACT, PROVIDER_DIM, DEPARTMENT_DIM' AS dynamic_tables_part3;