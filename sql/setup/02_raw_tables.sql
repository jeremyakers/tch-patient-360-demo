-- Texas Children's Hospital Patient 360 PoC - Raw Data Tables
-- Creates tables in the RAW_DATA schema to match Epic EHR and other source system structures

USE DATABASE TCH_PATIENT_360_POC;
USE SCHEMA RAW_DATA;
USE WAREHOUSE TCH_COMPUTE_WH;

-- =============================================================================
-- EPIC EHR TABLES
-- =============================================================================

-- Epic Patient Demographics
CREATE OR REPLACE TABLE EPIC.PATIENTS (
    patient_id VARCHAR(20) NOT NULL,
    mrn VARCHAR(20) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(1),
    race VARCHAR(100),
    ethnicity VARCHAR(100),
    zip_code VARCHAR(10),
    insurance_type VARCHAR(50),
    language VARCHAR(50),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (patient_id)
) 
CLUSTER BY (patient_id)
COMMENT = 'Epic EHR patient demographics and registration data';

-- Epic Encounters
CREATE OR REPLACE TABLE EPIC.ENCOUNTERS (
    encounter_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    encounter_date TIMESTAMP_NTZ,
    encounter_type VARCHAR(50),
    department VARCHAR(100),
    attending_physician VARCHAR(200),
    admission_date TIMESTAMP_NTZ,
    discharge_date TIMESTAMP_NTZ,
    length_of_stay INTEGER,
    chief_complaint VARCHAR(500),
    status VARCHAR(50),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (encounter_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id)
)
CLUSTER BY (patient_id, encounter_date)
COMMENT = 'Epic EHR encounter data including admissions, outpatient visits, and emergency visits';

-- Epic Diagnoses
CREATE OR REPLACE TABLE EPIC.DIAGNOSES (
    diagnosis_id VARCHAR(20) NOT NULL,
    encounter_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    diagnosis_code VARCHAR(20),
    diagnosis_description VARCHAR(500),
    diagnosis_type VARCHAR(50),
    diagnosis_date DATE,
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (diagnosis_id),
    FOREIGN KEY (encounter_id) REFERENCES EPIC.ENCOUNTERS(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id)
)
CLUSTER BY (patient_id, diagnosis_date)
COMMENT = 'Epic EHR diagnosis data with ICD-10 codes';

-- Epic Lab Results
CREATE OR REPLACE TABLE EPIC.LAB_RESULTS (
    lab_result_id VARCHAR(20) NOT NULL,
    encounter_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    test_name VARCHAR(200),
    test_value VARCHAR(100),
    reference_range VARCHAR(100),
    abnormal_flag VARCHAR(5),
    result_date TIMESTAMP_NTZ,
    ordering_provider VARCHAR(200),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (lab_result_id),
    FOREIGN KEY (encounter_id) REFERENCES EPIC.ENCOUNTERS(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id)
)
CLUSTER BY (patient_id, result_date)
COMMENT = 'Epic EHR laboratory results and diagnostic tests';

-- Epic Medications
CREATE OR REPLACE TABLE EPIC.MEDICATIONS (
    medication_id VARCHAR(20) NOT NULL,
    encounter_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    medication_name VARCHAR(200),
    dosage VARCHAR(100),
    frequency VARCHAR(100),
    route VARCHAR(50),
    start_date DATE,
    end_date DATE,
    prescribing_provider VARCHAR(200),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (medication_id),
    FOREIGN KEY (encounter_id) REFERENCES EPIC.ENCOUNTERS(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id)
)
CLUSTER BY (patient_id, start_date)
COMMENT = 'Epic EHR medication orders and prescriptions';

-- Epic Vital Signs
CREATE OR REPLACE TABLE EPIC.VITAL_SIGNS (
    vital_sign_id VARCHAR(20) NOT NULL,
    encounter_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    temperature DECIMAL(4,1),
    heart_rate INTEGER,
    respiratory_rate INTEGER,
    blood_pressure_systolic INTEGER,
    blood_pressure_diastolic INTEGER,
    oxygen_saturation INTEGER,
    weight_kg DECIMAL(6,2),
    height_cm DECIMAL(6,1),
    recorded_date TIMESTAMP_NTZ,
    recorded_by VARCHAR(200),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (vital_sign_id),
    FOREIGN KEY (encounter_id) REFERENCES EPIC.ENCOUNTERS(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id)
)
CLUSTER BY (patient_id, recorded_date)
COMMENT = 'Epic EHR vital signs and measurements';

-- =============================================================================
-- IMAGING AND RADIOLOGY TABLES
-- =============================================================================

-- Imaging Studies
CREATE OR REPLACE TABLE EPIC.IMAGING_STUDIES (
    imaging_study_id VARCHAR(20) NOT NULL,
    encounter_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    study_type VARCHAR(50),
    study_name VARCHAR(200),
    study_date TIMESTAMP_NTZ,
    ordering_provider VARCHAR(200),
    performing_department VARCHAR(100),
    study_status VARCHAR(50),
    modality VARCHAR(10),
    body_part VARCHAR(100),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (imaging_study_id),
    FOREIGN KEY (encounter_id) REFERENCES EPIC.ENCOUNTERS(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id)
)
CLUSTER BY (patient_id, study_date)
COMMENT = 'Imaging studies and radiology orders';

-- =============================================================================
-- CLINICAL DOCUMENTATION TABLES
-- =============================================================================

-- Clinical Notes Metadata
CREATE OR REPLACE TABLE EPIC.CLINICAL_NOTES (
    note_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    encounter_id VARCHAR(20),
    note_type VARCHAR(100),
    note_date TIMESTAMP_NTZ,
    author VARCHAR(200),
    department VARCHAR(100),
    note_content VARCHAR(16777216), -- Large text field for note content
    diagnosis_codes ARRAY,
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (note_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id)
)
CLUSTER BY (patient_id, note_date)
COMMENT = 'Clinical notes and documentation from Epic EHR';

-- Radiology Reports
CREATE OR REPLACE TABLE EPIC.RADIOLOGY_REPORTS (
    note_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    encounter_id VARCHAR(20),
    imaging_study_id VARCHAR(20),
    note_type VARCHAR(100),
    study_type VARCHAR(50),
    note_date TIMESTAMP_NTZ,
    author VARCHAR(200),
    department VARCHAR(100),
    note_content VARCHAR(16777216), -- Large text field for report content
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (note_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id),
    FOREIGN KEY (imaging_study_id) REFERENCES EPIC.IMAGING_STUDIES(imaging_study_id)
)
CLUSTER BY (patient_id, note_date)
COMMENT = 'Radiology reports and imaging interpretations';

-- =============================================================================
-- WORKDAY TABLES (Provider and Administrative Data)
-- =============================================================================

-- Providers/Physicians
CREATE OR REPLACE TABLE WORKDAY.PROVIDERS (
    provider_id VARCHAR(20) NOT NULL,
    npi VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    specialty VARCHAR(100),
    department VARCHAR(100),
    credentials VARCHAR(50),
    status VARCHAR(20),
    hire_date DATE,
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (provider_id)
)
CLUSTER BY (provider_id)
COMMENT = 'Provider and physician information from Workday';

-- Departments
CREATE OR REPLACE TABLE WORKDAY.DEPARTMENTS (
    department_id VARCHAR(20) NOT NULL,
    department_name VARCHAR(100),
    department_code VARCHAR(20),
    service_line VARCHAR(100),
    location VARCHAR(100),
    status VARCHAR(20),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (department_id)
)
CLUSTER BY (department_id)
COMMENT = 'Department and service line information from Workday';

-- =============================================================================
-- ORACLE ERP TABLES (Financial Data)
-- =============================================================================

-- Encounter Financial Data
CREATE OR REPLACE TABLE ORACLE_ERP.ENCOUNTER_FINANCIALS (
    financial_id VARCHAR(20) NOT NULL,
    encounter_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    total_charges DECIMAL(12,2),
    insurance_payments DECIMAL(12,2),
    patient_payments DECIMAL(12,2),
    adjustments DECIMAL(12,2),
    balance DECIMAL(12,2),
    payer_name VARCHAR(200),
    financial_class VARCHAR(50),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (financial_id),
    FOREIGN KEY (encounter_id) REFERENCES EPIC.ENCOUNTERS(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id)
)
CLUSTER BY (encounter_id)
COMMENT = 'Financial data for encounters from Oracle ERP';

-- =============================================================================
-- SALESFORCE TABLES (Patient Engagement Data)
-- =============================================================================

-- Patient Portal Activity
CREATE OR REPLACE TABLE SALESFORCE.PATIENT_PORTAL_ACTIVITY (
    activity_id VARCHAR(20) NOT NULL,
    patient_id VARCHAR(20) NOT NULL,
    activity_type VARCHAR(100),
    activity_date TIMESTAMP_NTZ,
    description VARCHAR(500),
    status VARCHAR(50),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    PRIMARY KEY (activity_id),
    FOREIGN KEY (patient_id) REFERENCES EPIC.PATIENTS(patient_id)
)
CLUSTER BY (patient_id, activity_date)
COMMENT = 'Patient portal activity and engagement data from Salesforce';

-- =============================================================================
-- RAW UNSTRUCTURED DATA TABLES
-- =============================================================================
-- These tables store raw unstructured file content for performance
-- Cortex Search tables will be built on top of these

-- Raw Clinical Notes
CREATE OR REPLACE TABLE RAW_DATA.CLINICAL_NOTES_RAW (
    file_id VARCHAR(100) NOT NULL,
    filename VARCHAR(500) NOT NULL,
    file_path VARCHAR(1000),
    file_size INTEGER,
    file_last_modified TIMESTAMP_NTZ,
    content_type VARCHAR(50),
    raw_content TEXT,
    mrn VARCHAR(50),  -- Extracted MRN using Cortex EXTRACT_ANSWER
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (file_id)
)
CLUSTER BY (filename, mrn)
COMMENT = 'Raw clinical notes content from unstructured files for performance';

-- Raw Radiology Reports  
CREATE OR REPLACE TABLE RAW_DATA.RADIOLOGY_REPORTS_RAW (
    file_id VARCHAR(100) NOT NULL,
    filename VARCHAR(500) NOT NULL,
    file_path VARCHAR(1000),
    file_size INTEGER,
    file_last_modified TIMESTAMP_NTZ,
    content_type VARCHAR(50),
    raw_content TEXT,
    mrn VARCHAR(50),  -- Extracted MRN using Cortex EXTRACT_ANSWER
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (file_id)
)
CLUSTER BY (filename, mrn)
COMMENT = 'Raw radiology reports content from unstructured files for performance';

-- =============================================================================
-- CREATE SCHEMAS FOR SOURCE SYSTEMS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS EPIC COMMENT = 'Epic EHR data tables';
CREATE SCHEMA IF NOT EXISTS WORKDAY COMMENT = 'Workday HR and provider data';
CREATE SCHEMA IF NOT EXISTS ORACLE_ERP COMMENT = 'Oracle ERP financial data';
CREATE SCHEMA IF NOT EXISTS SALESFORCE COMMENT = 'Salesforce patient engagement data';

-- Display completion message
SELECT 'Raw data tables created successfully. Ready for data loading.' AS table_creation_status;