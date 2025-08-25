-- Texas Children's Hospital Patient 360 PoC - Raw Data Loading
-- Loads generated mock data into raw data tables

USE DATABASE TCH_PATIENT_360_POC;
USE WAREHOUSE TCH_COMPUTE_WH;

-- =============================================================================
-- SETUP VARIABLES AND PARAMETERS
-- =============================================================================

-- Set variables for data loading (can be overridden by deployment script)
SET data_path = '@RAW_DATA.PATIENT_DATA_STAGE';

-- =============================================================================
-- LOAD PATIENT DEMOGRAPHICS
-- =============================================================================

-- Ensure we're in the correct database context for file format access
USE DATABASE TCH_PATIENT_360_POC;
-- Note: Staying at database level to access file formats, using fully qualified table names

-- Load patients
TRUNCATE TABLE EPIC.PATIENTS;

COPY INTO EPIC.PATIENTS (
    patient_id,
    mrn,
    first_name,
    last_name,
    date_of_birth,
    gender,
    race,
    ethnicity,
    zip_code,
    insurance_type,
    language,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS patient_id,
        $2::VARCHAR(20) AS mrn,
        $3::VARCHAR(100) AS first_name,
        $4::VARCHAR(100) AS last_name,
        $5::DATE AS date_of_birth,
        $6::VARCHAR(1) AS gender,
        $7::VARCHAR(100) AS race,
        $8::VARCHAR(100) AS ethnicity,
        $9::VARCHAR(10) AS zip_code,
        $10::VARCHAR(50) AS insurance_type,
        $11::VARCHAR(50) AS language,
        $12::TIMESTAMP_NTZ AS created_date,
        $13::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*patients_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- =============================================================================
-- LOAD ENCOUNTER DATA
-- =============================================================================

-- Load encounters
TRUNCATE TABLE EPIC.ENCOUNTERS;

COPY INTO EPIC.ENCOUNTERS (
    encounter_id,
    patient_id,
    encounter_date,
    encounter_type,
    department,
    attending_physician,
    admission_date,
    discharge_date,
    length_of_stay,
    chief_complaint,
    status,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS encounter_id,
        $2::VARCHAR(20) AS patient_id,
        $3::TIMESTAMP_NTZ AS encounter_date,
        $4::VARCHAR(50) AS encounter_type,
        $5::VARCHAR(100) AS department,
        $6::VARCHAR(200) AS attending_physician,
        $7::TIMESTAMP_NTZ AS admission_date,
        CASE WHEN $8 = 'None' OR $8 = '' THEN NULL ELSE $8::TIMESTAMP_NTZ END AS discharge_date,
        CASE WHEN $9 = 'None' OR $9 = '' THEN NULL ELSE $9::INTEGER END AS length_of_stay,
        $10::VARCHAR(500) AS chief_complaint,
        $11::VARCHAR(50) AS status,
        $12::TIMESTAMP_NTZ AS created_date,
        $13::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*encounters_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- =============================================================================
-- LOAD CLINICAL DATA
-- =============================================================================

-- Load diagnoses
TRUNCATE TABLE EPIC.DIAGNOSES;

COPY INTO EPIC.DIAGNOSES (
    diagnosis_id,
    encounter_id,
    patient_id,
    diagnosis_code,
    diagnosis_description,
    diagnosis_type,
    diagnosis_date,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS diagnosis_id,
        $2::VARCHAR(20) AS encounter_id,
        $3::VARCHAR(20) AS patient_id,
        $4::VARCHAR(20) AS diagnosis_code,
        $6::VARCHAR(500) AS diagnosis_description,
        $8::VARCHAR(50) AS diagnosis_type,
        TO_TIMESTAMP_NTZ($9)::DATE AS diagnosis_date,
        TO_TIMESTAMP_NTZ($10) AS created_date,
        TO_TIMESTAMP_NTZ($11) AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*diagnoses_part_[0-9]+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- Load lab results
TRUNCATE TABLE EPIC.LAB_RESULTS;

COPY INTO EPIC.LAB_RESULTS (
    lab_result_id,
    encounter_id,
    patient_id,
    test_name,
    test_value,
    reference_range,
    abnormal_flag,
    result_date,
    ordering_provider,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS lab_result_id,
        $2::VARCHAR(20) AS encounter_id,
        $3::VARCHAR(20) AS patient_id,
        $5::VARCHAR(200) AS test_name,
        $6::VARCHAR(100) AS test_value,
        $8::VARCHAR(100) AS reference_range,
        $9::VARCHAR(5) AS abnormal_flag,
        $11::TIMESTAMP_NTZ AS result_date,
        ''::VARCHAR(200) AS ordering_provider,
        $13::TIMESTAMP_NTZ AS created_date,
        $14::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*lab_results_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- Load medications
TRUNCATE TABLE EPIC.MEDICATIONS;

COPY INTO EPIC.MEDICATIONS (
    medication_id,
    encounter_id,
    patient_id,
    medication_name,
    dosage,
    frequency,
    route,
    start_date,
    end_date,
    prescribing_provider,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS medication_id,
        $2::VARCHAR(20) AS encounter_id,
        $3::VARCHAR(20) AS patient_id,
        $4::VARCHAR(200) AS medication_name,
        $5::VARCHAR(100) AS dosage,
        $6::VARCHAR(100) AS frequency,
        $7::VARCHAR(50) AS route,
        $8::DATE AS start_date,
        $9::DATE AS end_date,
        $10::VARCHAR(200) AS prescribing_provider,
        $11::TIMESTAMP_NTZ AS created_date,
        $12::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*medications_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- Load vital signs
TRUNCATE TABLE EPIC.VITAL_SIGNS;

COPY INTO EPIC.VITAL_SIGNS (
    vital_sign_id,
    encounter_id,
    patient_id,
    temperature,
    heart_rate,
    respiratory_rate,
    blood_pressure_systolic,
    blood_pressure_diastolic,
    oxygen_saturation,
    weight_kg,
    height_cm,
    recorded_date,
    recorded_by,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS vital_sign_id,
        $2::VARCHAR(20) AS encounter_id,
        $3::VARCHAR(20) AS patient_id,
        $4::DECIMAL(4,1) AS temperature,
        $5::INTEGER AS heart_rate,
        $6::INTEGER AS respiratory_rate,
        $7::INTEGER AS blood_pressure_systolic,
        $8::INTEGER AS blood_pressure_diastolic,
        $9::INTEGER AS oxygen_saturation,
        $10::DECIMAL(6,2) AS weight_kg,
        $11::DECIMAL(6,1) AS height_cm,
        $12::TIMESTAMP_NTZ AS recorded_date,
        $13::VARCHAR(200) AS recorded_by,
        $14::TIMESTAMP_NTZ AS created_date,
        $15::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*vital_signs_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- =============================================================================
-- LOAD IMAGING DATA
-- =============================================================================

-- Load imaging studies
TRUNCATE TABLE EPIC.IMAGING_STUDIES;

COPY INTO EPIC.IMAGING_STUDIES (
    imaging_study_id,
    encounter_id,
    patient_id,
    study_type,
    study_name,
    study_date,
    ordering_provider,
    performing_department,
    study_status,
    modality,
    body_part,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS imaging_study_id,
        $2::VARCHAR(20) AS encounter_id,
        $3::VARCHAR(20) AS patient_id,
        $4::VARCHAR(50) AS study_type,
        $5::VARCHAR(200) AS study_name,
        $6::TIMESTAMP_NTZ AS study_date,
        $7::VARCHAR(200) AS ordering_provider,
        $8::VARCHAR(100) AS performing_department,
        $9::VARCHAR(50) AS study_status,
        $10::VARCHAR(10) AS modality,
        $11::VARCHAR(100) AS body_part,
        $12::TIMESTAMP_NTZ AS created_date,
        $13::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*imaging_studies_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- =============================================================================
-- LOAD CLINICAL DOCUMENTATION
-- =============================================================================

-- Load clinical notes metadata
TRUNCATE TABLE EPIC.CLINICAL_NOTES;

COPY INTO EPIC.CLINICAL_NOTES (
    note_id,
    patient_id,
    encounter_id,
    note_type,
    note_date,
    author,
    department,
    note_content,
    diagnosis_codes,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS note_id,
        $2::VARCHAR(20) AS patient_id,
        $3::VARCHAR(20) AS encounter_id,
        $4::VARCHAR(100) AS note_type,
        $5::TIMESTAMP_NTZ AS note_date,
        $6::VARCHAR(200) AS author,
        $7::VARCHAR(100) AS department,
        '' AS note_content, -- Will be loaded separately from text files
        TRY_PARSE_JSON($8) AS diagnosis_codes,
        $9::TIMESTAMP_NTZ AS created_date,
        $10::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*clinical_notes_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- Load radiology reports metadata
TRUNCATE TABLE EPIC.RADIOLOGY_REPORTS;

COPY INTO EPIC.RADIOLOGY_REPORTS (
    note_id,
    patient_id,
    encounter_id,
    imaging_study_id,
    note_type,
    study_type,
    note_date,
    author,
    department,
    note_content,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS note_id,
        $2::VARCHAR(20) AS patient_id,
        $3::VARCHAR(20) AS encounter_id,
        $4::VARCHAR(20) AS imaging_study_id,
        $5::VARCHAR(100) AS note_type,
        $6::VARCHAR(50) AS study_type,
        $7::TIMESTAMP_NTZ AS note_date,
        $8::VARCHAR(200) AS author,
        $9::VARCHAR(100) AS department,
        '' AS note_content, -- Will be loaded separately from text files
        $10::TIMESTAMP_NTZ AS created_date,
        $11::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*radiology_reports_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- =============================================================================
-- LOAD REFERENCE DATA
-- =============================================================================

-- Load providers
TRUNCATE TABLE WORKDAY.PROVIDERS;

COPY INTO WORKDAY.PROVIDERS (
    provider_id,
    npi,
    first_name,
    last_name,
    specialty,
    department,
    credentials,
    status,
    hire_date,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS provider_id,
        $2::VARCHAR(20) AS npi,
        $3::VARCHAR(100) AS first_name,
        $4::VARCHAR(100) AS last_name,
        $5::VARCHAR(100) AS specialty,
        $6::VARCHAR(100) AS department,
        $7::VARCHAR(50) AS credentials,
        $8::VARCHAR(20) AS status,
        $9::DATE AS hire_date,
        $10::TIMESTAMP_NTZ AS created_date,
        $11::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*providers_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- Load departments
TRUNCATE TABLE WORKDAY.DEPARTMENTS;

COPY INTO WORKDAY.DEPARTMENTS (
    department_id,
    department_name,
    department_code,
    service_line,
    location,
    status,
    created_date,
    updated_date
)
FROM (
    SELECT 
        $1::VARCHAR(20) AS department_id,
        $2::VARCHAR(100) AS department_name,
        $3::VARCHAR(20) AS department_code,
        $4::VARCHAR(100) AS service_line,
        $5::VARCHAR(100) AS location,
        $6::VARCHAR(20) AS status,
        $7::TIMESTAMP_NTZ AS created_date,
        $8::TIMESTAMP_NTZ AS updated_date
    FROM @RAW_DATA.PATIENT_DATA_STAGE (PATTERN => '.*departments_part_\\d+\\.csv\\.gz')
)
FILE_FORMAT = RAW_DATA.CSV_FORMAT
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verification consolidated at end via DATA_LOAD_SUMMARY

-- =============================================================================
-- ORACLE ERP FINANCIAL DATA GENERATION (synthetic from encounters/patients)
-- =============================================================================

TRUNCATE TABLE ORACLE_ERP.ENCOUNTER_FINANCIALS;

INSERT INTO ORACLE_ERP.ENCOUNTER_FINANCIALS (
    financial_id,
    encounter_id,
    patient_id,
    total_charges,
    insurance_payments,
    patient_payments,
    adjustments,
    balance,
    payer_name,
    financial_class,
    created_date,
    updated_date
)
SELECT
    'FIN-' || LPAD(TO_VARCHAR(ROW_NUMBER() OVER (ORDER BY e.encounter_id)), 10, '0') AS financial_id,
    e.encounter_id,
    e.patient_id,
    -- Base charges by department complexity and LOS
    (
        CASE 
            WHEN UPPER(e.department) = 'EMERGENCY DEPARTMENT' THEN UNIFORM(500, 5000, RANDOM())
            WHEN UPPER(e.department) = 'PEDIATRIC ICU' THEN UNIFORM(10000, 50000, RANDOM())
            WHEN UPPER(e.department) = 'NEONATAL ICU' THEN UNIFORM(15000, 75000, RANDOM())
            ELSE UNIFORM(1000, 8000, RANDOM())
        END * (1 + COALESCE(e.length_of_stay, 0) * 0.20)
    )::NUMBER(12,2) AS total_charges,
    -- Insurance payment by payer type
    (
        CASE LOWER(p.insurance_type)
            WHEN 'medicaid' THEN 0.85
            WHEN 'commercial' THEN 0.92
            WHEN 'chip' THEN 0.88
            ELSE 0.70
        END
    ) * (
        CASE 
            WHEN UPPER(e.department) = 'EMERGENCY DEPARTMENT' THEN UNIFORM(500, 5000, RANDOM())
            WHEN UPPER(e.department) = 'PEDIATRIC ICU' THEN UNIFORM(10000, 50000, RANDOM())
            WHEN UPPER(e.department) = 'NEONATAL ICU' THEN UNIFORM(15000, 75000, RANDOM())
            ELSE UNIFORM(1000, 8000, RANDOM())
        END * (1 + COALESCE(e.length_of_stay, 0) * 0.20)
    )::NUMBER(12,2) AS insurance_payments,
    -- Patient coinsurance/copay
    ROUND(UNIFORM(0, 10, RANDOM()) / 100.0 * (
        CASE 
            WHEN UPPER(e.department) = 'EMERGENCY DEPARTMENT' THEN UNIFORM(500, 5000, RANDOM())
            WHEN UPPER(e.department) = 'PEDIATRIC ICU' THEN UNIFORM(10000, 50000, RANDOM())
            WHEN UPPER(e.department) = 'NEONATAL ICU' THEN UNIFORM(15000, 75000, RANDOM())
            ELSE UNIFORM(1000, 8000, RANDOM())
        END * (1 + COALESCE(e.length_of_stay, 0) * 0.20)
    ), 2) AS patient_payments,
    -- Contractual adjustments
    ROUND(UNIFORM(0, 5, RANDOM()) / 100.0 * (
        CASE 
            WHEN UPPER(e.department) = 'EMERGENCY DEPARTMENT' THEN UNIFORM(500, 5000, RANDOM())
            WHEN UPPER(e.department) = 'PEDIATRIC ICU' THEN UNIFORM(10000, 50000, RANDOM())
            WHEN UPPER(e.department) = 'NEONATAL ICU' THEN UNIFORM(15000, 75000, RANDOM())
            ELSE UNIFORM(1000, 8000, RANDOM())
        END * (1 + COALESCE(e.length_of_stay, 0) * 0.20)
    ), 2) AS adjustments,
    -- Remaining balance
    GREATEST(0,
        (
            CASE 
                WHEN UPPER(e.department) = 'EMERGENCY DEPARTMENT' THEN UNIFORM(500, 5000, RANDOM())
                WHEN UPPER(e.department) = 'PEDIATRIC ICU' THEN UNIFORM(10000, 50000, RANDOM())
                WHEN UPPER(e.department) = 'NEONATAL ICU' THEN UNIFORM(15000, 75000, RANDOM())
                ELSE UNIFORM(1000, 8000, RANDOM())
            END * (1 + COALESCE(e.length_of_stay, 0) * 0.20)
        )
        - (
            (
                CASE LOWER(p.insurance_type)
                    WHEN 'medicaid' THEN 0.85
                    WHEN 'commercial' THEN 0.92
                    WHEN 'chip' THEN 0.88
                    ELSE 0.70
                END
            ) * (
                CASE 
                    WHEN UPPER(e.department) = 'EMERGENCY DEPARTMENT' THEN UNIFORM(500, 5000, RANDOM())
                    WHEN UPPER(e.department) = 'PEDIATRIC ICU' THEN UNIFORM(10000, 50000, RANDOM())
                    WHEN UPPER(e.department) = 'NEONATAL ICU' THEN UNIFORM(15000, 75000, RANDOM())
                    ELSE UNIFORM(1000, 8000, RANDOM())
                END * (1 + COALESCE(e.length_of_stay, 0) * 0.20)
            )
        )
        - ROUND(UNIFORM(0, 10, RANDOM()) / 100.0 * (
            CASE 
                WHEN UPPER(e.department) = 'EMERGENCY DEPARTMENT' THEN UNIFORM(500, 5000, RANDOM())
                WHEN UPPER(e.department) = 'PEDIATRIC ICU' THEN UNIFORM(10000, 50000, RANDOM())
                WHEN UPPER(e.department) = 'NEONATAL ICU' THEN UNIFORM(15000, 75000, RANDOM())
                ELSE UNIFORM(1000, 8000, RANDOM())
            END * (1 + COALESCE(e.length_of_stay, 0) * 0.20)
        ), 2)
        - ROUND(UNIFORM(0, 5, RANDOM()) / 100.0 * (
            CASE 
                WHEN UPPER(e.department) = 'EMERGENCY DEPARTMENT' THEN UNIFORM(500, 5000, RANDOM())
                WHEN UPPER(e.department) = 'PEDIATRIC ICU' THEN UNIFORM(10000, 50000, RANDOM())
                WHEN UPPER(e.department) = 'NEONATAL ICU' THEN UNIFORM(15000, 75000, RANDOM())
                ELSE UNIFORM(1000, 8000, RANDOM())
            END * (1 + COALESCE(e.length_of_stay, 0) * 0.20)
        ), 2)
    )::NUMBER(12,2) AS balance,
    COALESCE(p.insurance_type, 'Unknown') AS payer_name,
    COALESCE(p.insurance_type, 'Unknown') AS financial_class,
    e.created_date,
    CURRENT_TIMESTAMP() AS updated_date
FROM EPIC.ENCOUNTERS e
JOIN EPIC.PATIENTS p ON p.patient_id = e.patient_id;

-- =============================================================================
-- SALESFORCE ENGAGEMENT DATA GENERATION (synthetic portal activity)
-- =============================================================================

TRUNCATE TABLE SALESFORCE.PATIENT_PORTAL_ACTIVITY;

INSERT INTO SALESFORCE.PATIENT_PORTAL_ACTIVITY (
    activity_id,
    patient_id,
    activity_type,
    activity_date,
    description,
    status,
    created_date,
    updated_date
)
WITH patient_base AS (
    SELECT 
        p.patient_id,
        DATEDIFF('year', p.date_of_birth, CURRENT_DATE()) AS age_years,
        CASE 
            WHEN DATEDIFF('year', p.date_of_birth, CURRENT_DATE()) <= 12 THEN UNIFORM(5, 15, RANDOM())
            WHEN DATEDIFF('year', p.date_of_birth, CURRENT_DATE()) <= 17 THEN UNIFORM(1, 8, RANDOM())
            ELSE UNIFORM(2, 10, RANDOM())
        END AS monthly_activities
    FROM EPIC.PATIENTS p
), expanded AS (
    SELECT 
        pb.patient_id,
        ROW_NUMBER() OVER (PARTITION BY pb.patient_id ORDER BY SEQ4()) AS rn
    FROM patient_base pb
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 30))
)
SELECT 
    'ACT-' || LPAD(TO_VARCHAR(ROW_NUMBER() OVER (ORDER BY e.patient_id, e.rn)), 12, '0') AS activity_id,
    e.patient_id,
    CASE 1 + MOD(UNIFORM(0, 1000, RANDOM()), 3)
        WHEN 1 THEN 'Portal Login'
        WHEN 2 THEN 'Message Sent'
        ELSE 'Document View'
    END AS activity_type,
    DATEADD('day', -UNIFORM(0, 90, RANDOM()), CURRENT_TIMESTAMP()) AS activity_date,
    'Auto-generated engagement event' AS description,
    'Completed' AS status,
    CURRENT_TIMESTAMP() AS created_date,
    CURRENT_TIMESTAMP() AS updated_date
FROM expanded e
JOIN patient_base pb ON pb.patient_id = e.patient_id
WHERE e.rn <= pb.monthly_activities;

-- =============================================================================
-- DATA LOADING SUMMARY
-- =============================================================================

-- Create summary view of loaded data
CREATE OR REPLACE VIEW RAW_DATA.DATA_LOAD_SUMMARY AS
SELECT 
    'Patients' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM EPIC.PATIENTS

UNION ALL

SELECT 
    'Encounters' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM EPIC.ENCOUNTERS

UNION ALL

SELECT 
    'Diagnoses' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM EPIC.DIAGNOSES

UNION ALL

SELECT 
    'Lab Results' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM EPIC.LAB_RESULTS

UNION ALL

SELECT 
    'Medications' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM EPIC.MEDICATIONS

UNION ALL

SELECT 
    'Vital Signs' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM EPIC.VITAL_SIGNS

UNION ALL

SELECT 
    'Imaging Studies' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM EPIC.IMAGING_STUDIES

UNION ALL

SELECT 
    'Clinical Notes' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM EPIC.CLINICAL_NOTES

UNION ALL

SELECT 
    'Radiology Reports' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM EPIC.RADIOLOGY_REPORTS

UNION ALL

SELECT 
    'Providers' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM WORKDAY.PROVIDERS

UNION ALL

SELECT 
    'Departments' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM WORKDAY.DEPARTMENTS

UNION ALL

SELECT 
    'Encounter Financials' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM ORACLE_ERP.ENCOUNTER_FINANCIALS

UNION ALL

SELECT 
    'Patient Portal Activity' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_date) AS earliest_record,
    MAX(updated_date) AS latest_record
FROM SALESFORCE.PATIENT_PORTAL_ACTIVITY

ORDER BY table_name;

-- Display single consolidated summary
SELECT * FROM RAW_DATA.DATA_LOAD_SUMMARY ORDER BY table_name;