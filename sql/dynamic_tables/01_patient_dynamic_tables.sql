-- Texas Children's Hospital Patient 360 PoC - Patient Dynamic Tables
-- Implements Dynamic Tables for real-time data transformation from raw to conformed layer

USE DATABASE TCH_PATIENT_360_POC;
USE WAREHOUSE TCH_COMPUTE_WH;

-- =============================================================================
-- DROP EXISTING TABLES TO AVOID CONFLICTS WITH DYNAMIC TABLES
-- =============================================================================

DROP TABLE IF EXISTS CONFORMED.PATIENT_BASE;
DROP VIEW IF EXISTS CONFORMED.PATIENT_MASTER;
DROP TABLE IF EXISTS CONFORMED.ENCOUNTER_SUMMARY;

-- =============================================================================
-- PATIENT BASE DYNAMIC TABLE
-- =============================================================================

-- Patient Base - Real-time patient demographics (incremental-safe)
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.PATIENT_BASE
TARGET_LAG = '2 minutes'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    -- Use stable natural key for incremental refresh compatibility
    ep.patient_id AS patient_key,
    
    -- Source identifiers
    ep.patient_id,
    ep.mrn,
    
    -- Demographics
    ep.first_name,
    ep.last_name,
    CONCAT(COALESCE(ep.first_name, ''), ' ', COALESCE(ep.last_name, '')) AS full_name,
    ep.date_of_birth,
    
    -- Standardize gender
    CASE 
        WHEN UPPER(ep.gender) = 'M' THEN 'Male'
        WHEN UPPER(ep.gender) = 'F' THEN 'Female'
        ELSE 'Unknown'
    END AS gender,
    
    -- Standardize race
    COALESCE(ep.race, 'Unknown') AS race,
    
    -- Standardize ethnicity
    COALESCE(ep.ethnicity, 'Unknown') AS ethnicity,
    
    ep.zip_code,
    
    -- Standardize insurance
    CASE 
        WHEN UPPER(ep.insurance_type) LIKE '%MEDICAID%' THEN 'Medicaid'
        WHEN UPPER(ep.insurance_type) LIKE '%COMMERCIAL%' THEN 'Commercial'
        WHEN UPPER(ep.insurance_type) LIKE '%CHIP%' THEN 'CHIP'
        WHEN UPPER(ep.insurance_type) LIKE '%SELF%' THEN 'Self-pay'
        ELSE COALESCE(ep.insurance_type, 'Unknown')
    END AS primary_insurance,
    
    -- Standardize language
    CASE 
        WHEN UPPER(ep.language) = 'SPANISH' THEN 'Spanish'
        WHEN UPPER(ep.language) = 'ENGLISH' THEN 'English'
        ELSE COALESCE(ep.language, 'English')
    END AS primary_language,
    
    -- Audit fields
    'EPIC' AS source_system,
    ep.created_date AS created_date,
    ep.updated_date AS updated_date,
    COALESCE(ep.created_date::DATE, DATE '1900-01-01') AS effective_date,
    DATE '9999-12-31' AS expiration_date,
    TRUE AS is_current

FROM EPIC.PATIENTS ep
;

-- =============================================================================
-- ENCOUNTER SUMMARY DYNAMIC TABLE
-- =============================================================================

-- Encounter Summary - Real-time encounter processing with business rules
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.ENCOUNTER_SUMMARY
TARGET_LAG = '2 minutes'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    -- Use stable natural key for incremental refresh compatibility
    ee.encounter_id AS encounter_key,
    
    -- Source identifiers
    ee.encounter_id,
    pb.patient_key,
    ee.patient_id,
    
    -- Encounter timing
    ee.encounter_date::DATE AS encounter_date,
    ee.encounter_date AS encounter_datetime,
    
    -- Encounter classification
    CASE 
        WHEN UPPER(ee.encounter_type) = 'INPATIENT' THEN 'Inpatient'
        WHEN UPPER(ee.encounter_type) = 'OUTPATIENT' THEN 'Outpatient'
        WHEN UPPER(ee.encounter_type) = 'EMERGENCY' THEN 'Emergency'
        WHEN UPPER(ee.encounter_type) = 'OBSERVATION' THEN 'Observation'
        ELSE COALESCE(ee.encounter_type, 'Unknown')
    END AS encounter_type,
    
    -- Derive encounter category
    CASE 
        WHEN UPPER(ee.encounter_type) = 'EMERGENCY' THEN 'Emergency'
        WHEN UPPER(ee.encounter_type) = 'INPATIENT' THEN 'Inpatient'
        WHEN UPPER(ee.encounter_type) = 'OBSERVATION' THEN 'Inpatient'
        ELSE 'Outpatient'
    END AS encounter_category,
    
    ee.department AS department_name,
    
    -- Derive service line from department
    CASE 
        WHEN UPPER(ee.department) LIKE '%EMERGENCY%' OR UPPER(ee.department) LIKE '%ED%' THEN 'Emergency Medicine'
        WHEN UPPER(ee.department) LIKE '%ICU%' OR UPPER(ee.department) LIKE '%INTENSIVE%' THEN 'Critical Care'
        WHEN UPPER(ee.department) LIKE '%NICU%' THEN 'Neonatology'
        WHEN UPPER(ee.department) LIKE '%CARDIO%' THEN 'Cardiovascular'
        WHEN UPPER(ee.department) LIKE '%NEURO%' THEN 'Neurosciences'
        WHEN UPPER(ee.department) LIKE '%ONCO%' OR UPPER(ee.department) LIKE '%CANCER%' THEN 'Oncology'
        WHEN UPPER(ee.department) LIKE '%ORTHO%' THEN 'Orthopedics'
        WHEN UPPER(ee.department) LIKE '%PULM%' OR UPPER(ee.department) LIKE '%RESPIRATORY%' THEN 'Pulmonary'
        WHEN UPPER(ee.department) LIKE '%GASTRO%' OR UPPER(ee.department) LIKE '%GI%' THEN 'Gastroenterology'
        WHEN UPPER(ee.department) LIKE '%ENDO%' THEN 'Endocrinology'
        WHEN UPPER(ee.department) LIKE '%GENERAL%' OR UPPER(ee.department) LIKE '%PEDIATRICS%' THEN 'General Pediatrics'
        ELSE 'Other'
    END AS service_line,
    
    ee.attending_physician AS attending_provider,
    
    -- Admission and discharge handling
    COALESCE(ee.admission_date, ee.encounter_date) AS admission_datetime,
    ee.discharge_date AS discharge_datetime,
    
    -- Calculate length of stay
    CASE 
        WHEN ee.discharge_date IS NOT NULL THEN 
            DATEDIFF('hour', COALESCE(ee.admission_date, ee.encounter_date), ee.discharge_date)
        ELSE NULL
    END AS length_of_stay_hours,
    
    CASE 
        WHEN ee.discharge_date IS NOT NULL THEN 
            GREATEST(1, DATEDIFF('day', COALESCE(ee.admission_date, ee.encounter_date)::DATE, ee.discharge_date::DATE))
        ELSE NULL
    END AS length_of_stay_days,
    
    -- Encounter status
    CASE 
        WHEN UPPER(ee.status) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(ee.status) = 'IN PROGRESS' THEN 'In Progress'
        WHEN UPPER(ee.status) = 'SCHEDULED' THEN 'Scheduled'
        WHEN UPPER(ee.status) = 'CANCELLED' THEN 'Cancelled'
        ELSE COALESCE(ee.status, 'Unknown')
    END AS encounter_status,
    
    ee.chief_complaint,
    
    -- Financial placeholder (would come from ERP integration) - Using deterministic hash for consistent charges
    CASE 
        WHEN UPPER(ee.encounter_type) = 'EMERGENCY' THEN ROUND((ABS(HASH(ee.encounter_id)) % 5000) + 1000, 2)
        WHEN UPPER(ee.encounter_type) = 'INPATIENT' THEN ROUND((ABS(HASH(ee.encounter_id)) % 25000) + 5000, 2)
        ELSE ROUND((ABS(HASH(ee.encounter_id)) % 2000) + 200, 2)
    END AS total_charges,
    
    pb.primary_insurance AS primary_payer,
    
    -- Derive financial class
    CASE 
        WHEN pb.primary_insurance = 'Medicaid' THEN 'Government'
        WHEN pb.primary_insurance = 'CHIP' THEN 'Government'
        WHEN pb.primary_insurance = 'Commercial' THEN 'Commercial'
        WHEN pb.primary_insurance = 'Self-pay' THEN 'Self Pay'
        ELSE 'Other'
    END AS financial_class,
    
    -- Calculate readmission flags (simplified for Dynamic Tables compatibility)
    CASE 
        WHEN readmission_data.days_since_last_admission IS NOT NULL 
        AND readmission_data.days_since_last_admission <= 30
        AND UPPER(ee.encounter_type) IN ('INPATIENT', 'EMERGENCY', 'OBSERVATION')
        THEN TRUE
        ELSE FALSE
    END AS readmission_flag,
    
    -- Days since last admission (simplified)
    readmission_data.days_since_last_admission AS readmission_days,
    
    -- Audit fields
    'EPIC' AS source_system,
    ee.created_date AS created_date,
    ee.updated_date AS updated_date

FROM EPIC.ENCOUNTERS ee

-- Left join to calculate readmission data using window functions (Dynamic Tables compatible)
LEFT JOIN (
    SELECT 
        encounter_id,
        patient_id,
        encounter_date,
        LAG(encounter_date) OVER (
            PARTITION BY patient_id 
            ORDER BY encounter_date
        ) AS prev_encounter_date,
        DATEDIFF('day', 
            LAG(encounter_date) OVER (
                PARTITION BY patient_id 
                ORDER BY encounter_date
            )::DATE, 
            encounter_date::DATE
        ) AS days_since_last_admission
    FROM EPIC.ENCOUNTERS 
    WHERE UPPER(encounter_type) IN ('INPATIENT', 'OBSERVATION')
) readmission_data ON ee.encounter_id = readmission_data.encounter_id

-- Join to patient master for demographics and insurance
INNER JOIN CONFORMED.PATIENT_BASE pb 
    ON ee.patient_id = pb.patient_id 
    AND pb.is_current = TRUE;

-- =============================================================================
-- DIAGNOSIS FACTS DYNAMIC TABLE
-- =============================================================================

-- Diagnosis Facts - Real-time diagnosis processing with pediatric categorization
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.DIAGNOSIS_FACT
TARGET_LAG = '10 minutes'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    -- Use stable natural key for incremental refresh compatibility
    ed.diagnosis_id AS diagnosis_key,
    
    -- Source identifiers
    ed.diagnosis_id,
    es.encounter_key,
    pb.patient_key,
    ed.patient_id,
    ed.encounter_id,
    
    -- Diagnosis details
    ed.diagnosis_code,
    ed.diagnosis_description,
    
    -- ICD-10 chapter categorization
    CASE 
        WHEN ed.diagnosis_code LIKE 'A%' OR ed.diagnosis_code LIKE 'B%' THEN 'Infectious and parasitic diseases'
        WHEN ed.diagnosis_code LIKE 'C%' OR ed.diagnosis_code LIKE 'D0%' OR ed.diagnosis_code LIKE 'D1%' OR ed.diagnosis_code LIKE 'D2%' OR ed.diagnosis_code LIKE 'D3%' OR ed.diagnosis_code LIKE 'D4%' THEN 'Neoplasms'
        WHEN ed.diagnosis_code LIKE 'D5%' OR ed.diagnosis_code LIKE 'D6%' OR ed.diagnosis_code LIKE 'D7%' OR ed.diagnosis_code LIKE 'D8%' THEN 'Blood and immune disorders'
        WHEN ed.diagnosis_code LIKE 'E%' THEN 'Endocrine, nutritional and metabolic diseases'
        WHEN ed.diagnosis_code LIKE 'F%' THEN 'Mental and behavioral disorders'
        WHEN ed.diagnosis_code LIKE 'G%' THEN 'Diseases of the nervous system'
        WHEN ed.diagnosis_code LIKE 'H0%' OR ed.diagnosis_code LIKE 'H1%' OR ed.diagnosis_code LIKE 'H2%' OR ed.diagnosis_code LIKE 'H3%' OR ed.diagnosis_code LIKE 'H4%' OR ed.diagnosis_code LIKE 'H5%' THEN 'Diseases of the eye'
        WHEN ed.diagnosis_code LIKE 'H6%' OR ed.diagnosis_code LIKE 'H7%' OR ed.diagnosis_code LIKE 'H8%' OR ed.diagnosis_code LIKE 'H9%' THEN 'Diseases of the ear'
        WHEN ed.diagnosis_code LIKE 'I%' THEN 'Diseases of the circulatory system'
        WHEN ed.diagnosis_code LIKE 'J%' THEN 'Diseases of the respiratory system'
        WHEN ed.diagnosis_code LIKE 'K%' THEN 'Diseases of the digestive system'
        WHEN ed.diagnosis_code LIKE 'L%' THEN 'Diseases of the skin'
        WHEN ed.diagnosis_code LIKE 'M%' THEN 'Diseases of the musculoskeletal system'
        WHEN ed.diagnosis_code LIKE 'N%' THEN 'Diseases of the genitourinary system'
        WHEN ed.diagnosis_code LIKE 'O%' THEN 'Pregnancy, childbirth and the puerperium'
        WHEN ed.diagnosis_code LIKE 'P%' THEN 'Perinatal conditions'
        WHEN ed.diagnosis_code LIKE 'Q%' THEN 'Congenital malformations'
        WHEN ed.diagnosis_code LIKE 'R%' THEN 'Symptoms, signs and abnormal findings'
        WHEN ed.diagnosis_code LIKE 'S%' OR ed.diagnosis_code LIKE 'T%' THEN 'Injury, poisoning and external causes'
        WHEN ed.diagnosis_code LIKE 'V%' OR ed.diagnosis_code LIKE 'W%' OR ed.diagnosis_code LIKE 'X%' OR ed.diagnosis_code LIKE 'Y%' THEN 'External causes of morbidity'
        WHEN ed.diagnosis_code LIKE 'Z%' THEN 'Factors influencing health status'
        ELSE 'Other'
    END AS diagnosis_category,
    
    -- Diagnosis type standardization
    CASE 
        WHEN UPPER(ed.diagnosis_type) = 'PRIMARY' THEN 'Primary'
        WHEN UPPER(ed.diagnosis_type) = 'SECONDARY' THEN 'Secondary'
        WHEN UPPER(ed.diagnosis_type) = 'ADMITTING' THEN 'Admitting'
        ELSE COALESCE(ed.diagnosis_type, 'Unknown')
    END AS diagnosis_type,
    
    ed.diagnosis_date,
    
    -- Chronic condition identification
    CASE 
        WHEN ed.diagnosis_code IN ('J45.9', 'F90.9', 'E10.9', 'E11.9', 'F84.0', 'G40.909', 'K21.9', 'E66.9') THEN TRUE
        ELSE FALSE
    END AS is_chronic_condition,
    
    -- Primary diagnosis flag
    CASE 
        WHEN UPPER(ed.diagnosis_type) = 'PRIMARY' THEN TRUE
        ELSE FALSE
    END AS is_primary_diagnosis,
    
    -- Age at diagnosis
    DATEDIFF('year', pb.date_of_birth, ed.diagnosis_date) AS age_at_diagnosis,
    
    -- Age group at diagnosis
    CASE 
        WHEN DATEDIFF('year', pb.date_of_birth, ed.diagnosis_date) = 0 THEN 'Newborn'
        WHEN DATEDIFF('year', pb.date_of_birth, ed.diagnosis_date) <= 1 THEN 'Infant'
        WHEN DATEDIFF('year', pb.date_of_birth, ed.diagnosis_date) <= 3 THEN 'Toddler'
        WHEN DATEDIFF('year', pb.date_of_birth, ed.diagnosis_date) <= 5 THEN 'Preschool'
        WHEN DATEDIFF('year', pb.date_of_birth, ed.diagnosis_date) <= 12 THEN 'School Age'
        WHEN DATEDIFF('year', pb.date_of_birth, ed.diagnosis_date) <= 18 THEN 'Adolescent'
        ELSE 'Young Adult'
    END AS age_group,
    
    -- Audit fields
    'EPIC' AS source_system,
    ed.created_date AS created_date,
    ed.updated_date AS updated_date

FROM EPIC.DIAGNOSES ed

-- Join to encounter summary
INNER JOIN CONFORMED.ENCOUNTER_SUMMARY es 
    ON ed.encounter_id = es.encounter_id

-- Join to patient master for age calculations
INNER JOIN CONFORMED.PATIENT_BASE pb 
    ON ed.patient_id = pb.patient_id 
    AND pb.is_current = TRUE;

-- =============================================================================
-- PATIENT MASTER VIEW (rollups over incremental-safe base)
-- =============================================================================

CREATE OR REPLACE VIEW CONFORMED.PATIENT_MASTER AS
SELECT 
  pb.patient_key,
  pb.patient_id,
  pb.mrn,
  pb.first_name,
  pb.last_name,
  pb.full_name,
  pb.date_of_birth,
  DATEDIFF('year', pb.date_of_birth, CURRENT_DATE()) AS current_age,
  pb.gender,
  pb.race,
  pb.ethnicity,
  pb.zip_code,
  pb.primary_insurance,
  pb.primary_language,
  -- chronic condition rollup from diagnosis fact
  COALESCE(cc.condition_count, 0) AS chronic_conditions_count,
  CASE 
    WHEN COALESCE(cc.condition_count,0) >= 3 THEN 'HIGH_RISK'
    WHEN COALESCE(cc.condition_count,0) >= 1 THEN 'MODERATE_RISK'
    ELSE 'LOW_RISK'
  END AS risk_category,
  -- encounter rollup from encounter summary
  es_roll.last_encounter_date,
  COALESCE(es_roll.total_encounters, 0) AS total_encounters,
  -- audit passthrough
  pb.source_system,
  pb.created_date,
  pb.updated_date,
  pb.effective_date,
  pb.expiration_date,
  pb.is_current
FROM CONFORMED.PATIENT_BASE pb
LEFT JOIN (
  SELECT patient_id,
         COUNT(*) AS condition_count
  FROM CONFORMED.DIAGNOSIS_FACT
  WHERE is_chronic_condition = TRUE
  GROUP BY patient_id
) cc ON pb.patient_id = cc.patient_id
LEFT JOIN (
  SELECT patient_id,
         MAX(encounter_date) AS last_encounter_date,
         COUNT(*) AS total_encounters
  FROM CONFORMED.ENCOUNTER_SUMMARY
  GROUP BY patient_id
) es_roll ON pb.patient_id = es_roll.patient_id;

-- =============================================================================
-- DATA QUALITY AND MONITORING
-- =============================================================================

-- Create view to monitor Dynamic Table refresh status
CREATE OR REPLACE VIEW CONFORMED.DYNAMIC_TABLE_MONITORING AS
SELECT 
    table_name,
    target_lag,
    data_timestamp,
    refresh_mode,
    last_successful_refresh,
    DATEDIFF('minute', last_successful_refresh, CURRENT_TIMESTAMP()) AS minutes_since_refresh,
    CASE 
        WHEN DATEDIFF('minute', last_successful_refresh, CURRENT_TIMESTAMP()) > 60 THEN 'WARNING'
        WHEN DATEDIFF('minute', last_successful_refresh, CURRENT_TIMESTAMP()) > 30 THEN 'CAUTION'
        ELSE 'OK'
    END AS refresh_status
FROM (
    SELECT 
        'PATIENT_BASE' AS table_name,
        '2 minutes' AS target_lag,
        (SELECT MAX(updated_date) FROM CONFORMED.PATIENT_BASE) AS data_timestamp,
        'AUTO' AS refresh_mode,
        CURRENT_TIMESTAMP() AS last_successful_refresh -- Placeholder for actual monitoring
    
    UNION ALL
    
    SELECT 
        'ENCOUNTER_SUMMARY' AS table_name,
        '2 minutes' AS target_lag,
        (SELECT MAX(updated_date) FROM CONFORMED.ENCOUNTER_SUMMARY) AS data_timestamp,
        'AUTO' AS refresh_mode,
        CURRENT_TIMESTAMP() AS last_successful_refresh
    
    UNION ALL
    
    SELECT 
        'DIAGNOSIS_FACT' AS table_name,
        '10 minutes' AS target_lag,
        (SELECT MAX(updated_date) FROM CONFORMED.DIAGNOSIS_FACT) AS data_timestamp,
        'AUTO' AS refresh_mode,
        CURRENT_TIMESTAMP() AS last_successful_refresh
);

-- Display completion message
SELECT 'Patient Dynamic Tables created successfully. Real-time data transformation is now active.' AS dynamic_tables_status;