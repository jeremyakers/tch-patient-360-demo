-- Texas Children's Hospital Patient 360 PoC - Clinical Dynamic Tables
-- Implements Dynamic Tables for real-time clinical data transformation

USE DATABASE TCH_PATIENT_360_POC;
USE WAREHOUSE TCH_COMPUTE_WH;

-- =============================================================================
-- DROP EXISTING TABLES TO AVOID CONFLICTS WITH DYNAMIC TABLES
-- =============================================================================

DROP TABLE IF EXISTS CONFORMED.LAB_RESULTS_FACT;
DROP TABLE IF EXISTS CONFORMED.MEDICATION_FACT;
DROP TABLE IF EXISTS CONFORMED.VITAL_SIGNS_FACT;
DROP TABLE IF EXISTS CONFORMED.IMAGING_STUDY_FACT;
DROP TABLE IF EXISTS CONFORMED.PROVIDER_DIM;
DROP TABLE IF EXISTS CONFORMED.DEPARTMENT_DIM;
DROP TABLE IF EXISTS CONFORMED.FINANCIAL_FACT;
DROP TABLE IF EXISTS CONFORMED.ENGAGEMENT_FACT;

-- =============================================================================
-- LAB RESULTS DYNAMIC TABLE
-- =============================================================================

-- Lab Results Facts - Real-time lab processing with age-adjusted interpretation
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.LAB_RESULTS_FACT
TARGET_LAG = '15 minutes'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    -- Use stable natural key for incremental refresh compatibility
    elr.lab_result_id AS lab_result_key,
    
    -- Source identifiers
    elr.lab_result_id,
    es.encounter_key,
    pb.patient_key,
    elr.patient_id,
    elr.encounter_id,
    
    -- Test details
    elr.test_name,
    
    -- Categorize lab tests
    CASE 
        WHEN UPPER(elr.test_name) LIKE '%HEMOGLOBIN%' OR UPPER(elr.test_name) LIKE '%HEMATOCRIT%' 
             OR UPPER(elr.test_name) LIKE '%WHITE BLOOD%' OR UPPER(elr.test_name) LIKE '%PLATELET%' 
             OR UPPER(elr.test_name) LIKE '%RBC%' OR UPPER(elr.test_name) LIKE '%WBC%' THEN 'Hematology'
        WHEN UPPER(elr.test_name) LIKE '%GLUCOSE%' OR UPPER(elr.test_name) LIKE '%SODIUM%' 
             OR UPPER(elr.test_name) LIKE '%POTASSIUM%' OR UPPER(elr.test_name) LIKE '%CREATININE%'
             OR UPPER(elr.test_name) LIKE '%BUN%' OR UPPER(elr.test_name) LIKE '%CHOLESTEROL%' THEN 'Chemistry'
        WHEN UPPER(elr.test_name) LIKE '%CULTURE%' OR UPPER(elr.test_name) LIKE '%BACTERIAL%' 
             OR UPPER(elr.test_name) LIKE '%VIRAL%' OR UPPER(elr.test_name) LIKE '%SENSITIVITY%' THEN 'Microbiology'
        WHEN UPPER(elr.test_name) LIKE '%THYROID%' OR UPPER(elr.test_name) LIKE '%TSH%' 
             OR UPPER(elr.test_name) LIKE '%T3%' OR UPPER(elr.test_name) LIKE '%T4%' 
             OR UPPER(elr.test_name) LIKE '%INSULIN%' OR UPPER(elr.test_name) LIKE '%A1C%' THEN 'Endocrinology'
        WHEN UPPER(elr.test_name) LIKE '%URINE%' OR UPPER(elr.test_name) LIKE '%URINALYSIS%' THEN 'Urinalysis'
        WHEN UPPER(elr.test_name) LIKE '%IMMUNOGLOBULIN%' OR UPPER(elr.test_name) LIKE '%IGG%' 
             OR UPPER(elr.test_name) LIKE '%IGM%' OR UPPER(elr.test_name) LIKE '%IGA%' THEN 'Immunology'
        ELSE 'Other'
    END AS test_category,
    
    -- Parse numeric values
    TRY_CAST(elr.test_value AS DECIMAL(18,6)) AS test_value_numeric,
    elr.test_value AS test_value_text,
    
    -- Parse reference ranges
    TRY_CAST(SPLIT_PART(elr.reference_range, '-', 1) AS DECIMAL(18,6)) AS reference_range_min,
    TRY_CAST(SPLIT_PART(elr.reference_range, '-', 2) AS DECIMAL(18,6)) AS reference_range_max,
    elr.reference_range AS reference_range_text,
    
    -- Abnormal flags
    CASE 
        WHEN UPPER(elr.abnormal_flag) = 'H' THEN 'H'
        WHEN UPPER(elr.abnormal_flag) = 'L' THEN 'L'
        WHEN UPPER(elr.abnormal_flag) = 'LL' THEN 'LL'
        WHEN UPPER(elr.abnormal_flag) = 'HH' THEN 'HH'
        ELSE ''
    END AS abnormal_flag,
    
    -- Critical value flags
    CASE 
        WHEN UPPER(elr.abnormal_flag) IN ('LL', 'HH') THEN TRUE
        WHEN UPPER(elr.test_name) LIKE '%GLUCOSE%' AND TRY_CAST(elr.test_value AS DECIMAL) < 50 THEN TRUE
        WHEN UPPER(elr.test_name) LIKE '%GLUCOSE%' AND TRY_CAST(elr.test_value AS DECIMAL) > 400 THEN TRUE
        WHEN UPPER(elr.test_name) LIKE '%HEMOGLOBIN%' AND TRY_CAST(elr.test_value AS DECIMAL) < 7 THEN TRUE
        WHEN UPPER(elr.test_name) LIKE '%PLATELET%' AND TRY_CAST(elr.test_value AS DECIMAL) < 50000 THEN TRUE
        ELSE FALSE
    END AS critical_flag,
    
    elr.result_date::DATE AS result_date,
    elr.result_date AS result_datetime,
    elr.ordering_provider,
    
    -- Age at test
    DATEDIFF('year', pb.date_of_birth, elr.result_date) AS age_at_test,
    
    -- Age group at test
    CASE 
        WHEN DATEDIFF('year', pb.date_of_birth, elr.result_date) = 0 THEN 'Newborn'
        WHEN DATEDIFF('year', pb.date_of_birth, elr.result_date) <= 1 THEN 'Infant'
        WHEN DATEDIFF('year', pb.date_of_birth, elr.result_date) <= 3 THEN 'Toddler'
        WHEN DATEDIFF('year', pb.date_of_birth, elr.result_date) <= 5 THEN 'Preschool'
        WHEN DATEDIFF('year', pb.date_of_birth, elr.result_date) <= 12 THEN 'School Age'
        WHEN DATEDIFF('year', pb.date_of_birth, elr.result_date) <= 18 THEN 'Adolescent'
        ELSE 'Young Adult'
    END AS age_group_at_test,
    
    -- Age-appropriate normal flag (simplified logic)
    CASE 
        WHEN elr.abnormal_flag = '' OR elr.abnormal_flag IS NULL THEN TRUE
        ELSE FALSE
    END AS normal_for_age,
    
    -- Audit fields
    'EPIC' AS source_system,
    elr.created_date AS created_date,
    elr.updated_date AS updated_date

FROM EPIC.LAB_RESULTS elr

-- Join to encounter summary (optional - some labs may be outreach)
INNER JOIN CONFORMED.ENCOUNTER_SUMMARY es 
    ON elr.encounter_id = es.encounter_id

-- Join to patient master
INNER JOIN CONFORMED.PATIENT_BASE pb 
    ON elr.patient_id = pb.patient_id 
    AND pb.is_current = TRUE;

-- =============================================================================
-- MEDICATION FACTS DYNAMIC TABLE
-- =============================================================================

-- Medication Facts - Real-time medication processing with pediatric safety considerations
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.MEDICATION_FACT
TARGET_LAG = '10 minutes'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    -- Use stable natural key for incremental refresh compatibility
    em.medication_id AS medication_key,
    
    -- Source identifiers
    em.medication_id,
    es.encounter_key,
    pb.patient_key,
    em.patient_id,
    em.encounter_id,
    
    -- Medication details
    em.medication_name,
    
    -- Generic name mapping (simplified for demo)
    CASE 
        WHEN UPPER(em.medication_name) LIKE '%ACETAMINOPHEN%' OR UPPER(em.medication_name) LIKE '%TYLENOL%' THEN 'Acetaminophen'
        WHEN UPPER(em.medication_name) LIKE '%IBUPROFEN%' OR UPPER(em.medication_name) LIKE '%ADVIL%' OR UPPER(em.medication_name) LIKE '%MOTRIN%' THEN 'Ibuprofen'
        WHEN UPPER(em.medication_name) LIKE '%ALBUTEROL%' THEN 'Albuterol'
        WHEN UPPER(em.medication_name) LIKE '%AMOXICILLIN%' THEN 'Amoxicillin'
        WHEN UPPER(em.medication_name) LIKE '%METHYLPHENIDATE%' OR UPPER(em.medication_name) LIKE '%RITALIN%' THEN 'Methylphenidate'
        WHEN UPPER(em.medication_name) LIKE '%INSULIN%' THEN 'Insulin'
        ELSE em.medication_name
    END AS generic_name,
    
    -- Medication class
    CASE 
        WHEN UPPER(em.medication_name) LIKE '%ACETAMINOPHEN%' OR UPPER(em.medication_name) LIKE '%IBUPROFEN%' THEN 'Analgesic/Antipyretic'
        WHEN UPPER(em.medication_name) LIKE '%ALBUTEROL%' OR UPPER(em.medication_name) LIKE '%FLUTICASONE%' THEN 'Respiratory'
        WHEN UPPER(em.medication_name) LIKE '%AMOXICILLIN%' OR UPPER(em.medication_name) LIKE '%AZITHROMYCIN%' THEN 'Antibiotic'
        WHEN UPPER(em.medication_name) LIKE '%METHYLPHENIDATE%' THEN 'CNS Stimulant'
        WHEN UPPER(em.medication_name) LIKE '%INSULIN%' THEN 'Antidiabetic'
        WHEN UPPER(em.medication_name) LIKE '%PREDNISONE%' OR UPPER(em.medication_name) LIKE '%HYDROCORTISONE%' THEN 'Corticosteroid'
        WHEN UPPER(em.medication_name) LIKE '%OMEPRAZOLE%' OR UPPER(em.medication_name) LIKE '%RANITIDINE%' THEN 'GI Agent'
        WHEN UPPER(em.medication_name) LIKE '%CETIRIZINE%' OR UPPER(em.medication_name) LIKE '%DIPHENHYDRAMINE%' THEN 'Antihistamine'
        ELSE 'Other'
    END AS medication_class,
    
    -- Therapeutic category
    CASE 
        WHEN UPPER(em.medication_name) LIKE '%ACETAMINOPHEN%' OR UPPER(em.medication_name) LIKE '%IBUPROFEN%' THEN 'Pain/Fever Management'
        WHEN UPPER(em.medication_name) LIKE '%ALBUTEROL%' OR UPPER(em.medication_name) LIKE '%FLUTICASONE%' THEN 'Asthma/Respiratory'
        WHEN UPPER(em.medication_name) LIKE '%AMOXICILLIN%' OR UPPER(em.medication_name) LIKE '%AZITHROMYCIN%' THEN 'Infection Treatment'
        WHEN UPPER(em.medication_name) LIKE '%METHYLPHENIDATE%' THEN 'ADHD Treatment'
        WHEN UPPER(em.medication_name) LIKE '%INSULIN%' THEN 'Diabetes Management'
        WHEN UPPER(em.medication_name) LIKE '%PREDNISONE%' THEN 'Anti-inflammatory'
        WHEN UPPER(em.medication_name) LIKE '%OMEPRAZOLE%' THEN 'GERD Treatment'
        WHEN UPPER(em.medication_name) LIKE '%CETIRIZINE%' THEN 'Allergy Treatment'
        ELSE 'Other'
    END AS therapeutic_category,
    
    em.dosage,
    
    -- Extract numeric dosage (simplified)
    TRY_CAST(REGEXP_SUBSTR(em.dosage, '[0-9]+\\.?[0-9]*') AS DECIMAL(10,4)) AS dosage_numeric,
    
    -- Extract dosage unit
    CASE 
        WHEN UPPER(em.dosage) LIKE '%MG%' THEN 'mg'
        WHEN UPPER(em.dosage) LIKE '%ML%' THEN 'mL'
        WHEN UPPER(em.dosage) LIKE '%UNITS%' THEN 'units'
        WHEN UPPER(em.dosage) LIKE '%PUFFS%' THEN 'puffs'
        WHEN UPPER(em.dosage) LIKE '%TABLETS%' THEN 'tablets'
        ELSE 'unknown'
    END AS dosage_unit,
    
    em.frequency,
    em.route,
    em.start_date,
    em.end_date,
    
    -- Calculate duration
    CASE 
        WHEN em.end_date IS NOT NULL THEN DATEDIFF('day', em.start_date, em.end_date)
        ELSE NULL
    END AS duration_days,
    
    em.prescribing_provider,
    
    -- Age at prescription
    DATEDIFF('year', pb.date_of_birth, em.start_date) AS age_at_prescription,
    
    -- Weight-based dosing flag (pediatric consideration)
    CASE 
        WHEN UPPER(em.dosage) LIKE '%MG/KG%' OR UPPER(em.dosage) LIKE '%UNITS/KG%' THEN TRUE
        WHEN DATEDIFF('year', pb.date_of_birth, em.start_date) < 12 AND 
             UPPER(em.medication_name) IN ('ACETAMINOPHEN', 'IBUPROFEN', 'AMOXICILLIN') THEN TRUE
        ELSE FALSE
    END AS weight_based_dosing,
    
    -- Pediatric approved flag (simplified - assume most common peds meds are approved)
    CASE 
        WHEN UPPER(em.medication_name) IN (
            'ACETAMINOPHEN', 'IBUPROFEN', 'ALBUTEROL', 'AMOXICILLIN', 
            'AZITHROMYCIN', 'FLUTICASONE', 'INSULIN', 'CETIRIZINE'
        ) THEN TRUE
        ELSE TRUE  -- Assume approved for demo
    END AS pediatric_approved,
    
    -- High risk medication flag
    CASE 
        WHEN UPPER(em.medication_name) LIKE '%INSULIN%' THEN TRUE
        WHEN UPPER(em.medication_name) LIKE '%WARFARIN%' THEN TRUE
        WHEN UPPER(em.medication_name) LIKE '%DIGOXIN%' THEN TRUE
        WHEN UPPER(em.medication_name) LIKE '%PHENYTOIN%' THEN TRUE
        ELSE FALSE
    END AS high_risk_medication,
    
    -- Allergy alert placeholder (would come from allergy data)
    FALSE AS allergy_alert,
    
    -- Audit fields
    'EPIC' AS source_system,
    em.created_date AS created_date,
    em.updated_date AS updated_date

FROM EPIC.MEDICATIONS em

-- Join to encounter summary
INNER JOIN CONFORMED.ENCOUNTER_SUMMARY es 
    ON em.encounter_id = es.encounter_id

-- Join to patient master
INNER JOIN CONFORMED.PATIENT_BASE pb 
    ON em.patient_id = pb.patient_id 
    AND pb.is_current = TRUE;

-- =============================================================================
-- VITAL SIGNS DYNAMIC TABLE
-- =============================================================================

-- Vital Signs Facts - Real-time vital signs with pediatric percentiles and alerts
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.VITAL_SIGNS_FACT
TARGET_LAG = '5 minutes'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    -- Use stable natural key for incremental refresh compatibility
    evs.vital_sign_id AS vital_signs_key,
    
    -- Source identifiers
    evs.vital_sign_id,
    es.encounter_key,
    pb.patient_key,
    evs.patient_id,
    evs.encounter_id,
    
    -- Timing
    evs.recorded_date AS measurement_datetime,
    evs.recorded_date::DATE AS measurement_date,
    
    -- Temperature
    evs.temperature AS temperature_celsius,
    ROUND(evs.temperature * 9/5 + 32, 1) AS temperature_fahrenheit,
    
    -- Cardiovascular
    evs.heart_rate,
    evs.respiratory_rate,
    evs.blood_pressure_systolic,
    evs.blood_pressure_diastolic,
    evs.oxygen_saturation,
    
    -- Growth measurements
    evs.weight_kg,
    ROUND(evs.weight_kg * 2.20462, 2) AS weight_pounds,
    evs.height_cm,
    ROUND(evs.height_cm / 2.54, 1) AS height_inches,
    
    -- Calculate BMI
    CASE 
        WHEN evs.weight_kg > 0 AND evs.height_cm > 0 
        THEN ROUND(evs.weight_kg / POWER(evs.height_cm / 100, 2), 2)
        ELSE NULL 
    END AS bmi,
    
    evs.recorded_by,
    
    -- Age at measurement
    DATEDIFF('year', pb.date_of_birth, evs.recorded_date) AS age_at_measurement,
    
    -- Age group at measurement
    CASE 
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) = 0 THEN 'Newborn'
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) <= 1 THEN 'Infant'
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) <= 3 THEN 'Toddler'
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) <= 5 THEN 'Preschool'
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) <= 12 THEN 'School Age'
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) <= 18 THEN 'Adolescent'
        ELSE 'Young Adult'
    END AS age_group_at_measurement,
    
    -- Growth percentiles (simplified calculation for demo)
    CASE 
        WHEN evs.weight_kg IS NULL THEN NULL
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) = 0 THEN 
            CASE WHEN evs.weight_kg < 2.5 THEN 10
                 WHEN evs.weight_kg < 3.0 THEN 25
                 WHEN evs.weight_kg < 3.5 THEN 50
                 WHEN evs.weight_kg < 4.0 THEN 75
                 ELSE 90 END
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) <= 2 THEN 
            CASE WHEN evs.weight_kg < 8 THEN 10
                 WHEN evs.weight_kg < 10 THEN 25
                 WHEN evs.weight_kg < 12 THEN 50
                 WHEN evs.weight_kg < 14 THEN 75
                 ELSE 90 END
        ELSE 50  -- Placeholder for older children
    END AS percentile_weight,
    
    -- Height percentiles (simplified)
    CASE 
        WHEN evs.height_cm IS NULL THEN NULL
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) = 0 THEN 
            CASE WHEN evs.height_cm < 47 THEN 10
                 WHEN evs.height_cm < 50 THEN 25
                 WHEN evs.height_cm < 52 THEN 50
                 WHEN evs.height_cm < 54 THEN 75
                 ELSE 90 END
        ELSE 50  -- Placeholder
    END AS percentile_height,
    
    -- BMI percentiles (simplified)
    CASE 
        WHEN evs.weight_kg IS NULL OR evs.height_cm IS NULL THEN NULL
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) >= 2 THEN
            CASE WHEN (evs.weight_kg / POWER(evs.height_cm / 100, 2)) < 15 THEN 5
                 WHEN (evs.weight_kg / POWER(evs.height_cm / 100, 2)) < 16 THEN 15
                 WHEN (evs.weight_kg / POWER(evs.height_cm / 100, 2)) < 18 THEN 50
                 WHEN (evs.weight_kg / POWER(evs.height_cm / 100, 2)) < 20 THEN 75
                 WHEN (evs.weight_kg / POWER(evs.height_cm / 100, 2)) < 25 THEN 85
                 ELSE 95 END
        ELSE NULL
    END AS percentile_bmi,
    
    -- Growth velocity flag (simplified)
    CASE 
        WHEN LAG(evs.weight_kg) OVER (PARTITION BY evs.patient_id ORDER BY evs.recorded_date) IS NULL THEN 'Unknown'
        WHEN evs.weight_kg > LAG(evs.weight_kg) OVER (PARTITION BY evs.patient_id ORDER BY evs.recorded_date) THEN 'Normal'
        WHEN evs.weight_kg = LAG(evs.weight_kg) OVER (PARTITION BY evs.patient_id ORDER BY evs.recorded_date) THEN 'Stable'
        ELSE 'Concerning'
    END AS growth_velocity_flag,
    
    -- Clinical alerts
    CASE WHEN evs.temperature >= 38.0 THEN TRUE ELSE FALSE END AS fever_flag,
    
    -- Age-based tachycardia (simplified)
    CASE 
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) = 0 AND evs.heart_rate > 160 THEN TRUE
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) <= 2 AND evs.heart_rate > 150 THEN TRUE
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) <= 12 AND evs.heart_rate > 120 THEN TRUE
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) > 12 AND evs.heart_rate > 100 THEN TRUE
        ELSE FALSE
    END AS tachycardia_flag,
    
    -- Blood pressure alerts (simplified)
    CASE 
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) >= 3 AND evs.blood_pressure_systolic > 110 THEN TRUE
        WHEN DATEDIFF('year', pb.date_of_birth, evs.recorded_date) >= 13 AND evs.blood_pressure_systolic > 120 THEN TRUE
        ELSE FALSE
    END AS hypertension_flag,
    
    -- Audit fields
    'EPIC' AS source_system,
    evs.created_date AS created_date,
    evs.updated_date AS updated_date

FROM EPIC.VITAL_SIGNS evs

-- Join to encounter summary
INNER JOIN CONFORMED.ENCOUNTER_SUMMARY es 
    ON evs.encounter_id = es.encounter_id

-- Join to patient master
INNER JOIN CONFORMED.PATIENT_BASE pb 
    ON evs.patient_id = pb.patient_id 
    AND pb.is_current = TRUE;

-- =============================================================================
-- IMAGING STUDIES DYNAMIC TABLE
-- =============================================================================

-- Imaging Study Facts - Real-time imaging with pediatric considerations
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.IMAGING_STUDY_FACT
TARGET_LAG = '30 minutes'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    -- Use stable natural key for incremental refresh compatibility
    eis.imaging_study_id AS imaging_study_key,
    
    -- Source identifiers
    eis.imaging_study_id,
    es.encounter_key,
    pb.patient_key,
    eis.patient_id,
    eis.encounter_id,
    
    -- Study details
    eis.study_type,
    eis.study_name,
    eis.modality,
    eis.body_part,
    eis.study_date::DATE AS study_date,
    eis.study_date AS study_datetime,
    eis.ordering_provider,
    eis.performing_department,
    
    -- Radiologist assignment (would come from scheduling system)
    'Dr. ' || 
    (ARRAY_CONSTRUCT('Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis'))[
        (HASH(eis.imaging_study_id) % 8) + 1
    ] || ', MD' AS radiologist,
    
    -- Study status
    CASE 
        WHEN UPPER(eis.study_status) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(eis.study_status) = 'PRELIMINARY' THEN 'Preliminary'
        WHEN UPPER(eis.study_status) = 'FINAL' THEN 'Final'
        WHEN UPPER(eis.study_status) = 'CANCELLED' THEN 'Cancelled'
        ELSE COALESCE(eis.study_status, 'Scheduled')
    END AS study_status,
    
    -- Report status (derived from study status)
    CASE 
        WHEN UPPER(eis.study_status) = 'COMPLETED' THEN 'Final'
        WHEN UPPER(eis.study_status) = 'PRELIMINARY' THEN 'Preliminary'
        WHEN UPPER(eis.study_status) = 'FINAL' THEN 'Final'
        WHEN UPPER(eis.study_status) = 'CANCELLED' THEN 'Cancelled'
        ELSE 'Pending'
    END AS report_status,
    
    -- Clinical indication (simplified)
    CASE 
        WHEN UPPER(eis.study_type) LIKE '%CHEST%' THEN 'Respiratory symptoms'
        WHEN UPPER(eis.study_type) LIKE '%BRAIN%' THEN 'Neurological evaluation'
        WHEN UPPER(eis.study_type) LIKE '%ABDOM%' THEN 'Abdominal pain'
        WHEN UPPER(eis.study_type) LIKE '%ECHO%' THEN 'Cardiac evaluation'
        ELSE 'Clinical evaluation'
    END AS clinical_indication,
    
    -- Contrast usage (simplified estimation) - Using deterministic hash
    CASE 
        WHEN UPPER(eis.study_type) LIKE '%CT%' AND (ABS(HASH(eis.imaging_study_id)) % 100) < 30 THEN TRUE
        WHEN UPPER(eis.study_type) LIKE '%MR%' AND (ABS(HASH(eis.imaging_study_id)) % 100) < 40 THEN TRUE
        ELSE FALSE
    END AS contrast_used,
    
    -- Radiation dose estimation (simplified) - Using deterministic hash
    CASE 
        WHEN UPPER(eis.modality) = 'XR' THEN ROUND((ABS(HASH(eis.imaging_study_id)) % 100) * 0.001 + 0.01, 3)  -- mSv
        WHEN UPPER(eis.modality) = 'CT' THEN ROUND((ABS(HASH(eis.imaging_study_id)) % 500) * 0.01 + 1, 2)        -- mSv  
        ELSE NULL  -- MR, US have no ionizing radiation
    END AS radiation_dose,
    
    -- Study completion flag
    CASE 
        WHEN UPPER(eis.study_status) IN ('COMPLETED', 'FINAL', 'PRELIMINARY') THEN TRUE
        ELSE FALSE
    END AS study_completion_flag,
    
    -- Turnaround time calculation (simplified) - Using deterministic hash
    ROUND((ABS(HASH(eis.imaging_study_id)) % 4800) * 0.01 + 2, 2) AS turnaround_time_hours,
    
    -- Age at study
    DATEDIFF('year', pb.date_of_birth, eis.study_date) AS age_at_study,
    
    -- Pediatric considerations
    CASE 
        WHEN DATEDIFF('year', pb.date_of_birth, eis.study_date) < 3 THEN TRUE
        WHEN UPPER(eis.study_type) LIKE '%MR%' AND DATEDIFF('year', pb.date_of_birth, eis.study_date) < 8 THEN TRUE
        ELSE FALSE
    END AS sedation_required,
    
    -- Child life involvement (estimated)
    CASE 
        WHEN DATEDIFF('year', pb.date_of_birth, eis.study_date) BETWEEN 2 AND 12 THEN TRUE
        ELSE FALSE
    END AS child_life_involved,
    
    -- Audit fields
    'EPIC' AS source_system,
    eis.created_date AS created_date,
    eis.updated_date AS updated_date

FROM EPIC.IMAGING_STUDIES eis

-- Join to encounter summary
INNER JOIN CONFORMED.ENCOUNTER_SUMMARY es 
    ON eis.encounter_id = es.encounter_id

-- Join to patient master
INNER JOIN CONFORMED.PATIENT_BASE pb 
    ON eis.patient_id = pb.patient_id 
    AND pb.is_current = TRUE;

-- =============================================================================
-- PROVIDER AND DEPARTMENT DIMENSIONS
-- =============================================================================

-- Provider Dimension - Real-time provider data
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.PROVIDER_DIM
TARGET_LAG = '1 hour'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    -- Use stable natural key for incremental refresh compatibility
    wp.provider_id AS provider_key,
    
    -- Source identifiers
    wp.provider_id,
    wp.npi,
    
    -- Name components
    CONCAT(COALESCE(wp.first_name, ''), ' ', COALESCE(wp.last_name, '')) AS full_name,
    wp.first_name,
    wp.last_name,
    
    -- Specialty information
    wp.specialty,
    
    -- Derive subspecialty (simplified)
    CASE 
        WHEN UPPER(wp.specialty) LIKE '%PEDIATRIC%' THEN 'Pediatric'
        WHEN UPPER(wp.specialty) LIKE '%NEONATAL%' THEN 'Neonatal'
        WHEN UPPER(wp.specialty) LIKE '%ADOLESCENT%' THEN 'Adolescent'
        ELSE NULL
    END AS subspecialty,
    
    wp.department,
    
    -- Map to service lines
    CASE 
        WHEN UPPER(wp.department) LIKE '%EMERGENCY%' THEN 'Emergency Medicine'
        WHEN UPPER(wp.department) LIKE '%ICU%' OR UPPER(wp.department) LIKE '%INTENSIVE%' THEN 'Critical Care'
        WHEN UPPER(wp.department) LIKE '%NICU%' THEN 'Neonatology'
        WHEN UPPER(wp.department) LIKE '%CARDIO%' THEN 'Cardiovascular'
        WHEN UPPER(wp.department) LIKE '%NEURO%' THEN 'Neurosciences'
        WHEN UPPER(wp.department) LIKE '%ONCO%' THEN 'Oncology'
        WHEN UPPER(wp.department) LIKE '%ORTHO%' THEN 'Orthopedics'
        WHEN UPPER(wp.department) LIKE '%GENERAL%' THEN 'General Pediatrics'
        ELSE 'Other'
    END AS service_line,
    
    wp.credentials,
    
    -- Provider type
    CASE 
        WHEN UPPER(wp.credentials) LIKE '%MD%' THEN 'Physician'
        WHEN UPPER(wp.credentials) LIKE '%DO%' THEN 'Physician'
        WHEN UPPER(wp.credentials) LIKE '%NP%' THEN 'Nurse Practitioner'
        WHEN UPPER(wp.credentials) LIKE '%PA%' THEN 'Physician Assistant'
        WHEN UPPER(wp.credentials) LIKE '%RN%' THEN 'Registered Nurse'
        ELSE 'Other'
    END AS provider_type,
    
    -- Employment status
    CASE 
        WHEN UPPER(wp.status) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(wp.status) = 'INACTIVE' THEN 'Inactive'
        ELSE COALESCE(wp.status, 'Unknown')
    END AS employment_status,
    
    wp.hire_date,
    
    -- Years of experience will be derived in presentation to keep DT incremental-friendly
    
    -- Board certifications (placeholder)
    ARRAY_CONSTRUCT(wp.specialty) AS board_certifications,
    
    -- Pediatric certification (assume true for this demo)
    TRUE AS pediatric_certified,
    
    -- Practice patterns (estimated) - Using deterministic hash
    ROUND((ABS(HASH(wp.provider_id)) % 150) * 0.1 + 5, 1) AS average_daily_patients,
    ARRAY_CONSTRUCT(wp.specialty) AS specialty_focus,
    
    -- Audit fields
    'WORKDAY' AS source_system,
    wp.created_date AS created_date,
    wp.updated_date AS updated_date,
    COALESCE(wp.created_date::DATE, DATE '1900-01-01') AS effective_date,
    DATE '9999-12-31' AS expiration_date,
    TRUE AS is_current

FROM WORKDAY.PROVIDERS wp;

-- Department Dimension - Real-time department data
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.DEPARTMENT_DIM
TARGET_LAG = '1 hour'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    -- Use stable natural key for incremental refresh compatibility
    wd.department_id AS department_key,
    
    -- Source identifiers
    wd.department_id,
    wd.department_name,
    wd.department_code,
    wd.service_line,
    
    -- Specialty classification
    CASE 
        WHEN UPPER(wd.service_line) LIKE '%EMERGENCY%' THEN 'Emergency Medicine'
        WHEN UPPER(wd.service_line) LIKE '%CRITICAL%' THEN 'Critical Care'
        WHEN UPPER(wd.service_line) LIKE '%PRIMARY%' THEN 'Primary Care'
        WHEN UPPER(wd.service_line) LIKE '%SPECIALTY%' THEN 'Specialty'
        WHEN UPPER(wd.service_line) LIKE '%ANCILLARY%' THEN 'Ancillary'
        ELSE 'Other'
    END AS specialty_type,
    
    wd.location,
    
    -- Derive campus
    CASE 
        WHEN UPPER(wd.location) LIKE '%MAIN%' THEN 'Main Campus'
        WHEN UPPER(wd.location) LIKE '%WEST%' THEN 'West Campus'
        WHEN UPPER(wd.location) LIKE '%WOODLANDS%' THEN 'The Woodlands'
        ELSE 'Main Campus'
    END AS campus,
    
    -- Floor (estimated) - Using deterministic hash
    CASE 
        WHEN UPPER(wd.department_name) LIKE '%ICU%' THEN ROUND((ABS(HASH(wd.department_id)) % 3) + 3)::VARCHAR
        WHEN UPPER(wd.department_name) LIKE '%ED%' THEN '1'
        ELSE ROUND((ABS(HASH(wd.department_id)) % 8) + 1)::VARCHAR
    END AS floor_number,
    
    -- Unit type
    CASE 
        WHEN UPPER(wd.department_name) LIKE '%ICU%' OR UPPER(wd.department_name) LIKE '%NICU%' THEN 'ICU'
        WHEN UPPER(wd.department_name) LIKE '%ED%' OR UPPER(wd.department_name) LIKE '%EMERGENCY%' THEN 'ED'
        WHEN UPPER(wd.service_line) = 'Ancillary' THEN 'Ancillary'
        ELSE 'General'
    END AS unit_type,
    
    -- Bed count (estimated) - Using deterministic hash
    CASE 
        WHEN UPPER(wd.department_name) LIKE '%ICU%' THEN ROUND((ABS(HASH(wd.department_id)) % 20) + 10)
        WHEN UPPER(wd.department_name) LIKE '%NICU%' THEN ROUND((ABS(HASH(wd.department_id)) % 30) + 20)
        WHEN UPPER(wd.department_name) LIKE '%ED%' THEN ROUND((ABS(HASH(wd.department_id)) % 15) + 25)
        WHEN UPPER(wd.service_line) = 'Ancillary' THEN 0
        ELSE ROUND((ABS(HASH(wd.department_id)) % 25) + 15)
    END AS bed_count,
    
    -- Operational status
    CASE 
        WHEN UPPER(wd.status) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(wd.status) = 'INACTIVE' THEN 'Inactive'
        ELSE COALESCE(wd.status, 'Active')
    END AS operational_status,
    
    -- Quality metrics (simulated) - Using deterministic hash
    ROUND((ABS(HASH(wd.department_id)) % 100) * 0.01 + 4, 2) AS patient_satisfaction_score,  -- 4.0-5.0 scale
    ROUND((ABS(HASH(CONCAT(wd.department_id, '_safety'))) % 100) * 0.01 + 4, 2) AS safety_score,  -- 4.0-5.0 scale
    
    -- Audit fields
    'WORKDAY' AS source_system,
    wd.created_date AS created_date,
    wd.updated_date AS updated_date,
    COALESCE(wd.created_date::DATE, DATE '1900-01-01') AS effective_date,
    DATE '9999-12-31' AS expiration_date,
    TRUE AS is_current

FROM WORKDAY.DEPARTMENTS wd;

-- Display completion message
SELECT 'Clinical Dynamic Tables created successfully. Real-time clinical data transformation is now active.' AS clinical_dynamic_tables_status;

-- =============================================================================
-- ORACLE ERP FINANCIAL FACT (Dynamic Table)
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE CONFORMED.FINANCIAL_FACT
TARGET_LAG = '30 minutes'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT 
    oef.financial_id AS financial_key,
    es.encounter_key,
    pb.patient_key,
    oef.financial_id,
    oef.total_charges,
    oef.insurance_payments,
    oef.patient_payments,
    oef.adjustments,
    oef.balance,
    oef.payer_name,
    oef.financial_class,
    CASE WHEN es.length_of_stay_days > 0 THEN oef.total_charges / es.length_of_stay_days ELSE oef.total_charges END AS cost_per_day,
    CASE WHEN oef.total_charges > 50000 THEN 'HIGH_COST'
         WHEN oef.total_charges > 10000 THEN 'MODERATE_COST'
         ELSE 'STANDARD_COST' END AS cost_category,
    CASE WHEN LOWER(oef.financial_class) IN ('medicaid','chip') THEN 'PUBLIC' ELSE 'COMMERCIAL' END AS payer_mix_category,
    'ORACLE_ERP' AS source_system,
    oef.created_date AS created_date,
    oef.updated_date AS updated_date
FROM ORACLE_ERP.ENCOUNTER_FINANCIALS oef
JOIN CONFORMED.ENCOUNTER_SUMMARY es ON oef.encounter_id = es.encounter_id
JOIN CONFORMED.PATIENT_BASE pb ON es.patient_id = pb.patient_id AND pb.is_current = TRUE
WHERE oef.total_charges > 0;

-- =============================================================================
-- SALESFORCE ENGAGEMENT FACT (Dynamic Table)
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE CONFORMED.ENGAGEMENT_FACT
TARGET_LAG = '1 hour'
WAREHOUSE = TCH_COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
WITH last_activity AS (
    SELECT spa.patient_id, MAX(spa.activity_date) AS last_activity_date
    FROM SALESFORCE.PATIENT_PORTAL_ACTIVITY spa
    GROUP BY spa.patient_id
),
activity_counts AS (
    SELECT spa.patient_id,
           COUNT(*) AS total_activities,
           COUNT(CASE WHEN spa.activity_type = 'Portal Login' THEN 1 END) AS portal_logins,
           COUNT(CASE WHEN spa.activity_type = 'Message Sent' THEN 1 END) AS messages_sent
    FROM SALESFORCE.PATIENT_PORTAL_ACTIVITY spa
    GROUP BY spa.patient_id
),
last30 AS (
    SELECT spa.patient_id,
           COUNT(*) AS activities_last_30_days
    FROM SALESFORCE.PATIENT_PORTAL_ACTIVITY spa
    JOIN last_activity la ON la.patient_id = spa.patient_id
    WHERE spa.activity_date >= DATEADD('day', -30, la.last_activity_date)
    GROUP BY spa.patient_id
)
SELECT 
    pb.patient_key AS engagement_key,
    pb.patient_key,
    pb.patient_id,
    DATEDIFF('year', pb.date_of_birth, la.last_activity_date) AS age_at_last_activity,
    CASE WHEN DATEDIFF('year', pb.date_of_birth, la.last_activity_date) <= 12 THEN
        CASE WHEN COALESCE(l30.activities_last_30_days, 0) >= 8 THEN 'HIGH'
             WHEN COALESCE(l30.activities_last_30_days, 0) >= 3 THEN 'MEDIUM'
             ELSE 'LOW' END
    ELSE
        CASE WHEN COALESCE(l30.activities_last_30_days, 0) >= 4 THEN 'HIGH'
             WHEN COALESCE(l30.activities_last_30_days, 0) >= 1 THEN 'MEDIUM'
             ELSE 'LOW' END
    END AS digital_health_adoption_level,
    LEAST(100, COALESCE(ac.portal_logins,0) * 10 + COALESCE(ac.messages_sent,0) * 15) AS engagement_score,
    COALESCE(l30.activities_last_30_days, 0) AS portal_logins_last_30_days,
    COALESCE(ac.total_activities, 0) AS total_activities,
    la.last_activity_date,
    'SALESFORCE' AS source_system,
    la.last_activity_date::TIMESTAMP_NTZ AS created_date,
    la.last_activity_date::TIMESTAMP_NTZ AS updated_date
FROM CONFORMED.PATIENT_BASE pb
JOIN last_activity la ON la.patient_id = pb.patient_id
JOIN activity_counts ac ON ac.patient_id = pb.patient_id
JOIN last30 l30 ON l30.patient_id = pb.patient_id
WHERE pb.is_current = TRUE;