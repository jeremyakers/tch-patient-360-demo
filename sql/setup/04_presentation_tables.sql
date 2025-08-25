-- Texas Children's Hospital Patient 360 PoC - Presentation Layer Tables
-- Creates analytics-optimized tables for visualization and reporting

USE DATABASE TCH_PATIENT_360_POC;
USE SCHEMA PRESENTATION;
USE WAREHOUSE TCH_ANALYTICS_WH;

-- =============================================================================
-- PATIENT 360 CORE VIEWS
-- =============================================================================

-- Patient 360 Comprehensive View
CREATE OR REPLACE VIEW PATIENT_360 AS
SELECT 
    -- Patient Demographics
    pm.patient_key,
    pm.patient_id,
    pm.mrn,
    pm.full_name,
    pm.first_name,
    pm.last_name,
    pm.date_of_birth,
    DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) AS current_age,
    pm.gender,
    pm.race,
    pm.ethnicity,
    pm.zip_code,
    pm.primary_insurance,
    pm.primary_language,
    pm.risk_category,
    pm.chronic_conditions_count,
    
    -- Age Group Classification
    CASE 
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) = 0 THEN 'Newborn'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 1 THEN 'Infant'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 3 THEN 'Toddler'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 5 THEN 'Preschool'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 12 THEN 'School Age'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 18 THEN 'Adolescent'
        ELSE 'Young Adult'
    END AS age_group,
    
    -- Encounter Summary
    pm.last_encounter_date,
    pm.total_encounters,
    DATEDIFF('day', pm.last_encounter_date, CURRENT_DATE()) AS days_since_last_visit,
    
    -- Recent Activity Indicators
    CASE 
        WHEN pm.last_encounter_date >= DATEADD('month', -6, CURRENT_DATE()) THEN 'Active'
        WHEN pm.last_encounter_date >= DATEADD('year', -1, CURRENT_DATE()) THEN 'Recent'
        ELSE 'Inactive'
    END AS patient_status,
    
    -- Most Recent Encounter Details
    last_enc.encounter_type AS last_encounter_type,
    last_enc.department_name AS last_department,
    last_enc.attending_provider AS last_provider,
    
    -- Chronic Conditions Summary
    chronic_dx.condition_list,
    chronic_dx.condition_count,
    
    -- Recent Vital Signs
    recent_vitals.last_weight_kg,
    recent_vitals.last_height_cm,
    recent_vitals.last_bmi,
    recent_vitals.growth_trend,
    
    -- Quality Metrics
    quality.immunization_status,
    quality.well_child_visits_ytd,
    quality.emergency_visits_ytd,
    quality.readmission_risk_score,

    -- Financial summary (Oracle ERP)
    COALESCE(fs.total_lifetime_charges, 0) AS total_lifetime_charges,
    COALESCE(fs.avg_cost_per_encounter, 0) AS avg_cost_per_encounter,
    COALESCE(fs.high_cost_episodes, 0) AS high_cost_episodes,
    COALESCE(fs.outstanding_balance, 0) AS outstanding_balance,
    CASE 
        WHEN fs.total_lifetime_charges > 100000 THEN 'HIGH_VALUE_PATIENT'
        WHEN fs.total_lifetime_charges > 25000 THEN 'MODERATE_VALUE_PATIENT'
        ELSE 'STANDARD_VALUE_PATIENT'
    END AS financial_value_category,

    -- Engagement metrics (Salesforce)
    COALESCE(esg.digital_health_adoption_level, 'UNKNOWN') AS digital_adoption_level,
    COALESCE(esg.portal_logins_last_30_days, 0) AS portal_logins_last_30_days,
    COALESCE(esg.engagement_score, 0) AS engagement_score,
    esg.last_activity_date AS last_engagement_date,
    
    pm.created_date,
    pm.updated_date

FROM CONFORMED.PATIENT_MASTER pm

-- Most recent encounter
LEFT JOIN (
    SELECT 
        patient_key,
        encounter_type,
        department_name,
        attending_provider,
        ROW_NUMBER() OVER (PARTITION BY patient_key ORDER BY encounter_date DESC) AS rn
    FROM CONFORMED.ENCOUNTER_SUMMARY
) last_enc ON pm.patient_key = last_enc.patient_key AND last_enc.rn = 1

-- Chronic conditions summary
LEFT JOIN (
    SELECT 
        patient_key,
        LISTAGG(diagnosis_description, '; ') WITHIN GROUP (ORDER BY diagnosis_date DESC) AS condition_list,
        COUNT(*) AS condition_count
    FROM CONFORMED.DIAGNOSIS_FACT
    WHERE is_chronic_condition = TRUE
    GROUP BY patient_key
) chronic_dx ON pm.patient_key = chronic_dx.patient_key

-- Recent vital signs
LEFT JOIN (
    SELECT 
        patient_key,
        weight_kg AS last_weight_kg,
        height_cm AS last_height_cm,
        bmi AS last_bmi,
        CASE 
            WHEN LAG(weight_kg) OVER (PARTITION BY patient_key ORDER BY measurement_date) < weight_kg THEN 'Increasing'
            WHEN LAG(weight_kg) OVER (PARTITION BY patient_key ORDER BY measurement_date) > weight_kg THEN 'Decreasing'
            ELSE 'Stable'
        END AS growth_trend,
        ROW_NUMBER() OVER (PARTITION BY patient_key ORDER BY measurement_date DESC) AS rn
    FROM CONFORMED.VITAL_SIGNS_FACT
    WHERE weight_kg IS NOT NULL AND height_cm IS NOT NULL
) recent_vitals ON pm.patient_key = recent_vitals.patient_key AND recent_vitals.rn = 1

-- Financial summary (aggregate FINANCIAL_FACT)
LEFT JOIN (
    SELECT patient_key,
           SUM(total_charges) AS total_lifetime_charges,
           AVG(total_charges) AS avg_cost_per_encounter,
           COUNT(CASE WHEN cost_category = 'HIGH_COST' THEN 1 END) AS high_cost_episodes,
           SUM(balance) AS outstanding_balance
    FROM CONFORMED.FINANCIAL_FACT
    GROUP BY patient_key
) fs ON pm.patient_key = fs.patient_key

-- Engagement summary (aggregate ENGAGEMENT_FACT)
LEFT JOIN (
    SELECT patient_key,
           MAX(digital_health_adoption_level) AS digital_health_adoption_level,
           MAX(portal_logins_last_30_days) AS portal_logins_last_30_days,
           MAX(engagement_score) AS engagement_score,
           MAX(last_activity_date) AS last_activity_date
    FROM CONFORMED.ENGAGEMENT_FACT
    GROUP BY patient_key
) esg ON pm.patient_key = esg.patient_key

-- Quality metrics (simplified for demo)
LEFT JOIN (
    SELECT 
        patient_key,
        'Up to Date' AS immunization_status, -- Placeholder
        COUNT(CASE WHEN encounter_type = 'Outpatient' AND encounter_date >= DATEADD('year', -1, CURRENT_DATE()) THEN 1 END) AS well_child_visits_ytd,
        COUNT(CASE WHEN encounter_type = 'Emergency' AND encounter_date >= DATEADD('year', -1, CURRENT_DATE()) THEN 1 END) AS emergency_visits_ytd,
        CASE 
            WHEN COUNT(CASE WHEN readmission_flag = TRUE THEN 1 END) > 0 THEN 'High'
            WHEN COUNT(CASE WHEN encounter_type = 'Emergency' THEN 1 END) > 3 THEN 'Medium'
            ELSE 'Low'
        END AS readmission_risk_score
    FROM CONFORMED.ENCOUNTER_SUMMARY
    GROUP BY patient_key
) quality ON pm.patient_key = quality.patient_key

WHERE pm.is_current = TRUE;

-- =============================================================================
-- ENCOUNTER ANALYTICS
-- =============================================================================

-- Encounter Analytics View
CREATE OR REPLACE VIEW ENCOUNTER_ANALYTICS AS
SELECT 
    es.encounter_key,
    es.encounter_id,
    es.patient_key,
    es.patient_id,
    
    -- Patient Demographics (joined for easy filtering)
    pm.full_name AS patient_name,
    DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) AS current_age,
    pm.gender,
    pm.race,
    pm.ethnicity,
    pm.primary_insurance,
    
    -- Encounter Details
    es.encounter_date,
    es.encounter_datetime,
    es.encounter_type,
    es.encounter_category,
    es.department_name,
    es.service_line,
    es.attending_provider,
    
    -- Timing Metrics
    es.length_of_stay_hours,
    es.length_of_stay_days,
    EXTRACT(YEAR FROM es.encounter_date) AS encounter_year,
    EXTRACT(MONTH FROM es.encounter_date) AS encounter_month,
    EXTRACT(QUARTER FROM es.encounter_date) AS encounter_quarter,
    EXTRACT(DAYOFWEEK FROM es.encounter_date) AS encounter_day_of_week,
    EXTRACT(HOUR FROM es.encounter_datetime) AS encounter_hour,
    
    -- Financial Metrics
    es.total_charges,
    es.primary_payer,
    es.financial_class,
    
    -- Quality Indicators
    es.readmission_flag,
    es.readmission_days,
    
    -- Chief Complaint Classification
    es.chief_complaint,
    CASE 
        WHEN UPPER(es.chief_complaint) LIKE '%FEVER%' THEN 'Fever'
        WHEN UPPER(es.chief_complaint) LIKE '%COUGH%' THEN 'Respiratory'
        WHEN UPPER(es.chief_complaint) LIKE '%PAIN%' OR UPPER(es.chief_complaint) LIKE '%ACHE%' THEN 'Pain'
        WHEN UPPER(es.chief_complaint) LIKE '%VOMIT%' OR UPPER(es.chief_complaint) LIKE '%NAUSEA%' THEN 'GI Symptoms'
        WHEN UPPER(es.chief_complaint) LIKE '%INJURY%' OR UPPER(es.chief_complaint) LIKE '%TRAUMA%' THEN 'Injury/Trauma'
        ELSE 'Other'
    END AS chief_complaint_category,
    
    -- Visit Complexity Score
    CASE 
        WHEN es.length_of_stay_days > 7 THEN 'High'
        WHEN es.length_of_stay_days > 1 THEN 'Medium'
        ELSE 'Low'
    END AS complexity_score,
    
    es.encounter_status,
    es.created_date,
    es.updated_date

FROM CONFORMED.ENCOUNTER_SUMMARY es
INNER JOIN CONFORMED.PATIENT_MASTER pm 
    ON es.patient_key = pm.patient_key 
    AND pm.is_current = TRUE;

-- =============================================================================
-- CLINICAL ANALYTICS
-- =============================================================================

-- Diagnosis Analytics View
CREATE OR REPLACE VIEW DIAGNOSIS_ANALYTICS AS
SELECT 
    df.diagnosis_key,
    df.patient_key,
    df.patient_id,
    df.encounter_key,
    df.encounter_id,
    
    -- Patient Context
    DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) AS patient_age,
    pm.gender AS patient_gender,
    pm.race AS patient_race,
    pm.primary_insurance,
    
    -- Diagnosis Details
    df.diagnosis_code,
    df.diagnosis_description,
    df.diagnosis_category,
    df.diagnosis_type,
    df.diagnosis_date,
    df.age_at_diagnosis,
    df.age_group,
    
    -- Clinical Significance
    df.is_chronic_condition,
    df.is_primary_diagnosis,
    /* diagnosis_sequence moved out of DTs for incremental mode; can be recomputed in views if needed */
    
    -- Time Dimensions
    EXTRACT(YEAR FROM df.diagnosis_date) AS diagnosis_year,
    EXTRACT(MONTH FROM df.diagnosis_date) AS diagnosis_month,
    EXTRACT(QUARTER FROM df.diagnosis_date) AS diagnosis_quarter,
    
    -- Common Pediatric Conditions Flags
    CASE WHEN df.diagnosis_code LIKE 'J45%' THEN TRUE ELSE FALSE END AS is_asthma,
    CASE WHEN df.diagnosis_code LIKE 'F90%' THEN TRUE ELSE FALSE END AS is_adhd,
    CASE WHEN df.diagnosis_code LIKE 'E10%' OR df.diagnosis_code LIKE 'E11%' THEN TRUE ELSE FALSE END AS is_diabetes,
    CASE WHEN df.diagnosis_code LIKE 'F84%' THEN TRUE ELSE FALSE END AS is_autism,
    CASE WHEN df.diagnosis_code LIKE 'E66%' THEN TRUE ELSE FALSE END AS is_obesity,
    CASE WHEN df.diagnosis_code LIKE 'J06%' OR df.diagnosis_code LIKE 'B34%' THEN TRUE ELSE FALSE END AS is_respiratory_infection,
    
    -- Encounter Context
    es.encounter_type,
    es.department_name,
    es.service_line,
    
    df.created_date,
    df.updated_date

FROM CONFORMED.DIAGNOSIS_FACT df
INNER JOIN CONFORMED.PATIENT_MASTER pm 
    ON df.patient_key = pm.patient_key 
    AND pm.is_current = TRUE
LEFT JOIN CONFORMED.ENCOUNTER_SUMMARY es 
    ON df.encounter_key = es.encounter_key;

-- Lab Results Analytics View
CREATE OR REPLACE VIEW LAB_RESULTS_ANALYTICS AS
SELECT 
    lrf.lab_result_key,
    lrf.patient_key,
    lrf.patient_id,
    lrf.encounter_key,
    lrf.encounter_id,

    -- Test details
    lrf.test_name,
    lrf.test_category,
    lrf.test_value_numeric,
    lrf.test_value_text,
    lrf.reference_range_min,
    lrf.reference_range_max,
    lrf.reference_range_text,
    lrf.abnormal_flag,
    lrf.critical_flag,

    -- Temporal
    lrf.result_date,
    lrf.result_datetime,

    -- Derived flags
    CASE 
        WHEN UPPER(lrf.test_name) LIKE '%HBA1C%'
          OR UPPER(lrf.test_name) LIKE '%A1C%'
          OR UPPER(lrf.test_name) LIKE '%HEMOGLOBIN A1C%'
        THEN TRUE ELSE FALSE END AS is_hba1c

FROM CONFORMED.LAB_RESULTS_FACT lrf;

-- Medication Analytics View
CREATE OR REPLACE VIEW MEDICATION_ANALYTICS AS
SELECT 
    -- Keys and identifiers (subset to ensure compatibility)
    mf.patient_id,
    mf.encounter_id,

    -- Medication details
    mf.medication_name,
    mf.dosage,
    mf.frequency,
    mf.route,
    mf.start_date,
    mf.end_date,

    -- Active medication flag (ongoing therapy)
    CASE 
        WHEN mf.end_date IS NULL OR mf.end_date >= CURRENT_DATE() THEN TRUE 
        ELSE FALSE 
    END AS is_active,

    mf.created_date,
    mf.updated_date

FROM CONFORMED.MEDICATION_FACT mf;

-- =============================================================================
-- FINANCIAL ANALYTICS (Oracle ERP + Clinical)
-- =============================================================================

CREATE OR REPLACE VIEW FINANCIAL_ANALYTICS AS
WITH cost_by_condition AS (
    SELECT 
        df.diagnosis_description,
        COUNT(DISTINCT pm.patient_id) AS patient_count,
        AVG(ff.total_charges) AS avg_cost_per_patient,
        SUM(ff.total_charges) AS total_cost,
        AVG(ff.cost_per_day) AS avg_cost_per_day,
        COUNT(CASE WHEN ff.cost_category = 'HIGH_COST' THEN 1 END) AS high_cost_cases,
        COUNT(CASE WHEN ff.payer_mix_category = 'PUBLIC' THEN 1 END) AS public_payer_count,
        COUNT(CASE WHEN ff.payer_mix_category = 'COMMERCIAL' THEN 1 END) AS commercial_payer_count
    FROM CONFORMED.DIAGNOSIS_FACT df
    INNER JOIN CONFORMED.FINANCIAL_FACT ff ON df.encounter_key = ff.encounter_key
    INNER JOIN CONFORMED.PATIENT_MASTER pm ON df.patient_key = pm.patient_key
    WHERE pm.is_current = TRUE
    GROUP BY df.diagnosis_description
    HAVING COUNT(DISTINCT pm.patient_id) >= 5
),
engagement_correlation AS (
    SELECT 
        df.diagnosis_description,
        AVG(ef.engagement_score) AS avg_engagement_score,
        COUNT(CASE WHEN ef.digital_health_adoption_level = 'HIGH' THEN 1 END) AS high_engagement_count
    FROM CONFORMED.DIAGNOSIS_FACT df
    INNER JOIN CONFORMED.ENGAGEMENT_FACT ef ON df.patient_key = ef.patient_key
    GROUP BY df.diagnosis_description
)
SELECT 
    cbc.diagnosis_description,
    cbc.patient_count,
    cbc.avg_cost_per_patient,
    cbc.total_cost,
    cbc.avg_cost_per_day,
    cbc.high_cost_cases,
    ROUND(cbc.high_cost_cases::FLOAT / NULLIF(cbc.patient_count,0) * 100, 2) AS high_cost_percentage,
    ROUND(cbc.public_payer_count::FLOAT / NULLIF(cbc.patient_count,0) * 100, 2) AS public_payer_percentage,
    ROUND(cbc.commercial_payer_count::FLOAT / NULLIF(cbc.patient_count,0) * 100, 2) AS commercial_payer_percentage,
    COALESCE(ec.avg_engagement_score, 0) AS avg_engagement_score,
    CURRENT_TIMESTAMP() AS analysis_generated_at
FROM cost_by_condition cbc
LEFT JOIN engagement_correlation ec ON cbc.diagnosis_description = ec.diagnosis_description
ORDER BY cbc.total_cost DESC;

-- =============================================================================
-- POPULATION HEALTH ANALYTICS
-- =============================================================================

-- Population Health Summary
CREATE OR REPLACE TABLE POPULATION_HEALTH_SUMMARY AS
SELECT 
    -- Time Dimensions
    CURRENT_DATE() AS report_date,
    DATE_TRUNC('month', CURRENT_DATE()) AS report_month,
    
    -- Overall Population
    COUNT(DISTINCT pm.patient_key) AS total_active_patients,
    
    -- Age Group Distribution
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) = 0 THEN pm.patient_key END) AS newborns,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 1 AND 1 THEN pm.patient_key END) AS infants,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 2 AND 3 THEN pm.patient_key END) AS toddlers,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 4 AND 5 THEN pm.patient_key END) AS preschoolers,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 6 AND 12 THEN pm.patient_key END) AS school_age,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 13 AND 18 THEN pm.patient_key END) AS adolescents,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 19 AND 21 THEN pm.patient_key END) AS young_adults,
    
    -- Gender Distribution
    COUNT(DISTINCT CASE WHEN pm.gender = 'M' THEN pm.patient_key END) AS male_patients,
    COUNT(DISTINCT CASE WHEN pm.gender = 'F' THEN pm.patient_key END) AS female_patients,
    
    -- Insurance Distribution
    COUNT(DISTINCT CASE WHEN pm.primary_insurance = 'Medicaid' THEN pm.patient_key END) AS medicaid_patients,
    COUNT(DISTINCT CASE WHEN pm.primary_insurance = 'Commercial' THEN pm.patient_key END) AS commercial_patients,
    COUNT(DISTINCT CASE WHEN pm.primary_insurance = 'CHIP' THEN pm.patient_key END) AS chip_patients,
    COUNT(DISTINCT CASE WHEN pm.primary_insurance = 'Self-pay' THEN pm.patient_key END) AS self_pay_patients,
    
    -- Common Conditions Prevalence
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND df.diagnosis_code LIKE 'J45%'
    ) THEN pm.patient_key END) AS asthma_patients,
    
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND df.diagnosis_code LIKE 'F90%'
    ) THEN pm.patient_key END) AS adhd_patients,
    
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND (df.diagnosis_code LIKE 'E10%' OR df.diagnosis_code LIKE 'E11%')
    ) THEN pm.patient_key END) AS diabetes_patients,
    
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND df.diagnosis_code LIKE 'E66%'
    ) THEN pm.patient_key END) AS obesity_patients,
    
    -- Risk Stratification
    COUNT(DISTINCT CASE WHEN pm.risk_category = 'HIGH_RISK' THEN pm.patient_key END) AS high_risk_patients,
    COUNT(DISTINCT CASE WHEN pm.risk_category = 'MODERATE_RISK' THEN pm.patient_key END) AS moderate_risk_patients,
    COUNT(DISTINCT CASE WHEN pm.risk_category = 'LOW_RISK' THEN pm.patient_key END) AS low_risk_patients,
    
    -- Utilization Metrics
    COUNT(DISTINCT CASE WHEN pm.last_encounter_date >= DATEADD('month', -12, CURRENT_DATE()) THEN pm.patient_key END) AS patients_seen_last_year,
    COUNT(DISTINCT CASE WHEN pm.last_encounter_date >= DATEADD('month', -6, CURRENT_DATE()) THEN pm.patient_key END) AS patients_seen_last_6_months,
    COUNT(DISTINCT CASE WHEN pm.last_encounter_date >= DATEADD('month', -3, CURRENT_DATE()) THEN pm.patient_key END) AS patients_seen_last_3_months

FROM CONFORMED.PATIENT_MASTER pm
WHERE pm.is_current = TRUE;

-- =============================================================================
-- QUALITY METRICS
-- =============================================================================

-- Quality Metrics Dashboard
CREATE OR REPLACE VIEW QUALITY_METRICS_DASHBOARD AS
SELECT 
    -- Report Period
    CURRENT_DATE() AS report_date,
    'Last 12 Months' AS report_period,
    
    -- Patient Safety Metrics
    COUNT(DISTINCT es.encounter_key) AS total_encounters,
    COUNT(DISTINCT CASE WHEN es.readmission_flag = TRUE THEN es.encounter_key END) AS readmissions,
    ROUND(
        COUNT(DISTINCT CASE WHEN es.readmission_flag = TRUE THEN es.encounter_key END) * 100.0 / 
        NULLIF(COUNT(DISTINCT es.encounter_key), 0), 2
    ) AS readmission_rate_percent,
    
    -- Length of Stay Metrics
    AVG(es.length_of_stay_days) AS avg_length_of_stay_days,
    MEDIAN(es.length_of_stay_days) AS median_length_of_stay_days,
    
    -- Emergency Department Metrics
    COUNT(DISTINCT CASE WHEN es.encounter_type = 'Emergency' THEN es.encounter_key END) AS total_ed_visits,
    COUNT(DISTINCT CASE WHEN es.encounter_type = 'Emergency' AND es.length_of_stay_hours <= 4 THEN es.encounter_key END) AS ed_visits_under_4_hours,
    ROUND(
        COUNT(DISTINCT CASE WHEN es.encounter_type = 'Emergency' AND es.length_of_stay_hours <= 4 THEN es.encounter_key END) * 100.0 / 
        NULLIF(COUNT(DISTINCT CASE WHEN es.encounter_type = 'Emergency' THEN es.encounter_key END), 0), 2
    ) AS ed_throughput_rate_percent,
    
    -- Chronic Disease Management
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.encounter_key = es.encounter_key AND df.diagnosis_code LIKE 'J45%'
    ) THEN es.patient_key END) AS asthma_patients_seen,
    
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.encounter_key = es.encounter_key AND (df.diagnosis_code LIKE 'E10%' OR df.diagnosis_code LIKE 'E11%')
    ) THEN es.patient_key END) AS diabetes_patients_seen,
    
    -- Preventive Care Metrics
    COUNT(DISTINCT CASE WHEN es.encounter_type = 'Outpatient' AND es.chief_complaint LIKE '%well%' THEN es.patient_key END) AS well_child_visits,
    
    -- Financial Metrics
    SUM(es.total_charges) AS total_charges,
    AVG(es.total_charges) AS avg_charge_per_encounter

FROM CONFORMED.ENCOUNTER_SUMMARY es
WHERE es.encounter_date >= DATEADD('year', -1, CURRENT_DATE());

-- =============================================================================
-- OPERATIONAL DASHBOARDS
-- =============================================================================

-- Department Performance
CREATE OR REPLACE VIEW DEPARTMENT_PERFORMANCE AS
SELECT 
    dd.department_name,
    dd.service_line,
    dd.campus,
    
    -- Volume Metrics
    COUNT(DISTINCT es.encounter_key) AS total_encounters,
    COUNT(DISTINCT es.patient_key) AS unique_patients,
    COUNT(DISTINCT es.attending_provider) AS active_providers,
    
    -- Quality Metrics
    AVG(es.length_of_stay_days) AS avg_length_of_stay,
    COUNT(CASE WHEN es.readmission_flag = TRUE THEN 1 END) AS readmissions,
    ROUND(
        COUNT(CASE WHEN es.readmission_flag = TRUE THEN 1 END) * 100.0 / 
        NULLIF(COUNT(es.encounter_key), 0), 2
    ) AS readmission_rate_percent,
    
    -- Financial Metrics
    SUM(es.total_charges) AS total_revenue,
    AVG(es.total_charges) AS avg_revenue_per_encounter,
    
    -- Patient Demographics
    AVG(DATEDIFF('year', pm.date_of_birth, CURRENT_DATE())) AS avg_patient_age,
    COUNT(CASE WHEN pm.gender = 'M' THEN 1 END) * 100.0 / COUNT(*) AS male_percentage,
    
    -- Seasonal Patterns
    COUNT(CASE WHEN EXTRACT(QUARTER FROM es.encounter_date) = 1 THEN 1 END) AS q1_encounters,
    COUNT(CASE WHEN EXTRACT(QUARTER FROM es.encounter_date) = 2 THEN 1 END) AS q2_encounters,
    COUNT(CASE WHEN EXTRACT(QUARTER FROM es.encounter_date) = 3 THEN 1 END) AS q3_encounters,
    COUNT(CASE WHEN EXTRACT(QUARTER FROM es.encounter_date) = 4 THEN 1 END) AS q4_encounters

FROM CONFORMED.ENCOUNTER_SUMMARY es
INNER JOIN CONFORMED.PATIENT_MASTER pm 
    ON es.patient_key = pm.patient_key 
    AND pm.is_current = TRUE
LEFT JOIN CONFORMED.DEPARTMENT_DIM dd 
    ON es.department_name = dd.department_name
WHERE es.encounter_date >= DATEADD('year', -1, CURRENT_DATE())
GROUP BY dd.department_name, dd.service_line, dd.campus
ORDER BY total_encounters DESC;

-- Display completion message
SELECT 'Presentation layer tables and views created successfully. Ready for analytics and visualization.' AS presentation_layer_status;