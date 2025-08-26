-- Texas Children's Hospital Patient 360 PoC - Cortex Analyst Setup
-- Configures Cortex Analyst semantic model for natural language querying

USE DATABASE TCH_PATIENT_360_POC;
USE SCHEMA AI_ML;
USE WAREHOUSE TCH_AI_ML_WH;

-- =============================================================================
-- CORTEX ANALYST SEMANTIC MODEL CONFIGURATION
-- =============================================================================

-- Create file format for YAML semantic models
CREATE OR REPLACE FILE FORMAT YAML_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = NONE
RECORD_DELIMITER = NONE
SKIP_HEADER = 0
FIELD_OPTIONALLY_ENCLOSED_BY = NONE
TRIM_SPACE = FALSE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
ESCAPE = NONE
ESCAPE_UNENCLOSED_FIELD = NONE
NULL_IF = ()
COMMENT = 'File format for YAML semantic model files';

-- Create stage for semantic model files
CREATE OR REPLACE STAGE SEMANTIC_MODEL_STAGE
  FILE_FORMAT = YAML_FORMAT
  DIRECTORY = ( ENABLE = TRUE )
  COMMENT = 'Stage for Cortex Analyst semantic model configuration files';

-- =============================================================================
-- PATIENT 360 SEMANTIC MODEL DEFINITION
-- =============================================================================

-- The semantic model is defined in semantic_model.yaml and uploaded to the stage
-- This script creates the base tables that the semantic model references

CREATE OR REPLACE VIEW CORTEX_ANALYST_SEMANTIC_MODEL_CONFIG AS
SELECT 
    'TCH_PATIENT_360_SEMANTIC_MODEL' AS model_name,
    'Texas Children\'s Hospital Patient 360 Analytics' AS model_description,
    'YAML-based semantic model for pediatric healthcare analytics' AS model_purpose,
    CURRENT_TIMESTAMP() AS created_date;

-- =============================================================================
-- BASE TABLES FOR SEMANTIC MODEL
-- =============================================================================

-- Patient Analytics Base Table
CREATE OR REPLACE VIEW AI_ML.PATIENT_ANALYTICS_BASE AS
SELECT 
    -- Patient Identifiers
    pm.patient_key,
    pm.patient_id,
    pm.mrn,
    
    -- Demographics
    pm.full_name AS patient_name,
    DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) AS age,
    pm.gender,
    pm.race,
    pm.ethnicity,
    pm.primary_insurance AS insurance_type,
    pm.primary_language AS language,
    pm.zip_code,
    
    -- Age grouping for analytics
    CASE 
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) = 0 THEN 'Newborn (0)'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 1 THEN 'Infant (0-1)'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 3 THEN 'Toddler (1-3)'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 5 THEN 'Preschool (3-5)'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 12 THEN 'School Age (5-12)'
        WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) <= 18 THEN 'Adolescent (12-18)'
        ELSE 'Young Adult (18-21)'
    END AS age_group,
    
    -- Risk stratification
    pm.risk_category,
    pm.chronic_conditions_count,
    
    -- Utilization metrics
    pm.total_encounters,
    pm.last_encounter_date,
    DATEDIFF('day', pm.last_encounter_date, CURRENT_DATE()) AS days_since_last_visit,
    
    -- Status classification
    CASE 
        WHEN pm.last_encounter_date >= DATEADD('month', -6, CURRENT_DATE()) THEN 'Active'
        WHEN pm.last_encounter_date >= DATEADD('year', -1, CURRENT_DATE()) THEN 'Recent'
        ELSE 'Inactive'
    END AS patient_status,
    
    -- Geographic classification
    CASE 
        WHEN SUBSTRING(pm.zip_code, 1, 3) IN ('770', '771', '772', '773', '774', '775') THEN 'Houston Metro'
        WHEN SUBSTRING(pm.zip_code, 1, 3) IN ('776', '777', '778', '779') THEN 'Greater Houston'
        ELSE 'Other'
    END AS geographic_region

FROM TCH_PATIENT_360_POC.CONFORMED.PATIENT_MASTER pm
WHERE pm.is_current = TRUE;

-- Encounter Analytics Base Table
CREATE OR REPLACE VIEW AI_ML.ENCOUNTER_ANALYTICS_BASE AS
SELECT 
    -- Identifiers
    es.encounter_key,
    es.encounter_id,
    es.patient_key,
    es.patient_id,
    
    -- Patient context
    DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) AS patient_age,
    pm.gender AS patient_gender,
    pm.primary_insurance AS patient_insurance,
    
    -- Temporal dimensions
    es.encounter_date,
    EXTRACT(YEAR FROM es.encounter_date) AS encounter_year,
    EXTRACT(MONTH FROM es.encounter_date) AS encounter_month,
    EXTRACT(QUARTER FROM es.encounter_date) AS encounter_quarter,
    TO_VARCHAR(es.encounter_date, 'MON YYYY') AS encounter_month_year,
    EXTRACT(DAYOFWEEK FROM es.encounter_date) AS day_of_week,
    
    -- Encounter classification
    es.encounter_type,
    es.encounter_category,
    es.department_name AS department,
    es.service_line,
    es.attending_provider AS provider,
    
    -- Metrics
    es.length_of_stay_days,
    es.total_charges,
    es.readmission_flag,
    es.readmission_days,
    
    -- Chief complaint categorization
    CASE 
        WHEN UPPER(es.chief_complaint) LIKE '%FEVER%' THEN 'Fever'
        WHEN UPPER(es.chief_complaint) LIKE '%COUGH%' OR UPPER(es.chief_complaint) LIKE '%RESPIRATORY%' THEN 'Respiratory'
        WHEN UPPER(es.chief_complaint) LIKE '%PAIN%' OR UPPER(es.chief_complaint) LIKE '%ACHE%' THEN 'Pain'
        WHEN UPPER(es.chief_complaint) LIKE '%GI%' OR UPPER(es.chief_complaint) LIKE '%STOMACH%' OR UPPER(es.chief_complaint) LIKE '%VOMIT%' THEN 'Gastrointestinal'
        WHEN UPPER(es.chief_complaint) LIKE '%INJURY%' OR UPPER(es.chief_complaint) LIKE '%TRAUMA%' THEN 'Injury/Trauma'
        WHEN UPPER(es.chief_complaint) LIKE '%WELL%' OR UPPER(es.chief_complaint) LIKE '%CHECK%' THEN 'Preventive Care'
        ELSE 'Other'
    END AS chief_complaint_category,
    
    -- Volume metrics
    COUNT(*) OVER (PARTITION BY DATE_TRUNC('month', es.encounter_date)) AS monthly_encounter_volume,
    COUNT(*) OVER (PARTITION BY es.department_name, DATE_TRUNC('month', es.encounter_date)) AS monthly_department_volume

FROM TCH_PATIENT_360_POC.CONFORMED.ENCOUNTER_SUMMARY es
INNER JOIN TCH_PATIENT_360_POC.CONFORMED.PATIENT_MASTER pm 
    ON es.patient_key = pm.patient_key 
    AND pm.is_current = TRUE;

-- Diagnosis Analytics Base Table
CREATE OR REPLACE VIEW AI_ML.DIAGNOSIS_ANALYTICS_BASE AS
SELECT 
    -- Identifiers
    df.diagnosis_key,
    df.patient_key,
    df.encounter_key,
    
    -- Patient context
    DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) AS patient_age,
    pm.gender AS patient_gender,
    pm.race AS patient_race,
    pm.primary_insurance AS patient_insurance,
    
    -- Diagnosis details
    df.diagnosis_code,
    df.diagnosis_description,
    df.diagnosis_category,
    df.diagnosis_type,
    df.age_at_diagnosis,
    df.age_group AS age_group_at_diagnosis,
    
    -- Temporal
    df.diagnosis_date,
    EXTRACT(YEAR FROM df.diagnosis_date) AS diagnosis_year,
    EXTRACT(MONTH FROM df.diagnosis_date) AS diagnosis_month,
    EXTRACT(QUARTER FROM df.diagnosis_date) AS diagnosis_quarter,
    
    -- Clinical significance
    df.is_chronic_condition,
    df.is_primary_diagnosis,
    
    -- Common pediatric conditions
    CASE WHEN df.diagnosis_code LIKE 'J45%' THEN 'Asthma'
         WHEN df.diagnosis_code LIKE 'F90%' THEN 'ADHD'
         WHEN df.diagnosis_code LIKE 'E10%' OR df.diagnosis_code LIKE 'E11%' THEN 'Diabetes'
         WHEN df.diagnosis_code LIKE 'F84%' THEN 'Autism Spectrum Disorder'
         WHEN df.diagnosis_code LIKE 'E66%' THEN 'Obesity'
         WHEN df.diagnosis_code LIKE 'J06%' OR df.diagnosis_code LIKE 'B34%' THEN 'Respiratory Infection'
         WHEN df.diagnosis_code LIKE 'G40%' THEN 'Epilepsy'
         WHEN df.diagnosis_code LIKE 'K21%' THEN 'GERD'
         WHEN df.diagnosis_code LIKE 'Q%' THEN 'Congenital Condition'
         WHEN df.diagnosis_code LIKE 'P%' THEN 'Perinatal Condition'
         ELSE 'Other'
    END AS condition_category,
    
    -- Encounter context
    es.encounter_type,
    es.department_name AS department,
    es.service_line

FROM TCH_PATIENT_360_POC.CONFORMED.DIAGNOSIS_FACT df
INNER JOIN TCH_PATIENT_360_POC.CONFORMED.PATIENT_MASTER pm 
    ON df.patient_key = pm.patient_key 
    AND pm.is_current = TRUE
INNER JOIN TCH_PATIENT_360_POC.CONFORMED.ENCOUNTER_SUMMARY es 
    ON df.encounter_key = es.encounter_key;

-- Population Health Metrics Base Table
CREATE OR REPLACE VIEW AI_ML.POPULATION_HEALTH_BASE AS
SELECT 
    -- Time dimensions
    CURRENT_DATE() AS report_date,
    DATE_TRUNC('month', CURRENT_DATE()) AS report_month,
    EXTRACT(YEAR FROM CURRENT_DATE()) AS report_year,
    
    -- Demographics summary
    COUNT(DISTINCT pm.patient_key) AS total_patients,
    
    -- Age distribution
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) = 0 THEN pm.patient_key END) AS newborn_count,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 1 AND 1 THEN pm.patient_key END) AS infant_count,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 2 AND 3 THEN pm.patient_key END) AS toddler_count,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 4 AND 5 THEN pm.patient_key END) AS preschool_count,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 6 AND 12 THEN pm.patient_key END) AS school_age_count,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 13 AND 18 THEN pm.patient_key END) AS adolescent_count,
    COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 19 AND 21 THEN pm.patient_key END) AS young_adult_count,
    
    -- Age percentages
    ROUND(COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) = 0 THEN pm.patient_key END) * 100.0 / COUNT(DISTINCT pm.patient_key), 1) AS newborn_percentage,
    ROUND(COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 1 AND 12 THEN pm.patient_key END) * 100.0 / COUNT(DISTINCT pm.patient_key), 1) AS pediatric_percentage,
    ROUND(COUNT(DISTINCT CASE WHEN DATEDIFF('year', pm.date_of_birth, CURRENT_DATE()) BETWEEN 13 AND 21 THEN pm.patient_key END) * 100.0 / COUNT(DISTINCT pm.patient_key), 1) AS adolescent_young_adult_percentage,
    
    -- Gender distribution
    COUNT(DISTINCT CASE WHEN pm.gender = 'Male' THEN pm.patient_key END) AS male_patients,
    COUNT(DISTINCT CASE WHEN pm.gender = 'Female' THEN pm.patient_key END) AS female_patients,
    ROUND(COUNT(DISTINCT CASE WHEN pm.gender = 'Male' THEN pm.patient_key END) * 100.0 / COUNT(DISTINCT pm.patient_key), 1) AS male_percentage,
    
    -- Insurance distribution
    COUNT(DISTINCT CASE WHEN pm.primary_insurance = 'Medicaid' THEN pm.patient_key END) AS medicaid_patients,
    COUNT(DISTINCT CASE WHEN pm.primary_insurance = 'Commercial' THEN pm.patient_key END) AS commercial_patients,
    COUNT(DISTINCT CASE WHEN pm.primary_insurance = 'CHIP' THEN pm.patient_key END) AS chip_patients,
    ROUND(COUNT(DISTINCT CASE WHEN pm.primary_insurance = 'Medicaid' THEN pm.patient_key END) * 100.0 / COUNT(DISTINCT pm.patient_key), 1) AS medicaid_percentage,
    
    -- Risk stratification
    COUNT(DISTINCT CASE WHEN pm.risk_category = 'HIGH_RISK' THEN pm.patient_key END) AS high_risk_patients,
    COUNT(DISTINCT CASE WHEN pm.risk_category = 'MODERATE_RISK' THEN pm.patient_key END) AS moderate_risk_patients,
    COUNT(DISTINCT CASE WHEN pm.risk_category = 'LOW_RISK' THEN pm.patient_key END) AS low_risk_patients,
    ROUND(COUNT(DISTINCT CASE WHEN pm.risk_category = 'HIGH_RISK' THEN pm.patient_key END) * 100.0 / COUNT(DISTINCT pm.patient_key), 1) AS high_risk_percentage,
    
    -- Chronic conditions prevalence
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM TCH_PATIENT_360_POC.CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND df.diagnosis_code LIKE 'J45%'
    ) THEN pm.patient_key END) AS asthma_patients,
    
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM TCH_PATIENT_360_POC.CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND df.diagnosis_code LIKE 'F90%'
    ) THEN pm.patient_key END) AS adhd_patients,
    
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM TCH_PATIENT_360_POC.CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND (df.diagnosis_code LIKE 'E10%' OR df.diagnosis_code LIKE 'E11%')
    ) THEN pm.patient_key END) AS diabetes_patients,
    
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM TCH_PATIENT_360_POC.CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND df.diagnosis_code LIKE 'E66%'
    ) THEN pm.patient_key END) AS obesity_patients,
    
    -- Prevalence rates
    ROUND(COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM TCH_PATIENT_360_POC.CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND df.diagnosis_code LIKE 'J45%'
    ) THEN pm.patient_key END) * 100.0 / COUNT(DISTINCT pm.patient_key), 1) AS asthma_prevalence_rate,
    
    ROUND(COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM TCH_PATIENT_360_POC.CONFORMED.DIAGNOSIS_FACT df 
        WHERE df.patient_key = pm.patient_key AND df.diagnosis_code LIKE 'E66%'
    ) THEN pm.patient_key END) * 100.0 / COUNT(DISTINCT pm.patient_key), 1) AS obesity_prevalence_rate

FROM TCH_PATIENT_360_POC.CONFORMED.PATIENT_MASTER pm
WHERE pm.is_current = TRUE;

-- =============================================================================
-- CORTEX ANALYST SEMANTIC MODEL JSON DEFINITION
-- =============================================================================

-- Create the semantic model definition as a stored procedure that generates JSON
CREATE OR REPLACE PROCEDURE GENERATE_CORTEX_ANALYST_SEMANTIC_MODEL()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    semantic_model_json STRING;
BEGIN
    semantic_model_json := '{
  "name": "TCH_Patient_360_Analytics",
  "description": "Texas Children\'s Hospital Patient 360 Analytics - Comprehensive pediatric healthcare data model",
  "snowflake_database": "TCH_PATIENT_360_POC",
  "snowflake_schema": "AI_ML",
  "tables": [
    {
      "name": "patient_analytics",
      "base_table": {
        "database": "TCH_PATIENT_360_POC",
        "schema": "AI_ML", 
        "table": "PATIENT_ANALYTICS_BASE"
      },
      "description": "Patient demographics and summary information",
      "columns": [
        {
          "name": "patient_key",
          "data_type": "NUMBER",
          "description": "Unique patient identifier"
        },
        {
          "name": "patient_name",
          "data_type": "TEXT",
          "description": "Patient full name"
        },
        {
          "name": "age",
          "data_type": "NUMBER",
          "description": "Current patient age in years"
        },
        {
          "name": "age_group", 
          "data_type": "TEXT",
          "description": "Pediatric age grouping (Newborn, Infant, Toddler, Preschool, School Age, Adolescent, Young Adult)"
        },
        {
          "name": "gender",
          "data_type": "TEXT", 
          "description": "Patient gender (Male, Female)"
        },
        {
          "name": "race",
          "data_type": "TEXT",
          "description": "Patient race/ethnicity"
        },
        {
          "name": "insurance_type",
          "data_type": "TEXT",
          "description": "Primary insurance type (Medicaid, Commercial, CHIP, Self-pay)"
        },
        {
          "name": "risk_category",
          "data_type": "TEXT", 
          "description": "Clinical risk level (HIGH_RISK, MODERATE_RISK, LOW_RISK)"
        },
        {
          "name": "chronic_conditions_count",
          "data_type": "NUMBER",
          "description": "Number of chronic conditions"
        },
        {
          "name": "total_encounters",
          "data_type": "NUMBER",
          "description": "Total number of healthcare encounters"
        },
        {
          "name": "patient_status",
          "data_type": "TEXT",
          "description": "Patient activity status (Active, Recent, Inactive)"
        },
        {
          "name": "geographic_region",
          "data_type": "TEXT",
          "description": "Geographic region (Houston Metro, Greater Houston, Other)"
        }
      ]
    },
    {
      "name": "encounter_analytics",
      "base_table": {
        "database": "TCH_PATIENT_360_POC",
        "schema": "AI_ML",
        "table": "ENCOUNTER_ANALYTICS_BASE"
      },
      "description": "Healthcare encounter and visit information",
      "columns": [
        {
          "name": "encounter_key",
          "data_type": "NUMBER", 
          "description": "Unique encounter identifier"
        },
        {
          "name": "patient_age",
          "data_type": "NUMBER",
          "description": "Patient age at time of encounter"
        },
        {
          "name": "encounter_date",
          "data_type": "DATE",
          "description": "Date of healthcare encounter"
        },
        {
          "name": "encounter_year",
          "data_type": "NUMBER",
          "description": "Year of encounter" 
        },
        {
          "name": "encounter_month",
          "data_type": "NUMBER",
          "description": "Month of encounter"
        },
        {
          "name": "encounter_quarter",
          "data_type": "NUMBER", 
          "description": "Quarter of encounter"
        },
        {
          "name": "encounter_month_year",
          "data_type": "TEXT",
          "description": "Month and year of encounter (e.g., JAN 2024)"
        },
        {
          "name": "encounter_type",
          "data_type": "TEXT",
          "description": "Type of encounter (Inpatient, Outpatient, Emergency, Observation)"
        },
        {
          "name": "department",
          "data_type": "TEXT",
          "description": "Hospital department or service"
        },
        {
          "name": "service_line", 
          "data_type": "TEXT",
          "description": "Clinical service line"
        },
        {
          "name": "length_of_stay_days",
          "data_type": "NUMBER",
          "description": "Length of stay in days for inpatient encounters"
        },
        {
          "name": "total_charges",
          "data_type": "NUMBER",
          "description": "Total financial charges for encounter"
        },
        {
          "name": "readmission_flag",
          "data_type": "BOOLEAN",
          "description": "Whether this encounter was a readmission"
        },
        {
          "name": "chief_complaint_category", 
          "data_type": "TEXT",
          "description": "Categorized chief complaint (Fever, Respiratory, Pain, GI, Injury, Preventive Care, Other)"
        },
        {
          "name": "monthly_encounter_volume",
          "data_type": "NUMBER",
          "description": "Total encounter volume for the month"
        }
      ]
    },
    {
      "name": "diagnosis_analytics",
      "base_table": {
        "database": "TCH_PATIENT_360_POC", 
        "schema": "AI_ML",
        "table": "DIAGNOSIS_ANALYTICS_BASE"
      },
      "description": "Clinical diagnosis and condition information",
      "columns": [
        {
          "name": "diagnosis_key",
          "data_type": "NUMBER",
          "description": "Unique diagnosis identifier"
        },
        {
          "name": "diagnosis_code",
          "data_type": "TEXT", 
          "description": "ICD-10 diagnosis code"
        },
        {
          "name": "diagnosis_description",
          "data_type": "TEXT",
          "description": "Diagnosis description"
        },
        {
          "name": "diagnosis_category",
          "data_type": "TEXT",
          "description": "ICD-10 chapter category"
        },
        {
          "name": "condition_category",
          "data_type": "TEXT",
          "description": "Common pediatric condition category (Asthma, ADHD, Diabetes, Autism, Obesity, etc.)"
        },
        {
          "name": "age_at_diagnosis",
          "data_type": "NUMBER",
          "description": "Patient age when diagnosed"
        },
        {
          "name": "age_group_at_diagnosis", 
          "data_type": "TEXT",
          "description": "Age group when diagnosed"
        },
        {
          "name": "diagnosis_date",
          "data_type": "DATE",
          "description": "Date of diagnosis"
        },
        {
          "name": "diagnosis_year",
          "data_type": "NUMBER",
          "description": "Year of diagnosis"
        },
        {
          "name": "is_chronic_condition",
          "data_type": "BOOLEAN", 
          "description": "Whether this is a chronic condition"
        },
        {
          "name": "is_primary_diagnosis",
          "data_type": "BOOLEAN",
          "description": "Whether this is the primary diagnosis for the encounter"
        },
        {
          "name": "encounter_type",
          "data_type": "TEXT",
          "description": "Type of encounter where diagnosed"
        },
        {
          "name": "department",
          "data_type": "TEXT",
          "description": "Department where diagnosed"
        }
      ]
    },
    {
      "name": "population_health",
      "base_table": {
        "database": "TCH_PATIENT_360_POC",
        "schema": "AI_ML", 
        "table": "POPULATION_HEALTH_BASE"
      },
      "description": "Population health metrics and statistics",
      "columns": [
        {
          "name": "report_date",
          "data_type": "DATE",
          "description": "Date of report generation"
        },
        {
          "name": "total_patients",
          "data_type": "NUMBER",
          "description": "Total number of patients"
        },
        {
          "name": "newborn_count",
          "data_type": "NUMBER", 
          "description": "Number of newborn patients"
        },
        {
          "name": "pediatric_percentage",
          "data_type": "NUMBER",
          "description": "Percentage of pediatric patients (1-12 years)"
        },
        {
          "name": "male_percentage",
          "data_type": "NUMBER",
          "description": "Percentage of male patients"
        },
        {
          "name": "medicaid_percentage", 
          "data_type": "NUMBER",
          "description": "Percentage of patients with Medicaid insurance"
        },
        {
          "name": "high_risk_patients",
          "data_type": "NUMBER",
          "description": "Number of high-risk patients"
        },
        {
          "name": "high_risk_percentage",
          "data_type": "NUMBER",
          "description": "Percentage of high-risk patients"
        },
        {
          "name": "asthma_patients",
          "data_type": "NUMBER",
          "description": "Number of patients with asthma"
        },
        {
          "name": "asthma_prevalence_rate",
          "data_type": "NUMBER", 
          "description": "Asthma prevalence rate as percentage"
        },
        {
          "name": "obesity_patients",
          "data_type": "NUMBER",
          "description": "Number of patients with obesity"
        },
        {
          "name": "obesity_prevalence_rate",
          "data_type": "NUMBER",
          "description": "Obesity prevalence rate as percentage"
        }
      ]
    }
  ],
  "measures": [
    {
      "name": "total_patients",
      "description": "Total number of unique patients",
      "expr": "COUNT(DISTINCT patient_analytics.patient_key)",
      "data_type": "NUMBER"
    },
    {
      "name": "total_encounters", 
      "description": "Total number of healthcare encounters",
      "expr": "COUNT(encounter_analytics.encounter_key)",
      "data_type": "NUMBER"
    },
    {
      "name": "average_age",
      "description": "Average patient age",
      "expr": "AVG(patient_analytics.age)",
      "data_type": "NUMBER"
    },
    {
      "name": "average_length_of_stay",
      "description": "Average length of stay in days",
      "expr": "AVG(encounter_analytics.length_of_stay_days)",
      "data_type": "NUMBER"
    },
    {
      "name": "total_charges",
      "description": "Total financial charges",
      "expr": "SUM(encounter_analytics.total_charges)", 
      "data_type": "NUMBER"
    },
    {
      "name": "average_charges_per_encounter",
      "description": "Average charges per encounter",
      "expr": "AVG(encounter_analytics.total_charges)",
      "data_type": "NUMBER"
    },
    {
      "name": "readmission_rate",
      "description": "Percentage of encounters that are readmissions",
      "expr": "COUNT(CASE WHEN encounter_analytics.readmission_flag THEN 1 END) * 100.0 / COUNT(encounter_analytics.encounter_key)",
      "data_type": "NUMBER"
    },
    {
      "name": "chronic_condition_rate",
      "description": "Percentage of diagnoses that are chronic conditions", 
      "expr": "COUNT(CASE WHEN diagnosis_analytics.is_chronic_condition THEN 1 END) * 100.0 / COUNT(diagnosis_analytics.diagnosis_key)",
      "data_type": "NUMBER"
    }
  ],
  "relationships": [
    {
      "name": "patient_encounters",
      "from": {
        "table": "patient_analytics",
        "field": "patient_key"
      },
      "to": {
        "table": "encounter_analytics", 
        "field": "patient_key"
      },
      "type": "one_to_many"
    },
    {
      "name": "encounter_diagnoses",
      "from": {
        "table": "encounter_analytics",
        "field": "encounter_key"
      },
      "to": {
        "table": "diagnosis_analytics",
        "field": "encounter_key"
      },
      "type": "one_to_many"
    }
  ],
  "time_dimensions": [
    {
      "name": "encounter_date",
      "table": "encounter_analytics",
      "field": "encounter_date",
      "granularities": ["day", "week", "month", "quarter", "year"]
    },
    {
      "name": "diagnosis_date", 
      "table": "diagnosis_analytics",
      "field": "diagnosis_date",
      "granularities": ["day", "week", "month", "quarter", "year"]
    }
  ],
  "verified_queries": [
    {
      "name": "Patient Count by Age Group",
      "question": "How many patients do we have in each age group?",
      "sql": "SELECT age_group, COUNT(DISTINCT patient_key) as patient_count FROM patient_analytics GROUP BY age_group ORDER BY patient_count DESC"
    },
    {
      "name": "Monthly Encounter Volume",
      "question": "What is our monthly encounter volume?", 
      "sql": "SELECT encounter_month_year, COUNT(*) as encounter_count FROM encounter_analytics GROUP BY encounter_month_year ORDER BY encounter_month_year"
    },
    {
      "name": "Top Diagnoses",
      "question": "What are the most common diagnoses?",
      "sql": "SELECT condition_category, COUNT(*) as diagnosis_count FROM diagnosis_analytics GROUP BY condition_category ORDER BY diagnosis_count DESC LIMIT 10"
    },
    {
      "name": "Readmission Analysis",
      "question": "What is our readmission rate by department?",
      "sql": "SELECT department, COUNT(CASE WHEN readmission_flag THEN 1 END) * 100.0 / COUNT(*) as readmission_rate FROM encounter_analytics GROUP BY department ORDER BY readmission_rate DESC"
    }
  ]
}';
    
    RETURN semantic_model_json;
END;
$$;

-- =============================================================================
-- SEMANTIC MODEL VALIDATION AND MONITORING
-- =============================================================================

-- Create view for monitoring semantic model usage
CREATE OR REPLACE VIEW CORTEX_ANALYST_USAGE_MONITORING AS
SELECT 
    'TCH_Patient_360_Analytics' AS semantic_model_name,
    COUNT(DISTINCT pm.patient_key) AS total_patients_available,
    COUNT(DISTINCT es.encounter_key) AS total_encounters_available,
    COUNT(DISTINCT df.diagnosis_key) AS total_diagnoses_available,
    MAX(es.encounter_date) AS latest_encounter_date,
    MIN(es.encounter_date) AS earliest_encounter_date,
    DATEDIFF('day', MIN(es.encounter_date), MAX(es.encounter_date)) AS date_range_days,
    CURRENT_TIMESTAMP() AS last_updated
FROM TCH_PATIENT_360_POC.CONFORMED.PATIENT_MASTER pm
LEFT JOIN TCH_PATIENT_360_POC.CONFORMED.ENCOUNTER_SUMMARY es ON pm.patient_key = es.patient_key
LEFT JOIN TCH_PATIENT_360_POC.CONFORMED.DIAGNOSIS_FACT df ON es.encounter_key = df.encounter_key
WHERE pm.is_current = TRUE;

-- =============================================================================
-- SEMANTIC MODEL FILE SETUP
-- =============================================================================

-- Note: The semantic model YAML file (semantic_model.yaml) will be uploaded by the deployment script
-- The SEMANTIC_MODEL_STAGE has been created above and is ready to receive the YAML file



-- Display setup completion
SELECT 'Cortex Analyst semantic model configuration created successfully. Ready for natural language querying.' AS cortex_analyst_status;