# Texas Children's Hospital - Patient 360 Streamlit in Snowflake (SiS) App

This Streamlit in Snowflake (SiS) application provides a comprehensive Patient 360 view for pediatric patients, showcasing Snowflake's native capabilities for healthcare data integration and analytics.

## Features

- **Patient Selection**: Search and select patients from the database
- **Patient 360 View**: Comprehensive patient dashboard with:
  - Demographics and basic information
  - Encounter timeline and history
  - Diagnoses and conditions
  - Lab results and trends
  - Current medications
- **AI-Powered Chat Interface**: Natural language querying using Snowflake Cortex
  - Cortex Analyst for structured data queries
  - Cortex Search for unstructured clinical notes
  - Intelligent routing between data sources
- **Population Cohort Builder**: Create patient cohorts using natural language
- **Interactive Visualizations**: Charts and graphs powered by Plotly

## Deployment Instructions

This is a **Streamlit in Snowflake (SiS)** application that runs natively within the Snowflake environment. It does not require external hosting or authentication setup.

### 1. Deployment via SQL

The application is deployed using SQL commands in Snowflake:

```sql
-- Create the Streamlit app
CREATE STREAMLIT TCH_PATIENT_360_APP
  ROOT_LOCATION = '@TCH_POC_STAGE/streamlit_app/'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'TCH_ANALYTICS_WH';

-- Grant access to roles
GRANT USAGE ON STREAMLIT TCH_PATIENT_360_APP TO ROLE TCH_POC_ROLE;
```

### 2. Upload Application Files

Upload the application files to the Snowflake stage:

```sql
-- Upload main application file
PUT file://app.py @TCH_POC_STAGE/streamlit_app/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Upload utility modules
PUT file://utils/sis_connection.py @TCH_POC_STAGE/streamlit_app/utils/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://utils/sis_cortex_utils.py @TCH_POC_STAGE/streamlit_app/utils/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Upload requirements (if needed)
PUT file://requirements.txt @TCH_POC_STAGE/streamlit_app/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

### 3. Access the Application

Once deployed, the application will be available in Snowsight under the "Streamlit" tab or via direct URL.

## Application Structure

```
streamlit_app/
├── app.py                      # Main SiS application
├── utils/
│   ├── sis_connection.py       # Snowpark session utilities
│   └── sis_cortex_utils.py     # Cortex AI utilities for SiS
├── requirements.txt            # Python dependencies (minimal for SiS)
└── README.md                   # This file
```

## Usage Guide

### Patient Selection

1. Use the sidebar to search for patients by name
2. Select a patient from the dropdown list
3. The main dashboard will load with the patient's data

### Patient Dashboard Tabs

- **Timeline**: View encounter history and timeline
- **Diagnoses**: Review patient's diagnoses and conditions
- **Labs**: Examine lab results and trends
- **Medications**: View current and past medications
- **AI Assistant**: Chat interface for natural language queries
- **Cohort Builder**: Create population cohorts with natural language

### AI Assistant

The AI Assistant can answer questions about:

- **Structured Data**: Demographics, encounter volumes, lab results, medications
  - "How many encounters has this patient had?"
  - "What are the lab results for this patient?"
  - "Show me patients with diabetes"

- **Unstructured Data**: Clinical notes, radiology reports, discharge summaries
  - "What did the doctor write about this patient's condition?"
  - "Find radiology reports mentioning chest pain"
  - "Search clinical notes for medication changes"

### Cohort Builder

Create patient cohorts by describing criteria in natural language:

- "Find all patients with asthma aged 5-12"
- "Show me diabetic patients with recent lab abnormalities"
- "Find patients with multiple emergency department visits"

## Demo Mode

If Snowpark tables are not available, the application will automatically use mock data for demonstration purposes. This allows you to explore the interface and functionality during development or testing.

## Technical Notes

- **Streamlit in Snowflake (SiS)**: Runs natively within Snowflake environment
- **Snowpark Integration**: Uses Snowpark DataFrames for optimal performance
- **Native Cortex Integration**: Direct access to Cortex Analyst and Cortex Search
- **Zero External Dependencies**: No external authentication or hosting required
- **Responsive Design**: Optimized for healthcare workflows
- **Cached Sessions**: Leverages Streamlit's built-in caching for performance

## Support

For technical support or questions about this application, please refer to the main project documentation or contact the development team.