# TCH Patient 360 PoC - SPCS Deployment Guide

## Overview

The TCH Patient 360 PoC is designed to run on **Streamlit in Snowflake with Snowpark Container Services (SPCS)** to enable advanced features including **real-time Server-Sent Events (SSE) streaming** for Cortex Agents v2 interactions.

## Why SPCS is Required

SPCS container runtime provides critical capabilities not available in standard warehouse runtime:

1. **✅ Real SSE Streaming**: True real-time updates for Cortex Agents thinking steps
2. **✅ Full HTTP Support**: Complete `requests` library functionality for external API calls
3. **✅ Latest Streamlit**: Access to newest Streamlit versions from PyPI
4. **✅ Enhanced Performance**: Long-running service with faster load times
5. **✅ Advanced Caching**: Full support for `st.cache_resource` and `st.cache_data`

## Prerequisites

### Account Requirements
- **SPCS enabled** on the Snowflake account (Private Preview access required)
- **ACCOUNTADMIN** privileges for initial setup
- **MANAGE COMPUTE** privilege capability

### Authentication Setup
- **Private key pair** for JWT authentication (included in `keypair/` folder)
- **Snowflake secret** for secure private key storage

## Deployment Architecture

### SPCS Infrastructure Components
1. **Compute Pool**: `TCH_PATIENT_360_POOL` for container runtime
2. **Network Rule**: `pypi_network_rule` for external package access
3. **External Access Integration**: `pypi_access_integration` for PyPI downloads
4. **Keypair Secret**: `keypair_secret` for secure JWT authentication

### Application Components
- **Streamlit App**: Runs on SPCS container runtime
- **Package Management**: Uses `requirements.txt` and `pyproject.toml`
- **Authentication**: JWT tokens generated from private key secret
- **SSE Streaming**: Real-time Cortex Agents interactions

## Step-by-Step Deployment

### 1. **Initial Snowflake Setup**

Run the ACCOUNTADMIN setup:
```sql
-- Execute as ACCOUNTADMIN
@sql/setup/00_accountadmin_setup.sql

-- Grant role to deployment user
GRANT ROLE TCH_PATIENT_360_ROLE TO USER <your_username>;
```

### 2. **Setup Private Key Secret**

**CRITICAL**: Set up the private key secret for JWT authentication:

#### **Step 2a: Read the Private Key**
```bash
cat keypair/rsa_key.p8
```
Copy the entire output (including `-----BEGIN/END-----` lines).

#### **Step 2b: Create the Secret**
```sql
-- Run as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;
USE DATABASE TCH_PATIENT_360_POC;

-- Replace <PRIVATE_KEY_CONTENT> with the actual key content from step 2a
CREATE OR REPLACE SECRET keypair_secret
TYPE = GENERIC_STRING
SECRET_STRING = '<PRIVATE_KEY_CONTENT>';

-- Grant access to the application role
GRANT READ ON SECRET keypair_secret TO ROLE TCH_PATIENT_360_ROLE;
```

#### **Step 2c: Verify Secret Access**
```sql
-- Switch to application role
USE ROLE TCH_PATIENT_360_ROLE;

-- Test reading the secret
SELECT SYSTEM$GET_SECRET('keypair_secret') as private_key_test;
```

### 3. **Deploy the Application**

Run the automated deployment:
```bash
./deploy/deploy_tch_poc.sh --size medium
```

This automatically:
1. **Creates SPCS infrastructure** (compute pools, external access)
2. **Generates mock data** for all source systems (Epic, Workday, Oracle ERP, Salesforce)
3. **Deploys database structure** with Dynamic Tables
4. **Sets up Cortex services** (Analyst, Search, Agents)
5. **Creates Streamlit app** with SPCS container runtime
6. **Configures SSE streaming** with JWT authentication

### 4. **Verify Deployment**

Check that everything is configured correctly:

#### **SPCS Infrastructure**
```sql
-- Check compute pool
SHOW COMPUTE POOLS LIKE 'TCH_PATIENT_360_POOL';

-- Check external access integration
SHOW INTEGRATIONS LIKE 'pypi_access_integration';

-- Check secret
SELECT SYSTEM$GET_SECRET('keypair_secret') as secret_test;
```

#### **Streamlit App Configuration**
```sql
-- Verify SPCS container runtime
DESCRIBE STREAMLIT PRESENTATION.TCH_PATIENT_360_APP;
```

Look for:
- `runtime_name`: `SYSTEM$ST_CONTAINER_RUNTIME_PY3_11`
- `compute_pool`: `TCH_PATIENT_360_POOL`
- `external_access_integrations`: `["PYPI_ACCESS_INTEGRATION"]`

## Testing SSE Streaming

### Access the Application
1. **Navigate to Projects → Streamlit** in Snowsight
2. **Open the TCH Patient 360 app**
3. **Go to AI Chatbot** page

### Test Real-Time Streaming
Try complex queries that trigger multi-step reasoning:

1. **Multi-step Analysis:**
   - "What are the top 5 diagnoses for patients aged 10-15 and create a chart showing their distribution"

2. **Tool Orchestration:**
   - "Find patients with asthma who had recent ER visits and analyze their medication patterns"

3. **Cross-source Queries:**
   - "Show me high-engagement patients with low financial risk and their recent clinical notes"

### Expected Behavior
You should see:
- ✅ **Live thinking steps** appearing in real-time
- ✅ **Tool usage indicators** (Cortex Analyst, Search)
- ✅ **Progressive SQL generation** and execution
- ✅ **Streaming search results** from clinical documents
- ✅ **Final response assembly** happening progressively

## Key Features Enabled

### Multi-Step Reasoning
- Agent breaks down complex queries into logical steps
- Each step visible in real-time thinking process
- Automatic tool selection and orchestration

### Tool Integration
- **Cortex Analyst**: Generates SQL for structured data queries
- **Cortex Search**: Searches clinical documents and reports
- **Intelligent Routing**: Agent decides which tools to use

### Real-Time Updates
- **Thinking Process**: See agent reasoning as it happens
- **Tool Execution**: Watch tools being called and results returned
- **Progressive Assembly**: Final response builds up naturally

## Troubleshooting

### Common Issues

**1. App Won't Start**
- Check compute pool status: `SHOW COMPUTE POOLS`
- Verify external access integration: `SHOW INTEGRATIONS`
- Check package installation logs in Snowsight

**2. Authentication Errors**
- Verify keypair secret exists: `SELECT SYSTEM$GET_SECRET('keypair_secret')`
- Check role permissions: `SHOW GRANTS TO ROLE TCH_PATIENT_360_ROLE`
- Confirm private key format is correct

**3. SSE Streaming Not Working**
- Check browser developer console for errors
- Verify JWT token generation in application logs
- Test with simple queries first

**4. Package Installation Failures**
- Verify external access integration is enabled in app settings
- Check PyPI network rule allows required domains
- Review container build logs in Snowsight

### Performance Considerations

**Container Startup:**
- **Initial build**: ~2-3 minutes for first container creation
- **Package installation**: Additional time for dependencies
- **Subsequent starts**: Much faster (~30 seconds)

**Resource Usage:**
- **Compute pool**: One app per node
- **Memory**: Sufficient for healthcare data processing
- **Network**: External access for PyPI and REST API calls

## Maintenance

### Updates and Changes
- **Code updates**: Use refresh button in Streamlit to pull latest Git changes
- **Package updates**: Modify `requirements.txt` and redeploy
- **Configuration changes**: Update app settings in Snowsight

### Monitoring
```sql
-- Check compute pool utilization
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COMPUTE_POOL_HISTORY 
WHERE COMPUTE_POOL_NAME = 'TCH_PATIENT_360_POOL'
ORDER BY START_TIME DESC;

-- Monitor app performance
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE QUERY_TEXT LIKE '%TCH_PATIENT_360%'
ORDER BY START_TIME DESC;
```

## Reference Documentation

- [Snowflake SPCS Streamlit Documentation](https://docs.snowflake.com/LIMITEDACCESS/streamlit/container-runtime)
- [Cortex Agents v2 REST API](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-rest-api)
- [Snowflake REST API Authentication](https://docs.snowflake.com/en/developer-guide/snowflake-rest-api/authentication)
- [Server-Sent Events Specification](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)

## Files and Components

### New SPCS-Specific Files
- `sql/setup/05_spcs_streamlit_setup.sql` - SPCS infrastructure setup
- `sql/setup/07_setup_keypair_secret.sql` - Keypair secret setup
- `python/streamlit_app/requirements.txt` - SPCS package management
- `python/streamlit_app/pyproject.toml` - SPCS project configuration
- `python/streamlit_app/utils/jwt_auth.py` - JWT token generation
- `python/streamlit_app/utils/snowflake_api.py` - Cross-runtime API handling

### Enhanced Components
- `python/streamlit_app/services/cortex_agents_sse.py` - Real SSE streaming
- `python/streamlit_app/page_modules/chat_interface.py` - Live UI updates
- `sql/00_master.sql` - Integrated SPCS setup

---

**Result**: The TCH Patient 360 PoC demonstrates advanced real-time AI capabilities that showcase Snowflake's cutting-edge features, impossible to achieve in traditional SQL Server environments.
