# TCH Patient 360 PoC - SPCS Migration Guide

## Overview

This guide details the migration from standard Streamlit in Snowflake (warehouse runtime) to **Streamlit in Snowflake on Snowpark Container Services (SPCS)** to enable **real Server-Sent Events (SSE) streaming** for Cortex Agents v2.

## Why SPCS is Required

Based on research and confirmation with Snowflake Product Managers:

- **Standard SiS (warehouse runtime)**: Does NOT support SSE streaming due to runtime limitations
- **SiS on SPCS (container runtime)**: DOES support SSE streaming with full HTTP request capabilities
- **Current Status**: SPCS for Streamlit is in Private Preview (PrPr)

## Key Benefits of SPCS Runtime

1. **Real SSE Streaming**: True real-time updates for Cortex Agents thinking steps
2. **Full HTTP Support**: Can use `requests` library and external packages
3. **Better Performance**: Long-running service with faster load times
4. **Latest Streamlit**: Access to newest Streamlit versions from PyPI
5. **Enhanced Caching**: Full support for `st.cache_resource` and `st.cache_data`

## Migration Prerequisites

### Account Requirements
- SPCS must be enabled on the account (Private Preview access)
- Account must have MANAGE COMPUTE privilege capability

### Role Requirements
The deploying role needs:
- `USAGE` on compute pool
- `USAGE` on external access integration  
- `USAGE` on database and `CREATE STREAMLIT` on schema
- `USAGE` on warehouse for query execution

## Migration Steps

### 1. **Setup SPCS Infrastructure**

Run the SPCS setup script:
```sql
-- This is automatically included in sql/00_master.sql
!source sql/setup/05_spcs_streamlit_setup.sql
```

This creates:
- **Compute Pool**: `tch_streamlit_compute_pool` (1-2 nodes, CPU_X64_XS)
- **Network Rule**: `pypi_network_rule` (allows PyPI access)
- **External Access Integration**: `pypi_access_integration`
- **Privilege Grants**: Required permissions for `TCH_PATIENT_360_ROLE`

### 2. **Update Package Management**

The migration includes updated package management files:

**`requirements.txt`** (new, preferred for SPCS):
```txt
streamlit>=1.49
snowflake-snowpark-python
pandas
numpy
python-dateutil
requests
sseclient-py
```

**`pyproject.toml`** (new, SPCS standard):
```toml
[project]
name = "tch-patient-360"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "streamlit>=1.49",
    "snowflake-snowpark-python",
    "pandas",
    "numpy",
    "python-dateutil", 
    "requests",
    "sseclient-py"
]
```

**Note**: `requirements.txt` takes precedence over `pyproject.toml` dependencies.

### 3. **Deploy Updated Application**

The standard deployment process now includes SPCS setup:
```bash
./deploy/deploy_tch_poc.sh --size medium
```

This will:
1. Run the SPCS setup automatically
2. Create the Streamlit app with standard warehouse runtime initially
3. Prepare for SPCS migration

### 4. **Migrate to SPCS Runtime**

After deployment completes, run the migration script:
```sql
!source sql/setup/06_migrate_to_spcs.sql
```

**Alternative: Migrate via Snowsight UI**
1. Go to Projects → Streamlit → Select `TCH_PATIENT_360_APP`
2. Click "More actions" → "App settings"
3. Change "Python environment" to **"Run on container"**
4. Select compute pool: `tch_streamlit_compute_pool`
5. Enable "External networks" → Select `pypi_access_integration`
6. Click "Save"

### 5. **Verify Migration**

Check that the app is running on SPCS:
```sql
DESCRIBE STREAMLIT PRESENTATION.TCH_PATIENT_360_APP;
```

Look for:
- `RUNTIME_NAME`: `'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'`
- `COMPUTE_POOL`: `tch_streamlit_compute_pool`
- `EXTERNAL_ACCESS_INTEGRATIONS`: `(pypi_access_integration)`

## Testing SSE Streaming

After migration, test the Cortex Agents chat interface:

1. **Open the Streamlit App**: Navigate to AI Chatbot page
2. **Ask Complex Questions**: Use queries that trigger multi-step reasoning:
   - "What are the top 3 most expensive conditions for patients under 10?"
   - "Show me patients with high engagement scores and low financial risk"
   - "Find clinical notes mentioning respiratory issues in the last month"

3. **Observe Live Updates**: You should see:
   - ✅ **Thinking steps** appearing in real-time
   - ✅ **Tool usage** (Analyst, Search) shown as it happens  
   - ✅ **SQL queries** displayed as they're generated
   - ✅ **Search results** streaming in
   - ✅ **Final response** assembled progressively

## Troubleshooting

### Common Issues

**1. Migration Fails with Privileges Error**
```sql
-- Check and re-grant privileges
GRANT USAGE ON COMPUTE POOL tch_streamlit_compute_pool TO ROLE TCH_PATIENT_360_ROLE;
GRANT USAGE ON INTEGRATION pypi_access_integration TO ROLE TCH_PATIENT_360_ROLE;
```

**2. Package Installation Errors**
- Verify `requirements.txt` format
- Check external access integration is enabled
- Ensure PyPI network rule allows required domains

**3. App Won't Start on Container**
```sql
-- Check compute pool status
SHOW COMPUTE POOLS;
DESCRIBE COMPUTE POOL tch_streamlit_compute_pool;

-- Check app configuration
DESCRIBE STREAMLIT PRESENTATION.TCH_PATIENT_360_APP;
```

**4. SSE Streaming Still Not Working**
- Verify app is actually running on SPCS runtime (check `RUNTIME_NAME`)
- Check browser developer console for WebSocket/SSE errors
- Test with simple queries first
- Review Snowflake telemetry logs:
```sql
SELECT * FROM SNOWFLAKE.TELEMETRY.EVENTS_VIEW 
WHERE VALUE LIKE '%SSE%' OR VALUE LIKE '%streaming%'
ORDER BY TIMESTAMP DESC;
```

### Rollback Plan

If issues occur, revert to warehouse runtime:

**Via Snowsight:**
1. App settings → "Run on warehouse"
2. Select warehouse: `TCH_ANALYTICS_WH`

**Via SQL:**
```sql
ALTER STREAMLIT PRESENTATION.TCH_PATIENT_360_APP 
SET RUNTIME_NAME = 'SYSTEM$WAREHOUSE_RUNTIME';
```

## Performance Considerations

### SPCS Runtime Characteristics
- **Startup Time**: ~2-3 minutes for initial container build
- **Persistent Service**: Runs for 3 days after last use (no sleep timer)
- **Resource Usage**: One app per compute pool node
- **Scaling**: Can scale compute pool nodes as needed

### Cost Optimization
- **Min Nodes = 1**: Ensures fast startup
- **Max Nodes = 2**: Allows scaling without over-provisioning
- **Instance Family**: CPU_X64_XS appropriate for Streamlit workload
- **Manual Shutdown**: Use Snowsight to shut down when not needed

## Monitoring and Maintenance

### Health Checks
```sql
-- Check compute pool utilization
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COMPUTE_POOL_HISTORY 
WHERE COMPUTE_POOL_NAME = 'TCH_STREAMLIT_COMPUTE_POOL'
ORDER BY START_TIME DESC;

-- Monitor app performance
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE QUERY_TEXT LIKE '%TCH_PATIENT_360_APP%'
ORDER BY START_TIME DESC;
```

### Updates and Maintenance
- **Package Updates**: Modify `requirements.txt` and redeploy
- **Streamlit Updates**: Update version in `requirements.txt`
- **Scaling**: Adjust `MAX_NODES` on compute pool as needed
- **Security**: Regularly review external access integration permissions

## Reference Documentation

- [Snowflake SPCS Streamlit Documentation](https://docs.snowflake.com/LIMITEDACCESS/streamlit/container-runtime)
- [Cortex Agents v2 REST API](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-rest-api)
- [Server-Sent Events Specification](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)

## Files Created/Modified

### New Files
- `sql/setup/05_spcs_streamlit_setup.sql` - SPCS infrastructure setup
- `sql/setup/06_migrate_to_spcs.sql` - Migration script
- `python/streamlit_app/requirements.txt` - SPCS package management
- `python/streamlit_app/pyproject.toml` - SPCS project configuration
- `docs/SPCS_MIGRATION_GUIDE.md` - This documentation

### Modified Files  
- `sql/00_master.sql` - Added SPCS setup step
- `python/streamlit_app/services/cortex_agents_sse.py` - Real SSE streaming implementation
- `python/streamlit_app/environment.yml` - Updated for SPCS compatibility

---

**Next Steps**: After successful migration, the TCH Patient 360 PoC will demonstrate true real-time AI agent interactions, showcasing Snowflake's advanced capabilities that are impossible in traditional SQL Server environments.
