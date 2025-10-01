# Cortex Agents API Bug: Cortex Search Services Not Persisting

## Issue Summary
When creating a Cortex Agent via REST API with Cortex Search tools in `tool_resources`, the API returns `200 OK` with "successfully created", but the Cortex Search services show as `<nil>` in Snowsight UI and the `max_results` parameter is not being honored (always returns 10 results instead of configured 50).

## Environment
- Account: SFSENORTHAMERICA-DEMO_JAKERS
- Database: TCH_PATIENT_360_POC
- Schema: AI_ML
- Agent Name: TCH_P360_AGENT
- Model: claude-3.5-sonnet

## Reproduction Steps

### 1. Verify Cortex Search Services Exist
```sql
SHOW CORTEX SEARCH SERVICES IN SCHEMA TCH_PATIENT_360_POC.AI_ML;
-- Returns:
-- CLINICAL_NOTES_SEARCH (Active)
-- RADIOLOGY_REPORTS_SEARCH (Active)
-- CLINICAL_DOCUMENTATION_SEARCH (Active)
```

### 2. Create Agent via REST API

**Endpoint:**
```
POST /api/v2/databases/TCH_PATIENT_360_POC/schemas/AI_ML/agents
```

**Payload:** (see `agent_creation_payload.json`)

Key fields in `tool_resources`:
```json
"clinical_notes_search": {
  "search_service": "TCH_PATIENT_360_POC.AI_ML.CLINICAL_NOTES_SEARCH",
  "max_results": 50,
  "id_column": "file_path",
  "title_column": "MRN",
  "filter": {}
}
```

**Response:**
```json
{
  "status": "Agent TCH_P360_AGENT successfully created."
}
HTTP Status: 200
```

### 3. Verify in Snowsight UI

Navigate to: Data > AI_ML > Agents > TCH_P360_AGENT > Tools

**Expected:**
- Cortex Search Services section shows:
  - CLINICAL_NOTES_SEARCH with max_results=50
  - RADIOLOGY_REPORTS_SEARCH with max_results=50
  - CLINICAL_DOCUMENTATION_SEARCH with max_results=50

**Actual:**
- Cortex Search Services section shows:
  - CLINICAL_NOTES_SEARCH: Service name = `<nil>`
  - RADIOLOGY_REPORTS_SEARCH: Service name = `<nil>`
  - CLINICAL_DOCUMENTATION_SEARCH: Service name = `<nil>`

### 4. Test Runtime Behavior

Send a query to the agent that triggers Cortex Search:
```
"Search for clinical notes mentioning medication allergies"
```

**Expected:**
- Agent uses cortex_search tool
- Returns up to 50 results (as configured in max_results)

**Actual:**
- Agent uses cortex_search tool
- Returns exactly 10 results (ignoring max_results=50 configuration)
- Agent thinking says: "✅ Found 10 search results"

## Evidence

1. **API Request Logs**: Confirmed payload contains correct `search_service` with fully qualified names and `max_results: 50`
2. **API Response**: Returns 200 OK with "successfully created" message
3. **Snowsight UI**: Shows `<nil>` for service names
4. **Runtime Behavior**: Always returns 10 results regardless of max_results setting

## Expected Behavior

1. Cortex Search services should be properly attached to the agent
2. Service names should display correctly in Snowsight UI
3. `max_results` parameter should be honored at runtime
4. Agent should return up to 50 results when configured with `max_results: 50`

## Suspected Root Cause

The Cortex Agents REST API appears to:
1. Accept the `tool_resources` configuration without error
2. Not properly persist the Cortex Search tool configurations
3. Fall back to a default `max_results` value of 10

This suggests either:
- A bug in the agent creation endpoint that silently ignores Cortex Search tool_resources
- A limitation in the current API version where Cortex Search tools cannot be pre-configured on persisted agents
- A mismatch between the documented API schema and actual implementation

## Related Documentation

- [Cortex Agents REST API - Create Agent](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-rest-api#create-cortex-agent)
- The documentation shows `tool_resources` for Cortex Search should include:
  - `search_service` (fully qualified name)
  - `title_column`
  - `id_column`
  - `filter`
  
Note: Documentation does NOT mention `max_results` as a valid field, but the Snowsight UI provides a field for it when manually adding Cortex Search services.

## Questions for Support

1. Is `max_results` a supported parameter for Cortex Search in `tool_resources` at agent creation time?
2. Why does the API accept the payload but the services show as `<nil>` in the UI?
3. Is there a different API endpoint or method required to attach Cortex Search services to a persisted agent?
4. Should `max_results` be configured at agent creation time or passed at runtime in each request?

