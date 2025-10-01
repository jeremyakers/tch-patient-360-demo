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
  "id_column": "file_path",
  "title_column": "MRN",
  "filter": {}
}
```

**Note:** `max_results` was initially included but removed as it's not documented in the API schema. Removing it did NOT fix the `<nil>` issue.

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
- Returns results using the configured search service

**Actual:**
- Agent uses cortex_search tool
- Returns exactly 10 results (appears to use default limit)
- Agent thinking says: "✅ Found 10 search results"
- Search services are not properly attached (show as `<nil>` in UI)

## Evidence

1. **API Request Logs**: Confirmed payload contains correct `search_service` with fully qualified names
2. **API Response**: Returns 200 OK with "successfully created" message
3. **Snowsight UI**: Shows `<nil>` for service names (even after removing `max_results`)
4. **Runtime Behavior**: Always returns 10 results (default limit)
5. **Configuration Variations Tested**:
   - With `max_results: 50` → `<nil>` services, 10 results
   - Without `max_results` (matching API docs) → `<nil>` services, 10 results

## Expected Behavior

1. Cortex Search services should be properly attached to the agent
2. Service names should display correctly in Snowsight UI (not `<nil>`)
3. Agent should be able to use the configured search services at runtime

## Suspected Root Cause

The Cortex Agents REST API appears to:
1. Accept the `tool_resources` configuration for Cortex Search without error
2. Return 200 OK "successfully created"
3. **But NOT properly persist the Cortex Search tool configurations**
4. The search services show as `<nil>` in Snowsight UI
5. Runtime behavior falls back to default limits (10 results)

This persists **even when the payload exactly matches the documented schema** (without `max_results`).

This suggests either:
- A bug in the agent creation endpoint that silently ignores Cortex Search `tool_resources`
- A limitation where Cortex Search tools cannot be pre-configured on persisted agents via REST API
- The API may only support Cortex Analyst in `tool_resources`, not Cortex Search
- A mismatch between Snowsight UI capabilities and REST API capabilities

## Related Documentation

- [Cortex Agents REST API - Create Agent](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-rest-api#create-cortex-agent)
- The documentation shows `tool_resources` for Cortex Search should include:
  - `search_service` (fully qualified name)
  - `title_column`
  - `id_column`
  - `filter`
  
Note: Documentation does NOT mention `max_results` as a valid field, but the Snowsight UI provides a field for it when manually adding Cortex Search services.

## Questions for Support

1. Is it possible to configure Cortex Search services in `tool_resources` when creating an agent via REST API?
2. Why does the API return 200 "successfully created" but the services show as `<nil>` in Snowsight?
3. Is there a different API endpoint or method required to attach Cortex Search services to a persisted agent?
4. Are Cortex Search tools only configurable via Snowsight UI, not REST API?
5. If REST API configuration is supported, what is the correct payload structure?
6. Is there a way to verify/describe the agent's actual stored configuration via REST API?

