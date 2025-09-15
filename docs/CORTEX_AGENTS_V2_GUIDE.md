# Snowflake Cortex Agents v2 Integration Guide

## Overview

This document describes the Cortex Agents v2 integration in the TCH Patient 360 PoC application, including multi-step reasoning, tool orchestration, and conversation threads.

## Key Features

### 1. Multi-Step Reasoning
The v2 Agents API enables sophisticated multi-step thinking:
- Agent breaks down complex queries into logical steps
- Each step is visible in the thinking process
- Automatic fallback strategies when initial approaches fail

### 2. Tool Orchestration
The agent automatically selects and combines multiple tools:
- **Cortex Analyst**: For structured SQL queries against patient data
- **Cortex Search**: For unstructured document searches (clinical notes, radiology reports)
- **Intelligent Routing**: Agent decides which tool(s) to use based on the query

### 3. Conversation Threads (Future Enhancement)
- Maintains context across multiple conversation turns
- Thread management for conversation continuity
- Currently disabled pending further testing

## Architecture

### Components

1. **SQL Setup** (`sql/cortex/03_cortex_agents_setup.sql`)
   - Grants `SNOWFLAKE.CORTEX_AGENT_USER` role
   - Creates `AI_ML.CORTEX_AGENT_CONFIG` table
   - Stores agent configuration

2. **Agent Service** (`python/streamlit_app/services/cortex_agents.py`)
   - Manages persisted agent lifecycle
   - Handles API communication
   - Processes multi-step responses

3. **Response Parser** (`python/streamlit_app/services/cortex_agents_parser.py`)
   - Parses complex v2 API response structure
   - Extracts thinking steps, SQL, and citations
   - Handles streaming event format

4. **UI Integration** (`python/streamlit_app/page_modules/chat_interface.py`)
   - Displays agent responses
   - Shows expandable thinking process
   - Presents SQL queries and citations

## Configuration

### Agent Settings
```python
CORTEX_AGENT_NAME = "TCH_P360_AGENT"
CORTEX_AGENT_MODEL = "claude-3-7-sonnet"
```

### Tools Configuration
```python
tools = [
    {
        "tool_spec": {
            "type": "cortex_analyst_text_to_sql",
            "name": "healthcare_analyst"
        }
    },
    {
        "tool_spec": {
            "type": "cortex_search",
            "name": "clinical_notes_search"
        }
    },
    {
        "tool_spec": {
            "type": "cortex_search",
            "name": "radiology_search"
        }
    }
]
```

### Tool Resources
```python
tool_resources = {
    "healthcare_analyst": {
        "semantic_model_file": "@AI_ML.SEMANTIC_MODEL_STAGE/semantic_model_chat.yaml",
        "execution_environment": {
            "database": "TCH_PATIENT_360_POC",
            "schema": "AI_ML",
            "warehouse": "TCH_AI_ML_WH"
        }
    },
    "clinical_notes_search": {
        "search_service": "AI_ML.CLINICAL_NOTES_SEARCH"
    },
    "radiology_search": {
        "search_service": "AI_ML.RADIOLOGY_SEARCH"
    }
}
```

## API Endpoints

### Agent Management
- **List Agents**: `GET /api/v2/databases/{db}/schemas/{schema}/agents`
- **Create Agent**: `POST /api/v2/databases/{db}/schemas/{schema}/agents`
- **Run Agent**: `POST /api/v2/cortex/agent:run`

### Thread Management (Future)
- **Create Thread**: `POST /api/v2/databases/{db}/schemas/{schema}/agents/{agent}/threads`
- **Delete Thread**: `DELETE /api/v2/databases/{db}/schemas/{schema}/agents/{agent}/threads/{thread}`

## Response Structure

The v2 API returns a streaming response with events:

```json
[
  {
    "event": "response",
    "data": {
      "content": [
        {
          "type": "thinking",
          "thinking": {
            "text": "I need to analyze this request..."
          }
        },
        {
          "type": "tool_use",
          "tool_use": {
            "type": "cortex_analyst_text_to_sql",
            "name": "healthcare_analyst"
          }
        },
        {
          "type": "tool_result",
          "tool_result": {
            "content": [{
              "type": "json",
              "json": {
                "sql": "SELECT ...",
                "result_set": {...}
              }
            }]
          }
        },
        {
          "type": "text",
          "text": "Based on the analysis..."
        }
      ]
    }
  }
]
```

## Debugging

### Telemetry Access
```sql
-- View agent execution logs
SELECT TIMESTAMP, VALUE 
FROM SNOWFLAKE.TELEMETRY.EVENTS_VIEW 
WHERE VALUE LIKE '%AGENTS DEBUG%'
ORDER BY TIMESTAMP DESC;

-- Check for errors
SELECT TIMESTAMP, VALUE 
FROM SNOWFLAKE.TELEMETRY.EVENTS_VIEW 
WHERE VALUE LIKE '%error%' 
OR VALUE LIKE '%failed%'
ORDER BY TIMESTAMP DESC;
```

### Common Issues

1. **404 Error on Persisted Agent Endpoint**
   - Currently using `/api/v2/cortex/agent:run` with agent reference in payload
   - Persisted agent direct endpoint format still being validated

2. **Empty SQL Results**
   - Check if Dynamic Tables have been refreshed
   - Verify data was loaded during deployment
   - Confirm warehouse permissions

3. **Missing Thinking Steps**
   - Ensure response parser is correctly extracting from nested structure
   - Check telemetry logs for full response content

## Rollback Plan

If issues arise with v2 integration:

1. **Switch to Previous Branch**
   ```bash
   git checkout main
   ```

2. **Revert Agent Configuration**
   ```sql
   -- Remove v2 specific grants
   REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER FROM ROLE TCH_PATIENT_360_ROLE;
   
   -- Drop agent config table
   DROP TABLE IF EXISTS AI_ML.CORTEX_AGENT_CONFIG;
   ```

3. **Update Streamlit App**
   - The app will automatically fall back to v1 behavior if agent creation fails
   - Previous Cortex integration remains functional

## Testing

### Manual Testing
1. Open the Streamlit app
2. Navigate to AI Chatbot
3. Try complex queries requiring multi-step reasoning:
   - "Show me the top 5 diagnoses for patients aged 10-15 and create a chart"
   - "Find patients with asthma who had recent ER visits and analyze patterns"

### Verification
- Check for "🧠 Agent Reasoning Process" expandable section
- Verify SQL queries are displayed
- Confirm citations from search results appear
- Review traces in Snowsight under the Agent

## Future Enhancements

1. **Enable Conversation Threads**
   - Implement thread persistence across sessions
   - Add thread management UI

2. **Custom Tools**
   - Add custom Python functions as tools
   - Integrate with external APIs

3. **Advanced Analytics**
   - Population health analysis tools
   - Predictive modeling integration
   - Real-time alerting

## References

- [Cortex Agents Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- [Cortex Agents REST API](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-rest-api)
- [Cortex Agents Threads](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-threads-rest-api)
- [Cortex Agents Tutorials](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-tutorials)
