# Payload Comparison: Document Search vs AI Chat

## Document Search (Working - Respects max_results parameter)

**API Endpoint:** `agent:run` (non-persisted, inline configuration)

**Payload:**
```json
{
  "model": "claude-3.5-sonnet",
  "stream": true,
  "messages": [
    {
      "role": "system",
      "content": [
        {
          "type": "text",
          "text": "You are a clinical document search assistant..."
        }
      ]
    },
    {
      "role": "user",
      "content": [
        {
          "type": "text",
          "text": "Search clinical documents for MRN..."
        }
      ]
    }
  ],
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "clinical_document_search"
      }
    }
  ],
  "tool_resources": {
    "clinical_document_search": {
      "name": "TCH_PATIENT_360_POC.AI_ML.CLINICAL_DOCUMENTATION_SEARCH",  ← KEY DIFFERENCE!
      "max_results": 10,  ← KEY DIFFERENCE! This works!
      "id_column": "file_path",
      "title_column": "MRN",
      "filter": {"@eq": {"MRN": "MRN00000001"}}
    }
  }
}
```

**Result:** ✅ Returns requested number of results (10, 15, etc.)

---

## AI Chat (Not Working - Ignores max_results, always returns 10)

**API Endpoint:** `agent:run` (using persisted agent TCH_P360_AGENT)

**Agent Creation Payload:**
```json
{
  "name": "TCH_P360_AGENT",
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "clinical_notes_search"
      }
    }
  ],
  "tool_resources": {
    "clinical_notes_search": {
      "search_service": "TCH_PATIENT_360_POC.AI_ML.CLINICAL_NOTES_SEARCH",  ← KEY DIFFERENCE!
      "id_column": "file_path",
      "title_column": "MRN",
      "filter": {}
    }
  }
}
```

**Runtime Payload:**
```json
{
  "agent": "TCH_P360_AGENT",
  "messages": [
    {
      "role": "user",
      "content": [
        {
          "type": "text",
          "text": "Search for clinical notes mentioning medication allergies"
        }
      ]
    }
  ],
  "tool_resources": {
    "clinical_notes_search": {
      "search_service": "TCH_PATIENT_360_POC.AI_ML.CLINICAL_NOTES_SEARCH",  ← KEY DIFFERENCE!
      "id_column": "file_path",
      "title_column": "MRN",
      "filter": {}
    }
  }
}
```

**Result:** ❌ Always returns exactly 10 results

---

## Key Differences

| Aspect | Document Search | AI Chat |
|--------|----------------|---------|
| **Field name for search service** | `"name"` | `"search_service"` |
| **max_results included** | ✅ Yes | ❌ No (tried, didn't work) |
| **Agent type** | Inline (no persisted agent) | Persisted agent (TCH_P360_AGENT) |
| **Works correctly** | ✅ Yes | ❌ No |

---

## Hypothesis

The REST API for Cortex Agents uses **`"name"`** for the search service, NOT **`"search_service"`**!

The documentation shows `"search_service"` but the actual working implementation uses `"name"`.

This would explain:
1. Why document search works (it uses `"name"`)
2. Why persisted agent shows `<nil>` in UI (it's looking for the wrong field)
3. Why max_results isn't honored (the whole configuration is being ignored)

## Next Step

Change AI Chat to use `"name"` instead of `"search_service"` in both:
1. Agent creation payload
2. Runtime tool_resources payload

