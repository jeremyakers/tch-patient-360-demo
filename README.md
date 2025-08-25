# TCH Patient 360 PoC – Snowflake Deployment Guide

This repository packages the Texas Children's Hospital Patient 360 PoC for zero‑install deployment inside Snowflake.

## Quick start (Snowsight Workspaces – recommended)

1) Create a Git Workspace
- In Snowsight: Projects → Workspaces → From Git repository
- Repository URL: your Git URL (for example, `https://github.com/jeremyakers/tch-patient-360-demo`)
- Choose your API Integration and auth (OAuth2 or PAT)
- Select the branch (for example, `main`)
- Ref: Snowflake docs – Create a Git workspace: https://docs.snowflake.com/en/user-guide/ui-snowsight/workspaces#label-create-a-git-workspace

2) Open and run the orchestrator
- Open `sql/00_master.sql` in the Workspace
- Set parameters as needed near the top:
  - `data_size` → `small|medium|large`
  - `enable_git_mode` → `false` to execute the Workspace file itself (edits run directly)
- Click Run All

3) Optional: Git‑orchestrated mode (executes from a Snowflake Git repository object)
- If your Snowflake admin created a Git repository object (with FETCH configured), set at the top of `sql/00_master.sql`:
  - `enable_git_mode = true`
  - Configure `git_db`, `git_schema`, `git_repo_name`, `git_ref_type`, `git_ref_name`
- Run All. The script will:
  - Execute step SQLs via `EXECUTE IMMEDIATE FROM @<repo>/...`
  - Create and execute the Notebook from the Git path
  - Create the Streamlit app from the Git path

4) Verify
- Run `sql/99_verification.sql` for a consolidated validation of raw loads, dynamic tables, and Cortex services.

## What gets deployed
- Raw schemas for source systems: `EPIC`, `WORKDAY`, `ORACLE_ERP`, `SALESFORCE`
- Conformed layer via Dynamic Tables (core PoC value)
- Presentation views for analytics and demos
- Cortex Search services for unstructured documents
- Cortex Analyst semantic model (YAML) staged and used by the app
- Streamlit in Snowflake application (Patient 360 app)

## Data generation
- Python is already inside Snowflake as a Notebook: `AI_ML.TCH_DATA_GENERATOR`
- `00_master.sql` can `CREATE NOTEBOOK FROM @repo` (Git mode) and `EXECUTE NOTEBOOK` with parameters:
  - `data_size=small|medium|large`, `parallel=true`
- The Notebook generates and uploads structured/unstructured files directly to internal stages.

## Optional: Snowflake Git repository object route
If your admins prefer running everything from a Snowflake Git repo object (stage‑like path):

```sql
-- Admin once
CREATE OR REPLACE GIT REPOSITORY TCH_P360_REPO
  ORIGIN = 'https://github.com/jeremyakers/tch-patient-360-demo'
  API_INTEGRATION = <api_integration>;
ALTER GIT REPOSITORY TCH_P360_REPO FETCH;

-- Then run from @repo
you_db.you_schema.TCH_P360_REPO/branches/main/sql/00_master.sql
```

Use `EXECUTE IMMEDIATE FROM @"DB"."SCHEMA"."TCH_P360_REPO"/branches/"main"/sql/00_master.sql;` or open `00_master.sql` in a Git Workspace and set `enable_git_mode=true`.

## Optional fallbacks: Bash/PowerShell deploy
If you prefer CLI automation (SnowCLI required):

```bash
# Bash
./deploy/deploy_tch_poc.sh --account <acct> --user <user> --password <pwd> --generate-data-size medium
```

```powershell
# PowerShell
./deploy/deploy_tch_poc.ps1 -Account <acct> -Username <user> -Password <pwd> -DataSize medium
```

The scripts:
- Create database/schemas, file formats, and stages
- Deploy raw structure, conformed Dynamic Tables, presentation views
- Deploy Cortex Search/Analyst, Streamlit app
- Execute Snowflake Notebook for data generation (uploads to stages)

## Parameters & warehouses
- Warehouses used (adjust in SQL/scripts as needed):
  - `TCH_COMPUTE_WH` – data processing and Dynamic Tables
  - `TCH_ANALYTICS_WH` – interactive analytics/Streamlit
  - `TCH_AI_ML_WH` – Cortex operations
- Role: `TCH_PATIENT_360_ROLE`
- Database: `TCH_PATIENT_360_POC`

## Repository layout
- `sql/00_master.sql` – single entrypoint orchestrator (Workspace‑first)
- `sql/` – setup, data_load, dynamic_tables, cortex, verification
- `python/` – Streamlit app and utilities; Snowflake Notebook sources
- `docs/README_WORKSPACES.md` – detailed Workspace/Git guidance
- `design_docs/` – design and planning docs

## Notes
- For Workspaces, your edits are the source of truth in inline mode (no `EXECUTE IMMEDIATE FROM`).
- In Git‑orchestrated mode, commit/push Workspace changes and ensure the repo object FETCHes the new commit.
- Cortex Analyst semantic YAMLs are under `sql/cortex/semantic_model/` and staged by the deploy steps.

