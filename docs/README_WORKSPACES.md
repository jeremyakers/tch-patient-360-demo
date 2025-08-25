# Deploy TCH Patient 360 with Snowsight Workspaces (Git-first, zero uploads)

This guide shows two ways to run the deployment entirely inside Snowflake without local tools:

- Edit-and-Run in Workspace (simplest; executes your live Workspace edits)
- Git-orchestrated (executes files from a Snowflake Git repository object; deterministic by tag/branch)

## 1) Create a Git Workspace (recommended for editing)

1. In Snowsight, go to Projects » Workspaces » From Git repository.
2. Paste your Git URL and choose the API Integration + auth method (OAuth2 or PAT secret).
3. Pick a branch (for example, main) and create. You can now view and edit `sql/` and app sources.

Reference: Create a Git workspace (Snowflake docs)

## 2) One-file Workspace run (edits execute directly)

- Open `sql/00_master.sql` in the Workspace.
- Ensure Role and Warehouse are set to your PoC values inside the script.
- Set `data_size` if needed (small|medium|large).
- Leave `enable_git_mode = false` to run inline.
- Click Run All. This runs the Workspace version you just edited.

## 3) Git-orchestrated run (executes committed files from a repo object)

This runs committed files from a Snowflake Git repository object, allowing pinning to a tag/branch.

One-time admin (documented; not auto-run by users):

    -- Example: create and fetch a repo object
    CREATE OR REPLACE GIT REPOSITORY TCH_P360_REPO
      ORIGIN = 'https://github.com/<org>/<repo>'
      API_INTEGRATION = <your_api_integration>;
    ALTER GIT REPOSITORY TCH_P360_REPO FETCH;

Run `00_master.sql` with Git mode:

- Open `sql/00_master.sql` and set:
  - `enable_git_mode = true`
  - `git_db`, `git_schema`, `git_repo_name`
  - `git_ref_type` (tags|branches) and `git_ref_name` (for example, `v1.0.0`)
- Run All. The script uses `EXECUTE IMMEDIATE FROM @<repo>/...` to run step SQLs.

## 4) Data generation Notebook

- When Git mode is enabled, `00_master.sql` will:
  - Create the Notebook object from the Git repo (`AI_ML.TCH_DATA_GENERATOR`).
  - Execute the Notebook with parameters like `data_size=medium` and `parallel=true`.
- If CREATE NOTEBOOK FROM is not enabled in your account, import the notebook once in Snowsight Notebooks, then re-run EXECUTE NOTEBOOK.

## 5) Streamlit app from Git

- When Git mode is enabled, `00_master.sql` will create the Streamlit app directly from the repo path:
  - `PRESENTATION.TCH_PATIENT_360_APP` with `ROOT_LOCATION=@<repo>/python/streamlit_app/` and `MAIN_FILE='main.py'`.
- No manual staging is required.

## 6) Verification

- Run `sql/99_verification.sql` to validate raw loads, dynamic tables, and Cortex services.

## Notes

- Keep Workspaces for transparency and easy customization.
- Use Git mode for reproducible, pinned deployments (switch tag/branch for upgrades).
- Optional fallbacks: existing bash/PowerShell scripts remain available but are not required for Workspaces.
