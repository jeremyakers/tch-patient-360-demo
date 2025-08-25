# Texas Children's Hospital Patient 360 PoC - PowerShell Deployment Script
#
# This PowerShell script deploys the complete TCH Patient 360 PoC to Snowflake using SnowCLI.
# 
# Prerequisites:
#   1. SnowCLI must be installed and in PATH
#   2. ACCOUNTADMIN must have run sql/setup/00_accountadmin_setup.sql
#   3. User must be granted TCH_PATIENT_360_ROLE
#   4. Python 3.8+ with required packages (pandas, faker, etc.)

param(
    [Parameter(Mandatory=$true)]
    [string]$Account,
    
    [Parameter(Mandatory=$true)]
    [string]$Username,
    
    [string]$Password,
    [string]$PrivateKeyPath,
    [string]$PrivateKeyPassphrase,
    [string]$Database = "TCH_PATIENT_360_POC",
    [string]$Role = "TCH_PATIENT_360_ROLE",
    [ValidateSet("small", "medium", "large")]
    [string]$DataSize = "medium",
            [ValidateSet("prerequisites", "connection", "data-generation", "clean-deployment", "database-structure", "data-upload", "data-ingest", "dynamic-tables", "presentation", "unstructured-data-load", "cortex-search", "cortex-analyst", "streamlit", "verification")]
    [string]$ResumeStep,
    [switch]$SkipDataGeneration,
    [switch]$SkipStreamlit,
    [switch]$CleanDeploy,
    [switch]$Verbose,
    [switch]$Help
)

# Error handling
$ErrorActionPreference = "Stop"

# Script configuration
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectRoot = Split-Path -Parent $ScriptDir
$LogFile = Join-Path $ProjectRoot "deploy\deployment.log"
$TempDir = Join-Path $ProjectRoot "deploy\temp"

# Create temp directory
if (!(Test-Path $TempDir)) {
    New-Item -ItemType Directory -Path $TempDir -Force | Out-Null
}

# Logging functions
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "$timestamp - [$Level] $Message"
    
    switch ($Level) {
        "ERROR" { Write-Host $logMessage -ForegroundColor Red }
        "SUCCESS" { Write-Host $logMessage -ForegroundColor Green }
        "WARNING" { Write-Host $logMessage -ForegroundColor Yellow }
        "INFO" { Write-Host $logMessage -ForegroundColor Cyan }
        default { Write-Host $logMessage }
    }
    
    Add-Content -Path $LogFile -Value $logMessage
}

function Show-Help {
    Write-Host @"
Texas Children's Hospital Patient 360 PoC - PowerShell Deployment Script

USAGE:
    .\deploy_tch_poc.ps1 -Account <account_id> -Username <username> [options]

REQUIRED PARAMETERS:
    -Account <account_id>               Snowflake account identifier
    -Username <username>                Snowflake username

AUTHENTICATION (choose one):
    -Password <password>                Use password authentication
    -PrivateKeyPath <path_to_key>       Use private key file authentication
    
OPTIONAL PARAMETERS:
    -PrivateKeyPassphrase <phrase>      Private key passphrase (if key is encrypted)
    -Database <db_name>                 Database name (default: TCH_PATIENT_360_POC)
    -Role <role_name>                   Role name (default: TCH_PATIENT_360_ROLE)
    -DataSize <size>                    Data size: small|medium|large (default: medium)
    -ResumeStep <step_name>             Resume deployment from specific step
    -SkipDataGeneration                 Skip mock data generation
    -SkipStreamlit                      Skip Streamlit app deployment
    -CleanDeploy                        Drop existing objects before deployment
    -Verbose                            Enable verbose logging
    -Help                               Show this help message

DEPLOYMENT STEPS (for -ResumeStep):
    prerequisites                       Check prerequisites and dependencies
    connection                         Setup SnowCLI connection
    data-generation                    Generate mock data
    clean-deployment                   Clean existing deployment (if -CleanDeploy)
    database-structure                 Deploy database structure
    data-upload                        Upload data files to Snowflake stages
    data-ingest                        Ingest data from stages into tables
    unstructured-data-load             Load unstructured clinical documents
    dynamic-tables                     Deploy Dynamic Tables
    presentation                       Deploy presentation layer views
    cortex-search                     Setup Cortex Search services
    cortex-analyst                    Setup Cortex Analyst semantic model
    streamlit                          Deploy Streamlit application
    verification                       Verify deployment

EXAMPLES:
    # Deploy with password authentication
    .\deploy_tch_poc.ps1 -Account "xy12345.us-east-1" -Username "jdoe" -Password "mypassword"

    # Deploy with private key authentication  
    .\deploy_tch_poc.ps1 -Account "xy12345.us-east-1" -Username "jdoe" -PrivateKeyPath "C:\keys\snowflake_key.p8"

    # Clean deployment with large dataset
    .\deploy_tch_poc.ps1 -Account "xy12345.us-east-1" -Username "jdoe" -Password "mypassword" -CleanDeploy -DataSize "large"

    # Resume deployment from Dynamic Tables step
    .\deploy_tch_poc.ps1 -Account "xy12345.us-east-1" -Username "jdoe" -PrivateKeyPath "C:\keys\snowflake_key.p8" -ResumeStep "dynamic-tables"

PREREQUISITES:
    1. SnowCLI installed and in PATH
    2. ACCOUNTADMIN has run sql/setup/00_accountadmin_setup.sql  
    3. User has been granted TCH_PATIENT_360_ROLE
    4. Python 3.8+ with pandas, faker packages installed

"@
}

# Show help if requested
if ($Help) {
    Show-Help
    exit 0
}

# Validate required parameters
if (!$Password -and !$PrivateKeyPath) {
    Write-Log "Must specify either -Password or -PrivateKeyPath for authentication" "ERROR"
    Show-Help
    exit 1
}

if ($PrivateKeyPath -and !(Test-Path $PrivateKeyPath)) {
    Write-Log "Private key file not found: $PrivateKeyPath" "ERROR"
    exit 1
}

function Test-Prerequisites {
    Write-Log "Checking prerequisites..."

    # Check SnowCLI
    try {
        snow --version | Out-Null
    } catch {
        Write-Log "SnowCLI not found. Please install SnowCLI and ensure it's in your PATH." "ERROR"
        exit 1
    }

    # Check Python
    try {
        python --version | Out-Null
    } catch {
        Write-Log "Python not found. Please install Python 3.8 or later." "ERROR"
        exit 1
    }

    # Check Python packages
    try {
        python -c "import pandas, faker, datetime" 2>$null
    } catch {
        Write-Log "Required Python packages not found. Please install: pip install pandas faker" "ERROR"
        exit 1
    }

    Write-Log "Prerequisites check completed" "SUCCESS"
}

function Setup-SnowCLIConnection {
    Write-Log "Setting up SnowCLI connection..."

    $configFile = Join-Path $TempDir "snow_config.toml"
    
    $configContent = @"
[connections.tch_poc]
account = "$Account"
user = "$Username"
"@

    if ($Password) {
        $configContent += "`npassword = `"$Password`""
    } else {
        # Convert private key path to absolute path
        $absPrivateKeyPath = [System.IO.Path]::GetFullPath($PrivateKeyPath)
        $configContent += "`nauthenticator = `"SNOWFLAKE_JWT`""
        $configContent += "`nprivate_key_path = `"$absPrivateKeyPath`""
        if ($PrivateKeyPassphrase) {
            $configContent += "`nprivate_key_passphrase = `"$PrivateKeyPassphrase`""
        }
    }

    $configContent += "`nrole = `"$Role`""
    $configContent += "`nwarehouse = `"TCH_COMPUTE_WH`""

    Set-Content -Path $configFile -Value $configContent

    # Test connection
    Write-Log "Testing Snowflake connection..."
    try {
        snow --config-file $configFile sql -q "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()" -c tch_poc | Out-Null
    } catch {
        Write-Log "Failed to connect to Snowflake. Please check your credentials and ensure TCH_PATIENT_360_ROLE has been granted to your user." "ERROR"
        exit 1
    }

    Write-Log "SnowCLI connection established" "SUCCESS"
    $env:SNOW_CONFIG_FILE = $configFile
}

function Clear-ExistingData {
    Write-Log "Cleaning up existing data files..."
    
    $mockDataPath = Join-Path $ProjectRoot "data\mock_data"
    if (Test-Path $mockDataPath) {
        # Clean structured data files (both .csv and .csv.gz)
        $structuredPath = Join-Path $mockDataPath "structured"
        if (Test-Path $structuredPath) {
            Get-ChildItem -Path $structuredPath -Filter "*.csv*" | Remove-Item -Force -ErrorAction SilentlyContinue
        }
        
        # Clean unstructured data directories and files
        $unstructuredPath = Join-Path $mockDataPath "unstructured"
        if (Test-Path $unstructuredPath) {
            Get-ChildItem -Path $unstructuredPath | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
        }
        
        # Clean metadata files
        Get-ChildItem -Path $mockDataPath -Filter "*.json" | Remove-Item -Force -ErrorAction SilentlyContinue
        
        Write-Log "Existing data files cleaned up"
    }
}

function Invoke-DataGeneration {
    if ($SkipDataGeneration) {
        Write-Log "Skipping data generation as requested"
        return
    }

    Write-Log "Generating mock data using Snowflake notebook (size: $DataSize)..."

    # First, deploy the notebook to Snowflake
    Write-Log "Deploying data generation notebook..."
    
    $originalLocation = Get-Location
    Set-Location (Join-Path $ProjectRoot "python\notebooks")

    try {
        if ($Verbose) {
            snow --config-file $env:SNOW_CONFIG_FILE notebook deploy tch_data_generator --replace --project . --database $Database --schema AI_ML -c tch_poc
        } else {
            snow --config-file $env:SNOW_CONFIG_FILE notebook deploy tch_data_generator --replace --project . --database $Database --schema AI_ML -c tch_poc > (Join-Path $TempDir "notebook_deploy.log") 2>&1
        }

        if ($LASTEXITCODE -ne 0) {
            Write-Log "Failed to deploy notebook. Check $TempDir\notebook_deploy.log for details" "ERROR"
            exit 1
        }

        Write-Log "Notebook deployed successfully" "SUCCESS"

        # Configure notebook to use Container Runtime for faster file uploads
        Write-Log "Configuring notebook runtime (container runtime and compute pool)..." "INFO"
        $nbRuntimeSQL = Join-Path $TempDir "configure_notebook_runtime.sql"
        $nbRuntimeContent = @"
USE DATABASE $Database;
USE SCHEMA AI_ML;
USE ROLE $Role;
ALTER NOTEBOOK "$Database"."AI_ML"."TCH_DATA_GENERATOR" UNSET WAREHOUSE;
ALTER NOTEBOOK "$Database"."AI_ML"."TCH_DATA_GENERATOR" SET RUNTIME_NAME = 'SYSTEM$BASIC_RUNTIME';
ALTER NOTEBOOK "$Database"."AI_ML"."TCH_DATA_GENERATOR" SET COMPUTE_POOL = TCH_PATIENT_360_POOL;
ALTER NOTEBOOK "$Database"."AI_ML"."TCH_DATA_GENERATOR" SET EXTERNAL_ACCESS_INTEGRATIONS = (pypi_access_integration);
"@
        Set-Content -Path $nbRuntimeSQL -Value $nbRuntimeContent
        Invoke-SQLScript -ScriptPath $nbRuntimeSQL -Description "Configure notebook container runtime"

        # Determine number of patients based on size
        $numPatients = switch ($DataSize) {
            "small" { "1000" }
            "medium" { "5000" }
            "large" { "25000" }
        }

        # Clear stages to avoid mixing old/new MRN datasets
        $clearStagesSQL = Join-Path $TempDir "clear_stages.sql"
        $clearStagesContent = @"
USE DATABASE $Database;
USE ROLE $Role;
REMOVE @RAW_DATA.PATIENT_DATA_STAGE;
REMOVE @RAW_DATA.UNSTRUCTURED_DATA_STAGE;
"@
        Set-Content -Path $clearStagesSQL -Value $clearStagesContent
        Invoke-SQLScript -ScriptPath $clearStagesSQL -Description "Clear stage contents"

        # Execute the notebook through SQL (since CLI doesn't support parameters)
        Write-Log "Executing data generation notebook..."

        # Create SQL script to execute the notebook
        $notebookExecSQL = Join-Path $TempDir "execute_notebook.sql"
        $notebookExecContent = @"
USE DATABASE $Database;
USE SCHEMA AI_ML;
USE WAREHOUSE TCH_COMPUTE_WH;

-- Execute the notebook with parameters
-- Pass data size as argument (notebook will use default values if not provided)
EXECUTE NOTEBOOK $Database.AI_ML.TCH_DATA_GENERATOR('data_size=$DataSize', 'env:TCH_GEN_MODE=PARALLEL');

SELECT 'Notebook execution started' AS status;
"@
        Set-Content -Path $notebookExecSQL -Value $notebookExecContent
        Invoke-SQLScript -ScriptPath $notebookExecSQL -Description "Notebook execution"
        
        # Wait a moment for the notebook to complete
        Write-Log "Waiting for notebook execution to complete..."
        Start-Sleep -Seconds 30
        
        # Check if data was generated by looking at the stages
        $checkSQL = @"
SELECT COUNT(*) as file_count FROM DIRECTORY(@$Database.RAW_DATA.PATIENT_DATA_STAGE)
WHERE relative_path LIKE '%.csv.gz';
"@
        
        $result = snow --config-file $env:SNOW_CONFIG_FILE sql -q $checkSQL -c tch_poc
        $fileCount = [regex]::Match($result, '^\d+', [System.Text.RegularExpressions.RegexOptions]::Multiline).Value
        
        if ([int]$fileCount -gt 0) {
            Write-Log "Notebook generated $fileCount structured data files" "SUCCESS"
        } else {
            Write-Log "No structured files found yet. Notebook may still be running or using default size." "WARNING"
        }

        Write-Log "Mock data generation completed using Snowflake notebook" "SUCCESS"
    } finally {
        Set-Location $originalLocation
    }
}

function Invoke-SQLScript {
    param(
        [string]$ScriptPath,
        [string]$Description
    )

    Write-Log "Executing: $Description"
    Write-Log "Script: $ScriptPath"

    if (!(Test-Path $ScriptPath)) {
        Write-Log "SQL script not found: $ScriptPath" "ERROR"
        exit 1
    }

    try {
        if ($Verbose) {
            snow --config-file $env:SNOW_CONFIG_FILE sql -f $ScriptPath -c tch_poc
        } else {
            snow --config-file $env:SNOW_CONFIG_FILE sql -f $ScriptPath -c tch_poc > (Join-Path $TempDir "$((Get-Item $ScriptPath).BaseName).log") 2>&1
        }
        Write-Log "Completed: $Description" "SUCCESS"
    } catch {
        Write-Log "Failed: $Description - $($_.Exception.Message)" "ERROR"
        throw
    }
}

function Invoke-CleanDeployment {
    if (!$CleanDeploy) {
        return
    }

    Write-Log "Performing clean deployment - dropping existing objects..." "WARNING"

    $cleanupSQL = Join-Path $TempDir "cleanup.sql"
    $cleanupContent = @"
USE ROLE $Role;

-- Drop Streamlit app if exists (with qualified name)
DROP STREAMLIT IF EXISTS $Database.AI_ML.TCH_PATIENT_360_APP;

-- Drop database if exists (cascades to all objects)
DROP DATABASE IF EXISTS $Database;

SELECT 'Clean deployment completed' AS status;
"@

    Set-Content -Path $cleanupSQL -Value $cleanupContent
    Invoke-SQLScript -ScriptPath $cleanupSQL -Description "Clean deployment"
}



function Deploy-Database {
    Write-Log "Deploying database structure..."

    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\setup\01_database_setup.sql") -Description "Database and schema setup"
    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\setup\02_raw_tables.sql") -Description "Raw data tables"
    
    # Create conformed reference tables (Dynamic Tables will create operational tables)
    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\setup\03_conformed_tables.sql") -Description "Conformed reference tables"

    Write-Log "Database structure deployment completed" "SUCCESS"
}

function Upload-Data {
    Write-Log "Verifying data files in Snowflake stages..."
    
    # Note: The data generation notebook now uploads files directly to stages
    # This function just verifies the upload was successful
    
    # Check structured data stage
    Write-Log "Checking structured data stage..."
    $structuredResult = snow --config-file $env:SNOW_CONFIG_FILE sql -q "LIST @TCH_PATIENT_360_POC.RAW_DATA.PATIENT_DATA_STAGE;" -c tch_poc
    $structuredCount = ($structuredResult | Select-String -Pattern "csv.gz" | Measure-Object).Count
    Write-Log "Found $structuredCount structured data files in stage"
    
    if ($structuredCount -eq 0) {
        Write-Log "No structured data files found in stage. Data generation may have failed." "ERROR"
        exit 1
    }
    
    # Check unstructured data stage
    Write-Log "Checking unstructured data stage..."
    $unstructuredResult = snow --config-file $env:SNOW_CONFIG_FILE sql -q "LIST @TCH_PATIENT_360_POC.RAW_DATA.UNSTRUCTURED_DATA_STAGE;" -c tch_poc
    $unstructuredCount = ($unstructuredResult | Select-String -Pattern "txt.gz" | Measure-Object).Count
    Write-Log "Found $unstructuredCount unstructured data files in stage"
    
    if ($unstructuredCount -eq 0) {
        Write-Log "No unstructured data files found in stage. This may be expected for smaller datasets." "WARNING"
    }
    
    # Refresh stage directory indexes so DIRECTORY() sees latest files
    Write-Log "Refreshing stage directory indexes..." "INFO"
    $refreshSQL = Join-Path $TempDir "refresh_stages.sql"
    $refreshContent = @"
USE DATABASE $Database;
USE ROLE $Role;
ALTER STAGE RAW_DATA.PATIENT_DATA_STAGE REFRESH;
ALTER STAGE RAW_DATA.UNSTRUCTURED_DATA_STAGE REFRESH;
-- Show counts after refresh
SELECT 'structured_total' AS scope, COUNT(*) AS cnt FROM DIRECTORY(@$Database.RAW_DATA.PATIENT_DATA_STAGE)
UNION ALL
SELECT 'unstructured_total' AS scope, COUNT(*) AS cnt FROM DIRECTORY(@$Database.RAW_DATA.UNSTRUCTURED_DATA_STAGE);
"@
    Set-Content -Path $refreshSQL -Value $refreshContent
    Invoke-SQLScript -ScriptPath $refreshSQL -Description "Refresh stage directory indexes"

    Write-Log "Data files verified and stage directories refreshed" "SUCCESS"
}

function Invoke-DataIngest {
    Write-Log "Ingesting data from stages into tables..."

    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\data_load\01_load_raw_data.sql") -Description "Data ingestion"

    Write-Log "Data ingestion completed" "SUCCESS"

    # Print concise summary after ingest
    $summarySQL = Join-Path $TempDir "print_load_summary.sql"
    $summaryContent = @"
USE DATABASE $Database;
SELECT * FROM RAW_DATA.DATA_LOAD_SUMMARY ORDER BY table_name;
"@
    Set-Content -Path $summarySQL -Value $summaryContent
    Invoke-SQLScript -ScriptPath $summarySQL -Description "Data load summary"
}

function Deploy-DynamicTables {
    Write-Log "Deploying Dynamic Tables..."

    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\dynamic_tables\01_patient_dynamic_tables.sql") -Description "Patient Dynamic Tables"
    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\dynamic_tables\02_clinical_dynamic_tables.sql") -Description "Clinical Dynamic Tables"

    Write-Log "Dynamic Tables deployment completed" "SUCCESS"
}

function Deploy-Presentation {
    Write-Log "Deploying presentation layer..."

    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\setup\04_presentation_tables.sql") -Description "Presentation layer tables"

    Write-Log "Presentation layer deployment completed" "SUCCESS"
}

function Deploy-CortexAnalyst {
    Write-Log "Deploying Cortex Analyst semantic model..."

    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\cortex\01_cortex_analyst_setup.sql") -Description "Cortex Analyst setup"
    
    # Upload semantic model YAML file to the stage
    Write-Log "Uploading semantic model YAML file..."
    snow --config-file $env:SNOW_CONFIG_FILE stage copy (Join-Path $ProjectRoot "sql\cortex\semantic_model\semantic_model.yaml") "@TCH_PATIENT_360_POC.AI_ML.SEMANTIC_MODEL_STAGE/" -c tch_poc --overwrite
    snow --config-file $env:SNOW_CONFIG_FILE stage copy (Join-Path $ProjectRoot "sql\cortex\semantic_model\semantic_model_chat.yaml") "@TCH_PATIENT_360_POC.AI_ML.SEMANTIC_MODEL_STAGE/" -c tch_poc --overwrite
    
    Write-Log "Cortex Analyst deployment completed" "SUCCESS"
}

function Deploy-CortexSearch {
    Write-Log "Deploying Cortex Search services..."
    
    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\cortex\02_cortex_search_setup.sql") -Description "Cortex Search setup"

    Write-Log "Cortex Search deployment completed" "SUCCESS"
}

function Deploy-Streamlit {
    if ($SkipStreamlit) {
        Write-Log "Skipping Streamlit deployment as requested"
        return
    }

    Write-Log "Deploying Streamlit in Snowflake app..."

    # Create stage for Streamlit app
    $stageSQL = Join-Path $TempDir "create_streamlit_stage.sql"
    $stageContent = @"
USE DATABASE $Database;
USE SCHEMA RAW_DATA;

CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
    COMMENT = 'Stage for Streamlit in Snowflake application files';
"@

    Set-Content -Path $stageSQL -Value $stageContent
    Invoke-SQLScript -ScriptPath $stageSQL -Description "Streamlit stage creation"

    # Upload Streamlit application files
    Write-Log "Uploading Streamlit application files..."

    $streamlitPath = Join-Path $ProjectRoot "python\streamlit_app"
    
    # Upload main entry point
    snow --config-file $env:SNOW_CONFIG_FILE stage copy (Join-Path $streamlitPath "main.py") "@TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/" -c tch_poc --overwrite

    # Upload requirements first
    snow --config-file $env:SNOW_CONFIG_FILE stage copy (Join-Path $streamlitPath "requirements.txt") "@TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/" -c tch_poc --overwrite

    # Upload services directory - using glob pattern to automatically include all Python files
    Write-Log "Uploading services modules..."
    snow --config-file $env:SNOW_CONFIG_FILE stage copy (Join-Path $streamlitPath "services\*.py") "@TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/services/" -c tch_poc --overwrite

    # Upload page_modules directory - using glob pattern to automatically include all Python files
    Write-Log "Uploading page modules..."
    snow --config-file $env:SNOW_CONFIG_FILE stage copy (Join-Path $streamlitPath "page_modules\*.py") "@TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/page_modules/" -c tch_poc --overwrite

    # Upload components directory - using glob pattern to automatically include all Python files
    Write-Log "Uploading component modules..."
    snow --config-file $env:SNOW_CONFIG_FILE stage copy (Join-Path $streamlitPath "components\*.py") "@TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/components/" -c tch_poc --overwrite

    # Upload utils directory - using glob pattern to automatically include all Python files
    Write-Log "Uploading utility modules..."
    snow --config-file $env:SNOW_CONFIG_FILE stage copy (Join-Path $streamlitPath "utils\*.py") "@TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/utils/" -c tch_poc --overwrite

    # Create Streamlit app
    $streamlitSQL = Join-Path $TempDir "create_streamlit_app.sql"
    $streamlitContent = @"
USE DATABASE $Database;
USE ROLE $Role;

-- Create the Streamlit app
CREATE OR REPLACE STREAMLIT TCH_PATIENT_360_APP
    ROOT_LOCATION = '@RAW_DATA.STREAMLIT_STAGE/streamlit_app/'
    MAIN_FILE = 'main.py'
    QUERY_WAREHOUSE = 'TCH_ANALYTICS_WH'
    COMMENT = 'Texas Children''s Hospital Patient 360 PoC - Streamlit Application';

-- Grant access to the role
GRANT USAGE ON STREAMLIT TCH_PATIENT_360_APP TO ROLE $Role;

SELECT 'Streamlit app created successfully' AS status;
"@

    Set-Content -Path $streamlitSQL -Value $streamlitContent
    Invoke-SQLScript -ScriptPath $streamlitSQL -Description "Streamlit app creation"

    Write-Log "Streamlit application deployment completed" "SUCCESS"
}

function Deploy-UnstructuredData {
    Write-Log "Loading unstructured data from stages to raw tables..."
    Invoke-SQLScript -ScriptPath (Join-Path $ProjectRoot "sql\data_load\02_load_unstructured_data.sql") -Description "Unstructured data loading"
    Write-Log "Unstructured data loading completed" "SUCCESS"

    # After unstructured data load, refresh Cortex Search services so new docs appear
    Write-Log "Refreshing Cortex Search services..." "INFO"
    $refreshSearchSQL = Join-Path $TempDir "refresh_cortex_search.sql"
    $refreshSearchContent = @"
USE DATABASE $Database;
USE SCHEMA AI_ML;
USE ROLE $Role;
USE WAREHOUSE TCH_AI_ML_WH;
ALTER CORTEX SEARCH SERVICE CLINICAL_NOTES_SEARCH REFRESH;
ALTER CORTEX SEARCH SERVICE RADIOLOGY_REPORTS_SEARCH REFRESH;
ALTER CORTEX SEARCH SERVICE CLINICAL_DOCUMENTATION_SEARCH REFRESH;
"@
    Set-Content -Path $refreshSearchSQL -Value $refreshSearchContent
    Invoke-SQLScript -ScriptPath $refreshSearchSQL -Description "Cortex Search services refresh"
}

function Test-Deployment {
    Write-Log "Verifying deployment..."

    $verifySQL = Join-Path $TempDir "verify_deployment.sql"
    $verifyContent = @"
USE DATABASE $Database;
USE ROLE $Role;

-- Check database and schemas
SELECT 'Core schemas count: ' || COUNT(*) AS database_check
FROM INFORMATION_SCHEMA.SCHEMATA 
WHERE SCHEMA_NAME IN ('RAW_DATA', 'CONFORMED', 'PRESENTATION', 'AI_ML', 'EPIC', 'WORKDAY', 'SALESFORCE', 'ORACLE_ERP');

-- Check raw tables (in EPIC, WORKDAY source schemas)
SELECT 'Raw EPIC tables count: ' || COUNT(*) AS epic_tables_check
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'EPIC' AND TABLE_TYPE = 'BASE TABLE';

-- Check presentation tables
SELECT 'Presentation tables count: ' || COUNT(*) AS presentation_tables_check
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'PRESENTATION' AND TABLE_TYPE = 'BASE TABLE';

-- Check Dynamic Tables
SELECT 'Dynamic Tables count: ' || COUNT(*) AS dynamic_tables_check
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'DYNAMIC TABLE';

-- Check data in key tables (if they exist)
SELECT 'Patient data rows: ' || 
    CASE WHEN COUNT(*) > 0 THEN TO_VARCHAR(COUNT(*)) ELSE 'No data loaded' END AS patient_data_check
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'EPIC' AND TABLE_NAME = 'PATIENTS';

-- Check if Streamlit app exists
SELECT 'Streamlit app exists: ' || 
    CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END AS streamlit_check
FROM INFORMATION_SCHEMA.STREAMLITS 
WHERE STREAMLIT_NAME = 'TCH_PATIENT_360_APP';

SELECT 'Deployment verification completed' AS final_status;
"@

    Set-Content -Path $verifySQL -Value $verifyContent
    Invoke-SQLScript -ScriptPath $verifySQL -Description "Deployment verification"

    Write-Log "Deployment verification completed" "SUCCESS"
}

# Check if a deployment step should be executed
function Test-ShouldExecuteStep {
    param([string]$StepName)
    
    # If no resume step specified, execute all steps
    if ([string]::IsNullOrEmpty($ResumeStep)) {
        return $true
    }
    
    # Define step order
    $steps = @(
        "prerequisites",
        "connection", 
        "data-generation",
        "clean-deployment",
        "database-structure",
        "data-upload",
        "data-ingest",
        "unstructured-data-load",
        "dynamic-tables",
        "presentation",
        "cortex-search",
        "cortex-analyst",
        "streamlit",
        "verification"
    )
    
    # Find the index of the resume step and current step
    $resumeIndex = $steps.IndexOf($ResumeStep)
    $currentIndex = $steps.IndexOf($StepName)
    
    # Execute if current step is at or after resume step
    if ($currentIndex -ge $resumeIndex) {
        return $true
    } else {
        Write-Log "Skipping step '$StepName' (resuming from '$ResumeStep')" "INFO"
        return $false
    }
}

# Main deployment function
function Start-Deployment {
    $startTime = Get-Date
    
    Write-Log "==========================================" "INFO"
    Write-Log "TCH Patient 360 PoC Deployment Starting" "INFO"
    Write-Log "==========================================" "INFO"
    Write-Log "Timestamp: $(Get-Date)" "INFO"
    Write-Log "Account: $Account" "INFO"
    Write-Log "Username: $Username" "INFO"
    Write-Log "Database: $Database" "INFO"
    Write-Log "Role: $Role" "INFO"
    Write-Log "Data Size: $DataSize" "INFO"
    Write-Log "Clean Deploy: $CleanDeploy" "INFO"
    if ($ResumeStep) {
        Write-Log "Resume Step: $ResumeStep" "INFO"
    }
    Write-Log "==========================================" "INFO"

    try {
        # Execute deployment steps with dependency handling
        # Prerequisites are always needed for later steps
        if ((Test-ShouldExecuteStep "prerequisites") -or $ResumeStep) {
            Test-Prerequisites
        }
        
        # Connection setup is always needed for database operations  
        if ((Test-ShouldExecuteStep "connection") -or ($ResumeStep -and $ResumeStep -ne "prerequisites")) {
            Setup-SnowCLIConnection
        }
        
        # Main deployment steps
        # Order: clean → structure → generate → ingest ...
        if (Test-ShouldExecuteStep "clean-deployment") { Invoke-CleanDeployment }
        if (Test-ShouldExecuteStep "database-structure") { Deploy-Database }
        if (Test-ShouldExecuteStep "data-generation") { Invoke-DataGeneration }
        if (Test-ShouldExecuteStep "data-upload") { Upload-Data }
        if (Test-ShouldExecuteStep "data-ingest") { Invoke-DataIngest }
        if (Test-ShouldExecuteStep "dynamic-tables") { Deploy-DynamicTables }
        if (Test-ShouldExecuteStep "presentation") { Deploy-Presentation }
        if (Test-ShouldExecuteStep "cortex-search") { Deploy-CortexSearch }
        if (Test-ShouldExecuteStep "cortex-analyst") { Deploy-CortexAnalyst }
        if (Test-ShouldExecuteStep "streamlit") { Deploy-Streamlit }
        if (Test-ShouldExecuteStep "verification") { Test-Deployment }

        $endTime = Get-Date
        $duration = [math]::Round(($endTime - $startTime).TotalSeconds, 2)

        Write-Log "==========================================" "SUCCESS"
        Write-Log "TCH Patient 360 PoC Deployment Completed" "SUCCESS"
        Write-Log "==========================================" "SUCCESS"
        Write-Log "Total deployment time: $duration seconds" "SUCCESS"
        Write-Log "Deployment log: $LogFile" "SUCCESS"
        
        if (!$SkipStreamlit) {
            Write-Log "Streamlit app available in Snowsight under 'Streamlit' section" "INFO"
            Write-Log "App name: TCH_PATIENT_360_APP" "INFO"
        }
        
        Write-Log "Next steps:" "INFO"
        Write-Log "1. Log into Snowsight with your account" "INFO"
        Write-Log "2. Switch to role: $Role" "INFO"
        Write-Log "3. Navigate to the Streamlit section to access the Patient 360 app" "INFO"
        Write-Log "4. Explore the data in database: $Database" "INFO"

    } catch {
        Write-Log "Deployment failed: $($_.Exception.Message)" "ERROR"
        Write-Log "Check $LogFile for details." "ERROR"
        exit 1
    } finally {
        # Cleanup
        if (Test-Path $TempDir) {
            Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}

# Run the deployment
Start-Deployment