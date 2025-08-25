#!/bin/bash

# Texas Children's Hospital Patient 360 PoC - SnowCLI Deployment Script
# 
# This script deploys the complete TCH Patient 360 PoC to Snowflake using SnowCLI.
# 
# Prerequisites:
#   1. SnowCLI must be installed and configured
#   2. ACCOUNTADMIN must have run sql/setup/00_accountadmin_setup.sql
#   3. User must be granted TCH_PATIENT_360_ROLE
#   4. Python 3.8+ with required packages (pandas, faker, etc.)
#
# Usage:
#   ./deploy_tch_poc.sh --account <account_id> --user <username> [options]
#
# Authentication Options:
#   --password <password>                 Use password authentication
#   --private-key <path_to_key>          Use private key authentication
#   --private-key-passphrase <passphrase> Private key passphrase (optional)
#
# Deployment Options:
#   --database <db_name>                 Database name (default: TCH_PATIENT_360_POC)
#   --role <role_name>                   Role name (default: TCH_PATIENT_360_ROLE)
#   --generate-data-size <size>          Data generation size: small|smallplus|medium|large (default: medium)
#   --skip-data-generation               Skip mock data generation step
#   --skip-streamlit                     Skip Streamlit app deployment
#   --clean-deploy                       Drop existing objects before deployment
#   --verbose                            Enable verbose logging

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_FILE="${PROJECT_ROOT}/deploy/deployment.log"
TEMP_DIR="${PROJECT_ROOT}/deploy/temp"

# Default values
DATABASE_NAME="TCH_PATIENT_360_POC"
ROLE_NAME="TCH_PATIENT_360_ROLE"
DATA_SIZE="medium"
SKIP_DATA_GENERATION=false
SKIP_STREAMLIT=false
CLEAN_DEPLOY=false
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}ERROR: $1${NC}" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}SUCCESS: $1${NC}" | tee -a "$LOG_FILE"
}

log_info() {
    echo -e "${BLUE}INFO: $1${NC}" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}" | tee -a "$LOG_FILE"
}

# Error handling
cleanup_on_error() {
    log_error "Deployment failed. Check $LOG_FILE for details."
    log_error "Cleaning up temporary files..."
    rm -rf "$TEMP_DIR" 2>/dev/null || true
    exit 1
}

trap cleanup_on_error ERR

# Help function
show_help() {
    cat << EOF
Texas Children's Hospital Patient 360 PoC - Deployment Script

USAGE:
    $0 --account <account_id> --user <username> [options]

REQUIRED ARGUMENTS:
    --account <account_id>               Snowflake account identifier
    --user <username>                    Snowflake username

AUTHENTICATION (choose one):
    --password <password>                Use password authentication
    --private-key <path_to_key>          Use private key file authentication
    
OPTIONAL ARGUMENTS:
    --private-key-passphrase <phrase>    Private key passphrase (if key is encrypted)
    --database <db_name>                 Database name (default: TCH_PATIENT_360_POC)
    --role <role_name>                   Role name (default: TCH_PATIENT_360_ROLE)
    --generate-data-size <size>          Data size: small|smallplus|medium|large (default: medium)
    --skip-data-generation               Skip mock data generation
    --skip-streamlit                     Skip Streamlit app deployment
    --clean-deploy                       Drop existing objects before deployment
    --resume-step <step_name>            Resume deployment from specific step
    --verbose                            Enable verbose logging
    --help                               Show this help message

DEPLOYMENT STEPS (for --resume-step):
    prerequisites                        Check prerequisites and dependencies
    connection                          Setup SnowCLI connection
    data-generation                     Generate mock data
    clean-deployment                    Clean existing deployment (if --clean-deploy)
    database-structure                  Deploy database structure
    data-upload                         Upload data files to Snowflake stages
    data-ingest                         Ingest data from stages into tables
    dynamic-tables                      Deploy Dynamic Tables
    presentation                        Deploy presentation layer views
    unstructured-data-load              Load unstructured clinical documents
    cortex-search                      Setup Cortex Search services
    cortex-analyst                     Setup Cortex Analyst semantic model
    streamlit                           Deploy Streamlit application
    verification                        Verify deployment

EXAMPLES:
    # Deploy with password authentication
    $0 --account xy12345.us-east-1 --user jdoe --password mypassword

    # Deploy with private key authentication  
    $0 --account xy12345.us-east-1 --user jdoe --private-key ~/.ssh/snowflake_key.p8

    # Clean deployment with large dataset
    $0 --account xy12345.us-east-1 --user jdoe --password mypassword --clean-deploy --generate-data-size large

    # Resume deployment from Dynamic Tables step
    $0 --account xy12345.us-east-1 --user jdoe --private-key ~/.ssh/snowflake_key.p8 --resume-step dynamic-tables
    
    # Resume from data ingestion if upload already completed
    $0 --account xy12345.us-east-1 --user jdoe --private-key ~/.ssh/snowflake_key.p8 --resume-step data-ingest

PREREQUISITES:
    1. SnowCLI installed and in PATH
    2. ACCOUNTADMIN has run sql/setup/00_accountadmin_setup.sql  
    3. User has been granted TCH_PATIENT_360_ROLE
    4. Python 3.8+ with pandas, faker packages installed

EOF
}

# Parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --account)
                ACCOUNT="$2"
                shift 2
                ;;
            --user)
                USERNAME="$2"
                shift 2
                ;;
            --password)
                PASSWORD="$2"
                AUTH_METHOD="password"
                shift 2
                ;;
            --private-key)
                PRIVATE_KEY_PATH="$2"
                AUTH_METHOD="private_key"
                shift 2
                ;;
            --private-key-passphrase)
                PRIVATE_KEY_PASSPHRASE="$2"
                shift 2
                ;;
            --database)
                DATABASE_NAME="$2"
                shift 2
                ;;
            --role)
                ROLE_NAME="$2"
                shift 2
                ;;
            --generate-data-size)
                DATA_SIZE="$2"
                if [[ ! "$DATA_SIZE" =~ ^(small|smallplus|medium|large)$ ]]; then
                    log_error "Invalid data size: $DATA_SIZE. Must be small, smallplus, medium, or large."
                    exit 1
                fi
                shift 2
                ;;
            --skip-data-generation)
                SKIP_DATA_GENERATION=true
                shift
                ;;
            --skip-streamlit)
                SKIP_STREAMLIT=true
                shift
                ;;
            --clean-deploy)
                CLEAN_DEPLOY=true
                shift
                ;;
            --resume-step)
                RESUME_STEP="$2"
                valid_steps="prerequisites connection data-generation clean-deployment database-structure data-upload data-ingest dynamic-tables presentation unstructured-data-load cortex-search cortex-analyst streamlit verification"
                if [[ ! " $valid_steps " =~ " $RESUME_STEP " ]]; then
                    log_error "Invalid resume step: $RESUME_STEP. Valid steps: $valid_steps"
                    exit 1
                fi
                shift 2
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown argument: $1"
                show_help
                exit 1
                ;;
        esac
    done

    # Validate required arguments
    if [[ -z "${ACCOUNT:-}" ]]; then
        log_error "Missing required argument: --account"
        show_help
        exit 1
    fi

    if [[ -z "${USERNAME:-}" ]]; then
        log_error "Missing required argument: --user"
        show_help
        exit 1
    fi

    if [[ -z "${AUTH_METHOD:-}" ]]; then
        log_error "Must specify either --password or --private-key for authentication"
        show_help
        exit 1
    fi

    # Validate authentication
    if [[ "$AUTH_METHOD" == "private_key" ]]; then
        if [[ ! -f "$PRIVATE_KEY_PATH" ]]; then
            log_error "Private key file not found: $PRIVATE_KEY_PATH"
            exit 1
        fi
    fi
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check SnowCLI
    if ! command -v snow &> /dev/null; then
        log_error "SnowCLI not found. Please install SnowCLI and ensure it's in your PATH."
        exit 1
    fi

    # Check Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 not found. Please install Python 3.8 or later."
        exit 1
    fi

    # Check required Python packages
    log_info "Checking Python dependencies..."
    python3 -c "import pandas, faker, datetime" 2>/dev/null || {
        log_error "Required Python packages not found. Please install: pip install pandas faker"
        exit 1
    }

    # Create directories
    mkdir -p "$TEMP_DIR"
    mkdir -p "$(dirname "$LOG_FILE")"

    log_success "Prerequisites check completed"
}

# Setup SnowCLI connection
setup_snowcli_connection() {
    log_info "Setting up SnowCLI connection..."

    # Create SnowCLI configuration
    local config_file="$TEMP_DIR/snow_config.toml"
    
    cat > "$config_file" << EOF
[connections.tch_poc]
account = "$ACCOUNT"
user = "$USERNAME"
EOF

    if [[ "$AUTH_METHOD" == "password" ]]; then
        echo "password = \"$PASSWORD\"" >> "$config_file"
    else
        # Convert private key path to absolute path
        local abs_private_key_path
        if [[ "$PRIVATE_KEY_PATH" = /* ]]; then
            abs_private_key_path="$PRIVATE_KEY_PATH"
        else
            abs_private_key_path="$(cd "$(dirname "$PRIVATE_KEY_PATH")" && pwd)/$(basename "$PRIVATE_KEY_PATH")"
        fi
        echo "authenticator = \"SNOWFLAKE_JWT\"" >> "$config_file"
        echo "private_key_path = \"$abs_private_key_path\"" >> "$config_file"
        if [[ -n "${PRIVATE_KEY_PASSPHRASE:-}" ]]; then
            echo "private_key_passphrase = \"$PRIVATE_KEY_PASSPHRASE\"" >> "$config_file"
        fi
    fi

    echo "role = \"$ROLE_NAME\"" >> "$config_file"
    echo "warehouse = \"TCH_COMPUTE_WH\"" >> "$config_file"

    # Test connection
    log_info "Testing Snowflake connection..."
    if ! snow --config-file "$config_file" sql -q "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()" -c tch_poc &>/dev/null; then
        log_error "Failed to connect to Snowflake. Please check your credentials and ensure TCH_PATIENT_360_ROLE has been granted to your user."
        exit 1
    fi

    log_success "SnowCLI connection established"
    export SNOW_CONFIG_FILE="$config_file"
}

# Clean up existing data files
cleanup_existing_data() {
    log_info "Cleaning up existing data files..."
    
    # Clean main project data directory
    if [[ -d "$PROJECT_ROOT/data/mock_data" ]]; then
        # Clean structured data files (both .csv and .csv.gz)
        find "$PROJECT_ROOT/data/mock_data/structured" -type f \( -name "*.csv" -o -name "*.csv.gz" \) -delete 2>/dev/null || true
        
        # Clean unstructured data directories and files
        if [[ -d "$PROJECT_ROOT/data/mock_data/unstructured" ]]; then
            rm -rf "$PROJECT_ROOT/data/mock_data/unstructured"/* 2>/dev/null || true
        fi
        
        # Clean metadata files
        rm -f "$PROJECT_ROOT/data/mock_data"/*.json 2>/dev/null || true
        
        log_info "Existing data files cleaned up"
    fi
}

# Generate mock data using Snowflake notebook
generate_mock_data() {
    if [[ "$SKIP_DATA_GENERATION" == true ]]; then
        log_info "Skipping data generation as requested"
        return 0
    fi

    log_info "Generating mock data using Snowflake notebook (size: $DATA_SIZE)..."

    # First, deploy the notebook to Snowflake
    log_info "Deploying data generation notebook..."
    
    cd "$PROJECT_ROOT/python/notebooks"
    
    if [[ "$VERBOSE" == true ]]; then
        snow --config-file "$SNOW_CONFIG_FILE" notebook deploy tch_data_generator --replace --project . --database "$DATABASE_NAME" --schema AI_ML -c tch_poc
    else
        snow --config-file "$SNOW_CONFIG_FILE" notebook deploy tch_data_generator --replace --project . --database "$DATABASE_NAME" --schema AI_ML -c tch_poc > "$TEMP_DIR/notebook_deploy.log" 2>&1
    fi
    
    if [[ $? -ne 0 ]]; then
        log_error "Failed to deploy notebook. Check $TEMP_DIR/notebook_deploy.log for details"
        return 1
    fi
    
    log_success "Notebook deployed successfully"

    # Configure notebook to use Container Runtime for faster file uploads
    log_info "Configuring notebook runtime (container runtime and compute pool)..."
    local nb_runtime_sql="$TEMP_DIR/configure_notebook_runtime.sql"
    # Expand our vars but escape the $ in SYSTEM$BASIC_RUNTIME so bash does not expand it
    cat > "$nb_runtime_sql" << EOF
USE DATABASE $DATABASE_NAME;
USE SCHEMA AI_ML;
USE ROLE $ROLE_NAME;
ALTER NOTEBOOK $DATABASE_NAME.AI_ML.TCH_DATA_GENERATOR UNSET WAREHOUSE;
ALTER NOTEBOOK $DATABASE_NAME.AI_ML.TCH_DATA_GENERATOR SET RUNTIME_NAME = 'SYSTEM\$BASIC_RUNTIME';
ALTER NOTEBOOK $DATABASE_NAME.AI_ML.TCH_DATA_GENERATOR SET COMPUTE_POOL = TCH_PATIENT_360_POOL;
ALTER NOTEBOOK $DATABASE_NAME.AI_ML.TCH_DATA_GENERATOR SET EXTERNAL_ACCESS_INTEGRATIONS = (pypi_access_integration);
EOF
    execute_sql_script "$nb_runtime_sql" "Configure notebook container runtime"

    # Determine number of patients based on size
    local num_patients=""
    case $DATA_SIZE in
        small)
            num_patients="1000"
            ;;
        smallplus)
            num_patients="2500"
            ;;
        medium)
            num_patients="5000"
            ;;
        large)
            num_patients="25000"
            ;;
    esac

    # Ensure stages are clean to avoid mixing old/new datasets
    log_info "Clearing existing stage contents before generation (structured and unstructured)..."
    local clear_stage_sql="$TEMP_DIR/clear_stages.sql"
    cat > "$clear_stage_sql" << EOF
USE DATABASE $DATABASE_NAME;
USE ROLE $ROLE_NAME;
-- Remove all prior files to prevent MRN mismatches across runs
REMOVE @RAW_DATA.PATIENT_DATA_STAGE;
REMOVE @RAW_DATA.UNSTRUCTURED_DATA_STAGE;
EOF
    execute_sql_script "$clear_stage_sql" "Clear stage contents"

    # Execute the notebook with parallel generation enabled (env var)
    log_info "Executing data generation notebook (parallel generation enabled)..."
    
    # Create SQL script to execute the notebook
    local notebook_exec_sql="$TEMP_DIR/execute_notebook.sql"
    cat > "$notebook_exec_sql" << EOF
USE DATABASE $DATABASE_NAME;
USE SCHEMA AI_ML;
USE WAREHOUSE TCH_COMPUTE_WH;
USE ROLE $ROLE_NAME;

-- Execute the notebook with parameters
-- Pass data size as argument (notebook will use default values if not provided)
EXECUTE NOTEBOOK $DATABASE_NAME.AI_ML.TCH_DATA_GENERATOR('data_size=$DATA_SIZE', 'rows_per_file=15000', 'parallel=true');

SELECT 'Notebook execution started' AS status;
EOF

    execute_sql_script "$notebook_exec_sql" "Notebook execution"
    
    # Poll for required sharded files using DIRECTORY() with retries (robust to case/formatting)
    log_info "Waiting for notebook to upload sharded structured files to stage..."
    patterns=(
        ".*patients_part_[0-9]+\\.csv\\.gz$"
        ".*encounters_part_[0-9]+\\.csv\\.gz$"
        ".*diagnoses_part_[0-9]+\\.csv\\.gz$"
        ".*lab_results_part_[0-9]+\\.csv\\.gz$"
        ".*medications_part_[0-9]+\\.csv\\.gz$"
        ".*vital_signs_part_[0-9]+\\.csv\\.gz$"
        ".*imaging_studies_part_[0-9]+\\.csv\\.gz$"
        ".*providers_part_[0-9]+\\.csv\\.gz$"
        ".*departments_part_[0-9]+\\.csv\\.gz$"
        ".*clinical_notes_part_[0-9]+\\.csv\\.gz$"
        ".*radiology_reports_part_[0-9]+\\.csv\\.gz$"
    )
    tries=0
    all_present=0
    while [[ $tries -lt 20 ]]; do
        all_present=1
        for pat in "${patterns[@]}"; do
            count=$(snow --config-file "$SNOW_CONFIG_FILE" sql -q "SELECT COUNT(*) FROM DIRECTORY(@$DATABASE_NAME.RAW_DATA.PATIENT_DATA_STAGE) WHERE relative_path RLIKE '${pat}';" -c tch_poc | awk 'NR==2{print $1}')
            if [[ -z "$count" || "$count" == "0" ]]; then all_present=0; break; fi
        done
        if [[ $all_present -eq 1 ]]; then break; fi
        tries=$((tries+1))
        sleep 6
    done
    if [[ $all_present -ne 1 ]]; then
        log_error "Notebook did not produce required sharded files in stage. Aborting."
        exit 1
    fi

    log_success "Mock data generation completed using Snowflake notebook"
}

# Execute SQL script
execute_sql_script() {
    local script_path="$1"
    local description="$2"

    log_info "Executing: $description"
    log_info "Script: $script_path"

    if [[ ! -f "$script_path" ]]; then
        log_error "SQL script not found: $script_path"
        exit 1
    fi

    if [[ "$VERBOSE" == true ]]; then
        snow --config-file "$SNOW_CONFIG_FILE" sql -f "$script_path" -c tch_poc
    else
        snow --config-file "$SNOW_CONFIG_FILE" sql -f "$script_path" -c tch_poc > "$TEMP_DIR/$(basename "$script_path").log" 2>&1
    fi

    log_success "Completed: $description"
}

# Clean existing deployment
clean_existing_deployment() {
    if [[ "$CLEAN_DEPLOY" != true ]]; then
        return 0
    fi

    log_warning "Performing clean deployment - dropping existing objects..."

    local cleanup_sql="$TEMP_DIR/cleanup.sql"
    cat > "$cleanup_sql" << EOF
USE ROLE $ROLE_NAME;

-- Drop Streamlit app if exists (with qualified name)
DROP STREAMLIT IF EXISTS $DATABASE_NAME.AI_ML.TCH_PATIENT_360_APP;

-- Drop database if exists (cascades to all objects)
DROP DATABASE IF EXISTS $DATABASE_NAME;

SELECT 'Clean deployment completed' AS status;
EOF

    execute_sql_script "$cleanup_sql" "Clean deployment"
}



# Deploy database structure
deploy_database() {
    log_info "Deploying database structure..."

    execute_sql_script "$PROJECT_ROOT/sql/setup/01_database_setup.sql" "Database and schema setup"
    execute_sql_script "$PROJECT_ROOT/sql/setup/02_raw_tables.sql" "Raw data tables"
    
    # Create conformed reference tables (Dynamic Tables will create operational tables)
    execute_sql_script "$PROJECT_ROOT/sql/setup/03_conformed_tables.sql" "Conformed reference tables"

    log_success "Database structure deployment completed"
}

# Verify data files in Snowflake stages (data is uploaded by the notebook)
upload_data() {
    log_info "Verifying data files in Snowflake stages..."
    
    # Note: The data generation notebook now uploads files directly to stages
    # This function just verifies the upload was successful
    
    # Check structured data stage
    log_info "Checking structured data stage..."
    local structured_count=$(snow --config-file "$SNOW_CONFIG_FILE" sql -q "SELECT COUNT(*) FROM DIRECTORY(@$DATABASE_NAME.RAW_DATA.PATIENT_DATA_STAGE) WHERE relative_path RLIKE '.*\\.csv(\\.gz)?$';" -c tch_poc | awk 'NR==2{print $1}')
    log_info "Found ${structured_count} structured data files in stage"
    
    if [[ ${structured_count} -eq 0 ]]; then
        log_error "No structured data files found in stage. Data generation may have failed."
        return 1
    fi
    
    # Check unstructured data stage
    log_info "Checking unstructured data stage..."
    local unstructured_count=$(snow --config-file "$SNOW_CONFIG_FILE" sql -q "SELECT COUNT(*) FROM DIRECTORY(@$DATABASE_NAME.RAW_DATA.UNSTRUCTURED_DATA_STAGE) WHERE relative_path RLIKE '.*\\.txt(\\.gz)?$';" -c tch_poc | awk 'NR==2{print $1}')
    log_info "Found ${unstructured_count} unstructured data files in stage"
    
    if [[ ${unstructured_count} -eq 0 ]]; then
        log_warning "No unstructured data files found in stage. This may be expected for smaller datasets."
    fi
    
    # Refresh stage directory indexes so DIRECTORY() sees latest files
    log_info "Refreshing stage directory indexes..."
    local refresh_sql="$TEMP_DIR/refresh_stages.sql"
    cat > "$refresh_sql" << EOF
USE DATABASE $DATABASE_NAME;
USE ROLE $ROLE_NAME;
ALTER STAGE RAW_DATA.PATIENT_DATA_STAGE REFRESH;
ALTER STAGE RAW_DATA.UNSTRUCTURED_DATA_STAGE REFRESH;
-- Show counts after refresh
SELECT 'structured_total' AS scope, COUNT(*) AS cnt FROM DIRECTORY(@$DATABASE_NAME.RAW_DATA.PATIENT_DATA_STAGE)
UNION ALL
SELECT 'unstructured_total' AS scope, COUNT(*) AS cnt FROM DIRECTORY(@$DATABASE_NAME.RAW_DATA.UNSTRUCTURED_DATA_STAGE);
EOF
    execute_sql_script "$refresh_sql" "Refresh stage directory indexes"

    log_success "Data files verified and stage directories refreshed"
}

# Ingest data from stages into tables using COPY commands
ingest_data() {
    log_info "Ingesting data from stages into tables..."

    # Guard: verify required sharded files exist in stage before truncating/loading
    log_info "Verifying required sharded files exist in @${DATABASE_NAME}.RAW_DATA.PATIENT_DATA_STAGE..."

    require_stage_file() {
        local label="$1"
        local pattern="$2"
        local sql="SELECT COUNT(*) FROM DIRECTORY(@${DATABASE_NAME}.RAW_DATA.PATIENT_DATA_STAGE) WHERE relative_path RLIKE '${pattern}';"
        local count=$(snow --config-file "$SNOW_CONFIG_FILE" sql -q "$sql" -c tch_poc | awk 'NR==2{print $1}')
        if [[ -z "$count" ]]; then count=0; fi
        if [[ "$count" -eq 0 ]]; then
            log_error "Missing required staged files for ${label}. Expected pattern: ${pattern}"
            log_error "Aborting ingest to avoid truncating tables without data. Run data-generation first."
            exit 1
        else
            log_info "Found ${count} file(s) for ${label}."
        fi
    }

    # Structured datasets required for ingest
    require_stage_file "patients"            ".*patients_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "encounters"          ".*encounters_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "diagnoses"           ".*diagnoses_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "lab_results"         ".*lab_results_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "medications"         ".*medications_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "vital_signs"         ".*vital_signs_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "imaging_studies"     ".*imaging_studies_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "providers"           ".*providers_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "departments"         ".*departments_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "clinical_notes"      ".*clinical_notes_part_[0-9]+\\.csv\\.gz$"
    require_stage_file "radiology_reports"   ".*radiology_reports_part_[0-9]+\\.csv\\.gz$"

    # Execute data loading script
    execute_sql_script "$PROJECT_ROOT/sql/data_load/01_load_raw_data.sql" "Data ingestion"

    log_success "Data ingestion completed"

    # Print a concise summary after ingest
    local summary_sql="$TEMP_DIR/print_load_summary.sql"
    cat > "$summary_sql" << EOF
USE DATABASE $DATABASE_NAME;
SELECT * FROM RAW_DATA.DATA_LOAD_SUMMARY ORDER BY table_name;
EOF
    execute_sql_script "$summary_sql" "Data load summary"
}

# Deploy dynamic tables
deploy_dynamic_tables() {
    log_info "Deploying Dynamic Tables..."

    execute_sql_script "$PROJECT_ROOT/sql/dynamic_tables/01_patient_dynamic_tables.sql" "Patient Dynamic Tables"
    execute_sql_script "$PROJECT_ROOT/sql/dynamic_tables/02_clinical_dynamic_tables.sql" "Clinical Dynamic Tables"

    log_success "Dynamic Tables deployment completed"
}

# Deploy presentation layer
deploy_presentation() {
    log_info "Deploying presentation layer..."

    execute_sql_script "$PROJECT_ROOT/sql/setup/04_presentation_tables.sql" "Presentation layer tables"

    log_success "Presentation layer deployment completed"
}

# Deploy unstructured data loading
deploy_unstructured_data() {
    log_info "Loading unstructured data from stages to raw tables..."
    # Guard: verify unstructured stage contains both clinical notes and radiology reports
    log_info "Verifying unstructured files exist in stage..."
    local clinical_count=$(snow --config-file "$SNOW_CONFIG_FILE" sql -q "SELECT COUNT(*) FROM DIRECTORY(@$DATABASE_NAME.RAW_DATA.UNSTRUCTURED_DATA_STAGE) WHERE relative_path ILIKE 'clinical_notes/%';" -c tch_poc | awk 'NR==2{print $1}')
    local radiology_count=$(snow --config-file "$SNOW_CONFIG_FILE" sql -q "SELECT COUNT(*) FROM DIRECTORY(@$DATABASE_NAME.RAW_DATA.UNSTRUCTURED_DATA_STAGE) WHERE relative_path ILIKE 'radiology_reports/%';" -c tch_poc | awk 'NR==2{print $1}')
    clinical_count=${clinical_count:-0}
    radiology_count=${radiology_count:-0}
    log_info "Unstructured stage counts → clinical_notes=${clinical_count}, radiology_reports=${radiology_count}"
    if [[ "$clinical_count" -eq 0 ]]; then
        log_error "No clinical notes found in @${DATABASE_NAME}.RAW_DATA.UNSTRUCTURED_DATA_STAGE/clinical_notes/. Aborting unstructured load."
        log_error "Root cause is likely generation path not uploading clinical notes. Fix notebook generation, then rerun."
        exit 1
    fi
    if [[ "$radiology_count" -eq 0 ]]; then
        log_error "No radiology reports found in @${DATABASE_NAME}.RAW_DATA.UNSTRUCTURED_DATA_STAGE/radiology_reports/. Aborting unstructured load."
        exit 1
    fi

    execute_sql_script "$PROJECT_ROOT/sql/data_load/02_load_unstructured_data.sql" "Unstructured data loading"

    log_success "Unstructured data loading completed"
}

# Deploy Cortex Analyst
deploy_cortex_analyst() {
    log_info "Deploying Cortex Analyst semantic model..."

    execute_sql_script "$PROJECT_ROOT/sql/cortex/01_cortex_analyst_setup.sql" "Cortex Analyst setup"
    
    # Upload semantic model YAML file to the stage
    log_info "Uploading semantic model YAML file..."
    snow --config-file "$SNOW_CONFIG_FILE" stage copy "$PROJECT_ROOT/sql/cortex/semantic_model/semantic_model.yaml" @TCH_PATIENT_360_POC.AI_ML.SEMANTIC_MODEL_STAGE/ -c tch_poc --overwrite
    snow --config-file "$SNOW_CONFIG_FILE" stage copy "$PROJECT_ROOT/sql/cortex/semantic_model/semantic_model_chat.yaml" @TCH_PATIENT_360_POC.AI_ML.SEMANTIC_MODEL_STAGE/ -c tch_poc --overwrite
    
    log_success "Cortex Analyst deployment completed"
}

# Deploy Cortex Search
deploy_cortex_search() {
    log_info "Deploying Cortex Search services..."

    execute_sql_script "$PROJECT_ROOT/sql/cortex/02_cortex_search_setup.sql" "Cortex Search setup"

    log_success "Cortex Search deployment completed"

    # Refresh Cortex Search services after creation to pick up latest data
    log_info "Refreshing Cortex Search services..."
    local refresh_search_sql="$TEMP_DIR/refresh_cortex_search.sql"
    cat > "$refresh_search_sql" << EOF
USE DATABASE $DATABASE_NAME;
USE SCHEMA AI_ML;
USE ROLE $ROLE_NAME;
USE WAREHOUSE TCH_AI_ML_WH;
ALTER CORTEX SEARCH SERVICE CLINICAL_NOTES_SEARCH REFRESH;
ALTER CORTEX SEARCH SERVICE RADIOLOGY_REPORTS_SEARCH REFRESH;
ALTER CORTEX SEARCH SERVICE CLINICAL_DOCUMENTATION_SEARCH REFRESH;
EOF
    execute_sql_script "$refresh_search_sql" "Cortex Search services refresh"
}

# Deploy Streamlit app
deploy_streamlit() {
    if [[ "$SKIP_STREAMLIT" == true ]]; then
        log_info "Skipping Streamlit deployment as requested"
        return 0
    fi

    log_info "Deploying Streamlit in Snowflake app..."

    # Create stage for Streamlit app if it doesn't exist
    local stage_sql="$TEMP_DIR/create_streamlit_stage.sql"
    cat > "$stage_sql" << EOF
USE DATABASE $DATABASE_NAME;
USE SCHEMA RAW_DATA;

CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
    COMMENT = 'Stage for Streamlit in Snowflake application files';
EOF

    execute_sql_script "$stage_sql" "Streamlit stage creation"

    # Clean up old stage files that might cause duplicate navigation
    log_info "Cleaning up old stage files..."
    local cleanup_sql_file="$TEMP_DIR/cleanup_stage.sql"
    cat > "$cleanup_sql_file" << 'EOF'
USE DATABASE TCH_PATIENT_360_POC;
USE SCHEMA RAW_DATA;

-- Remove old pages directory files that cause duplicate navigation
REMOVE @STREAMLIT_STAGE/streamlit_app/pages/;

-- Remove old app.py if it exists  
REMOVE @STREAMLIT_STAGE/streamlit_app/app.py;

SELECT 'Stage cleaned successfully' AS status;
EOF
    execute_sql_script "$cleanup_sql_file" "Stage cleanup"

    # Upload Streamlit application files
    log_info "Uploading Streamlit application files..."

    # Upload main entry point
    snow --config-file "$SNOW_CONFIG_FILE" stage copy "$PROJECT_ROOT/python/streamlit_app/main.py" @TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/ -c tch_poc --overwrite

    # Upload environment.yml for SiS package management
    snow --config-file "$SNOW_CONFIG_FILE" stage copy "$PROJECT_ROOT/python/streamlit_app/environment.yml" @TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/ -c tch_poc --overwrite

    # Upload services directory - using glob pattern to automatically include all Python files
    log_info "Uploading services modules..."
    snow --config-file "$SNOW_CONFIG_FILE" stage copy "$PROJECT_ROOT/python/streamlit_app/services/*.py" @TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/services/ -c tch_poc --overwrite

    # Upload page_modules directory - using glob pattern to automatically include all Python files
    log_info "Uploading page modules..."
    snow --config-file "$SNOW_CONFIG_FILE" stage copy "$PROJECT_ROOT/python/streamlit_app/page_modules/*.py" @TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/page_modules/ -c tch_poc --overwrite

    # Upload components directory - using glob pattern to automatically include all Python files
    log_info "Uploading component modules..."
    snow --config-file "$SNOW_CONFIG_FILE" stage copy "$PROJECT_ROOT/python/streamlit_app/components/*.py" @TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/components/ -c tch_poc --overwrite

    # Upload utils directory - using glob pattern to automatically include all Python files
    log_info "Uploading utility modules..."
    snow --config-file "$SNOW_CONFIG_FILE" stage copy "$PROJECT_ROOT/python/streamlit_app/utils/*.py" @TCH_PATIENT_360_POC.RAW_DATA.STREAMLIT_STAGE/streamlit_app/utils/ -c tch_poc --overwrite

    # Create Streamlit app
    local streamlit_sql="$TEMP_DIR/create_streamlit_app.sql"
    cat > "$streamlit_sql" << EOF
USE DATABASE $DATABASE_NAME;
USE ROLE $ROLE_NAME;

-- Create the Streamlit app
CREATE OR REPLACE STREAMLIT TCH_PATIENT_360_APP
    ROOT_LOCATION = '@RAW_DATA.STREAMLIT_STAGE/streamlit_app/'
    MAIN_FILE = 'main.py'
    QUERY_WAREHOUSE = 'TCH_ANALYTICS_WH'
    COMMENT = 'Texas Children\'s Hospital Patient 360 PoC - Streamlit Application';

-- Grant access to the role
GRANT USAGE ON STREAMLIT TCH_PATIENT_360_APP TO ROLE $ROLE_NAME;

SELECT 'Streamlit app created successfully' AS status;
EOF

    execute_sql_script "$streamlit_sql" "Streamlit app creation"

    log_success "Streamlit application deployment completed"
}

# Verify deployment
verify_deployment() {
    log_info "Verifying deployment..."

    local verify_sql="$TEMP_DIR/verify_deployment.sql"
    cat > "$verify_sql" << EOF
USE DATABASE $DATABASE_NAME;
USE ROLE $ROLE_NAME;

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

-- Check Dynamic Tables (use SHOW + RESULT_SCAN for cross-account compatibility)
SHOW DYNAMIC TABLES IN DATABASE $DATABASE_NAME;
SELECT 'Dynamic Tables count: ' || COUNT(*) AS dynamic_tables_check
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

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
EOF

    execute_sql_script "$verify_sql" "Deployment verification"

    log_success "Deployment verification completed"
}

# Check if a deployment step should be executed
should_execute_step() {
    local step_name="$1"
    
    # If no resume step specified, execute all steps
    if [[ -z "${RESUME_STEP:-}" ]]; then
        return 0
    fi
    
    # Define step order
    local steps=(
        "prerequisites"
        "connection" 
        "data-generation"
        "clean-deployment"
        "database-structure"
        "data-upload"
        "data-ingest"
        "dynamic-tables"
        "presentation"
        "unstructured-data-load"
        "cortex-search"
        "cortex-analyst"
        "streamlit"
        "verification"
    )
    
    # Find the index of the resume step and current step
    local resume_index=-1
    local current_index=-1
    
    for i in "${!steps[@]}"; do
        if [[ "${steps[i]}" == "$RESUME_STEP" ]]; then
            resume_index=$i
        fi
        if [[ "${steps[i]}" == "$step_name" ]]; then
            current_index=$i
        fi
    done
    
    # Execute if current step is at or after resume step
    if [[ $current_index -ge $resume_index ]]; then
        return 0
    else
        log_info "Skipping step '$step_name' (resuming from '$RESUME_STEP')"
        return 1
    fi
}

# Main deployment function
main() {
    local start_time=$(date +%s)
    
    log_info "=========================================="
    log_info "TCH Patient 360 PoC Deployment Starting"
    log_info "=========================================="
    log_info "Timestamp: $(date)"
    log_info "Account: $ACCOUNT"
    log_info "User: $USERNAME"
    log_info "Database: $DATABASE_NAME"
    log_info "Role: $ROLE_NAME"
    log_info "Data Size: $DATA_SIZE"
    log_info "Clean Deploy: $CLEAN_DEPLOY"
    if [[ -n "${RESUME_STEP:-}" ]]; then
        log_info "Resume Step: $RESUME_STEP"
    fi
    log_info "=========================================="

    # Execute deployment steps with dependency handling
    # Prerequisites are always needed for later steps
    if should_execute_step "prerequisites" || [[ -n "${RESUME_STEP:-}" ]]; then
        check_prerequisites
    fi
    
    # Connection setup is always needed for database operations
    if should_execute_step "connection" || [[ -n "${RESUME_STEP:-}" && ! " prerequisites " =~ " $RESUME_STEP " ]]; then
        setup_snowcli_connection
    fi
    
    # Main deployment steps (order: clean → structure → generate → ingest ...)
    should_execute_step "clean-deployment" && clean_existing_deployment
    should_execute_step "database-structure" && deploy_database
    should_execute_step "data-generation" && generate_mock_data
    should_execute_step "data-upload" && upload_data
    should_execute_step "data-ingest" && ingest_data
    should_execute_step "dynamic-tables" && deploy_dynamic_tables
    should_execute_step "presentation" && deploy_presentation
    should_execute_step "unstructured-data-load" && deploy_unstructured_data
    should_execute_step "cortex-search" && deploy_cortex_search
    should_execute_step "cortex-analyst" && deploy_cortex_analyst
    should_execute_step "streamlit" && deploy_streamlit
    should_execute_step "verification" && verify_deployment

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    log_success "=========================================="
    log_success "TCH Patient 360 PoC Deployment Completed"
    log_success "=========================================="
    log_success "Total deployment time: ${duration} seconds"
    log_success "Deployment log: $LOG_FILE"
    
    if [[ "$SKIP_STREAMLIT" != true ]]; then
        log_info "Streamlit app available in Snowsight under 'Streamlit' section"
        log_info "App name: TCH_PATIENT_360_APP"
    fi
    
    log_info "Next steps:"
    log_info "1. Log into Snowsight with your account"
    log_info "2. Switch to role: $ROLE_NAME"
    log_info "3. Navigate to the Streamlit section to access the Patient 360 app"
    log_info "4. Explore the data in database: $DATABASE_NAME"

    # Cleanup
    rm -rf "$TEMP_DIR"
}

# Script entry point
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    parse_arguments "$@"
    main
fi