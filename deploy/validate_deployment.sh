#!/bin/bash

# Texas Children's Hospital Patient 360 PoC - Deployment Validation Script
# 
# This script validates that the TCH Patient 360 PoC has been deployed correctly.
# Run this after deployment to verify all components are working.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
DATABASE_NAME="${DATABASE_NAME:-TCH_PATIENT_360_POC}"
ROLE_NAME="${ROLE_NAME:-TCH_PATIENT_360_ROLE}"
CONNECTION_NAME="${CONNECTION_NAME:-tch_poc}"

# Counters
TOTAL_TESTS=0
PASSED_TESTS=0

# Test functions
test_result() {
    local test_name="$1"
    local result="$2"
    local details="${3:-}"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if [[ "$result" == "PASS" ]]; then
        echo -e "${GREEN}✓ PASS${NC}: $test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        if [[ -n "$details" ]]; then
            echo -e "  ${BLUE}$details${NC}"
        fi
    else
        echo -e "${RED}✗ FAIL${NC}: $test_name"
        if [[ -n "$details" ]]; then
            echo -e "  ${RED}$details${NC}"
        fi
    fi
}

run_sql_test() {
    local test_name="$1"
    local sql_query="$2"
    local expected_min="${3:-0}"
    
    echo -e "${BLUE}Running test: $test_name${NC}"
    
    if result=$(snow sql -q "$sql_query" -c "$CONNECTION_NAME" --format plain 2>/dev/null); then
        # Extract numeric value from result
        value=$(echo "$result" | grep -oE '[0-9]+' | head -1)
        
        if [[ -n "$value" && "$value" -ge "$expected_min" ]]; then
            test_result "$test_name" "PASS" "Found: $value (expected: >= $expected_min)"
        else
            test_result "$test_name" "FAIL" "Found: ${value:-0}, expected: >= $expected_min"
        fi
    else
        test_result "$test_name" "FAIL" "SQL query failed"
    fi
}

run_existence_test() {
    local test_name="$1"
    local sql_query="$2"
    
    echo -e "${BLUE}Running test: $test_name${NC}"
    
    if result=$(snow sql -q "$sql_query" -c "$CONNECTION_NAME" --format plain 2>/dev/null); then
        if echo "$result" | grep -q "1\|YES\|TRUE"; then
            test_result "$test_name" "PASS"
        else
            test_result "$test_name" "FAIL" "Object not found"
        fi
    else
        test_result "$test_name" "FAIL" "SQL query failed"
    fi
}

show_help() {
    cat << EOF
TCH Patient 360 PoC - Deployment Validation Script

USAGE:
    $0 [options]

OPTIONS:
    --connection <name>    SnowCLI connection name (default: tch_poc)
    --database <name>      Database name (default: TCH_PATIENT_360_POC)
    --role <name>          Role name (default: TCH_PATIENT_360_ROLE)
    --help                 Show this help message

PREREQUISITES:
    - SnowCLI must be configured with a working connection
    - User must have access to TCH_PATIENT_360_ROLE
    - Deployment must be completed

EXAMPLES:
    # Use default settings
    $0
    
    # Specify custom connection
    $0 --connection my_snow_connection
    
    # Custom database and role
    $0 --database MY_POC_DB --role MY_POC_ROLE

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --connection)
            CONNECTION_NAME="$2"
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
        --help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            show_help
            exit 1
            ;;
    esac
done

echo -e "${BLUE}=========================================="
echo -e "TCH Patient 360 PoC - Validation Report"
echo -e "==========================================${NC}"
echo -e "Database: $DATABASE_NAME"
echo -e "Role: $ROLE_NAME"
echo -e "Connection: $CONNECTION_NAME"
echo -e "Timestamp: $(date)"
echo -e "${BLUE}==========================================${NC}"
echo

# Test 1: Connection and Role
echo -e "${YELLOW}1. Testing Connection and Permissions${NC}"
run_existence_test "Snowflake Connection" "SELECT 1"
run_existence_test "Role Access" "USE ROLE $ROLE_NAME; SELECT 1"

# Test 2: Database Structure
echo -e "\n${YELLOW}2. Testing Database Structure${NC}"
run_existence_test "Database Exists" "USE DATABASE $DATABASE_NAME; SELECT 1"
run_sql_test "Schema Count" "USE DATABASE $DATABASE_NAME; SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME IN ('RAW_DATA', 'CONFORMED', 'PRESENTATION', 'AI_ML')" 4
run_existence_test "Warehouses Exist" "SHOW WAREHOUSES LIKE 'TCH_%'; SELECT CASE WHEN COUNT(*) >= 3 THEN 1 ELSE 0 END FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"

# Test 3: Raw Data Tables
echo -e "\n${YELLOW}3. Testing Raw Data Tables${NC}"
run_sql_test "Raw Tables Count" "USE DATABASE $DATABASE_NAME; USE SCHEMA RAW_DATA; SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'RAW_DATA' AND TABLE_TYPE = 'BASE TABLE'" 8
run_existence_test "Stages Exist" "USE DATABASE $DATABASE_NAME; USE SCHEMA RAW_DATA; SHOW STAGES; SELECT CASE WHEN COUNT(*) >= 2 THEN 1 ELSE 0 END FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"

# Test 4: Data Content
echo -e "\n${YELLOW}4. Testing Data Content${NC}"
run_sql_test "Patient Records" "USE DATABASE $DATABASE_NAME; SELECT COUNT(*) FROM RAW_DATA.PATIENTS" 100
run_sql_test "Encounter Records" "USE DATABASE $DATABASE_NAME; SELECT COUNT(*) FROM RAW_DATA.ENCOUNTERS" 100
run_sql_test "Diagnosis Records" "USE DATABASE $DATABASE_NAME; SELECT COUNT(*) FROM RAW_DATA.DIAGNOSES" 50

# Test 5: Presentation Layer
echo -e "\n${YELLOW}5. Testing Presentation Layer${NC}"
run_sql_test "Presentation Tables" "USE DATABASE $DATABASE_NAME; USE SCHEMA PRESENTATION; SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PRESENTATION'" 5
run_existence_test "Patient 360 View" "USE DATABASE $DATABASE_NAME; USE SCHEMA PRESENTATION; SELECT 1 FROM PATIENT_360 LIMIT 1"

# Test 6: Dynamic Tables
echo -e "\n${YELLOW}6. Testing Dynamic Tables${NC}"
run_sql_test "Dynamic Tables Count" "USE DATABASE $DATABASE_NAME; SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'DYNAMIC TABLE'" 5
run_existence_test "Dynamic Tables Active" "USE DATABASE $DATABASE_NAME; SHOW DYNAMIC TABLES; SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE \"state\" = 'RUNNING'"

# Test 7: AI/ML Components
echo -e "\n${YELLOW}7. Testing AI/ML Components${NC}"
run_existence_test "AI_ML Schema" "USE DATABASE $DATABASE_NAME; USE SCHEMA AI_ML; SELECT 1"
run_existence_test "Cortex Functions" "USE DATABASE $DATABASE_NAME; USE SCHEMA AI_ML; SHOW FUNCTIONS LIKE '%SEARCH%'; SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"

# Test 8: Streamlit Application
echo -e "\n${YELLOW}8. Testing Streamlit Application${NC}"
run_existence_test "Streamlit App Exists" "SHOW STREAMLITS LIKE 'TCH_PATIENT_360_APP'; SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"
run_existence_test "App Permissions" "SHOW GRANTS ON STREAMLIT TCH_PATIENT_360_APP; SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"

# Test 9: Data Quality Checks
echo -e "\n${YELLOW}9. Testing Data Quality${NC}"
run_existence_test "Patient Age Distribution" "USE DATABASE $DATABASE_NAME; SELECT CASE WHEN COUNT(DISTINCT age_group) >= 4 THEN 1 ELSE 0 END FROM PRESENTATION.PATIENT_360"
run_existence_test "Diagnosis Variety" "USE DATABASE $DATABASE_NAME; SELECT CASE WHEN COUNT(DISTINCT diagnosis_category) >= 5 THEN 1 ELSE 0 END FROM PRESENTATION.DIAGNOSIS_ANALYTICS"
run_existence_test "Encounter Types" "USE DATABASE $DATABASE_NAME; SELECT CASE WHEN COUNT(DISTINCT encounter_type) >= 3 THEN 1 ELSE 0 END FROM PRESENTATION.ENCOUNTER_ANALYTICS"

# Test 10: Performance Check
echo -e "\n${YELLOW}10. Testing Performance${NC}"
run_existence_test "Query Performance" "USE DATABASE $DATABASE_NAME; USE WAREHOUSE TCH_ANALYTICS_WH; SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM PRESENTATION.PATIENT_360 WHERE current_age BETWEEN 5 AND 15"

echo -e "\n${BLUE}=========================================="
echo -e "Validation Summary"
echo -e "==========================================${NC}"

if [[ $PASSED_TESTS -eq $TOTAL_TESTS ]]; then
    echo -e "${GREEN}🎉 ALL TESTS PASSED!${NC}"
    echo -e "✅ $PASSED_TESTS out of $TOTAL_TESTS tests successful"
    echo -e "\nDeployment appears to be working correctly."
    echo -e "You can now:"
    echo -e "  1. Access Snowsight and switch to role: $ROLE_NAME"
    echo -e "  2. Navigate to the Streamlit section"
    echo -e "  3. Launch the TCH_PATIENT_360_APP"
    echo -e "  4. Explore the Patient 360 dashboard"
    exit_code=0
else
    echo -e "${RED}❌ SOME TESTS FAILED${NC}"
    echo -e "✅ $PASSED_TESTS out of $TOTAL_TESTS tests successful"
    echo -e "${RED}❌ $((TOTAL_TESTS - PASSED_TESTS)) tests failed${NC}"
    echo -e "\nPlease review the failed tests above and:"
    echo -e "  1. Check the deployment logs"
    echo -e "  2. Verify ACCOUNTADMIN setup was completed"
    echo -e "  3. Ensure sufficient time for Dynamic Tables to initialize"
    echo -e "  4. Re-run deployment if necessary"
    exit_code=1
fi

echo -e "\nFor troubleshooting, see: deploy/README.md"
echo -e "Deployment logs: deploy/deployment.log"

exit $exit_code