# Texas Children's Hospital Patient 360 PoC - Role and Privileges Guide

## Overview

This PoC uses a dedicated role (`TCH_PATIENT_360_ROLE`) instead of system roles like `SYSADMIN` to follow security best practices and provide proper resource isolation.

## Deployment Process

### Step 1: ACCOUNTADMIN Setup (Required First)

**File**: `sql/setup/00_accountadmin_setup.sql`

**Who**: Must be run by a user with `ACCOUNTADMIN` role
**Purpose**: Creates the dedicated role and grants minimal necessary account-level privileges

**Privileges Granted**:
- `CREATE DATABASE ON ACCOUNT` - Allows creating the PoC database
- Ownership of three dedicated warehouses: `TCH_COMPUTE_WH`, `TCH_ANALYTICS_WH`, `TCH_AI_ML_WH`

**Additional Privileges** (commented out, can be enabled if needed):
- `EXECUTE MANAGED TASK ON ACCOUNT` - For advanced Cortex features requiring tasks
- `EXECUTE TASK ON ACCOUNT` - For task-based automation
- `CREATE INTEGRATION ON ACCOUNT` - For Streamlit apps with external access

**Note**: Most Cortex functions and Streamlit in Snowflake apps work with database ownership alone and don't require additional account-level privileges.

### Step 2: Grant Role to Users

After running the ACCOUNTADMIN setup, grant the role to users who will deploy or manage the PoC:

```sql
USE ROLE ACCOUNTADMIN;
GRANT ROLE TCH_PATIENT_360_ROLE TO USER <your_username>;
```

### Step 3: Run Deployment Scripts

All other scripts in the PoC can now be run using the `TCH_PATIENT_360_ROLE`.

## Resource Ownership

Once deployed, `TCH_PATIENT_360_ROLE` owns:

### Warehouses
- `TCH_COMPUTE_WH` - General data processing and transformations
- `TCH_ANALYTICS_WH` - Heavy analytical workloads and reporting  
- `TCH_AI_ML_WH` - AI/ML workloads including Cortex functions

### Database and Schemas
- `TCH_PATIENT_360_POC` database
- All schemas: `RAW_DATA`, `CONFORMED`, `PRESENTATION`, `AI_ML`
- All tables, views, stages, and file formats within these schemas

### AI/ML Services
- Cortex Analyst semantic models
- Cortex Search services
- Streamlit in Snowflake applications

## Security Considerations

### Principle of Least Privilege
- The role only has the minimum privileges required for the PoC
- No access to other databases or account-level objects beyond what's necessary
- Can be easily dropped to clean up all PoC resources

### Access Control
- Role can be granted to specific users who need PoC access
- Role is granted to `SYSADMIN` for administrative oversight
- No `PUBLIC` grants are used in the PoC

### Resource Isolation
- All PoC resources are contained within the dedicated database
- Dedicated warehouses prevent resource conflicts with other workloads
- Clear ownership makes cleanup straightforward

## Cleanup

To completely remove the PoC:

```sql
USE ROLE ACCOUNTADMIN;

-- Drop the Streamlit app
DROP STREAMLIT IF EXISTS TCH_PATIENT_360_APP;

-- Drop the database (cascades to all schemas, tables, views)
DROP DATABASE IF EXISTS TCH_PATIENT_360_POC;

-- Drop the warehouses
DROP WAREHOUSE IF EXISTS TCH_COMPUTE_WH;
DROP WAREHOUSE IF EXISTS TCH_ANALYTICS_WH;
DROP WAREHOUSE IF EXISTS TCH_AI_ML_WH;

-- Drop the role
DROP ROLE IF EXISTS TCH_PATIENT_360_ROLE;
```

## Troubleshooting

### Common Issues

1. **"Insufficient privileges" errors**
   - Ensure `00_accountadmin_setup.sql` was run first by ACCOUNTADMIN
   - Verify the role was granted to your user account

2. **"Object does not exist" errors for warehouses**
   - Check that the ACCOUNTADMIN setup created the warehouses
   - Ensure you're using the correct warehouse names with `TCH_` prefix

3. **Cortex function errors**
   - Most Cortex functions should work with database ownership alone
   - If advanced Cortex features fail, uncomment and run the task-related privileges in the ACCOUNTADMIN setup
   - Check that you're using the correct AI/ML warehouse (`TCH_AI_ML_WH`)

4. **Streamlit deployment errors**
   - Streamlit in Snowflake apps should work with database ownership alone
   - If external access is needed, uncomment the `CREATE INTEGRATION` privilege
   - Verify the stage exists and contains the application files

5. **Additional privilege requirements**
   - If specific features require additional privileges, uncomment the relevant lines in `00_accountadmin_setup.sql`
   - Re-run the ACCOUNTADMIN setup after uncommenting needed privileges
   - The script is designed with minimal privileges that can be expanded as needed