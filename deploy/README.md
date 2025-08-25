# Texas Children's Hospital Patient 360 PoC - Deployment Guide

This directory contains automated deployment scripts and configuration for the TCH Patient 360 Proof of Concept.

## 📋 Prerequisites

### 1. Software Requirements
- **SnowCLI**: Install from [Snowflake CLI Documentation](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)
- **Python 3.8+**: With packages `pandas` and `faker`
  ```bash
  pip install pandas faker
  ```

### 2. Snowflake Setup (ACCOUNTADMIN)
Before running the deployment, an ACCOUNTADMIN must execute:
```sql
-- Run this once as ACCOUNTADMIN
@sql/setup/00_accountadmin_setup.sql

-- Grant the role to deployment users
GRANT ROLE TCH_PATIENT_360_ROLE TO USER <your_username>;
```

## 🚀 Quick Start

### Option 1: Command Line Deployment

**Linux/macOS:**
```bash
# Navigate to project root
cd TCH_Patient_360

# Make script executable
chmod +x deploy/deploy_tch_poc.sh

# Deploy with password authentication
./deploy/deploy_tch_poc.sh \
  --account "xy12345.us-east-1" \
  --user "your_username" \
  --password "your_password"

# Deploy with private key authentication
./deploy/deploy_tch_poc.sh \
  --account "xy12345.us-east-1" \
  --user "your_username" \
  --private-key "/path/to/key.p8"
```

**Windows PowerShell:**
```powershell
# Navigate to project root
cd TCH_Patient_360

# Deploy with password authentication
.\deploy\deploy_tch_poc.ps1 `
  -Account "xy12345.us-east-1" `
  -Username "your_username" `
  -Password "your_password"

# Deploy with private key authentication
.\deploy\deploy_tch_poc.ps1 `
  -Account "xy12345.us-east-1" `
  -Username "your_username" `
  -PrivateKeyPath "C:\path\to\key.p8"
```

### Option 2: Configuration File (Future Enhancement)
```bash
# Copy example configuration
cp deploy/config.example.toml deploy/config.toml

# Edit with your settings
vim deploy/config.toml

# Deploy using configuration
./deploy/deploy_tch_poc.sh --config deploy/config.toml
```

## 🛠️ Deployment Options

### Data Generation Sizes
- `--generate-data-size small`: 1,000 patients (~50MB)
- `--generate-data-size medium`: 5,000 patients (~250MB) **(default)**
- `--generate-data-size large`: 25,000 patients (~1.2GB)

### Deployment Modes
- `--clean-deploy`: Drop existing objects before deployment
- `--skip-data-generation`: Use existing data files
- `--skip-streamlit`: Skip Streamlit app deployment
- `--verbose`: Enable detailed logging

### Authentication Methods
- **Password**: `--password "your_password"`
- **Private Key**: `--private-key "/path/to/key.p8"`
- **Key with Passphrase**: `--private-key "/path/to/key.p8" --private-key-passphrase "phrase"`

## 📁 Deployment Process

The script executes these steps in order:

1. **Prerequisites Check**
   - Verify SnowCLI installation
   - Check Python and required packages
   - Validate authentication credentials

2. **Data Generation** (unless skipped)
   - Generate realistic mock healthcare data
   - Create structured CSV files
   - Generate unstructured clinical documents

3. **Database Setup**
   - Create database and schemas
   - Set up file formats and stages
   - Create raw data tables

4. **Data Loading**
   - Upload files to Snowflake stages
   - Load structured data into raw tables
   - Stage unstructured documents

5. **Data Transformation**
   - Deploy Dynamic Tables for real-time transformation
   - Create conformed and presentation layers
   - Set up data pipelines

6. **AI/ML Services**
   - Configure Cortex Analyst semantic model
   - Set up Cortex Search for unstructured data
   - Create AI functions and procedures

7. **Streamlit App** (unless skipped)
   - Upload Streamlit application files
   - Create Streamlit in Snowflake app
   - Configure app permissions

8. **Verification**
   - Validate all components
   - Check data loading
   - Confirm app accessibility

## 📊 What Gets Deployed

### Database Structure
```
TCH_PATIENT_360_POC/
├── RAW_DATA/           # Source system data
├── CONFORMED/          # Standardized data
├── PRESENTATION/       # Analytics-ready views
└── AI_ML/              # Cortex services
```

### Warehouses
- `TCH_COMPUTE_WH`: General data processing
- `TCH_ANALYTICS_WH`: Heavy analytical workloads
- `TCH_AI_ML_WH`: AI/ML and Cortex functions

### Applications
- **Streamlit App**: `TCH_PATIENT_360_APP`
  - Patient search and selection
  - Comprehensive Patient 360 dashboard
  - AI-powered chat interface
  - Population cohort builder

## 🔍 Verification

After deployment, verify success by:

1. **Database Access**
   ```sql
   USE ROLE TCH_PATIENT_360_ROLE;
   USE DATABASE TCH_PATIENT_360_POC;
   
   -- Check patient data
   SELECT COUNT(*) FROM RAW_DATA.PATIENTS;
   
   -- Check Dynamic Tables
   SHOW DYNAMIC TABLES;
   ```

2. **Streamlit App**
   - Log into Snowsight
   - Navigate to "Streamlit" section
   - Launch "TCH_PATIENT_360_APP"

3. **Data Quality**
   - Patient records with realistic demographics
   - Clinical data (encounters, diagnoses, labs)
   - Unstructured clinical notes

## 🐛 Troubleshooting

### Common Issues

**Connection Errors**
```
Error: Failed to connect to Snowflake
```
- Verify account identifier format
- Check username and password/key
- Ensure TCH_PATIENT_360_ROLE is granted to user

**Permission Errors**
```
Error: Insufficient privileges
```
- Confirm ACCOUNTADMIN setup was completed
- Verify role grant: `SHOW GRANTS TO USER <username>`

**Data Generation Errors**
```
Error: Python packages not found
```
```bash
pip install pandas faker
```

**Streamlit Deployment Errors**
```
Error: Cannot create Streamlit app
```
- Check if files were uploaded to stage
- Verify warehouse permissions
- Ensure role has necessary privileges

### Debug Options

**Verbose Logging**
```bash
./deploy/deploy_tch_poc.sh --verbose [other options]
```

**Skip Components**
```bash
# Skip data generation (use existing data)
./deploy/deploy_tch_poc.sh --skip-data-generation [other options]

# Skip Streamlit (database only)
./deploy/deploy_tch_poc.sh --skip-streamlit [other options]
```

**Clean Deployment**
```bash
# Start fresh (removes existing objects)
./deploy/deploy_tch_poc.sh --clean-deploy [other options]
```

## 📝 Logs

Deployment logs are saved to:
- **Location**: `deploy/deployment.log`
- **Format**: Timestamped with step details
- **Retention**: Appends to existing log

## 🧹 Cleanup

To remove the PoC completely:

```sql
-- Run as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Drop all PoC objects
DROP STREAMLIT IF EXISTS TCH_PATIENT_360_APP;
DROP DATABASE IF EXISTS TCH_PATIENT_360_POC;
DROP WAREHOUSE IF EXISTS TCH_COMPUTE_WH;
DROP WAREHOUSE IF EXISTS TCH_ANALYTICS_WH;
DROP WAREHOUSE IF EXISTS TCH_AI_ML_WH;
DROP ROLE IF EXISTS TCH_PATIENT_360_ROLE;
```

## 🆘 Support

For deployment issues:
1. Check the deployment log: `deploy/deployment.log`
2. Review the troubleshooting section above
3. Refer to the main project documentation
4. Check Snowflake documentation for SnowCLI and Cortex features

## 📋 Deployment Checklist

- [ ] SnowCLI installed and configured
- [ ] Python 3.8+ with pandas, faker packages
- [ ] ACCOUNTADMIN ran `00_accountadmin_setup.sql`
- [ ] User granted `TCH_PATIENT_360_ROLE`
- [ ] Snowflake account details ready
- [ ] Authentication method chosen (password/key)
- [ ] Deployment script permissions set (`chmod +x` on Linux/macOS)
- [ ] Run deployment script with appropriate options
- [ ] Verify deployment success in Snowsight
- [ ] Test Streamlit application functionality