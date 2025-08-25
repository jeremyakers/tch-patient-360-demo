# TCH Data Generation Notebook

## Overview

The `tch_data_generator.ipynb` notebook generates comprehensive, realistic synthetic healthcare data for the Texas Children's Hospital Patient 360 PoC demonstration. This notebook runs directly in Snowflake, leveraging cloud compute for fast data generation and direct upload to Snowflake stages.

## Key Benefits

1. **Cloud-Based Generation**: Runs on Snowflake compute, not your local machine
2. **Direct Stage Upload**: Uses `session.file.put()` to upload files directly to Snowflake stages  
3. **No Local Upload Required**: Eliminates slow WiFi/internet bottlenecks
4. **Scalable Performance**: Leverages Snowflake's elastic compute for any data size

## How It Works

1. **Generates Data Locally**: Creates CSV files in the Snowflake compute environment's `/tmp` directory
2. **Uploads to Stages**: Uses `session.file.put()` to upload files to internal stages:
   - Structured data → `@TCH_PATIENT_360_POC.RAW_DATA.PATIENT_DATA_STAGE`
   - Unstructured data → `@TCH_PATIENT_360_POC.RAW_DATA.UNSTRUCTURED_DATA_STAGE`
3. **Ready for COPY INTO**: The deployment script then runs `COPY INTO` commands to load data into tables

## Execution Parameters

The notebook accepts the following parameters:

- `data_size`: Size preset - 'small' (1K patients), 'medium' (5K), or 'large' (25K)
- `num_patients`: Override patient count (optional, overrides data_size)
- `encounters_per_patient`: Average encounters per patient (default: 5)
- `compress_files`: Whether to gzip files (default: true)

### Parameter Passing

When executed via `EXECUTE NOTEBOOK`, parameters are passed as arguments:
```sql
EXECUTE NOTEBOOK TCH_PATIENT_360_POC.AI_ML.TCH_DATA_GENERATOR(
    'data_size=small',
    'encounters_per_patient=10'
);
```

## Data Generated

### Structured Data (CSV files)
- **patients.csv**: Patient demographics from Epic
- **encounters.csv**: Healthcare encounters from Epic
- **diagnoses.csv**: ICD-10 diagnosis records from Epic
- **lab_results.csv**: Laboratory test results from Epic
- **medications.csv**: Prescription records from Epic
- **vital_signs.csv**: Vital sign measurements from Epic
- **imaging_studies.csv**: Radiology orders from Epic
- **providers.csv**: Healthcare provider data from Workday
- **departments.csv**: Hospital departments from Workday

### Unstructured Data (Text files)
- **clinical_notes/**: Progress notes, nursing notes, discharge summaries
- **radiology_reports/**: Radiology interpretation reports

## Dependencies

The notebook requires these Python packages (defined in `environment.yml`):
- faker
- pandas  
- numpy
- snowflake-snowpark-python

## Deployment

### Deploy the notebook:
```bash
cd python/notebooks
snow notebook deploy --database TCH_PATIENT_360_POC --schema AI_ML
```

### Execute via SQL:
```sql
EXECUTE NOTEBOOK TCH_PATIENT_360_POC.AI_ML.TCH_DATA_GENERATOR('data_size=small');
```

### Or via deployment script:
```bash
./deploy/deploy_tch_poc.sh \
    --account ACCOUNT_ID \
    --user USERNAME \
    --private-key path/to/key.p8 \
    --generate-data-size small
```

## Technical Implementation

### Key Components:

1. **PediatricDataGenerator**: Generates age-appropriate patient demographics
2. **ClinicalNotesGenerator**: Creates realistic clinical documentation  
3. **TCHDataGenerationOrchestrator**: Coordinates all data generation

### Data Format Consistency

The notebook ensures exact schema compatibility with the original Python scripts:
- Consistent column names and data types
- Proper date formatting (MM/DD/YYYY for DOB in clinical notes)
- All required fields present (no extra columns)

### Multi-Source Data Simulation

While generated in the notebook, data is organized to simulate multiple source systems:
- Epic EHR (clinical data)
- Workday HCM (provider/department data)
- Oracle ERP (financial data - placeholder)
- Salesforce (engagement data - placeholder)

## Troubleshooting

### Common Issues:

1. **Authentication errors**: Ensure keypair authentication is configured
2. **Schema not found**: Verify AI_ML schema exists
3. **Stage access**: Check permissions on RAW_DATA stages

### Verifying Data Upload:
```sql
-- Check structured data stage
LIST @TCH_PATIENT_360_POC.RAW_DATA.PATIENT_DATA_STAGE;

-- Check unstructured data stage  
LIST @TCH_PATIENT_360_POC.RAW_DATA.UNSTRUCTURED_DATA_STAGE;
```

## Important Notes

- **No Local Files**: All generation happens in Snowflake compute
- **Direct Upload**: Files go straight from compute to stages via `session.file.put()`
- **COPY INTO Compatible**: Maintains same file format for existing `COPY INTO` commands
- **Scalable**: Can generate 25K+ patients efficiently on Snowflake compute

## Migration from Shell Script

This notebook replaces the previous shell script approach (`generate_tch_data.py`) with these advantages:
- No local compute requirements
- No network upload bottlenecks  
- Integrated Snowflake authentication
- Faster execution on cloud resources

---

**Note**: The SnowCLI `snow notebook execute` command does not currently support the `--env` parameter for passing environment variables. Instead, use `EXECUTE NOTEBOOK` SQL command with arguments.