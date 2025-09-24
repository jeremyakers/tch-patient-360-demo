# SPCS Keypair Secret Setup Guide

## Overview

For Streamlit in Snowflake on SPCS (container runtime) to make REST API calls to Cortex Agents, we need to authenticate using JWT tokens generated from a private key. This guide explains how to securely set up the private key using Snowflake secrets.

## Why This is Required

In SPCS container runtime:
- No access to internal `_snowflake` helper libraries
- Must authenticate to Snowflake REST APIs like any external application
- Uses JWT authentication with public/private key pairs
- Private key must be stored securely (never in Git)

## Setup Steps

### Step 1: Locate the Private Key

The private key should already exist in your local repository:
```bash
ls -la keypair/
# Should show: rsa_key.p8 and rsa_key.pub
```

### Step 2: Read the Private Key Content

```bash
cat keypair/rsa_key.p8
```

**Copy the entire output**, including the `-----BEGIN ENCRYPTED PRIVATE KEY-----` and `-----END ENCRYPTED PRIVATE KEY-----` lines.

### Step 3: Create the Snowflake Secret

Run the following SQL commands in Snowsight:

```sql
-- Switch to ACCOUNTADMIN (required for creating secrets)
USE ROLE ACCOUNTADMIN;
USE DATABASE TCH_PATIENT_360_POC;

-- Create the secret with the private key content
-- Replace <PRIVATE_KEY_CONTENT> with the actual content from Step 2
CREATE OR REPLACE SECRET keypair_secret
TYPE = GENERIC_STRING
SECRET_STRING = '<PRIVATE_KEY_CONTENT>';

-- Grant read access to the application role
GRANT READ ON SECRET keypair_secret TO ROLE TCH_PATIENT_360_ROLE;
```

### Step 4: Verify Secret Access

Test that the secret is accessible:

```sql
-- Switch to the application role
USE ROLE TCH_PATIENT_360_ROLE;

-- Test reading the secret (should return the private key content)
SELECT SYSTEM$GET_SECRET('keypair_secret') as private_key_test;
```

If successful, you should see the private key content returned.

### Step 5: Update Deployment Scripts (Optional)

The deployment scripts have been updated to include keypair secret setup automatically. However, the secret creation step requires manual intervention to paste the private key content.

## Security Benefits

This approach provides several security advantages:

1. **✅ Private key never in Git**: Key stays out of version control
2. **✅ Encrypted at rest**: Snowflake secrets are encrypted in storage
3. **✅ Role-based access**: Only `TCH_PATIENT_360_ROLE` can access the secret
4. **✅ Audit trail**: Secret access is logged in Snowflake
5. **✅ Centralized management**: Key stored in Snowflake, not distributed

## Alternative Approaches

If Snowflake secrets are not available, the implementation also supports:

### Environment Variable (Less Secure)
```bash
export SNOWFLAKE_PRIVATE_KEY="$(cat keypair/rsa_key.p8)"
```

### File System (Development Only)
Copy the private key to the container file system (not recommended for production).

## Troubleshooting

### Secret Not Found
```sql
-- Check if secret exists
SHOW SECRETS;

-- Check grants
SHOW GRANTS TO ROLE TCH_PATIENT_360_ROLE;
```

### Authentication Errors
- Verify the private key content is complete (including BEGIN/END lines)
- Check that the public key is properly assigned to the user account
- Ensure the account identifier and username match exactly

### JWT Token Errors
- Check that `pyjwt` and `cryptography` packages are installed
- Verify the private key is in valid PKCS#8 format
- Confirm the key fingerprint matches the user's public key

## Integration with SPCS

Once the secret is set up:

1. **SPCS containers** automatically read the secret via `SYSTEM$GET_SECRET()`
2. **JWT tokens** are generated on-demand for REST API calls
3. **Cortex Agents API** calls work with proper authentication
4. **SSE streaming** is enabled for real-time agent interactions

## Files Modified

- `utils/jwt_auth.py` - JWT token generation logic
- `utils/snowflake_api.py` - SPCS authentication handling
- `sql/setup/07_setup_keypair_secret.sql` - Secret setup script
- `sql/00_master.sql` - Includes secret setup in deployment
- `requirements.txt` - Added JWT dependencies (`pyjwt`, `cryptography`)

---

**Next**: After completing this setup, proceed with the SPCS migration as described in the main migration guide.
