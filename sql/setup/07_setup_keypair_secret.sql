-- =====================================================
-- TCH Patient 360 PoC - Setup Private Key Secret
-- =====================================================
-- This script creates a Snowflake secret containing the
-- private key for JWT authentication in SPCS environment
-- 
-- SECURITY NOTE: This approach keeps the private key
-- secure within Snowflake and never exposes it in Git
-- =====================================================

USE ROLE ACCOUNTADMIN;  -- Secrets require ACCOUNTADMIN
USE DATABASE TCH_PATIENT_360_POC;

-- =====================================================
-- Step 1: Create Secret for Private Key
-- =====================================================
-- NOTE: You need to manually replace <PRIVATE_KEY_CONTENT> 
-- with the actual content of keypair/rsa_key.p8
-- 
-- To get the private key content:
-- cat keypair/rsa_key.p8

-- IMPORTANT: Replace <PRIVATE_KEY_CONTENT> with the actual private key
-- Example format:
-- -----BEGIN ENCRYPTED PRIVATE KEY-----
-- MIIFHDBOBgkqhkiG9w0BBQ0wQTApBgkqhkiG9w0BBQwwHAQI...
-- -----END ENCRYPTED PRIVATE KEY-----

/*
CREATE OR REPLACE SECRET keypair_secret
TYPE = GENERIC_STRING
SECRET_STRING = '<PRIVATE_KEY_CONTENT>';
*/

-- =====================================================
-- Step 2: Grant Access to TCH Role
-- =====================================================
/*
GRANT READ ON SECRET keypair_secret TO ROLE TCH_PATIENT_360_ROLE;
*/

-- =====================================================
-- Step 3: Test Secret Access
-- =====================================================
-- Switch to the application role to test access
USE ROLE TCH_PATIENT_360_ROLE;

-- Test reading the secret (this should work after setup)
/*
SELECT SYSTEM$GET_SECRET('keypair_secret') as private_key_test;
*/

-- =====================================================
-- MANUAL SETUP INSTRUCTIONS
-- =====================================================
-- 1. Read the private key content:
--    cat /Users/jakers/cursor/TCH_Patient_360/keypair/rsa_key.p8
--
-- 2. Copy the entire key content (including BEGIN/END lines)
--
-- 3. Replace <PRIVATE_KEY_CONTENT> in the CREATE SECRET statement above
--
-- 4. Uncomment and run the CREATE SECRET and GRANT statements
--
-- 5. Test with the SELECT SYSTEM$GET_SECRET statement
-- =====================================================

SELECT 'Private key secret setup required' AS status,
       'Manually replace <PRIVATE_KEY_CONTENT> and run CREATE SECRET' AS instruction;
