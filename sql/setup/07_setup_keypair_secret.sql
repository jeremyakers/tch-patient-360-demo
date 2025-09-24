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

-- NOTE: The private key content should be provided via the main README setup
-- This script provides a template for the secret creation

-- Template for secret creation (to be executed manually):
/*
CREATE OR REPLACE SECRET keypair_secret
TYPE = GENERIC_STRING
SECRET_STRING = '<PRIVATE_KEY_CONTENT>';

GRANT READ ON SECRET keypair_secret TO ROLE TCH_PATIENT_360_ROLE;
*/

-- Check if secret already exists
SELECT 'Checking for existing keypair secret...' as status;

-- Try to access the secret (will fail if not created)
SELECT 
    CASE 
        WHEN SYSTEM$GET_SECRET('keypair_secret') IS NOT NULL 
        THEN 'Keypair secret exists and is accessible'
        ELSE 'Keypair secret not found'
    END as secret_status;

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
