-- User-Level Network Policy for SPCS with Existing Account IPs
-- Run this with your main account that has ACCOUNTADMIN or SECURITYADMIN privileges

USE ROLE ACCOUNTADMIN;

-- Step 1: First check what network policy is currently applied at the account level
SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;

-- Step 2: Get the details of the current account network policy
-- Replace 'YOUR_ACCOUNT_POLICY_NAME' with the actual policy name from Step 1
-- DESCRIBE NETWORK POLICY YOUR_ACCOUNT_POLICY_NAME;

-- Step 3: Create a user-level policy that includes both existing IPs and SPCS range
-- You'll need to manually copy the ALLOWED_IP_LIST from the account policy and add to it
CREATE OR REPLACE NETWORK POLICY TCH_PATIENT_360_USER_NETWORK_POLICY
    ALLOWED_IP_LIST = (
        -- Copy existing allowed IPs from your account policy here, for example:
        -- 'YOUR.OFFICE.IP.RANGE/24',
        -- 'YOUR.VPN.IP.RANGE/16', 
        -- Then add SPCS internal range:
        '10.0.0.0/8'      -- SPCS internal IP range (includes 10.16.68.179 from your error)
    )
    COMMENT = 'Allow TCH_PATIENT_360_USER from existing IPs plus SPCS containers';

-- Step 4: Apply the policy to the user
ALTER USER TCH_PATIENT_360_USER SET NETWORK_POLICY = TCH_PATIENT_360_USER_NETWORK_POLICY;

-- Step 5: Verify it worked
DESCRIBE USER TCH_PATIENT_360_USER;
-- Look for: NETWORK_POLICY | TCH_PATIENT_360_USER_NETWORK_POLICY

-- Example with common corporate IPs (adjust based on your actual account policy):
/*
CREATE OR REPLACE NETWORK POLICY TCH_PATIENT_360_USER_NETWORK_POLICY
    ALLOWED_IP_LIST = (
        '10.0.0.0/8',          -- SPCS internal range
        '192.168.1.0/24',      -- Example: Office network
        '172.20.0.0/16',       -- Example: VPN range
        '52.89.214.238/32',    -- Example: Specific allowed IP
        '34.223.103.190/32'    -- Example: Another specific IP
    )
    COMMENT = 'TCH_PATIENT_360_USER policy with SPCS and existing allowed IPs';
*/
