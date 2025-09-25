-- Helper Script to Check Current Network Policies
-- This helps you understand what IPs are currently allowed so you can add them to the user policy

USE ROLE ACCOUNTADMIN;

-- 1. Check account-level network policy
SELECT 'Account Level Policy' as POLICY_TYPE, * 
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1)))
WHERE "key" = 'NETWORK_POLICY';

SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;

-- 2. List all network policies in the account
SHOW NETWORK POLICIES;

-- 3. Get details of each network policy (you'll need to run this for each policy name)
-- Look for the one that's applied to your account from step 1
-- Then uncomment and run with the actual policy name:

-- DESCRIBE NETWORK POLICY <YOUR_ACCOUNT_POLICY_NAME>;

-- The output will show:
-- - name: The policy name
-- - allowed_ip_list: The IPs currently allowed
-- - blocked_ip_list: Any blocked IPs
-- - comment: Description

-- 4. Check if any user already has a network policy
SHOW USERS;
-- Look for the NETWORK_POLICY column

-- 5. Specifically check TCH_PATIENT_360_USER
DESCRIBE USER TCH_PATIENT_360_USER;

-- After you get the allowed_ip_list from your account policy,
-- you can create the user policy with those IPs plus 10.0.0.0/8:
/*
CREATE OR REPLACE NETWORK POLICY TCH_PATIENT_360_USER_NETWORK_POLICY
    ALLOWED_IP_LIST = (
        -- Paste the IPs from your account policy here
        -- Then add:
        '10.0.0.0/8'  -- SPCS internal range
    )
    COMMENT = 'User policy with existing allowed IPs plus SPCS range';

ALTER USER TCH_PATIENT_360_USER SET NETWORK_POLICY = TCH_PATIENT_360_USER_NETWORK_POLICY;
*/
