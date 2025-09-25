-- Create User-Level Network Policy for SPCS Access
-- 
-- This creates a less restrictive network policy specifically for the TCH_PATIENT_360_USER
-- to allow SPCS containers to make REST API calls back to Snowflake.
--
-- User-level policies override account-level policies, allowing us to work around
-- restrictive account policies without affecting other users.
--
-- IMPORTANT: This script must be run by a user with ACCOUNTADMIN or SECURITYADMIN privileges,
-- not by TCH_PATIENT_360_USER itself.

USE ROLE ACCOUNTADMIN;  -- Or SECURITYADMIN if you don't have ACCOUNTADMIN

-- Create a network policy that allows SPCS internal IPs
CREATE OR REPLACE NETWORK POLICY TCH_PATIENT_360_USER_NETWORK_POLICY
    ALLOWED_IP_LIST = (
        '0.0.0.0/0'       -- Allow all IPs for this specific user
        -- Or if you want to be more restrictive:
        -- '10.0.0.0/8',     -- SPCS internal network (includes 10.16.68.179)
        -- '172.16.0.0/12',  -- Additional private network range
        -- '192.168.0.0/16', -- Additional private network range
        -- 'YOUR.PUBLIC.IP.HERE/32'  -- Your own IP if needed
    )
    COMMENT = 'Allow TCH_PATIENT_360_USER to access from SPCS containers and bypass account restrictions';

-- Apply the network policy to the TCH_PATIENT_360_USER
ALTER USER TCH_PATIENT_360_USER SET NETWORK_POLICY = TCH_PATIENT_360_USER_NETWORK_POLICY;

-- Verify the policy is applied
DESCRIBE USER TCH_PATIENT_360_USER;

-- You should see NETWORK_POLICY = TCH_PATIENT_360_USER_NETWORK_POLICY in the output

-- To check what network policies exist:
SHOW NETWORK POLICIES;

-- To see details of our new policy:
DESCRIBE NETWORK POLICY TCH_PATIENT_360_USER_NETWORK_POLICY;

-- IMPORTANT: After running this, the TCH_PATIENT_360_USER will be able to connect from any IP
-- This is necessary for SPCS containers which get dynamic internal IPs
-- The account-level policy will still apply to all other users
