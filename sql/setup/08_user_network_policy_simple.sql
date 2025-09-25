-- Simple User-Level Network Policy for SPCS
-- Run this with your main account that has ACCOUNTADMIN or SECURITYADMIN privileges

-- Option 1: Allow all IPs for TCH_PATIENT_360_USER (simplest for testing)
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NETWORK POLICY TCH_PATIENT_360_USER_NETWORK_POLICY
    ALLOWED_IP_LIST = ('0.0.0.0/0')
    COMMENT = 'Allow TCH_PATIENT_360_USER to connect from any IP (including SPCS containers)';

ALTER USER TCH_PATIENT_360_USER SET NETWORK_POLICY = TCH_PATIENT_360_USER_NETWORK_POLICY;


-- Option 2: More restrictive - only allow private IP ranges typically used by SPCS
/*
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NETWORK POLICY TCH_PATIENT_360_USER_NETWORK_POLICY
    ALLOWED_IP_LIST = (
        '10.0.0.0/8',      -- Private IP range (includes 10.16.68.179 from your error)
        '172.16.0.0/12',   -- Private IP range
        '192.168.0.0/16'   -- Private IP range
    )
    COMMENT = 'Allow TCH_PATIENT_360_USER to connect from SPCS internal IPs';

ALTER USER TCH_PATIENT_360_USER SET NETWORK_POLICY = TCH_PATIENT_360_USER_NETWORK_POLICY;
*/

-- Verify it worked:
DESCRIBE USER TCH_PATIENT_360_USER;
-- Look for: NETWORK_POLICY | TCH_PATIENT_360_USER_NETWORK_POLICY
