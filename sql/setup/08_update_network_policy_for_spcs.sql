-- Update Network Policy to Allow SPCS Internal IPs
-- 
-- SPCS containers run in Snowflake's internal network and need to be allowed
-- to make REST API calls back to Snowflake endpoints.
--
-- The IP 10.16.68.179 shown in the error is from the SPCS container's internal network.
-- We need to allow the SPCS internal IP range.

USE ROLE ACCOUNTADMIN;

-- First, check if there's an existing network policy
SHOW NETWORK POLICIES;

-- If you have an existing network policy, you'll need to ALTER it
-- Replace 'YOUR_NETWORK_POLICY_NAME' with your actual policy name
/*
ALTER NETWORK POLICY YOUR_NETWORK_POLICY_NAME SET
    ALLOWED_IP_LIST = (
        -- Add your existing allowed IPs here
        -- Plus the SPCS internal network range:
        '10.0.0.0/8'  -- This covers all internal 10.x.x.x addresses used by SPCS
    );
*/

-- If you want to create a new network policy specifically for SPCS:
CREATE OR REPLACE NETWORK POLICY SPCS_INTERNAL_ACCESS
    ALLOWED_IP_LIST = (
        '10.0.0.0/8',     -- SPCS internal network
        '172.16.0.0/12',  -- Additional private network range that might be used
        '192.168.0.0/16'  -- Additional private network range
        -- Add any other IPs you need to allow (your own IP, etc.)
    )
    COMMENT = 'Allow SPCS containers to access Snowflake REST APIs';

-- Apply the network policy to your account (be careful - this affects all access!)
-- ALTER ACCOUNT SET NETWORK_POLICY = SPCS_INTERNAL_ACCESS;

-- Or better: Create a specific network rule for SPCS (Snowflake's newer approach)
CREATE OR REPLACE NETWORK RULE spcs_internal_access_rule
    TYPE = IPV4
    VALUE_LIST = ('10.0.0.0/8', '172.16.0.0/12')
    MODE = INGRESS
    COMMENT = 'Allow SPCS containers to call Snowflake REST APIs';

-- Note: After creating the network rule, you may need to associate it with 
-- your SPCS services or modify your existing network policy to include it.

-- To check current network policies on your account:
SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;
