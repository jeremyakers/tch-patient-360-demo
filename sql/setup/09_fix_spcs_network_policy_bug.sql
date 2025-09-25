-- Fix for SPCS OAuth Network Policy Bug
-- 
-- This is a known Snowflake bug where SPCS OAuth tokens are incorrectly blocked by network policies.
-- Bug reference: SNOW-2231422
-- 
-- The bug affects REST API calls from SPCS containers using:
-- 1. SNOWFLAKE_HOST environment variable
-- 2. OAuth token from /snowflake/session/token
-- 3. Making calls to Snowflake REST APIs (like Cortex)
--
-- Network policies should be skipped for SPCS OAuth, but due to the bug they are being applied.

USE ROLE ACCOUNTADMIN;

-- Option 1: Enable the specific fix (may require Snowflake support to enable)
-- This parameter fixes the network policy evaluation precedence issue
ALTER ACCOUNT SET FIX_NETWORK_POLICY_EVALUATION_PRECEDENCE = TRUE;

-- Option 2: Enable the 2025_06 behavior change bundle which includes this fix
-- This is the recommended long-term solution
ALTER ACCOUNT SET BEHAVIOR_CHANGE_BUNDLE = '2025_06_RELEASE';

-- Verify the settings
SHOW PARAMETERS LIKE '%FIX_NETWORK_POLICY%' IN ACCOUNT;
SHOW PARAMETERS LIKE '%BEHAVIOR_CHANGE_BUNDLE%' IN ACCOUNT;

-- After applying either fix, the SPCS OAuth tokens should no longer be blocked by network policies
-- and REST API calls from SPCS containers should work correctly.

-- Note: If you get an error that the parameter doesn't exist or you don't have permission,
-- you'll need to contact Snowflake support and reference:
-- - Bug ID: SNOW-2231422
-- - Request: Enable FIX_NETWORK_POLICY_EVALUATION_PRECEDENCE for your account
