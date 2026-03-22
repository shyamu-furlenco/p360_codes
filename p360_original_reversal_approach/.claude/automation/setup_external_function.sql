-- =============================================================================
-- P360 EXTERNAL FUNCTION — SLACK NOTIFIER
-- Run once in Redshift as a superuser (or a user with CREATE EXTERNAL FUNCTION
-- privilege) after the Lambda function has been deployed.
--
-- Prerequisites:
--   1. Lambda function "p360-slack-notifier" deployed (see lambda_slack_notifier.py)
--   2. IAM role with trust policy allowing redshift.amazonaws.com to assume it
--      and with lambda:InvokeFunction permission on p360-slack-notifier
--   3. Replace <ACCOUNT_ID> and <REDSHIFT_LAMBDA_ROLE> below before running
-- =============================================================================

CREATE EXTERNAL FUNCTION f_slack_notify(message VARCHAR(4096))
RETURNS BOOLEAN
VOLATILE
LAMBDA 'p360-slack-notifier'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/<REDSHIFT_LAMBDA_ROLE>';

-- =============================================================================
-- Grant usage to the role that runs the scheduled procedures
-- =============================================================================
GRANT EXECUTE ON FUNCTION f_slack_notify(VARCHAR) TO <SCHEDULER_ROLE_OR_USER>;

-- =============================================================================
-- Smoke test — should produce a Slack message and return TRUE
-- =============================================================================
-- SELECT f_slack_notify('P360 external function connected ✅');
