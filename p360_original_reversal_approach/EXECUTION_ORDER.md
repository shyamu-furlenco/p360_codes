# P360 Batch Processing — Execution Order

## Initial Setup (Run Once)

Execute these files in order when setting up the system for the first time:

```
1. p360_staging_ddl.sql          -- Create staging table
2. p360_submissions_ddl.sql      -- Create submissions history table
3. p360_delta_state_ddl.sql      -- Create state table (compact current state)
4. p360_batch_control_ddl.sql    -- Create batch sequence control table
5. p360_batch_audit_ddl.sql      -- Create audit logging table
6. p360_outbox_ddl.sql           -- Create outbox table (P360-facing rows)
7. p360_reconciliation_views.sql -- Create reconciliation views
8. p360_state_init.sql           -- (Only if migrating existing data) Populate state from submissions
```

## Automation Setup (Run Once, After Initial Setup)

Deploy the automation layer in order. See `automation/SCHEDULE_SETUP.md` for detailed AWS steps.

```
1. Deploy automation/lambda_slack_notifier.py to AWS Lambda
2. Create IAM role + trust policy (see automation/SCHEDULE_SETUP.md)
3. automation/setup_external_function.sql  -- Register Lambda as Redshift external function f_slack_notify()
4. automation/sp_p360_staging_refresh.sql  -- CREATE PROCEDURE sp_p360_staging_refresh in Redshift
5. automation/sp_p360_batch_runner.sql     -- CREATE PROCEDURE sp_p360_batch_runner + sp_p360_batch_force_commit
6. Schedule sp_p360_staging_refresh: daily cron 0 20 * * ? * (2:00 AM IST)
7. Schedule sp_p360_batch_runner: weekly/monthly cron per business cycle
```

## Daily/Recurring Operations

Execute these files for each batch cycle:

```
1. p360_staging_refresh.sql      -- Refresh staging table from source data
2. p360_batch_runner.sql         -- Run with :run_mode = 'PREVIEW' first, then 'COMMIT'
```

## Diagnostic / Testing

```
-- Run anytime to check system health (read-only, no writes)
p360_system_health_check.sql

-- Run test suite before production changes (self-contained, uses temp tables)
p360_batch_runner_test.sql
```

## File Descriptions

| File | Purpose |
|------|---------|
| `p360_final_view_claude.sql` | Source view definition (aggregates data from multiple verticals) |
| `copy_p360.sql` | Reference copy of p360_final_view_claude (backup/duplicate) |
| `p360_staging_ddl.sql` | DDL for staging table |
| `p360_staging_refresh.sql` | Truncate and reload staging from p360_final_view_claude |
| `p360_submissions_ddl.sql` | DDL for submissions history (audit trail) |
| `p360_delta_state_ddl.sql` | DDL for compact state table (1 row per business key) |
| `p360_batch_control_ddl.sql` | DDL for atomic batch_id sequence |
| `p360_batch_audit_ddl.sql` | DDL for batch audit logging |
| `p360_outbox_ddl.sql` | DDL for outbox table (P360-facing rows: ORIGINAL, REVERSAL, CORRECTION_DELTA) |
| `p360_reconciliation_views.sql` | Views for external reconciliation |
| `p360_state_init.sql` | One-time migration: populate state from existing submissions |
| `p360_batch_runner.sql` | Main workflow: PREVIEW → COMMIT → VERIFY |
| `p360_batch_runner_test.sql` | Test suite (7 scenarios) |
| `p360_system_health_check.sql` | Read-only diagnostics: freshness, balance, NULL codes, cross-table consistency |
| `automation/lambda_slack_notifier.py` | AWS Lambda function for Slack notifications via f_slack_notify() |
| `automation/setup_external_function.sql` | Registers Lambda as Redshift external function f_slack_notify() |
| `automation/sp_p360_staging_refresh.sql` | NONATOMIC stored procedure wrapping daily staging refresh |
| `automation/sp_p360_batch_runner.sql` | NONATOMIC stored procedure for fully automated batch + sp_p360_batch_force_commit |
| `automation/SCHEDULE_SETUP.md` | Step-by-step AWS + Redshift automation setup guide |
| `automation/test_demo.sql` | Demo/test script for the automation layer |

## Batch Runner Usage

```sql
-- Step 1: Preview changes (no commits)
-- Set :run_mode = 'PREVIEW', :submission_date = CURRENT_DATE

-- Step 2: Commit changes
-- Set :run_mode = 'COMMIT', :submission_date = CURRENT_DATE

-- Step 3: Verify (optional)
-- Set :run_mode = 'VERIFY', :submission_date = CURRENT_DATE

-- Automated (via stored procedure):
CALL sp_p360_batch_runner();

-- Manual override for a SKIPPED batch:
CALL sp_p360_batch_force_commit('B_YYYYMMDD_NNN');
```
