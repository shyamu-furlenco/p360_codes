# P360 Automation — Setup & Schedule Guide

End-to-end guide for deploying the P360 Redshift-native automation.
Follow the steps in order; each section can only be completed after the previous one.

---

## 1. Deploy the Lambda Slack Notifier (one-time)

### 1a. Create the Lambda function

1. Open AWS Console → Lambda → **Create function**
2. **Name:** `p360-slack-notifier`
3. **Runtime:** Python 3.12
4. Paste the code from `automation/lambda_slack_notifier.py`
5. **Environment variables → Add:**
   - Key: `SLACK_WEBHOOK_URL`
   - Value: your Slack incoming webhook URL
     *(Slack → Your App → Incoming Webhooks → copy the URL)*
6. **Save / Deploy**

### 1b. Create the IAM role that Redshift will assume

Create an IAM role (e.g. `p360-redshift-lambda-role`) with:

**Trust policy** — lets Redshift assume it:
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "redshift.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

**Permissions policy** — lets it invoke the Lambda:
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "lambda:InvokeFunction",
    "Resource": "arn:aws:lambda:<REGION>:<ACCOUNT_ID>:function:p360-slack-notifier"
  }]
}
```

Attach this role to your Redshift cluster under **Cluster → Properties → IAM roles**.

### 1c. Smoke-test the Lambda

In AWS Console → Lambda → `p360-slack-notifier` → **Test**, use this event:

```json
{
  "arguments": [["P360 Lambda smoke test OK"]]
}
```

Expected response:
```json
{ "success": true, "results": [true] }
```

Confirm the message appears in Slack.

---

## 2. Register the External Function in Redshift (one-time)

Open **Redshift Query Editor v2**, connect as a superuser, and run:

```sql
-- Edit setup_external_function.sql: replace <ACCOUNT_ID>, <REDSHIFT_LAMBDA_ROLE>,
-- and <SCHEDULER_ROLE_OR_USER> with real values, then execute the file.
```

```sql
CREATE EXTERNAL FUNCTION f_slack_notify(message VARCHAR(4096))
RETURNS BOOLEAN
VOLATILE
LAMBDA 'p360-slack-notifier'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/p360-redshift-lambda-role';

GRANT EXECUTE ON FUNCTION f_slack_notify(VARCHAR) TO <SCHEDULER_ROLE_OR_USER>;
```

**Verify:**
```sql
SELECT f_slack_notify('P360 external function connected');
-- Expected: TRUE  + Slack message appears
```

---

## 3. Create the Stored Procedures (one-time)

Run both procedure files in Redshift Query Editor v2 (as a superuser or procedure owner):

```sql
-- Run the contents of each file:
-- 1. automation/sp_p360_staging_refresh.sql
-- 2. automation/sp_p360_batch_runner.sql   (creates both sp_p360_batch_runner
--                                           and sp_p360_batch_force_commit)
```

**Verify:**
```sql
SELECT routine_name FROM information_schema.routines
WHERE routine_schema = 'public'
  AND routine_type   = 'PROCEDURE'
  AND routine_name LIKE 'sp_p360%';
-- Should return 3 rows
```

---

## 4. Schedule the Staging Refresh

The staging refresh runs **every day** at 2:00 AM IST (8:30 PM UTC previous day).

### Steps in Redshift Query Editor v2

1. Write the query:
   ```sql
   CALL sp_p360_staging_refresh();
   ```
2. Click **Schedule** (top-right)
3. **Schedule name:** `p360-staging-refresh-daily`
4. **IAM role:** select the role attached to your cluster
5. **Cron expression (UTC):** `0 20 * * ? *`
   - Adjust if your Redshift cluster is in a different UTC offset
   - 2:00 AM IST = UTC+5:30 → 8:30 PM UTC → use `30 20 * * ? *` for exact timing
6. Enable **SNS notifications** for `FAILED` status as a backup alert channel
7. **Save**

---

## 5. Schedule the Batch Runner

The batch runner runs on your accounting cycle. Configure to match your cadence.

### Common cron examples

| Cadence | Cron (UTC) | Fires at |
|---|---|---|
| Every Monday | `0 1 ? * 2 *` | Monday 1:00 AM UTC |
| 1st of month | `0 1 1 * ? *` | 1st of each month 1:00 AM UTC |
| Every weekday | `0 1 ? * 2-6 *` | Mon–Fri 1:00 AM UTC |

### Steps in Redshift Query Editor v2

1. Write the query:
   ```sql
   CALL sp_p360_batch_runner();
   ```
2. Click **Schedule**
3. **Schedule name:** `p360-batch-runner-weekly` (or your cadence name)
4. **IAM role:** same role as staging refresh
5. Set the cron expression from the table above
6. Enable **SNS notifications** for `FAILED` status
7. **Save**

---

## 6. Verify the Full Pipeline

Run this checklist before trusting the scheduler in production:

### 6a. Test the staging procedure
```sql
CALL sp_p360_staging_refresh();
-- Check: Slack shows "P360 staging refresh complete — N rows loaded"
-- Check: MAX(refreshed_at) = CURRENT_DATE
SELECT MAX(refreshed_at), COUNT(*) FROM p360_staging;
```

### 6b. Test the batch dry-run (approval gate)
Temporarily lower the row-count threshold to force a SKIPPED outcome:
```sql
-- Edit sp_p360_batch_runner and set:
--   ROW_COUNT_VARIANCE_THRESHOLD NUMERIC := 0.0;
-- Re-create the procedure, then run:
CALL sp_p360_batch_runner();
-- Check: Slack shows "MANUAL REVIEW REQUIRED"
-- Check: p360_batch_audit.status = 'SKIPPED'
SELECT batch_id, status, error_message FROM p360_batch_audit ORDER BY started_at DESC LIMIT 3;
```

### 6c. Test the force commit (manual override)
```sql
-- Use the SKIPPED batch_id from 6b:
CALL sp_p360_batch_force_commit('B_YYYYMMDD_001');
-- Check: Slack shows "committed (manually approved)"
-- Check: p360_batch_audit.status = 'COMMITTED'
```

### 6d. Test a normal committed batch
Reset the threshold back to 5.0, then run again on the next batch day:
```sql
CALL sp_p360_batch_runner();
-- Check: Slack shows "P360 Batch B_XXX committed"
-- Check: p360_batch_audit.status = 'COMMITTED'
```

### 6e. Test the error path
```sql
-- Temporarily break the procedure by referencing a non-existent table,
-- then run CALL sp_p360_batch_runner() and confirm:
-- Slack: "P360 BATCH FAILED"
-- p360_batch_audit.status = 'FAILED'
```

---

## 7. Operational Reference

### Approval thresholds

| Check | Default | How to change |
|---|---|---|
| Balance tolerance | ABS(DR−CR) < 0.01 | Computed in Step 1.5; see `p360_batch_audit_ddl.sql` line 57 |
| Row count variance | ±5% vs 30-day avg | Change `ROW_COUNT_VARIANCE_THRESHOLD` in `sp_p360_batch_runner.sql` |
| Correction rate | ≤20% of P360 rows | Change `CORRECTION_RATE_THRESHOLD` in `sp_p360_batch_runner.sql` |

After changing thresholds, re-run the `CREATE OR REPLACE PROCEDURE` block.

### Monitoring audit history
```sql
SELECT batch_id, submission_date, status, preview_rows, is_balanced,
       total_dr, total_cr, error_message
FROM p360_batch_audit
ORDER BY started_at DESC
LIMIT 20;
```

### Manually committing a SKIPPED batch
```sql
-- Review it first:
SELECT * FROM p360_batch_audit WHERE batch_id = 'B_YYYYMMDD_001';

-- Approve:
CALL sp_p360_batch_force_commit('B_YYYYMMDD_001');
```

### Viewing batch summary after commit
```sql
SELECT * FROM p360_batch_summary ORDER BY submission_date DESC LIMIT 5;
```

---

## 8. Slack Message Reference

| Emoji prefix | Meaning | Action required |
|---|---|---|
| `P360 staging refresh complete` | Daily staging ran OK | None |
| `P360 STAGING FAILED` | Staging SQL errored | Check `stl_error` / `svl_qlog` in Redshift; re-run manually |
| `P360 Batch B_XXX committed` | Batch auto-approved and committed | None |
| `MANUAL REVIEW REQUIRED` | Auto-approval threshold failed | Review reason in message; run `CALL sp_p360_batch_force_commit(...)` when satisfied |
| `P360 BATCH FAILED` | Unhandled error during batch | Check `p360_batch_audit.error_message`; fix root cause; retry with `CALL sp_p360_batch_runner()` |
| `P360 FORCE COMMIT FAILED` | Force commit errored | Same as above, but check the batch is back in SKIPPED/FAILED state before retrying |
