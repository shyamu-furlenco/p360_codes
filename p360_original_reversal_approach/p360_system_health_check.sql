-- =============================================================================
-- P360 SYSTEM HEALTH CHECK — Read-only diagnostics across every table
-- =============================================================================
-- PURPOSE:  Run these SELECT queries at any time to understand the current
--           state of every layer in the pipeline without modifying any data.
--
-- HOW TO USE:
--   Run each numbered section independently in your SQL client.
--   Each section tells you what to look for and what "healthy" looks like.
--
-- PIPELINE ORDER (top to bottom):
--   [1] p360_staging         ← daily refreshed source of truth from financial_events
--   [2] p360_delta_state     ← compact table of what has been committed so far
--   [3] p360_batch_control   ← batch ID sequence counter
--   [4] p360_batch_audit     ← log of every batch run (pass/fail, row counts)
--   [5] p360_submissions     ← permanent record of every row ever sent to P360
--   [6] p360_outbox          ← rows queued for current/next P360 upload
--   [7] Cross-table checks   ← staging vs state vs submissions consistency
-- =============================================================================


-- =============================================================================
-- [1] STAGING TABLE — p360_staging
-- =============================================================================
-- This table is rebuilt daily by p360_staging_refresh.sql.
-- It represents what the journal SHOULD look like right now (full snapshot).
-- =============================================================================

-- [1a] Freshness check — is staging up to date?
-- Healthy: refreshed_at = today. If stale, re-run p360_staging_refresh.sql.
SELECT
    MAX(refreshed_at)   AS last_refresh,
    CURRENT_DATE        AS today,
    MAX(refreshed_at) = CURRENT_DATE AS is_fresh,
    COUNT(*)            AS total_rows
FROM p360_staging;

-- [1b] Row breakdown by cycle_type and vertical
-- Healthy: all expected cycle_types present (Normal_billing_cycle, Swap, VAS, MTP, Deferral, etc.)
SELECT
    cycle_type,
    vertical,
    COUNT(*)            AS rows,
    SUM(COALESCE(DR,0)) AS total_DR,
    SUM(COALESCE(CR,0)) AS total_CR,
    ABS(SUM(COALESCE(DR,0)) - SUM(COALESCE(CR,0))) AS imbalance
FROM p360_staging
GROUP BY cycle_type, vertical
ORDER BY cycle_type, vertical;

-- [1c] Balance check per city + date
-- Healthy: imbalance = 0 for every group. Any non-zero row is a bad journal entry.
SELECT
    city_name,
    recognised_date,
    cycle_type,
    SUM(COALESCE(DR,0)) AS total_DR,
    SUM(COALESCE(CR,0)) AS total_CR,
    ROUND(ABS(SUM(COALESCE(DR,0)) - SUM(COALESCE(CR,0))), 4) AS imbalance
FROM p360_staging
GROUP BY city_name, recognised_date, cycle_type
HAVING ABS(SUM(COALESCE(DR,0)) - SUM(COALESCE(CR,0))) > 0.01
ORDER BY imbalance DESC;
-- Expected: 0 rows. If rows appear, those city+date+cycle combinations are unbalanced.

-- [1d] NULL code_number check
-- Healthy: 0 rows. A NULL code_number means a ledger account could not be resolved.
-- This was the Swap bug — fix applied in gst_fix CTE.
SELECT
    cycle_type,
    particulars,
    COUNT(*)            AS null_code_rows,
    SUM(COALESCE(CR,0)) AS total_CR,
    SUM(COALESCE(DR,0)) AS total_DR
FROM p360_staging
WHERE code_number IS NULL
GROUP BY cycle_type, particulars
ORDER BY cycle_type;
-- Expected: 0 rows.

-- [1e] Full staging contents — browse all rows
-- Use filters (WHERE cycle_type = '...', WHERE city_name = '...') as needed.
SELECT
    code_number, particulars,
    DR, CR,
    city_name, cycle_type, vertical,
    recognised_date,
    store_id, organization_id
FROM p360_staging
ORDER BY recognised_date DESC, city_name, cycle_type, code_number
LIMIT 200;


-- =============================================================================
-- [2] STATE TABLE — p360_delta_state
-- =============================================================================
-- Stores the LAST COMMITTED version of each business key (one row per key).
-- Used by the batch runner to diff against staging and detect new/changed rows.
-- cum_dr / cum_cr = the amounts that were last sent to P360 for that key.
-- =============================================================================

-- [2a] Row count and coverage
SELECT
    COUNT(*)                        AS state_rows,
    COUNT(DISTINCT cycle_type)      AS distinct_cycle_types,
    COUNT(DISTINCT city_name)       AS distinct_cities,
    MIN(recognised_date)            AS earliest_date,
    MAX(recognised_date)            AS latest_date,
    MAX(updated_at)                 AS last_updated
FROM p360_delta_state;

-- [2b] State breakdown by cycle_type
SELECT
    cycle_type,
    COUNT(*)                        AS state_rows,
    SUM(cum_dr)                     AS total_cum_DR,
    SUM(cum_cr)                     AS total_cum_CR,
    ABS(SUM(cum_dr) - SUM(cum_cr)) AS imbalance
FROM p360_delta_state
GROUP BY cycle_type
ORDER BY cycle_type;
-- Healthy: imbalance close to 0 per cycle_type.

-- [2c] State balance overall
SELECT
    SUM(cum_dr)                     AS total_state_DR,
    SUM(cum_cr)                     AS total_state_CR,
    ROUND(ABS(SUM(cum_dr) - SUM(cum_cr)), 4) AS state_imbalance
FROM p360_delta_state;
-- Healthy: state_imbalance ≈ 0. A large number means past batches had unbalanced entries.

-- [2d] What is in state but NOT in staging?
-- These rows will appear as REVERSAL in the next batch (they disappeared from source).
SELECT
    st.code_number, st.particulars,
    st.city_name, st.cycle_type, st.vertical,
    st.recognised_date,
    st.cum_dr, st.cum_cr,
    st.last_batch_id
FROM p360_delta_state st
WHERE NOT EXISTS (
    SELECT 1 FROM p360_staging s
    WHERE s.code_number     = st.code_number
      AND s.city_id         = st.city_id
      AND s.vertical        = st.vertical
      AND s.cycle_type      = st.cycle_type
      AND s.recognised_date = st.recognised_date
      AND COALESCE(s.organization_id, '') = COALESCE(st.organization_id, '')
      AND COALESCE(s.store_id, '')        = COALESCE(st.store_id, '')
)
ORDER BY st.recognised_date DESC, st.city_name;
-- These rows will be REVERSED in the next batch run.

-- [2e] What is in staging but NOT in state?
-- These rows will appear as ORIGINAL in the next batch (never been sent).
SELECT
    s.code_number, s.particulars,
    s.city_name, s.cycle_type, s.vertical,
    s.recognised_date,
    s.DR, s.CR
FROM p360_staging s
WHERE NOT EXISTS (
    SELECT 1 FROM p360_delta_state st
    WHERE st.code_number     = s.code_number
      AND st.city_id         = s.city_id
      AND st.vertical        = s.vertical
      AND st.cycle_type      = s.cycle_type
      AND st.recognised_date = s.recognised_date
      AND COALESCE(st.organization_id, '') = COALESCE(s.organization_id, '')
      AND COALESCE(st.store_id, '')        = COALESCE(s.store_id, '')
)
ORDER BY s.recognised_date DESC, s.city_name;
-- These rows will be sent as ORIGINAL in the next batch run.

-- [2f] What is in BOTH but amounts differ?
-- These will appear as CORRECTION in the next batch.
SELECT
    s.code_number, s.particulars,
    s.city_name, s.cycle_type, s.vertical,
    s.recognised_date,
    st.cum_dr AS state_DR,  st.cum_cr AS state_CR,
    s.DR      AS staging_DR, s.CR     AS staging_CR,
    ROUND(COALESCE(s.DR,0) - COALESCE(st.cum_dr,0), 4) AS delta_DR,
    ROUND(COALESCE(s.CR,0) - COALESCE(st.cum_cr,0), 4) AS delta_CR
FROM p360_staging s
JOIN p360_delta_state st
  ON st.code_number     = s.code_number
 AND st.city_id         = s.city_id
 AND st.vertical        = s.vertical
 AND st.cycle_type      = s.cycle_type
 AND st.recognised_date = s.recognised_date
 AND COALESCE(st.organization_id,'') = COALESCE(s.organization_id,'')
 AND COALESCE(st.store_id,'')        = COALESCE(s.store_id,'')
WHERE ROUND(COALESCE(s.DR,0)::NUMERIC, 4) <> ROUND(COALESCE(st.cum_dr,0)::NUMERIC, 4)
   OR ROUND(COALESCE(s.CR,0)::NUMERIC, 4) <> ROUND(COALESCE(st.cum_cr,0)::NUMERIC, 4)
ORDER BY s.recognised_date DESC;
-- These rows will generate a CORRECTION_DELTA + RESTATEMENT pair in the next batch.


-- =============================================================================
-- [3] BATCH CONTROL — p360_batch_control
-- =============================================================================
-- Single-row counter that allocates the next batch ID atomically.
-- =============================================================================

-- [3a] Current state of the counter
SELECT
    control_key,
    last_batch_date,
    last_batch_seq,
    'B_' || TO_CHAR(last_batch_date, 'YYYYMMDD') || '_' ||
    LPAD(CAST(last_batch_seq AS VARCHAR), 3, '0') AS last_allocated_batch_id,
    updated_at
FROM p360_batch_control;
-- Shows the batch ID that was most recently allocated (Step 0 of batch runner).


-- =============================================================================
-- [4] BATCH AUDIT — p360_batch_audit
-- =============================================================================
-- Log of every batch run. Check this to understand what happened in past runs.
-- Status values: PENDING → IN_PROGRESS → COMMITTED | FAILED | SKIPPED
-- =============================================================================

-- [4a] Last 10 batch runs
SELECT
    batch_id,
    submission_date,
    cycle_start,
    cycle_end,
    staging_rows,
    original_rows,
    reversal_rows,
    correction_delta_rows,
    restatement_rows,
    committed_rows,
    ROUND(total_dr, 2)  AS total_DR,
    ROUND(total_cr, 2)  AS total_CR,
    is_balanced,
    status,
    started_at,
    committed_at,
    error_message
FROM p360_batch_audit
ORDER BY started_at DESC
LIMIT 10;

-- [4b] Any failed or stuck batches?
-- Healthy: 0 rows. FAILED = something went wrong. IN_PROGRESS = possibly stuck.
SELECT
    batch_id, status, started_at, committed_at, error_message
FROM p360_batch_audit
WHERE status IN ('FAILED', 'IN_PROGRESS')
ORDER BY started_at DESC;

-- [4c] Unbalanced committed batches (should never happen)
SELECT
    batch_id, submission_date, total_dr, total_cr,
    ROUND(ABS(total_dr - total_cr), 4) AS imbalance
FROM p360_batch_audit
WHERE is_balanced = FALSE
  AND status = 'COMMITTED'
ORDER BY submission_date DESC;
-- Expected: 0 rows.


-- =============================================================================
-- [5] SUBMISSIONS — p360_submissions
-- =============================================================================
-- Permanent audit trail of every row ever sent to P360 (and RESTATEMENT markers).
-- row_type values:
--   ORIGINAL         → first-time send
--   REVERSAL         → row deleted from source; amounts flipped
--   RESTATEMENT      → silent state update (NOT sent to P360); full new amount stored
--   CORRECTION_DELTA → net delta sent to P360 when an existing row changed
-- =============================================================================

-- [5a] Overall submission history summary
SELECT
    row_type,
    COUNT(*)                        AS row_count,
    MIN(submission_date)            AS first_submission,
    MAX(submission_date)            AS last_submission,
    ROUND(SUM(COALESCE(DR,0)), 2)   AS total_DR,
    ROUND(SUM(COALESCE(CR,0)), 2)   AS total_CR
FROM p360_submissions
GROUP BY row_type
ORDER BY row_type;

-- [5b] Submissions by batch (last 10 batches)
SELECT
    batch_id,
    submission_date,
    COUNT(*) FILTER (WHERE row_type = 'ORIGINAL')          AS original,
    COUNT(*) FILTER (WHERE row_type = 'REVERSAL')          AS reversal,
    COUNT(*) FILTER (WHERE row_type = 'CORRECTION_DELTA')  AS correction_delta,
    COUNT(*) FILTER (WHERE row_type = 'RESTATEMENT')       AS restatement,
    ROUND(SUM(COALESCE(DR,0)) FILTER (WHERE row_type <> 'RESTATEMENT'), 2) AS p360_DR,
    ROUND(SUM(COALESCE(CR,0)) FILTER (WHERE row_type <> 'RESTATEMENT'), 2) AS p360_CR
FROM p360_submissions
GROUP BY batch_id, submission_date
ORDER BY submission_date DESC
LIMIT 10;

-- [5c] Lifetime balance of all P360-facing rows (excluding RESTATEMENT)
-- Healthy: net_DR ≈ net_CR for each business key group.
SELECT
    code_number, particulars, city_name, cycle_type, vertical,
    ROUND(SUM(COALESCE(DR,0)), 2)   AS net_DR,
    ROUND(SUM(COALESCE(CR,0)), 2)   AS net_CR,
    ROUND(ABS(SUM(COALESCE(DR,0)) - SUM(COALESCE(CR,0))), 4) AS imbalance
FROM p360_submissions
WHERE row_type <> 'RESTATEMENT'
GROUP BY code_number, particulars, city_name, cycle_type, vertical
HAVING ABS(SUM(COALESCE(DR,0)) - SUM(COALESCE(CR,0))) > 0.01
ORDER BY imbalance DESC;
-- Expected: 0 rows. Any rows here mean P360 received unbalanced entries.

-- [5d] Full history for a specific batch (replace batch ID below)
SELECT
    row_type, code_number, particulars,
    DR, CR,
    city_name, cycle_type, vertical, recognised_date,
    reference_batch_id, correction_period
FROM p360_submissions
WHERE batch_id = 'B_20260301_001'   -- ← replace with actual batch_id
ORDER BY row_type, code_number;


-- =============================================================================
-- [6] OUTBOX — p360_outbox
-- =============================================================================
-- Rows queued for the current/upcoming P360 upload.
-- Only contains P360-facing rows (ORIGINAL, REVERSAL, CORRECTION_DELTA).
-- RESTATEMENT rows are NOT written here (they are internal state markers).
-- After upload to P360, this table should be cleared / marked as sent.
-- =============================================================================

-- [6a] What is currently in the outbox?
SELECT
    row_type,
    COUNT(*)                        AS rows,
    ROUND(SUM(COALESCE(DR,0)), 2)   AS total_DR,
    ROUND(SUM(COALESCE(CR,0)), 2)   AS total_CR
FROM p360_outbox
GROUP BY row_type
ORDER BY row_type;

-- [6b] Full outbox contents
SELECT
    batch_id, row_type, code_number, particulars,
    DR, CR,
    city_name, cycle_type, vertical, recognised_date,
    reference_batch_id, correction_period
FROM p360_outbox
ORDER BY batch_id, row_type, code_number;


-- =============================================================================
-- [7] CROSS-TABLE CONSISTENCY CHECKS
-- =============================================================================
-- Compares tables against each other to catch sync issues.
-- =============================================================================

-- [7a] State vs Submissions — do they agree on committed amounts?
-- For each business key, the state's cum_dr/cum_cr should equal the net of all
-- ORIGINAL + RESTATEMENT rows in submissions (the last restatement wins per key).
-- Any mismatch means state drifted from submission history.
SELECT
    sub.code_number, sub.city_name, sub.cycle_type, sub.vertical, sub.recognised_date,
    ROUND(sub.last_dr, 2)   AS submissions_last_DR,
    ROUND(sub.last_cr, 2)   AS submissions_last_CR,
    ROUND(st.cum_dr, 2)     AS state_cum_DR,
    ROUND(st.cum_cr, 2)     AS state_cum_CR
FROM (
    -- Most recent ORIGINAL or RESTATEMENT amount per key from submissions
    SELECT
        code_number, city_id, vertical, cycle_type, recognised_date,
        organization_id, store_id,
        city_name,
        FIRST_VALUE(COALESCE(DR,0)) OVER (
            PARTITION BY code_number, city_id, vertical, cycle_type,
                         recognised_date, organization_id, store_id
            ORDER BY submission_date DESC, batch_id DESC
        ) AS last_dr,
        FIRST_VALUE(COALESCE(CR,0)) OVER (
            PARTITION BY code_number, city_id, vertical, cycle_type,
                         recognised_date, organization_id, store_id
            ORDER BY submission_date DESC, batch_id DESC
        ) AS last_cr
    FROM p360_submissions
    WHERE row_type IN ('ORIGINAL', 'RESTATEMENT')
) sub
JOIN p360_delta_state st
  ON st.code_number     = sub.code_number
 AND st.city_id         = sub.city_id
 AND st.vertical        = sub.vertical
 AND st.cycle_type      = sub.cycle_type
 AND st.recognised_date = sub.recognised_date
 AND COALESCE(st.organization_id,'') = COALESCE(sub.organization_id,'')
 AND COALESCE(st.store_id,'')        = COALESCE(sub.store_id,'')
WHERE ROUND(sub.last_dr::NUMERIC, 4) <> ROUND(st.cum_dr::NUMERIC, 4)
   OR ROUND(sub.last_cr::NUMERIC, 4) <> ROUND(st.cum_cr::NUMERIC, 4)
ORDER BY sub.recognised_date DESC;
-- Expected: 0 rows. Any rows = state table is out of sync with submission history.

-- [7b] Staging total vs State total — pending volume
-- Difference = what will be sent in the next batch.
SELECT
    'Staging total'     AS source,
    ROUND(SUM(COALESCE(DR,0)), 2) AS total_DR,
    ROUND(SUM(COALESCE(CR,0)), 2) AS total_CR
FROM p360_staging
UNION ALL
SELECT
    'State total (last committed)',
    ROUND(SUM(cum_dr), 2),
    ROUND(SUM(cum_cr), 2)
FROM p360_delta_state
UNION ALL
SELECT
    'Difference (pending next batch)',
    ROUND((SELECT SUM(COALESCE(DR,0)) FROM p360_staging) - (SELECT SUM(cum_dr) FROM p360_delta_state), 2),
    ROUND((SELECT SUM(COALESCE(CR,0)) FROM p360_staging) - (SELECT SUM(cum_cr) FROM p360_delta_state), 2);

-- [7c] Quick next-batch preview — what action will each row get?
-- Shows counts without building the full temp table.
-- ORIGINAL = new rows to send | REVERSAL_ONLY = rows to reverse | CORRECTION = changed rows
SELECT
    CASE
        WHEN st.code_number IS NULL THEN 'ORIGINAL'
        WHEN s.code_number  IS NULL THEN 'REVERSAL_ONLY'
        WHEN ROUND(COALESCE(s.DR,0)::NUMERIC,4) <> ROUND(COALESCE(st.cum_dr,0)::NUMERIC,4)
          OR ROUND(COALESCE(s.CR,0)::NUMERIC,4) <> ROUND(COALESCE(st.cum_cr,0)::NUMERIC,4)
            THEN 'CORRECTION'
        ELSE 'UNCHANGED'
    END AS next_batch_action,
    COUNT(*) AS row_count
FROM p360_staging s
FULL OUTER JOIN p360_delta_state st
  ON st.code_number     = s.code_number
 AND st.city_id         = s.city_id
 AND st.vertical        = s.vertical
 AND st.cycle_type      = s.cycle_type
 AND st.recognised_date = s.recognised_date
 AND COALESCE(st.organization_id,'') = COALESCE(s.organization_id,'')
 AND COALESCE(st.store_id,'')        = COALESCE(s.store_id,'')
GROUP BY 1
ORDER BY 1;
-- UNCHANGED rows = nothing to do. ORIGINAL/REVERSAL_ONLY/CORRECTION = next batch volume.
