-- =============================================================================
-- P360 BATCH RUNNER — STATE-BASED WORKFLOW (v2)
-- Run on batch day (weekly / monthly, or on-demand for corrections).
-- No parameters to edit — everything is auto-detected.
--
-- STEP 0 — ALLOCATE: Run the batch_id allocation query first.
-- STEP 1 — PREVIEW:  Run the CREATE TEMP TABLE block below.
--                    Then run: SELECT * FROM p360_batch_preview ORDER BY ...
--                    Review output before committing.
--
-- STEP 2 — COMMIT:   When satisfied, run the INSERT + MERGE block at the bottom.
--
-- STEP 3 — VERIFY:   Run the balance-check query at the bottom.
--
-- PREREQUISITES:
--   - p360_batch_control table must exist (see p360_batch_control_ddl.sql)
--   - p360_batch_audit table must exist (see p360_batch_audit_ddl.sql)
--   - p360_delta_state table must exist and be initialized (see p360_delta_state_ddl.sql, p360_state_init.sql)
--
-- ARCHITECTURE NOTE:
--   This version uses p360_delta_state for efficient delta computation.
--   Diffs against compact state table (O(current_keys)) instead of
--   scanning full p360_submissions history (O(history)).
-- =============================================================================

-- =============================================================================
-- PRE-CHECK: Confirm staging was refreshed today before running Step 0.
-- (Uncomment and run this first)
-- =============================================================================
-- SELECT MAX(refreshed_at) AS last_refresh, COUNT(*) AS staging_rows
-- FROM p360_staging;
-- Expected: last_refresh = today's date. If stale, re-run p360_staging_refresh.sql first.

-- =============================================================================
-- STEP 0 — ALLOCATE BATCH ID (ATOMIC)
-- Run this ONCE at the start of the batch process.
-- Stores result in a temp table for use by Step 1.
-- Uses p360_batch_control for atomic sequence allocation (prevents race conditions).
-- =============================================================================

DROP TABLE IF EXISTS p360_current_batch;

-- Atomically allocate next batch_id
UPDATE p360_batch_control
SET
    last_batch_seq = CASE
        WHEN last_batch_date = CURRENT_DATE THEN last_batch_seq + 1
        ELSE 1
    END,
    last_batch_date = CURRENT_DATE,
    updated_at = CURRENT_TIMESTAMP
WHERE control_key = 'BATCH_SEQ';

-- Store the allocated batch_id for this session
CREATE TEMP TABLE p360_current_batch AS
SELECT
    'B_' || TO_CHAR(last_batch_date, 'YYYYMMDD') || '_' ||
    LPAD(CAST(last_batch_seq AS VARCHAR), 3, '0') AS batch_id
FROM p360_batch_control
WHERE control_key = 'BATCH_SEQ';

-- Verify: SELECT * FROM p360_current_batch;

-- =============================================================================
-- STEP 1 — PREVIEW
-- Creates a temp table with all rows to be sent in this batch.
-- Safe to re-run: DROP TABLE IF EXISTS ensures a clean slate each time.
--
-- PERFORMANCE: Diffs p360_staging against p360_delta_state (compact).
-- This is O(staging_rows + state_rows), not O(submissions_history).
-- =============================================================================

DROP TABLE IF EXISTS p360_batch_preview;

CREATE TEMP TABLE p360_batch_preview AS

-- ===========================================================================
-- CTE chain:
--   new_batch     → uses pre-allocated batch_id from Step 0
--   current_state → current cumulative state per business key (from p360_delta_state)
--   current_data  → all rows currently in p360_staging
--   comparison    → FULL OUTER JOIN; labels each row's action
--   batch_bounds  → MIN/MAX start_date across all rows being emitted
--   delta_date    → start_date/end_date to stamp on CORRECTION_DELTA rows
--   output_rows   → expands to ORIGINAL / REVERSAL / RESTATEMENT / CORRECTION_DELTA rows
-- ===========================================================================
WITH

-- ---------------------------------------------------------------------------
-- Use pre-allocated batch_id from Step 0 (atomic, no race condition)
-- ---------------------------------------------------------------------------
new_batch AS (
    SELECT batch_id FROM p360_current_batch
),

-- ---------------------------------------------------------------------------
-- Current cumulative state per business key (from compact state table).
-- This replaces the expensive last_sent CTE that scanned all submissions.
-- ---------------------------------------------------------------------------
current_state AS (
    SELECT
        code_number, particulars, cum_dr AS DR, cum_cr AS CR,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        start_date, end_date, remarks,
        last_batch_id AS batch_id
    FROM p360_delta_state
),

-- ---------------------------------------------------------------------------
-- Current state: everything in the daily-refreshed staging table.
-- No date restriction — covers all periods back to April 2024.
-- ---------------------------------------------------------------------------
current_data AS (
    SELECT
        code_number, particulars, DR, CR,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        start_date, end_date, remarks
    FROM p360_staging
),

-- ---------------------------------------------------------------------------
-- Compare current_data vs current_state (FULL OUTER JOIN on business key).
-- Actions:
--   ORIGINAL      — in current_data only (never sent before)
--   REVERSAL_ONLY — in current_state only (row disappeared from source)
--   CORRECTION    — in both but DR or CR differs
--   UNCHANGED     — in both and amounts match (skip — no rows emitted)
-- ---------------------------------------------------------------------------
comparison AS (
    SELECT
        -- Derive business key from whichever side is non-null
        COALESCE(cur.code_number,           st.code_number)           AS code_number,
        COALESCE(cur.particulars,           st.particulars)           AS particulars,
        COALESCE(cur.city_name,             st.city_name)             AS city_name,
        COALESCE(cur.cycle_type,            st.cycle_type)            AS cycle_type,
        COALESCE(cur.vertical,              st.vertical)              AS vertical,
        COALESCE(cur.city_id,               st.city_id)               AS city_id,
        COALESCE(cur.store_id,              st.store_id)              AS store_id,
        COALESCE(cur.organization_id,       st.organization_id)       AS organization_id,
        COALESCE(cur.organization_email_id, st.organization_email_id) AS organization_email_id,
        COALESCE(cur.start_date,            st.start_date)            AS start_date,
        COALESCE(cur.end_date,              st.end_date)              AS end_date,
        COALESCE(cur.remarks,               st.remarks)               AS remarks,

        -- Current (staging) amounts — NULL if row no longer exists in source
        cur.DR   AS cur_DR,
        cur.CR   AS cur_CR,

        -- State amounts — NULL if never sent before
        st.DR    AS old_DR,
        st.CR    AS old_CR,

        -- batch_id from state (used as reference_batch_id on REVERSAL/RESTATEMENT)
        st.batch_id AS last_batch_id,

        -- Classify action
        CASE
            WHEN st.code_number IS NULL
                THEN 'ORIGINAL'
            WHEN cur.code_number IS NULL
                THEN 'REVERSAL_ONLY'
            WHEN ROUND(COALESCE(cur.DR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.DR, 0)::NUMERIC, 4)
              OR ROUND(COALESCE(cur.CR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.CR, 0)::NUMERIC, 4)
                THEN 'CORRECTION'
            ELSE 'UNCHANGED'
        END AS action

    FROM current_data cur
    FULL OUTER JOIN current_state st
      ON cur.code_number     = st.code_number
     AND cur.city_id         = st.city_id
     AND cur.vertical        = st.vertical
     AND cur.cycle_type      = st.cycle_type
     AND cur.start_date      = st.start_date
     AND cur.end_date        = st.end_date
     AND COALESCE(cur.organization_id, '') = COALESCE(st.organization_id, '')
     AND COALESCE(cur.store_id, '') = COALESCE(st.store_id, '')
),

-- ---------------------------------------------------------------------------
-- Compute cycle_start / cycle_end across all rows being emitted this batch.
-- If nothing changed (all UNCHANGED), both return NULL and output_rows is empty.
-- ---------------------------------------------------------------------------
batch_bounds AS (
    SELECT
        MIN(start_date) AS cycle_start,
        MAX(start_date) AS cycle_end
    FROM comparison
    WHERE action IN ('ORIGINAL', 'REVERSAL_ONLY', 'CORRECTION')
),

-- ---------------------------------------------------------------------------
-- start_date/end_date to stamp on CORRECTION_DELTA rows: the latest ORIGINAL
-- period in the current batch (the "current cycle" period). Falls back to
-- CURRENT_DATE when a batch contains only corrections and no new ORIGINALs.
-- ---------------------------------------------------------------------------
delta_date AS (
    SELECT
        COALESCE(MAX(CASE WHEN action = 'ORIGINAL' THEN start_date END), CURRENT_DATE) AS start_date,
        COALESCE(MAX(CASE WHEN action = 'ORIGINAL' THEN end_date   END), CURRENT_DATE) AS end_date
    FROM comparison
),

-- ---------------------------------------------------------------------------
-- Expand each action into output rows:
--   ORIGINAL      → 1 row  (row_type = ORIGINAL)
--   REVERSAL_ONLY → 1 row  (row_type = REVERSAL, amounts flipped)
--   CORRECTION    → 1 row sent to P360 (row_type = CORRECTION_DELTA, delta only)
--                 + 1 silent state marker (row_type = RESTATEMENT, full new amount)
-- ---------------------------------------------------------------------------
output_rows AS (

    -- ORIGINAL: new row, never submitted before
    SELECT
        cmp.code_number, cmp.particulars,
        cmp.cur_DR  AS DR,
        cmp.cur_CR  AS CR,
        cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
        cmp.store_id, cmp.organization_id, cmp.organization_email_id,
        cmp.start_date, cmp.end_date, cmp.remarks,
        nb.batch_id,
        CURRENT_DATE         AS submission_date,
        bb.cycle_start,
        bb.cycle_end,
        'ORIGINAL'::VARCHAR  AS row_type,
        NULL::VARCHAR        AS reference_batch_id,
        NULL::DATE           AS correction_period
    FROM comparison cmp
    CROSS JOIN new_batch nb
    CROSS JOIN batch_bounds bb
    WHERE cmp.action = 'ORIGINAL'

    UNION ALL

    -- REVERSAL: flip old amounts (old_CR → DR, old_DR → CR)
    -- Emitted for REVERSAL_ONLY only (row disappeared from source).
    SELECT
        cmp.code_number, cmp.particulars,
        cmp.old_CR  AS DR,   -- flip: old CR → new DR
        cmp.old_DR  AS CR,   -- flip: old DR → new CR
        cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
        cmp.store_id, cmp.organization_id, cmp.organization_email_id,
        cmp.start_date, cmp.end_date, cmp.remarks,
        nb.batch_id,
        CURRENT_DATE          AS submission_date,
        bb.cycle_start,
        bb.cycle_end,
        'REVERSAL'::VARCHAR   AS row_type,
        cmp.last_batch_id     AS reference_batch_id,
        NULL::DATE            AS correction_period
    FROM comparison cmp
    CROSS JOIN new_batch nb
    CROSS JOIN batch_bounds bb
    WHERE cmp.action = 'REVERSAL_ONLY'

    UNION ALL

    -- RESTATEMENT: full new amount — silent state marker for CORRECTION.
    -- NOT sent to P360; stored in p360_submissions so we have audit trail.
    SELECT
        cmp.code_number, cmp.particulars,
        cmp.cur_DR  AS DR,
        cmp.cur_CR  AS CR,
        cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
        cmp.store_id, cmp.organization_id, cmp.organization_email_id,
        cmp.start_date, cmp.end_date, cmp.remarks,
        nb.batch_id,
        CURRENT_DATE             AS submission_date,
        bb.cycle_start,
        bb.cycle_end,
        'RESTATEMENT'::VARCHAR   AS row_type,
        cmp.last_batch_id        AS reference_batch_id,
        NULL::DATE               AS correction_period
    FROM comparison cmp
    CROSS JOIN new_batch nb
    CROSS JOIN batch_bounds bb
    WHERE cmp.action = 'CORRECTION'

    UNION ALL

    -- CORRECTION_DELTA: net incremental change only — sent to P360.
    -- Amount logic (double-entry): positive delta stays on its side;
    -- negative delta on one side flips to the opposite side.
    SELECT
        cmp.code_number, cmp.particulars,
        CASE
            WHEN COALESCE(cmp.cur_DR, 0) - COALESCE(cmp.old_DR, 0) > 0
                THEN COALESCE(cmp.cur_DR, 0) - COALESCE(cmp.old_DR, 0)   -- DR increased
            WHEN COALESCE(cmp.old_CR, 0) - COALESCE(cmp.cur_CR, 0) > 0
                THEN COALESCE(cmp.old_CR, 0) - COALESCE(cmp.cur_CR, 0)   -- CR decreased → flip to DR
            ELSE NULL
        END AS DR,
        CASE
            WHEN COALESCE(cmp.cur_CR, 0) - COALESCE(cmp.old_CR, 0) > 0
                THEN COALESCE(cmp.cur_CR, 0) - COALESCE(cmp.old_CR, 0)   -- CR increased
            WHEN COALESCE(cmp.old_DR, 0) - COALESCE(cmp.cur_DR, 0) > 0
                THEN COALESCE(cmp.old_DR, 0) - COALESCE(cmp.cur_DR, 0)   -- DR decreased → flip to CR
            ELSE NULL
        END AS CR,
        cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
        cmp.store_id, cmp.organization_id, cmp.organization_email_id,
        dd.start_date, dd.end_date, cmp.remarks,
        nb.batch_id,
        CURRENT_DATE                  AS submission_date,
        bb.cycle_start,
        bb.cycle_end,
        'CORRECTION_DELTA'::VARCHAR   AS row_type,
        cmp.last_batch_id             AS reference_batch_id,
        cmp.start_date                AS correction_period   -- start_date of the original period being corrected
    FROM comparison cmp
    CROSS JOIN new_batch nb
    CROSS JOIN batch_bounds bb
    CROSS JOIN delta_date dd
    WHERE cmp.action = 'CORRECTION'
)

SELECT
    code_number, particulars, DR, CR,
    city_name, cycle_type, vertical, city_id,
    store_id, organization_id, organization_email_id,
    start_date, end_date, remarks,
    batch_id, submission_date, cycle_start, cycle_end,
    row_type, reference_batch_id, correction_period
FROM output_rows;


-- =============================================================================
-- Review the batch output (run after Step 1):
-- =============================================================================

-- All rows for P360 (excludes silent RESTATEMENT state markers)
SELECT *
FROM p360_batch_preview
WHERE row_type <> 'RESTATEMENT'
ORDER BY
    CASE row_type WHEN 'ORIGINAL' THEN 1 WHEN 'REVERSAL' THEN 2 WHEN 'CORRECTION_DELTA' THEN 3 END,
    city_name,
    start_date,
    cycle_type,
    vertical,
    code_number;

-- Summary by row_type — P360-facing rows only
SELECT
    row_type,
    COUNT(*)                    AS row_count,
    SUM(COALESCE(DR, 0))        AS total_DR,
    SUM(COALESCE(CR, 0))        AS total_CR
FROM p360_batch_preview
WHERE row_type <> 'RESTATEMENT'
GROUP BY row_type
ORDER BY 1;


-- =============================================================================
-- STEP 1.5 — LOG AUDIT (run after Step 1, before Step 2)
-- Records the batch preview for audit trail before commit.
-- =============================================================================

INSERT INTO p360_batch_audit (
    batch_id, submission_date, cycle_start, cycle_end,
    staging_rows, preview_rows, original_rows, reversal_rows,
    restatement_rows, correction_delta_rows,
    total_dr, total_cr, is_balanced, started_at, status
)
SELECT
    batch_id,
    CURRENT_DATE,
    MIN(cycle_start),
    MAX(cycle_end),
    (SELECT COUNT(*) FROM p360_staging),
    SUM(CASE WHEN row_type <> 'RESTATEMENT' THEN 1 ELSE 0 END),  -- P360-facing rows only
    SUM(CASE WHEN row_type = 'ORIGINAL' THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_type = 'REVERSAL' THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_type = 'RESTATEMENT' THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_type = 'CORRECTION_DELTA' THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_type <> 'RESTATEMENT' THEN COALESCE(DR, 0) ELSE 0 END),  -- excludes internal state markers
    SUM(CASE WHEN row_type <> 'RESTATEMENT' THEN COALESCE(CR, 0) ELSE 0 END),
    ABS(SUM(CASE WHEN row_type <> 'RESTATEMENT' THEN COALESCE(DR, 0) ELSE 0 END)
      - SUM(CASE WHEN row_type <> 'RESTATEMENT' THEN COALESCE(CR, 0) ELSE 0 END)) < 0.01,
    CURRENT_TIMESTAMP,
    'PENDING'
FROM p360_batch_preview
GROUP BY batch_id;


-- =============================================================================
-- STEP 2 — COMMIT
-- Run this block only after reviewing Step 1 output above.
-- IMPORTANT: This block includes:
--   - Table lock to prevent concurrent modifications
--   - Idempotent INSERT (skips if batch_id already committed)
--   - State table MERGE to update cumulative totals
--   - Audit log update
-- 
-- ERROR HANDLING NOTE:
-- Redshift does not support TRY/CATCH in raw SQL. To capture failures:
--   1. Run COMMIT block via application code with try/catch wrapper
--   2. On exception, run the ROLLBACK section below before re-raising
--   3. Or wrap this in a stored procedure with EXCEPTION block
-- =============================================================================

-- Mark batch as IN_PROGRESS before attempting commit
UPDATE p360_batch_audit
SET status = 'IN_PROGRESS', committed_at = CURRENT_TIMESTAMP
WHERE batch_id = (SELECT batch_id FROM p360_current_batch)
  AND status = 'PENDING';

BEGIN;

-- Acquire exclusive lock to prevent concurrent batch commits
LOCK TABLE p360_submissions;

-- Capture row count before INSERT for accurate audit
DROP TABLE IF EXISTS p360_pre_insert_count;
CREATE TEMP TABLE p360_pre_insert_count AS
SELECT COUNT(*) AS cnt FROM p360_submissions
WHERE batch_id = (SELECT batch_id FROM p360_current_batch);

-- Idempotent INSERT: only insert if batch_id not already in submissions
INSERT INTO p360_submissions (
    code_number, particulars, DR, CR, city_name, cycle_type, vertical,
    city_id, store_id, organization_id, organization_email_id,
    start_date, end_date, remarks, batch_id, submission_date,
    cycle_start, cycle_end, row_type, reference_batch_id, correction_period
)
SELECT
    p.code_number, p.particulars, p.DR, p.CR, p.city_name, p.cycle_type, p.vertical,
    p.city_id, p.store_id, p.organization_id, p.organization_email_id,
    p.start_date, p.end_date, p.remarks, p.batch_id, p.submission_date,
    p.cycle_start, p.cycle_end, p.row_type, p.reference_batch_id, p.correction_period
FROM p360_batch_preview p
WHERE NOT EXISTS (
    SELECT 1 FROM p360_submissions s
    WHERE s.batch_id = p.batch_id
      AND s.code_number = p.code_number
      AND s.city_id = p.city_id
      AND s.vertical = p.vertical
      AND s.cycle_type = p.cycle_type
      AND s.start_date = p.start_date
      AND s.end_date = p.end_date
      AND s.organization_id = p.organization_id
      AND COALESCE(s.store_id, '') = COALESCE(p.store_id, '')
      AND COALESCE(s.correction_period, '1900-01-01') = COALESCE(p.correction_period, '1900-01-01')
      AND s.row_type = p.row_type
);

-- Mirror P360-facing rows into outbox (excludes RESTATEMENT internal markers)
INSERT INTO p360_outbox (
    code_number, particulars, DR, CR, city_name, cycle_type, vertical,
    city_id, store_id, organization_id, organization_email_id,
    start_date, end_date, remarks, batch_id, submission_date,
    cycle_start, cycle_end, row_type, reference_batch_id, correction_period
)
SELECT
    p.code_number, p.particulars, p.DR, p.CR, p.city_name, p.cycle_type, p.vertical,
    p.city_id, p.store_id, p.organization_id, p.organization_email_id,
    p.start_date, p.end_date, p.remarks, p.batch_id, p.submission_date,
    p.cycle_start, p.cycle_end, p.row_type, p.reference_batch_id, p.correction_period
FROM p360_batch_preview p
WHERE p.row_type IN ('ORIGINAL', 'REVERSAL', 'CORRECTION_DELTA')
  AND NOT EXISTS (
    SELECT 1 FROM p360_outbox s
    WHERE s.batch_id = p.batch_id
      AND s.code_number = p.code_number
      AND s.city_id = p.city_id
      AND s.vertical = p.vertical
      AND s.cycle_type = p.cycle_type
      AND s.start_date = p.start_date
      AND s.end_date = p.end_date
      AND s.organization_id = p.organization_id
      AND COALESCE(s.store_id, '') = COALESCE(p.store_id, '')
      AND COALESCE(s.correction_period, '1900-01-01') = COALESCE(p.correction_period, '1900-01-01')
      AND s.row_type = p.row_type
);

-- =============================================================================
-- MERGE state table with new cumulative values
-- For ORIGINAL/RESTATEMENT: update cum_dr/cum_cr to staging values
-- For REVERSAL: delete the row from state
-- =============================================================================

-- Step 2a: Delete reversed rows from state
DELETE FROM p360_delta_state
WHERE (code_number, city_id, vertical, cycle_type, start_date, end_date,
       COALESCE(organization_id, ''), COALESCE(store_id, '')) IN (
    SELECT code_number, city_id, vertical, cycle_type, start_date, end_date,
           COALESCE(organization_id, ''), COALESCE(store_id, '')
    FROM p360_batch_preview
    WHERE row_type = 'REVERSAL'
);

-- Step 2b: Upsert for ORIGINAL and RESTATEMENT rows
-- Update existing state rows
UPDATE p360_delta_state
SET
    cum_dr = COALESCE(p.DR, 0),
    cum_cr = COALESCE(p.CR, 0),
    particulars = p.particulars,
    city_name = p.city_name,
    organization_email_id = p.organization_email_id,
    remarks = p.remarks,
    last_batch_id = p.batch_id,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT DISTINCT code_number, city_id, vertical, cycle_type, start_date, end_date,
           organization_id, store_id, DR, CR, particulars, city_name,
           organization_email_id, remarks, batch_id
    FROM p360_batch_preview
    WHERE row_type IN ('ORIGINAL', 'RESTATEMENT')
) p
WHERE p360_delta_state.code_number = p.code_number
  AND p360_delta_state.city_id = p.city_id
  AND p360_delta_state.vertical = p.vertical
  AND p360_delta_state.cycle_type = p.cycle_type
  AND p360_delta_state.start_date = p.start_date
  AND p360_delta_state.end_date = p.end_date
  AND COALESCE(p360_delta_state.organization_id, '') = COALESCE(p.organization_id, '')
  AND COALESCE(p360_delta_state.store_id, '') = COALESCE(p.store_id, '');

-- Insert new state rows (ORIGINAL only - these don't exist in state yet)
INSERT INTO p360_delta_state (
    code_number, city_id, vertical, cycle_type, start_date, end_date,
    organization_id, store_id, particulars, city_name,
    organization_email_id, remarks, cum_dr, cum_cr,
    last_batch_id, created_at, updated_at
)
SELECT
    p.code_number, p.city_id, p.vertical, p.cycle_type, p.start_date, p.end_date,
    COALESCE(p.organization_id, ''), COALESCE(p.store_id, ''), p.particulars, p.city_name,
    p.organization_email_id, p.remarks, COALESCE(p.DR, 0), COALESCE(p.CR, 0),
    p.batch_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM p360_batch_preview p
WHERE p.row_type = 'ORIGINAL'
  AND NOT EXISTS (
      SELECT 1 FROM p360_delta_state st
      WHERE st.code_number = p.code_number
        AND st.city_id = p.city_id
        AND st.vertical = p.vertical
        AND st.cycle_type = p.cycle_type
        AND st.start_date = p.start_date
        AND st.end_date = p.end_date
        AND COALESCE(st.organization_id, '') = COALESCE(p.organization_id, '')
        AND COALESCE(st.store_id, '') = COALESCE(p.store_id, '')
  );

-- Update audit log with actual committed row count
UPDATE p360_batch_audit
SET
    committed_rows = (
        SELECT COUNT(*) FROM p360_submissions
        WHERE batch_id = (SELECT batch_id FROM p360_current_batch)
    ) - (SELECT cnt FROM p360_pre_insert_count),
    committed_at = CURRENT_TIMESTAMP,
    status = 'COMMITTED'
WHERE batch_id = (SELECT batch_id FROM p360_current_batch)
  AND status = 'IN_PROGRESS';

COMMIT;

DROP TABLE IF EXISTS p360_batch_preview;
DROP TABLE IF EXISTS p360_current_batch;
DROP TABLE IF EXISTS p360_pre_insert_count;


-- =============================================================================
-- ROLLBACK SECTION (run manually on failure)
-- If the COMMIT block fails, run this to mark the batch as FAILED:
-- =============================================================================

-- ROLLBACK;  -- Undo any partial changes
-- 
-- UPDATE p360_batch_audit
-- SET
--     status = 'FAILED',
--     error_message = 'Manual rollback - describe error here',
--     committed_at = CURRENT_TIMESTAMP
-- WHERE batch_id = (SELECT batch_id FROM p360_current_batch)
--   AND status = 'IN_PROGRESS';
-- 
-- DROP TABLE IF EXISTS p360_batch_preview;
-- DROP TABLE IF EXISTS p360_current_batch;
-- DROP TABLE IF EXISTS p360_pre_insert_count;


-- =============================================================================
-- STEP 3 — VERIFY (run after COMMIT)
-- net_DR should equal net_CR for every group (balanced double-entry).
-- =============================================================================

-- SELECT
--     code_number, particulars, city_name, cycle_type, vertical,
--     SUM(COALESCE(DR, 0)) AS net_DR,
--     SUM(COALESCE(CR, 0)) AS net_CR
-- FROM p360_submissions
-- GROUP BY code_number, particulars, city_name, cycle_type, vertical
-- ORDER BY city_name, vertical;

-- Check state table balance
-- SELECT
--     SUM(cum_dr) AS total_state_dr,
--     SUM(cum_cr) AS total_state_cr,
--     SUM(cum_dr) - SUM(cum_cr) AS state_imbalance
-- FROM p360_delta_state;

-- Check latest audit record
-- SELECT * FROM p360_batch_audit ORDER BY started_at DESC LIMIT 5;
