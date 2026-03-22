-- =============================================================================
-- P360 BATCH RUNNER — THREE-STEP WORKFLOW
-- Run on batch day (weekly / monthly, or on-demand for corrections).
-- No parameters to edit — everything is auto-detected.
--
-- STEP 0 — ALLOCATE: Run the batch_id allocation query first.
-- STEP 1 — PREVIEW:  Run the CREATE TEMP TABLE block below.
--                    Then run: SELECT * FROM p360_batch_preview ORDER BY ...
--                    Review output before committing.
--
-- STEP 2 — COMMIT:   When satisfied, run the INSERT block at the bottom.
--
-- STEP 3 — VERIFY:   Run the balance-check query at the bottom.
--
-- PREREQUISITES:
--   - p360_batch_control table must exist (see p360_batch_control_ddl.sql)
--   - p360_batch_audit table must exist (see p360_batch_audit_ddl.sql)
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
-- =============================================================================

DROP TABLE IF EXISTS p360_batch_preview;

CREATE TEMP TABLE p360_batch_preview AS

-- ===========================================================================
-- CTE chain:
--   new_batch     → uses pre-allocated batch_id from Step 0
--   last_sent     → most-recent ORIGINAL or RESTATEMENT per business key
--   current_data  → all rows currently in p360_staging
--   comparison    → FULL OUTER JOIN; labels each row's action
--   batch_bounds  → MIN/MAX recognised_date across all rows being emitted
--   delta_date    → recognised_date to stamp on CORRECTION_DELTA rows
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
-- Most-recent ORIGINAL or RESTATEMENT row per business key.
-- Business key: code_number + city_id + vertical + cycle_type +
--               recognised_date + organization_id + store_id
-- Handles any number of past corrections automatically.
-- ---------------------------------------------------------------------------
last_sent AS (
    SELECT
        code_number, particulars, DR, CR,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        recognised_date, remarks,
        batch_id
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    code_number, city_id, vertical, cycle_type,
                    recognised_date, organization_id,
                    COALESCE(store_id, '')
                ORDER BY submission_date DESC, batch_id DESC
            ) AS rn
        FROM p360_submissions
        WHERE row_type IN ('ORIGINAL', 'RESTATEMENT')
    ) t
    WHERE rn = 1
),

-- ---------------------------------------------------------------------------
-- Current state: everything in the daily-refreshed staging table.
-- No date restriction — covers all recognised_dates back to April 2024.
-- ---------------------------------------------------------------------------
current_data AS (
    SELECT
        code_number, particulars, DR, CR,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        recognised_date, remarks
    FROM p360_staging
),

-- ---------------------------------------------------------------------------
-- Compare current_data vs last_sent (FULL OUTER JOIN on business key).
-- Actions:
--   ORIGINAL      — in current_data only (never sent before)
--   REVERSAL_ONLY — in last_sent only (row disappeared from source)
--   CORRECTION    — in both but DR or CR differs
--   UNCHANGED     — in both and amounts match (skip — no rows emitted)
-- ---------------------------------------------------------------------------
comparison AS (
    SELECT
        -- Derive business key from whichever side is non-null
        COALESCE(cur.code_number,           ls.code_number)           AS code_number,
        COALESCE(cur.particulars,           ls.particulars)           AS particulars,
        COALESCE(cur.city_name,             ls.city_name)             AS city_name,
        COALESCE(cur.cycle_type,            ls.cycle_type)            AS cycle_type,
        COALESCE(cur.vertical,              ls.vertical)              AS vertical,
        COALESCE(cur.city_id,               ls.city_id)               AS city_id,
        COALESCE(cur.store_id,              ls.store_id)              AS store_id,
        COALESCE(cur.organization_id,       ls.organization_id)       AS organization_id,
        COALESCE(cur.organization_email_id, ls.organization_email_id) AS organization_email_id,
        COALESCE(cur.recognised_date,       ls.recognised_date)       AS recognised_date,
        COALESCE(cur.remarks,               ls.remarks)               AS remarks,

        -- Current (staging) amounts — NULL if row no longer exists in source
        cur.DR   AS cur_DR,
        cur.CR   AS cur_CR,

        -- Last-submitted amounts — NULL if never sent before
        ls.DR    AS old_DR,
        ls.CR    AS old_CR,

        -- batch_id of the most-recent submission (used as reference_batch_id on REVERSAL/RESTATEMENT)
        ls.batch_id AS last_batch_id,

        -- Classify action
        CASE
            WHEN ls.code_number IS NULL
                THEN 'ORIGINAL'
            WHEN cur.code_number IS NULL
                THEN 'REVERSAL_ONLY'
            WHEN ROUND(COALESCE(cur.DR, 0)::NUMERIC, 4) <> ROUND(COALESCE(ls.DR, 0)::NUMERIC, 4)
              OR ROUND(COALESCE(cur.CR, 0)::NUMERIC, 4) <> ROUND(COALESCE(ls.CR, 0)::NUMERIC, 4)
                THEN 'CORRECTION'
            ELSE 'UNCHANGED'
        END AS action

    FROM current_data cur
    FULL OUTER JOIN last_sent ls
      ON cur.code_number     = ls.code_number
     AND cur.city_id         = ls.city_id
     AND cur.vertical        = ls.vertical
     AND cur.cycle_type      = ls.cycle_type
     AND cur.recognised_date = ls.recognised_date
     AND cur.organization_id = ls.organization_id
     AND COALESCE(cur.store_id, '') = COALESCE(ls.store_id, '')
),

-- ---------------------------------------------------------------------------
-- Compute cycle_start / cycle_end across all rows being emitted this batch.
-- If nothing changed (all UNCHANGED), both return NULL and output_rows is empty.
-- ---------------------------------------------------------------------------
batch_bounds AS (
    SELECT
        MIN(recognised_date) AS cycle_start,
        MAX(recognised_date) AS cycle_end
    FROM comparison
    WHERE action IN ('ORIGINAL', 'REVERSAL_ONLY', 'CORRECTION')
),

-- ---------------------------------------------------------------------------
-- recognised_date to use on CORRECTION_DELTA rows: the latest ORIGINAL date
-- in the current batch (the "current cycle" date). Falls back to CURRENT_DATE
-- when a batch contains only corrections and no new ORIGINAL entries.
-- ---------------------------------------------------------------------------
delta_date AS (
    SELECT COALESCE(
        MAX(CASE WHEN action = 'ORIGINAL' THEN recognised_date END),
        CURRENT_DATE
    ) AS recognised_date
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
        cmp.recognised_date, cmp.remarks,
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
    -- CORRECTION no longer emits a REVERSAL — uses CORRECTION_DELTA instead.
    SELECT
        cmp.code_number, cmp.particulars,
        cmp.old_CR  AS DR,   -- flip: old CR → new DR
        cmp.old_DR  AS CR,   -- flip: old DR → new CR
        cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
        cmp.store_id, cmp.organization_id, cmp.organization_email_id,
        cmp.recognised_date, cmp.remarks,
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
    -- NOT sent to P360; stored in p360_submissions so future batches can
    -- find the correct "last sent" baseline via the last_sent CTE.
    SELECT
        cmp.code_number, cmp.particulars,
        cmp.cur_DR  AS DR,
        cmp.cur_CR  AS CR,
        cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
        cmp.store_id, cmp.organization_id, cmp.organization_email_id,
        cmp.recognised_date, cmp.remarks,
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

    -- CORRECTION_DELTA: net incremental change only — sent to P360 for CORRECTION.
    -- Amount logic (double-entry): positive delta stays on its side;
    -- negative delta on one side flips to the opposite side.
    -- recognised_date uses the current cycle date (from delta_date CTE),
    -- not the original entry date (which is stored in correction_period).
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
        dd.recognised_date, cmp.remarks,
        nb.batch_id,
        CURRENT_DATE                  AS submission_date,
        bb.cycle_start,
        bb.cycle_end,
        'CORRECTION_DELTA'::VARCHAR   AS row_type,
        cmp.last_batch_id             AS reference_batch_id,
        cmp.recognised_date           AS correction_period   -- original date being corrected
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
    recognised_date, remarks,
    batch_id, submission_date, cycle_start, cycle_end,
    row_type, reference_batch_id, correction_period
FROM output_rows;


-- =============================================================================
-- Review the batch output (run after Step 1):
-- =============================================================================

-- All rows for P360 (excludes silent RESTATEMENT state markers)
-- Ordered ORIGINAL → REVERSAL → CORRECTION_DELTA (correction pairs stay together)
SELECT *
FROM p360_batch_preview
WHERE row_type <> 'RESTATEMENT'
ORDER BY
    CASE row_type WHEN 'ORIGINAL' THEN 1 WHEN 'REVERSAL' THEN 2 WHEN 'CORRECTION_DELTA' THEN 3 END,
    city_name,
    recognised_date,
    cycle_type,
    vertical,
    code_number;

-- Summary by row_type — P360-facing rows only (excludes silent RESTATEMENT state markers)
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
    COUNT(*),
    SUM(CASE WHEN row_type = 'ORIGINAL' THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_type = 'REVERSAL' THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_type = 'RESTATEMENT' THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_type = 'CORRECTION_DELTA' THEN 1 ELSE 0 END),
    SUM(COALESCE(DR, 0)),
    SUM(COALESCE(CR, 0)),
    ABS(SUM(COALESCE(DR, 0)) - SUM(COALESCE(CR, 0))) < 0.01,
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
--   - Audit log update
-- =============================================================================

BEGIN;

-- Acquire exclusive lock to prevent concurrent batch commits
LOCK TABLE p360_submissions;

-- Idempotent INSERT: only insert if batch_id not already in submissions
-- Prevents duplicate rows on accidental re-run
INSERT INTO p360_submissions (
    code_number, particulars, DR, CR, city_name, cycle_type, vertical,
    city_id, store_id, organization_id, organization_email_id,
    recognised_date, remarks, batch_id, submission_date,
    cycle_start, cycle_end, row_type, reference_batch_id, correction_period
)
SELECT
    p.code_number, p.particulars, p.DR, p.CR, p.city_name, p.cycle_type, p.vertical,
    p.city_id, p.store_id, p.organization_id, p.organization_email_id,
    p.recognised_date, p.remarks, p.batch_id, p.submission_date,
    p.cycle_start, p.cycle_end, p.row_type, p.reference_batch_id, p.correction_period
FROM p360_batch_preview p
WHERE NOT EXISTS (
    SELECT 1 FROM p360_submissions s
    WHERE s.batch_id = p.batch_id
      AND s.code_number = p.code_number
      AND s.city_id = p.city_id
      AND s.vertical = p.vertical
      AND s.cycle_type = p.cycle_type
      AND s.recognised_date = p.recognised_date
      AND s.organization_id = p.organization_id
      AND COALESCE(s.store_id, '') = COALESCE(p.store_id, '')
      AND s.row_type = p.row_type
);

-- Update audit log with commit status
UPDATE p360_batch_audit
SET
    committed_rows = (SELECT COUNT(*) FROM p360_batch_preview),
    committed_at = CURRENT_TIMESTAMP,
    status = 'COMMITTED'
WHERE batch_id = (SELECT batch_id FROM p360_current_batch)
  AND status = 'PENDING';

COMMIT;

DROP TABLE IF EXISTS p360_batch_preview;
DROP TABLE IF EXISTS p360_current_batch;


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

-- Check latest audit record
-- SELECT * FROM p360_batch_audit ORDER BY started_at DESC LIMIT 5;
