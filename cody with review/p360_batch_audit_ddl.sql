-- =============================================================================
-- P360 BATCH AUDIT — DDL
-- Audit table for tracking batch execution history, row counts, and balance
-- check results. Provides error capture and operational visibility.
-- Run once to create the table.
-- =============================================================================

CREATE TABLE p360_batch_audit (
    audit_id             INT           IDENTITY(1,1) PRIMARY KEY,
    batch_id             VARCHAR(20)   NOT NULL,
    submission_date      DATE          NOT NULL,
    cycle_start          DATE,
    cycle_end            DATE,

    -- Row counts
    staging_rows         INT,          -- COUNT(*) from p360_staging at batch start
    preview_rows         INT,          -- COUNT(*) from p360_batch_preview
    original_rows        INT,          -- COUNT(*) WHERE row_type = 'ORIGINAL'
    reversal_rows        INT,          -- COUNT(*) WHERE row_type = 'REVERSAL'
    restatement_rows     INT,          -- COUNT(*) WHERE row_type = 'RESTATEMENT'
    correction_delta_rows INT,         -- COUNT(*) WHERE row_type = 'CORRECTION_DELTA'
    committed_rows       INT,          -- Actual rows inserted into p360_submissions

    -- Balance check
    total_dr             DECIMAL(18,4),
    total_cr             DECIMAL(18,4),
    is_balanced          BOOLEAN,      -- TRUE if total_dr = total_cr (within tolerance)

    -- Timestamps
    started_at           TIMESTAMP,
    committed_at         TIMESTAMP,
    status               VARCHAR(20)   DEFAULT 'PENDING',  -- PENDING / COMMITTED / FAILED / SKIPPED

    -- Error capture
    error_message        VARCHAR(1000)
)
SORTKEY (submission_date, batch_id);

-- =============================================================================
-- USAGE (in p360_batch_runner.sql):
--
-- After Step 1 (PREVIEW):
--   INSERT INTO p360_batch_audit (batch_id, submission_date, cycle_start, cycle_end,
--       staging_rows, preview_rows, original_rows, reversal_rows,
--       restatement_rows, correction_delta_rows, total_dr, total_cr,
--       is_balanced, started_at, status)
--   SELECT
--       batch_id, CURRENT_DATE, MIN(cycle_start), MAX(cycle_end),
--       (SELECT COUNT(*) FROM p360_staging),
--       COUNT(*),
--       SUM(CASE WHEN row_type = 'ORIGINAL' THEN 1 ELSE 0 END),
--       SUM(CASE WHEN row_type = 'REVERSAL' THEN 1 ELSE 0 END),
--       SUM(CASE WHEN row_type = 'RESTATEMENT' THEN 1 ELSE 0 END),
--       SUM(CASE WHEN row_type = 'CORRECTION_DELTA' THEN 1 ELSE 0 END),
--       SUM(COALESCE(DR, 0)),
--       SUM(COALESCE(CR, 0)),
--       ABS(SUM(COALESCE(DR, 0)) - SUM(COALESCE(CR, 0))) < 0.01,
--       CURRENT_TIMESTAMP,
--       'PENDING'
--   FROM p360_batch_preview
--   GROUP BY batch_id;
--
-- After Step 2 (COMMIT):
--   UPDATE p360_batch_audit
--   SET committed_rows = <rows_inserted>,
--       committed_at = CURRENT_TIMESTAMP,
--       status = 'COMMITTED'
--   WHERE batch_id = <current_batch_id> AND status = 'PENDING';
-- =============================================================================
