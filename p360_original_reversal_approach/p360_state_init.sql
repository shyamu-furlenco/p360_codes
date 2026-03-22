-- =============================================================================
-- P360 STATE INITIALIZATION
-- One-time migration script to populate p360_delta_state from existing
-- p360_submissions history.
--
-- Run this ONCE after creating p360_delta_state table and BEFORE
-- switching p360_batch_runner.sql to the state-based logic.
--
-- This computes the "effective state" for each business key by finding
-- the most recent ORIGINAL or RESTATEMENT row (same logic as old last_sent CTE).
-- =============================================================================

-- Safety check: ensure state table is empty before populating
-- SELECT COUNT(*) FROM p360_delta_state;
-- If non-zero, TRUNCATE or DROP and recreate first.

BEGIN;

-- Populate state from submissions history
INSERT INTO p360_delta_state (
    code_number, city_id, vertical, cycle_type, recognised_date,
    organization_id, store_id,
    particulars, city_name, organization_email_id, remarks,
    cum_dr, cum_cr,
    last_batch_id, created_at, updated_at
)
SELECT
    code_number,
    city_id,
    vertical,
    cycle_type,
    recognised_date,
    organization_id,
    COALESCE(store_id, ''),  -- Normalize NULL to empty string for PK
    particulars,
    city_name,
    organization_email_id,
    remarks,
    COALESCE(DR, 0) AS cum_dr,
    COALESCE(CR, 0) AS cum_cr,
    batch_id AS last_batch_id,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM (
    SELECT
        code_number, particulars, DR, CR,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        recognised_date, remarks, batch_id,
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
WHERE rn = 1;

-- Handle REVERSAL_ONLY rows: these keys should NOT appear in state
-- (the row was deleted from source). The above query naturally excludes
-- them because REVERSAL rows are not in ('ORIGINAL', 'RESTATEMENT').
-- But if a key had ORIGINAL then REVERSAL, we should remove it.

-- Delete any state rows where the most recent submission was a REVERSAL
DELETE FROM p360_delta_state
WHERE (code_number, city_id, vertical, cycle_type, recognised_date,
       organization_id, COALESCE(store_id, '')) IN (
    SELECT
        code_number, city_id, vertical, cycle_type, recognised_date,
        organization_id, COALESCE(store_id, '')
    FROM (
        SELECT
            code_number, city_id, vertical, cycle_type, recognised_date,
            organization_id, store_id, row_type,
            ROW_NUMBER() OVER (
                PARTITION BY
                    code_number, city_id, vertical, cycle_type,
                    recognised_date, organization_id,
                    COALESCE(store_id, '')
                ORDER BY submission_date DESC, batch_id DESC
            ) AS rn
        FROM p360_submissions
    ) t
    WHERE rn = 1 AND row_type = 'REVERSAL'
);

COMMIT;

-- =============================================================================
-- VERIFICATION
-- Run these after the migration to confirm state is correct.
-- =============================================================================

-- Count of state rows should match count of distinct business keys
-- that have an active (non-reversed) position:
-- SELECT COUNT(*) FROM p360_delta_state;

-- Spot check: compare state to last_sent CTE output for a sample key
-- SELECT * FROM p360_delta_state WHERE city_id = 10 LIMIT 5;

-- Balance check: total DR should equal total CR across all state
-- (if original data was balanced)
-- SELECT
--     SUM(cum_dr) AS total_dr,
--     SUM(cum_cr) AS total_cr,
--     SUM(cum_dr) - SUM(cum_cr) AS imbalance
-- FROM p360_delta_state;
