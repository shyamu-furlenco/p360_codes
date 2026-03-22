-- =============================================================================
-- P360 STATE INITIALIZATION — Run ONCE (migration only)
-- =============================================================================
-- Populates p360_erp.p360_delta_state from existing p360_erp.p360_submissions
-- history when switching to the state-based batch runner.
--
-- Run this AFTER 01_setup.sql and BEFORE running 04_batch_runner.sql for
-- the first time on an existing dataset.
--
-- If this is a brand-new installation with no prior submissions, skip this
-- file entirely — the state table starts empty and gets populated automatically.
-- =============================================================================

-- Safety check: confirm state table is empty before populating.
-- Run this first; if count > 0, TRUNCATE p360_erp.p360_delta_state first.
-- SELECT COUNT(*) FROM p360_erp.p360_delta_state;

BEGIN;

-- Populate state from submissions history.
-- Logic: find the most recent ORIGINAL or RESTATEMENT per business key.
INSERT INTO p360_erp.p360_delta_state (
    code_number, city_id, vertical, cycle_type, start_date, end_date,
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
    start_date,
    end_date,
    organization_id,
    COALESCE(store_id, ''),   -- Normalize NULL to empty string for PK
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
        start_date, end_date, remarks, batch_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                code_number, city_id, vertical, cycle_type,
                start_date, end_date, organization_id,
                COALESCE(store_id, '')
            ORDER BY submission_date DESC, batch_id DESC
        ) AS rn
    FROM p360_erp.p360_submissions
    WHERE row_type IN ('ORIGINAL', 'RESTATEMENT')
) t
WHERE rn = 1;

-- Remove any keys whose most recent submission was a REVERSAL.
-- (Row was deleted from source — should not exist in state.)
DELETE FROM p360_erp.p360_delta_state
WHERE (code_number, city_id, vertical, cycle_type, start_date, end_date,
       organization_id, COALESCE(store_id, '')) IN (
    SELECT
        code_number, city_id, vertical, cycle_type, start_date, end_date,
        organization_id, COALESCE(store_id, '')
    FROM (
        SELECT
            code_number, city_id, vertical, cycle_type, start_date, end_date,
            organization_id, store_id, row_type,
            ROW_NUMBER() OVER (
                PARTITION BY
                    code_number, city_id, vertical, cycle_type,
                    start_date, end_date, organization_id,
                    COALESCE(store_id, '')
                ORDER BY submission_date DESC, batch_id DESC
            ) AS rn
        FROM p360_erp.p360_submissions
    ) t
    WHERE rn = 1 AND row_type = 'REVERSAL'
);

COMMIT;

-- =============================================================================
-- VERIFICATION — Run after migration to confirm state is correct
-- =============================================================================

-- Row count should match distinct active (non-reversed) business keys:
-- SELECT COUNT(*) FROM p360_erp.p360_delta_state;

-- Spot check for a specific city:
-- SELECT * FROM p360_erp.p360_delta_state WHERE city_id = 10 LIMIT 5;

-- Balance check — state DR should equal state CR (if original data was balanced):
-- SELECT
--     SUM(cum_dr) AS total_dr,
--     SUM(cum_cr) AS total_cr,
--     SUM(cum_dr) - SUM(cum_cr) AS imbalance
-- FROM p360_erp.p360_delta_state;
