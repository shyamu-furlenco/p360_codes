-- =============================================================================
-- P360 RECONCILIATION VIEWS
-- Views for external consumers to understand what changed and why.
-- Run after creating all base tables.
-- =============================================================================

-- =============================================================================
-- VIEW: p360_batch_summary
-- Summary of each batch with row counts and balance status.
-- =============================================================================
CREATE OR REPLACE VIEW p360_batch_summary AS
SELECT
    batch_id,
    submission_date,
    cycle_start,
    cycle_end,
    SUM(CASE WHEN row_type = 'ORIGINAL' THEN 1 ELSE 0 END) AS original_count,
    SUM(CASE WHEN row_type = 'REVERSAL' THEN 1 ELSE 0 END) AS reversal_count,
    SUM(CASE WHEN row_type = 'RESTATEMENT' THEN 1 ELSE 0 END) AS restatement_count,
    SUM(CASE WHEN row_type = 'CORRECTION_DELTA' THEN 1 ELSE 0 END) AS correction_delta_count,
    COUNT(*) AS total_rows,
    SUM(COALESCE(DR, 0)) AS total_dr,
    SUM(COALESCE(CR, 0)) AS total_cr,
    ROUND(SUM(COALESCE(DR, 0)) - SUM(COALESCE(CR, 0)), 4) AS imbalance
FROM p360_submissions
GROUP BY batch_id, submission_date, cycle_start, cycle_end
ORDER BY submission_date DESC, batch_id DESC;


-- =============================================================================
-- VIEW: p360_corrections_detail
-- Detail of all correction entries showing what changed from what.
-- =============================================================================
CREATE OR REPLACE VIEW p360_corrections_detail AS
SELECT
    cd.batch_id AS correction_batch_id,
    cd.submission_date AS correction_date,
    cd.code_number,
    cd.city_name,
    cd.vertical,
    cd.cycle_type,
    cd.correction_period AS original_period,
    cd.recognised_date AS posted_to_period,
    cd.DR AS delta_dr,
    cd.CR AS delta_cr,
    cd.reference_batch_id AS original_batch_id,
    -- Join to get original amounts
    orig.DR AS original_dr,
    orig.CR AS original_cr,
    -- Join to get restated amounts
    rest.DR AS restated_dr,
    rest.CR AS restated_cr
FROM p360_submissions cd
LEFT JOIN p360_submissions orig
  ON orig.batch_id = cd.reference_batch_id
 AND orig.code_number = cd.code_number
 AND orig.city_id = cd.city_id
 AND orig.vertical = cd.vertical
 AND orig.cycle_type = cd.cycle_type
 AND orig.recognised_date = cd.correction_period
 AND orig.organization_id = cd.organization_id
 AND COALESCE(orig.store_id, '') = COALESCE(cd.store_id, '')
 AND orig.row_type IN ('ORIGINAL', 'RESTATEMENT')
LEFT JOIN p360_submissions rest
  ON rest.batch_id = cd.batch_id
 AND rest.code_number = cd.code_number
 AND rest.city_id = cd.city_id
 AND rest.vertical = cd.vertical
 AND rest.cycle_type = cd.cycle_type
 AND rest.recognised_date = cd.correction_period
 AND rest.organization_id = cd.organization_id
 AND COALESCE(rest.store_id, '') = COALESCE(cd.store_id, '')
 AND rest.row_type = 'RESTATEMENT'
WHERE cd.row_type = 'CORRECTION_DELTA'
ORDER BY cd.submission_date DESC, cd.batch_id, cd.code_number;


-- =============================================================================
-- VIEW: p360_current_state
-- Current effective state per business key (what P360 currently has).
-- Equivalent to p360_delta_state but computed from submissions for verification.
-- =============================================================================
CREATE OR REPLACE VIEW p360_current_state AS
SELECT
    code_number,
    particulars,
    city_name,
    city_id,
    vertical,
    cycle_type,
    recognised_date,
    organization_id,
    store_id,
    organization_email_id,
    DR AS current_dr,
    CR AS current_cr,
    batch_id AS last_batch_id,
    submission_date AS last_submission_date,
    row_type AS last_row_type
FROM (
    SELECT
        *,
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


-- =============================================================================
-- VIEW: p360_reversals_history
-- All reversed entries (rows that were deleted from source).
-- =============================================================================
CREATE OR REPLACE VIEW p360_reversals_history AS
SELECT
    batch_id,
    submission_date,
    code_number,
    city_name,
    vertical,
    cycle_type,
    recognised_date,
    DR AS reversal_dr,   -- This is the flipped amount (old_CR)
    CR AS reversal_cr,   -- This is the flipped amount (old_DR)
    reference_batch_id AS original_batch_id
FROM p360_submissions
WHERE row_type = 'REVERSAL'
ORDER BY submission_date DESC, batch_id;


-- =============================================================================
-- VIEW: p360_period_totals
-- Totals by recognised_date period (for period-based reconciliation).
-- =============================================================================
CREATE OR REPLACE VIEW p360_period_totals AS
SELECT
    recognised_date,
    vertical,
    city_name,
    cycle_type,
    SUM(CASE WHEN row_type IN ('ORIGINAL', 'RESTATEMENT') THEN COALESCE(DR, 0) ELSE 0 END) AS gross_dr,
    SUM(CASE WHEN row_type IN ('ORIGINAL', 'RESTATEMENT') THEN COALESCE(CR, 0) ELSE 0 END) AS gross_cr,
    SUM(CASE WHEN row_type = 'CORRECTION_DELTA' THEN COALESCE(DR, 0) ELSE 0 END) AS correction_dr,
    SUM(CASE WHEN row_type = 'CORRECTION_DELTA' THEN COALESCE(CR, 0) ELSE 0 END) AS correction_cr,
    SUM(CASE WHEN row_type = 'REVERSAL' THEN COALESCE(DR, 0) ELSE 0 END) AS reversal_dr,
    SUM(CASE WHEN row_type = 'REVERSAL' THEN COALESCE(CR, 0) ELSE 0 END) AS reversal_cr
FROM p360_submissions
GROUP BY recognised_date, vertical, city_name, cycle_type
ORDER BY recognised_date DESC, vertical, city_name;


-- =============================================================================
-- VIEW: p360_state_vs_staging_diff
-- Shows differences between current state and staging (preview of next batch).
-- Useful for pre-batch analysis.
-- =============================================================================
CREATE OR REPLACE VIEW p360_state_vs_staging_diff AS
SELECT
    COALESCE(s.code_number, st.code_number) AS code_number,
    COALESCE(s.city_name, st.city_name) AS city_name,
    COALESCE(s.vertical, st.vertical) AS vertical,
    COALESCE(s.cycle_type, st.cycle_type) AS cycle_type,
    COALESCE(s.recognised_date, st.recognised_date) AS recognised_date,
    s.DR AS staging_dr,
    s.CR AS staging_cr,
    st.cum_dr AS state_dr,
    st.cum_cr AS state_cr,
    COALESCE(s.DR, 0) - COALESCE(st.cum_dr, 0) AS delta_dr,
    COALESCE(s.CR, 0) - COALESCE(st.cum_cr, 0) AS delta_cr,
    CASE
        WHEN st.code_number IS NULL THEN 'ORIGINAL'
        WHEN s.code_number IS NULL THEN 'REVERSAL'
        WHEN ROUND(COALESCE(s.DR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.cum_dr, 0)::NUMERIC, 4)
          OR ROUND(COALESCE(s.CR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.cum_cr, 0)::NUMERIC, 4)
            THEN 'CORRECTION'
        ELSE 'UNCHANGED'
    END AS expected_action
FROM p360_staging s
FULL OUTER JOIN p360_delta_state st
  ON s.code_number = st.code_number
 AND s.city_id = st.city_id
 AND s.vertical = st.vertical
 AND s.cycle_type = st.cycle_type
 AND s.recognised_date = st.recognised_date
 AND COALESCE(s.organization_id, '') = COALESCE(st.organization_id, '')
 AND COALESCE(s.store_id, '') = COALESCE(st.store_id, '')
WHERE (st.code_number IS NULL OR s.code_number IS NULL
    OR ROUND(COALESCE(s.DR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.cum_dr, 0)::NUMERIC, 4)
    OR ROUND(COALESCE(s.CR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.cum_cr, 0)::NUMERIC, 4));
