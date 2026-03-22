-- =============================================================================
-- P360 SUBMISSIONS — DDL
-- Tracking table for every row ever sent to P360.
-- Run once to create the table.
-- =============================================================================

CREATE TABLE p360_erp.p360_submissions (

    -- -------------------------------------------------------------------------
    -- Business Key columns (NOT NULL for data integrity)
    -- -------------------------------------------------------------------------
    code_number              VARCHAR    NOT NULL,
    particulars              VARCHAR,
    DR                       DECIMAL(15,4),         -- DECIMAL avoids float precision issues in a permanent ledger
    CR                       DECIMAL(15,4),
    city_name                VARCHAR,
    cycle_type               VARCHAR    NOT NULL,
    vertical                 VARCHAR    NOT NULL,
    city_id                  INT        NOT NULL,
    store_id                 VARCHAR,              -- Can be NULL; use COALESCE('') in joins
    organization_id          VARCHAR    NOT NULL,
    organization_email_id    VARCHAR,
    start_date               DATE       NOT NULL,
    end_date                 DATE       NOT NULL,
    remarks                  VARCHAR,

    -- -------------------------------------------------------------------------
    -- Batch tracking columns
    -- -------------------------------------------------------------------------
    batch_id                 VARCHAR    NOT NULL,   -- e.g. B_20250316_001
    submission_date          DATE       NOT NULL,
    cycle_start              DATE,                  -- MIN(start_date) in this batch
    cycle_end                DATE,                  -- MAX(start_date) in this batch
    row_type                 VARCHAR    NOT NULL,   -- ORIGINAL / REVERSAL / RESTATEMENT / CORRECTION_DELTA
    reference_batch_id       VARCHAR,               -- NULL for ORIGINAL; prior batch_id for REVERSAL/RESTATEMENT/CORRECTION_DELTA
    correction_period        DATE                   -- NULL except for CORRECTION_DELTA: the original start_date of the period being corrected
)
DISTKEY (city_id)                                  -- co-locate with p360_staging for efficient FULL OUTER JOIN
SORTKEY (submission_date, row_type);               -- speeds up last_sent CTE filter (row_type IN ..., ORDER BY submission_date DESC)

-- =============================================================================
-- UNIQUENESS CONSTRAINT
-- Prevents duplicate rows for the same business key within a batch.
-- Note: Redshift does not enforce PRIMARY KEY/UNIQUE constraints, but this
-- serves as documentation and enables query optimizer hints.
-- For actual enforcement, use the idempotent INSERT guard in p360_batch_runner.sql.
-- =============================================================================
ALTER TABLE p360_submissions ADD CONSTRAINT p360_submissions_unique
    UNIQUE (batch_id, code_number, city_id, vertical, cycle_type,
            start_date, end_date, organization_id, store_id, row_type, correction_period);
