-- =============================================================================
-- P360 OUTBOX — DDL
-- P360-facing rows only: ORIGINAL, REVERSAL, CORRECTION_DELTA.
-- RESTATEMENT rows (internal state markers) are NOT stored here.
-- Run once to create the table.
-- =============================================================================
--
-- This table is populated by p360_batch_runner.sql (Step 2) immediately
-- after the INSERT into p360_submissions, filtered to row_type IN
-- ('ORIGINAL', 'REVERSAL', 'CORRECTION_DELTA').
--
-- Purpose: P360 team can query this table directly without filtering by
-- row_type — every row here was actually sent to P360.
-- =============================================================================

CREATE TABLE p360_outbox (

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
    row_type                 VARCHAR    NOT NULL,   -- ORIGINAL / REVERSAL / CORRECTION_DELTA (never RESTATEMENT)
    reference_batch_id       VARCHAR,               -- NULL for ORIGINAL; prior batch_id for REVERSAL/CORRECTION_DELTA
    correction_period        DATE                   -- NULL except for CORRECTION_DELTA: the original start_date of the period being corrected
)
DISTKEY (city_id)                                  -- co-locate with p360_staging for efficient FULL OUTER JOIN
SORTKEY (submission_date, row_type);               -- speeds up date-range queries and row_type filters

-- =============================================================================
-- UNIQUENESS CONSTRAINT
-- correction_period is included (unlike p360_submissions) because
-- CORRECTION_DELTA rows for different original start_dates all land on the
-- same batch start_date (delta period), and correction_period is the
-- only column that distinguishes them in the same batch.
-- Note: Redshift does not enforce PRIMARY KEY/UNIQUE constraints, but this
-- serves as documentation and enables query optimizer hints.
-- For actual enforcement, use the idempotent INSERT guard in p360_batch_runner.sql.
-- =============================================================================
ALTER TABLE p360_outbox ADD CONSTRAINT p360_outbox_unique
    UNIQUE (batch_id, code_number, city_id, vertical, cycle_type,
            start_date, end_date, organization_id, store_id, correction_period);
