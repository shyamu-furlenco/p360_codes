-- =============================================================================
-- P360 DELTA STATE — DDL
-- Compact state table for efficient delta computation.
-- Stores one row per business key with cumulative DR/CR totals.
-- Replaces full-history scanning with O(current_keys) lookups.
-- Run once to create the table.
-- =============================================================================

CREATE TABLE p360_delta_state (

    -- -------------------------------------------------------------------------
    -- Business Key (matches p360_submissions)
    -- -------------------------------------------------------------------------
    code_number              VARCHAR       NOT NULL,
    city_id                  INT           NOT NULL,
    vertical                 VARCHAR       NOT NULL,
    cycle_type               VARCHAR       NOT NULL,
    start_date               DATE          NOT NULL,
    end_date                 DATE          NOT NULL,
    organization_id          VARCHAR       NOT NULL,
    store_id                 VARCHAR       NOT NULL,  -- Use COALESCE('') on insert

    -- -------------------------------------------------------------------------
    -- Descriptive fields (latest values)
    -- -------------------------------------------------------------------------
    particulars              VARCHAR,
    city_name                VARCHAR,
    organization_email_id    VARCHAR,
    remarks                  VARCHAR,

    -- -------------------------------------------------------------------------
    -- Cumulative state (sum of all ORIGINAL + RESTATEMENT amounts)
    -- This represents "what P360 currently believes" for this business key.
    -- -------------------------------------------------------------------------
    cum_dr                   DECIMAL(18,4) NOT NULL DEFAULT 0,
    cum_cr                   DECIMAL(18,4) NOT NULL DEFAULT 0,

    -- -------------------------------------------------------------------------
    -- Tracking
    -- -------------------------------------------------------------------------
    last_batch_id            VARCHAR       NOT NULL,
    created_at               TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

    -- -------------------------------------------------------------------------
    -- Primary Key (business key)
    -- Note: Redshift doesn't enforce PK but uses it for optimization hints
    -- -------------------------------------------------------------------------
    PRIMARY KEY (code_number, city_id, vertical, cycle_type,
                 start_date, end_date, organization_id, store_id)
)
DISTKEY (city_id)                                               -- co-locate with p360_staging and p360_submissions
SORTKEY (code_number, city_id, vertical, cycle_type, start_date);  -- optimize FULL OUTER JOIN

-- =============================================================================
-- NOTES:
--
-- 1. This table is MUTABLE — updated via MERGE after each batch.
--
-- 2. p360_submissions remains the IMMUTABLE audit log.
--
-- 3. cum_dr/cum_cr represent the "effective state" that P360 has received:
--    - For ORIGINAL rows: adds the amount
--    - For RESTATEMENT rows: replaces with new amount
--    - For REVERSAL rows: this row is deleted from state (or cum=0)
--
-- 4. Delta computation becomes:
--    delta_dr = staging.DR - state.cum_dr
--    delta_cr = staging.CR - state.cum_cr
--
-- 5. MERGE pattern after INSERT:
--    MERGE INTO p360_delta_state USING delta_rows ON <business_key>
--    WHEN MATCHED THEN UPDATE SET cum_dr = new_dr, cum_cr = new_cr, ...
--    WHEN NOT MATCHED THEN INSERT (...)
-- =============================================================================
