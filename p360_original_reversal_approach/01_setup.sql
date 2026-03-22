-- =============================================================================
-- P360 SETUP — Run ONCE to create all tables and views
-- =============================================================================
-- Creates the p360_erp schema and all supporting objects in dependency order:
--
--   Step 1 : CREATE SCHEMA
--   Step 2 : p360_erp.p360_staging          — daily snapshot of source data
--   Step 3 : p360_erp.p360_submissions       — immutable audit log of all rows sent to P360
--   Step 4 : p360_erp.p360_delta_state       — compact mutable state for delta computation
--   Step 5 : p360_erp.p360_batch_control     — atomic batch_id sequence (prevents race conditions)
--   Step 6 : p360_erp.p360_batch_audit       — batch execution history and diagnostics
--   Step 7 : p360_erp.p360_outbox            — P360-facing rows only (no RESTATEMENT rows)
--   Step 8 : Reconciliation views
--
-- After running this file, proceed to:
--   02_state_init.sql  — only if migrating from existing p360_submissions history
--   03_staging_refresh.sql — run daily to populate p360_erp.p360_staging
--   04_batch_runner.sql    — run weekly on batch day
-- =============================================================================


-- =============================================================================
-- STEP 1 — SCHEMA
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS p360_erp;


-- =============================================================================
-- STEP 2 — p360_erp.p360_staging
-- Daily snapshot of all current source data. Truncated and reloaded each
-- day by 03_staging_refresh.sql.
-- =============================================================================

CREATE TABLE p360_erp.p360_staging (

    -- -------------------------------------------------------------------------
    -- Output columns (mirror 03_staging_refresh.sql final SELECT)
    -- -------------------------------------------------------------------------
    code_number              VARCHAR,
    particulars              VARCHAR,
    DR                       DECIMAL(15,4),         -- DECIMAL avoids float precision issues
    CR                       DECIMAL(15,4),
    city_name                VARCHAR,
    cycle_type               VARCHAR,
    vertical                 VARCHAR,
    city_id                  INT,
    store_id                 VARCHAR,
    organization_id          VARCHAR,
    organization_email_id    VARCHAR,
    start_date               DATE,
    end_date                 DATE,
    remarks                  VARCHAR,

    -- -------------------------------------------------------------------------
    -- Housekeeping
    -- -------------------------------------------------------------------------
    refreshed_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTKEY (city_id)                                              -- matches p360_submissions DISTKEY for co-located FULL OUTER JOIN
SORTKEY (code_number, city_id, vertical, cycle_type, start_date);  -- aligns with batch_runner FULL OUTER JOIN key


-- =============================================================================
-- STEP 3 — p360_erp.p360_submissions
-- Immutable audit log. Every row ever sent to P360 lives here.
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
    correction_period        DATE                   -- NULL except for CORRECTION_DELTA: the original start_date being corrected
)
DISTKEY (city_id)                                  -- co-locate with p360_staging for efficient FULL OUTER JOIN
SORTKEY (submission_date, row_type);               -- speeds up last_sent CTE filter (row_type IN ..., ORDER BY submission_date DESC)

-- Uniqueness constraint (Redshift does not enforce, but documents intent and aids optimizer)
ALTER TABLE p360_erp.p360_submissions ADD CONSTRAINT p360_submissions_unique
    UNIQUE (batch_id, code_number, city_id, vertical, cycle_type,
            start_date, end_date, organization_id, store_id, row_type, correction_period);


-- =============================================================================
-- STEP 4 — p360_erp.p360_delta_state
-- Compact mutable state table. One row per business key.
-- Stores cumulative DR/CR so delta computation is O(current_keys) not O(history).
-- =============================================================================

CREATE TABLE p360_erp.p360_delta_state (

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
    store_id                 VARCHAR       NOT NULL,  -- Use COALESCE('') on insert; never NULL here

    -- -------------------------------------------------------------------------
    -- Descriptive fields (latest values)
    -- -------------------------------------------------------------------------
    particulars              VARCHAR,
    city_name                VARCHAR,
    organization_email_id    VARCHAR,
    remarks                  VARCHAR,

    -- -------------------------------------------------------------------------
    -- Cumulative state — what P360 currently believes for this business key
    -- -------------------------------------------------------------------------
    cum_dr                   DECIMAL(18,4) NOT NULL DEFAULT 0,
    cum_cr                   DECIMAL(18,4) NOT NULL DEFAULT 0,

    -- -------------------------------------------------------------------------
    -- Tracking
    -- -------------------------------------------------------------------------
    last_batch_id            VARCHAR       NOT NULL,
    created_at               TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (code_number, city_id, vertical, cycle_type,
                 start_date, end_date, organization_id, store_id)
)
DISTKEY (city_id)
SORTKEY (code_number, city_id, vertical, cycle_type, start_date);

-- =============================================================================
-- How cum_dr / cum_cr are maintained:
--   ORIGINAL row inserted  → add amount to cum_dr / cum_cr
--   RESTATEMENT row        → replace cum_dr / cum_cr with new amount
--   REVERSAL row           → delete this key from state (row gone from source)
-- Delta = staging.DR - state.cum_dr  (positive → CORRECTION, same side)
--         staging.CR - state.cum_cr  (negative → flip to opposite side)
-- =============================================================================


-- =============================================================================
-- STEP 5 — p360_erp.p360_batch_control
-- Atomic batch_id sequence. Prevents duplicate batch_ids under concurrent runs.
-- =============================================================================

CREATE TABLE p360_erp.p360_batch_control (
    control_key          VARCHAR(50)   PRIMARY KEY DEFAULT 'BATCH_SEQ',
    last_batch_date      DATE          NOT NULL,
    last_batch_seq       INT           NOT NULL DEFAULT 0,
    updated_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- Seed the control row (run once after CREATE TABLE)
INSERT INTO p360_erp.p360_batch_control (control_key, last_batch_date, last_batch_seq)
VALUES ('BATCH_SEQ', CURRENT_DATE, 0);

-- =============================================================================
-- Atomic batch_id allocation pattern (used in 04_batch_runner.sql Step 0):
--
--   UPDATE p360_erp.p360_batch_control
--   SET
--       last_batch_seq = CASE
--           WHEN last_batch_date = CURRENT_DATE THEN last_batch_seq + 1
--           ELSE 1
--       END,
--       last_batch_date = CURRENT_DATE,
--       updated_at      = CURRENT_TIMESTAMP
--   WHERE control_key = 'BATCH_SEQ'
--   RETURNING 'B_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || '_' ||
--             LPAD(CAST(last_batch_seq AS VARCHAR), 3, '0') AS batch_id;
-- =============================================================================


-- =============================================================================
-- STEP 6 — p360_erp.p360_batch_audit
-- Batch execution history. Row inserted at PREVIEW, updated at COMMIT.
-- =============================================================================

CREATE TABLE p360_erp.p360_batch_audit (
    audit_id              INT           IDENTITY(1,1) PRIMARY KEY,
    batch_id              VARCHAR(20)   NOT NULL,
    submission_date       DATE          NOT NULL,
    cycle_start           DATE,
    cycle_end             DATE,

    -- Row counts
    staging_rows          INT,          -- COUNT(*) from p360_erp.p360_staging at batch start
    preview_rows          INT,          -- COUNT(*) from p360_batch_preview
    original_rows         INT,          -- COUNT(*) WHERE row_type = 'ORIGINAL'
    reversal_rows         INT,          -- COUNT(*) WHERE row_type = 'REVERSAL'
    restatement_rows      INT,          -- COUNT(*) WHERE row_type = 'RESTATEMENT'
    correction_delta_rows INT,          -- COUNT(*) WHERE row_type = 'CORRECTION_DELTA'
    committed_rows        INT,          -- Actual rows inserted into p360_erp.p360_submissions

    -- Balance check
    total_dr              DECIMAL(18,4),
    total_cr              DECIMAL(18,4),
    is_balanced           BOOLEAN,      -- TRUE if total_dr = total_cr (within 0.01 tolerance)

    -- Timestamps
    started_at            TIMESTAMP,
    committed_at          TIMESTAMP,
    status                VARCHAR(20)   DEFAULT 'PENDING',  -- PENDING / IN_PROGRESS / COMMITTED / FAILED / SKIPPED

    -- Error capture
    error_message         VARCHAR(1000)
)
SORTKEY (submission_date, batch_id);

-- =============================================================================
-- Audit lifecycle (in 04_batch_runner.sql):
--
--   After PREVIEW:  INSERT with status = 'PENDING'
--   Before COMMIT:  UPDATE status = 'IN_PROGRESS'
--   After COMMIT:   UPDATE committed_rows, status = 'COMMITTED'
--   On failure:     UPDATE status = 'FAILED', error_message = <detail>
-- =============================================================================


-- =============================================================================
-- STEP 7 — RECONCILIATION VIEWS
-- Read-only views for Finance / P360 team to query without raw table access.
-- =============================================================================

-- Summary of each batch with row counts and balance status
CREATE OR REPLACE VIEW p360_erp.p360_batch_summary AS
SELECT
    batch_id,
    submission_date,
    cycle_start,
    cycle_end,
    SUM(CASE WHEN row_type = 'ORIGINAL'          THEN 1 ELSE 0 END) AS original_count,
    SUM(CASE WHEN row_type = 'REVERSAL'          THEN 1 ELSE 0 END) AS reversal_count,
    SUM(CASE WHEN row_type = 'RESTATEMENT'       THEN 1 ELSE 0 END) AS restatement_count,
    SUM(CASE WHEN row_type = 'CORRECTION_DELTA'  THEN 1 ELSE 0 END) AS correction_delta_count,
    COUNT(*) AS total_rows,
    SUM(COALESCE(DR, 0)) AS total_dr,
    SUM(COALESCE(CR, 0)) AS total_cr,
    ROUND(SUM(COALESCE(DR, 0)) - SUM(COALESCE(CR, 0)), 4) AS imbalance
FROM p360_erp.p360_submissions
GROUP BY batch_id, submission_date, cycle_start, cycle_end
ORDER BY submission_date DESC, batch_id DESC;


-- Detail of all correction entries — what changed from what
CREATE OR REPLACE VIEW p360_erp.p360_corrections_detail AS
SELECT
    cd.batch_id                    AS correction_batch_id,
    cd.submission_date             AS correction_date,
    cd.code_number,
    cd.city_name,
    cd.vertical,
    cd.cycle_type,
    cd.correction_period           AS original_period,
    cd.start_date                  AS posted_to_period,
    cd.DR                          AS delta_dr,
    cd.CR                          AS delta_cr,
    cd.reference_batch_id          AS original_batch_id,
    orig.DR                        AS original_dr,
    orig.CR                        AS original_cr,
    rest.DR                        AS restated_dr,
    rest.CR                        AS restated_cr
FROM p360_erp.p360_submissions cd
LEFT JOIN p360_erp.p360_submissions orig
  ON orig.batch_id       = cd.reference_batch_id
 AND orig.code_number    = cd.code_number
 AND orig.city_id        = cd.city_id
 AND orig.vertical       = cd.vertical
 AND orig.cycle_type     = cd.cycle_type
 AND orig.start_date     = cd.correction_period
 AND orig.organization_id = cd.organization_id
 AND COALESCE(orig.store_id, '') = COALESCE(cd.store_id, '')
 AND orig.row_type IN ('ORIGINAL', 'RESTATEMENT')
LEFT JOIN p360_erp.p360_submissions rest
  ON rest.batch_id       = cd.batch_id
 AND rest.code_number    = cd.code_number
 AND rest.city_id        = cd.city_id
 AND rest.vertical       = cd.vertical
 AND rest.cycle_type     = cd.cycle_type
 AND rest.start_date     = cd.correction_period
 AND rest.organization_id = cd.organization_id
 AND COALESCE(rest.store_id, '') = COALESCE(cd.store_id, '')
 AND rest.row_type = 'RESTATEMENT'
WHERE cd.row_type = 'CORRECTION_DELTA'
ORDER BY cd.submission_date DESC, cd.batch_id, cd.code_number;


-- All reversed entries (rows deleted from source since original submission)
CREATE OR REPLACE VIEW p360_erp.p360_reversals_history AS
SELECT
    batch_id,
    submission_date,
    code_number,
    city_name,
    vertical,
    cycle_type,
    start_date,
    DR             AS reversal_dr,   -- flipped from original CR
    CR             AS reversal_cr,   -- flipped from original DR
    reference_batch_id AS original_batch_id
FROM p360_erp.p360_submissions
WHERE row_type = 'REVERSAL'
ORDER BY submission_date DESC, batch_id;


-- Period totals — net DR/CR by period for reconciliation
CREATE OR REPLACE VIEW p360_erp.p360_period_totals AS
SELECT
    start_date,
    vertical,
    city_name,
    cycle_type,
    SUM(CASE WHEN row_type IN ('ORIGINAL','RESTATEMENT') THEN COALESCE(DR,0) ELSE 0 END) AS gross_dr,
    SUM(CASE WHEN row_type IN ('ORIGINAL','RESTATEMENT') THEN COALESCE(CR,0) ELSE 0 END) AS gross_cr,
    SUM(CASE WHEN row_type = 'CORRECTION_DELTA'          THEN COALESCE(DR,0) ELSE 0 END) AS correction_dr,
    SUM(CASE WHEN row_type = 'CORRECTION_DELTA'          THEN COALESCE(CR,0) ELSE 0 END) AS correction_cr,
    SUM(CASE WHEN row_type = 'REVERSAL'                  THEN COALESCE(DR,0) ELSE 0 END) AS reversal_dr,
    SUM(CASE WHEN row_type = 'REVERSAL'                  THEN COALESCE(CR,0) ELSE 0 END) AS reversal_cr
FROM p360_erp.p360_submissions
GROUP BY start_date, vertical, city_name, cycle_type
ORDER BY start_date DESC, vertical, city_name;


-- Preview of what the next batch will produce (differences between staging and state)
CREATE OR REPLACE VIEW p360_erp.p360_state_vs_staging_diff AS
SELECT
    COALESCE(s.code_number,  st.code_number)  AS code_number,
    COALESCE(s.city_name,    st.city_name)    AS city_name,
    COALESCE(s.vertical,     st.vertical)     AS vertical,
    COALESCE(s.cycle_type,   st.cycle_type)   AS cycle_type,
    COALESCE(s.start_date,   st.start_date)   AS start_date,
    s.DR  AS staging_dr,
    s.CR  AS staging_cr,
    st.cum_dr AS state_dr,
    st.cum_cr AS state_cr,
    COALESCE(s.DR, 0) - COALESCE(st.cum_dr, 0) AS delta_dr,
    COALESCE(s.CR, 0) - COALESCE(st.cum_cr, 0) AS delta_cr,
    CASE
        WHEN st.code_number IS NULL THEN 'ORIGINAL'
        WHEN s.code_number  IS NULL THEN 'REVERSAL'
        WHEN ROUND(COALESCE(s.DR,0)::NUMERIC,4) <> ROUND(COALESCE(st.cum_dr,0)::NUMERIC,4)
          OR ROUND(COALESCE(s.CR,0)::NUMERIC,4) <> ROUND(COALESCE(st.cum_cr,0)::NUMERIC,4)
            THEN 'CORRECTION'
        ELSE 'UNCHANGED'
    END AS expected_action
FROM p360_erp.p360_staging s
FULL OUTER JOIN p360_erp.p360_delta_state st
  ON s.code_number     = st.code_number
 AND s.city_id         = st.city_id
 AND s.vertical        = st.vertical
 AND s.cycle_type      = st.cycle_type
 AND s.start_date      = st.start_date
 AND COALESCE(s.organization_id, '') = COALESCE(st.organization_id, '')
 AND COALESCE(s.store_id, '')        = COALESCE(st.store_id, '')
WHERE (st.code_number IS NULL OR s.code_number IS NULL
    OR ROUND(COALESCE(s.DR,0)::NUMERIC,4) <> ROUND(COALESCE(st.cum_dr,0)::NUMERIC,4)
    OR ROUND(COALESCE(s.CR,0)::NUMERIC,4) <> ROUND(COALESCE(st.cum_cr,0)::NUMERIC,4));


-- Final P360-facing view: 14 columns only (no internal batch tracking columns).
-- Reads from p360_submissions filtered to P360-facing row types only
-- (ORIGINAL, REVERSAL, CORRECTION_DELTA — excludes RESTATEMENT internal markers).
CREATE OR REPLACE VIEW p360_erp.p360_outbound_entries AS
SELECT
    code_number,
    particulars,
    DR                    AS dr,
    CR                    AS cr,
    city_name,
    cycle_type,
    vertical,
    city_id,
    store_id,
    organization_id,
    organization_email_id,
    start_date,
    end_date,
    remarks
FROM p360_erp.p360_submissions
WHERE row_type IN ('ORIGINAL', 'REVERSAL', 'CORRECTION_DELTA');
