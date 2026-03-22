-- ================================================================
-- P360 AUTOMATION — TEST DEMO
-- ================================================================
-- PURPOSE
--   Exercises the full pipeline end-to-end against synthetic data:
--     1. Staging refresh  (sp_p360_staging_refresh)
--     2. First batch      (all ORIGINALs, auto-approved)
--     3. Data correction  → second batch (RESTATEMENT + CORRECTION_DELTA)
--     4. SKIPPED batch    (how to trigger manual review + force commit)
--
-- NO LAMBDA NEEDED
--   A Python UDF stub replaces f_slack_notify so the pipeline runs
--   without AWS. Messages are printed to Redshift query logs.
--
-- WARNING
--   This script creates schemas and tables that mirror production
--   source names (furbooks_evolve, order_management_systems_evolve,
--   etc.). Run this ONLY in a dev / sandbox Redshift database that
--   does not already contain those schemas with real data.
--
-- PRE-REQUISITE
--   Run these two procedure files before starting (they register the
--   stored procedures used in sections 6–8):
--     automation/sp_p360_staging_refresh.sql
--     automation/sp_p360_batch_runner.sql
--
-- RUN ORDER
--   Execute sections 1 → 9 in order.
--   Each section is safe to re-run on its own after the first pass.
-- ================================================================


-- ================================================================
-- SECTION 1: Slack notifier stub
-- ================================================================
-- Replaces the Lambda-backed external function with a Python UDF
-- that simply prints messages and returns TRUE.
-- Remove or comment this out if you have already registered the
-- real external function from setup_external_function.sql.

CREATE OR REPLACE FUNCTION f_slack_notify(message VARCHAR(4096))
RETURNS BOOLEAN
VOLATILE
AS $$
    print('[DEMO SLACK]', message)
    return True
$$ LANGUAGE plpythonu;

-- Verify
SELECT f_slack_notify('Stub connected — no Lambda required');
-- Expected: TRUE


-- ================================================================
-- SECTION 2: P360 infrastructure tables
-- ================================================================
-- These are the tables the stored procedures read from and write to.
-- In production these are created once by the DDL files.

-- Rebuilt daily by sp_p360_staging_refresh
DROP TABLE IF EXISTS p360_staging CASCADE;
CREATE TABLE p360_staging (
    code_number           VARCHAR(20),
    particulars           VARCHAR(200),
    DR                    DECIMAL(15,4),
    CR                    DECIMAL(15,4),
    city_name             VARCHAR(100),
    cycle_type            VARCHAR(50),
    vertical              VARCHAR(50),
    city_id               INTEGER,
    store_id              VARCHAR(50),
    organization_id       VARCHAR(50),
    organization_email_id VARCHAR(200),
    recognised_date       DATE,
    remarks               VARCHAR(500),
    refreshed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- used by freshness check
);

-- Cumulative snapshot used for delta comparison across batches
DROP TABLE IF EXISTS p360_delta_state CASCADE;
CREATE TABLE p360_delta_state (
    code_number           VARCHAR(20)      NOT NULL,
    city_id               INTEGER          NOT NULL,
    vertical              VARCHAR(50)      NOT NULL,
    cycle_type            VARCHAR(50)      NOT NULL,
    recognised_date       DATE             NOT NULL,
    organization_id       VARCHAR(50)      NOT NULL,
    store_id              VARCHAR(50)      NOT NULL DEFAULT '',
    particulars           VARCHAR(200),
    city_name             VARCHAR(100),
    organization_email_id VARCHAR(200),
    remarks               VARCHAR(500),
    cum_dr                DECIMAL(15,4)    DEFAULT 0,
    cum_cr                DECIMAL(15,4)    DEFAULT 0,
    last_batch_id         VARCHAR(20),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP
);

-- Single-row sequence table that drives batch ID generation
DROP TABLE IF EXISTS p360_batch_control CASCADE;
CREATE TABLE p360_batch_control (
    control_key     VARCHAR(20)   PRIMARY KEY,
    last_batch_seq  INTEGER       DEFAULT 0,
    last_batch_date DATE          DEFAULT '2000-01-01',
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO p360_batch_control (control_key) VALUES ('BATCH_SEQ');

-- One row per batch attempt; tracks status and approval decisions
DROP TABLE IF EXISTS p360_batch_audit CASCADE;
CREATE TABLE p360_batch_audit (
    batch_id              VARCHAR(20),
    submission_date       DATE,
    cycle_start           DATE,
    cycle_end             DATE,
    staging_rows          INTEGER,
    preview_rows          INTEGER,
    original_rows         INTEGER,
    reversal_rows         INTEGER,
    restatement_rows      INTEGER,
    correction_delta_rows INTEGER,
    committed_rows        INTEGER,
    total_dr              NUMERIC(15,4),
    total_cr              NUMERIC(15,4),
    is_balanced           BOOLEAN,
    started_at            TIMESTAMP,
    committed_at          TIMESTAMP,
    status                VARCHAR(20),
    error_message         VARCHAR(2000)
);

-- Permanent ledger of every committed accounting row
DROP TABLE IF EXISTS p360_submissions CASCADE;
CREATE TABLE p360_submissions (
    code_number           VARCHAR(20),
    particulars           VARCHAR(200),
    DR                    DECIMAL(15,4),
    CR                    DECIMAL(15,4),
    city_name             VARCHAR(100),
    cycle_type            VARCHAR(50),
    vertical              VARCHAR(50),
    city_id               INTEGER,
    store_id              VARCHAR(50),
    organization_id       VARCHAR(50),
    organization_email_id VARCHAR(200),
    recognised_date       DATE,
    remarks               VARCHAR(500),
    batch_id              VARCHAR(20),
    submission_date       DATE,
    cycle_start           DATE,
    cycle_end             DATE,
    row_type              VARCHAR(30),
    reference_batch_id    VARCHAR(20),
    correction_period     DATE
);

-- Convenience view for operational monitoring (referenced in SCHEDULE_SETUP.md)
DROP VIEW IF EXISTS p360_batch_summary;
CREATE VIEW p360_batch_summary AS
SELECT
    batch_id,
    submission_date,
    status,
    preview_rows,
    original_rows,
    reversal_rows,
    restatement_rows,
    correction_delta_rows,
    committed_rows,
    total_dr,
    total_cr,
    is_balanced,
    committed_at,
    error_message
FROM p360_batch_audit
ORDER BY started_at DESC;


-- ================================================================
-- SECTION 3: Mock source schemas and tables
-- ================================================================
-- Minimal column sets — only the columns the procedures actually use.
-- Production tables have many more columns; these are sufficient
-- for the demo queries to succeed.

-- ── Cities ──────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS panem_evolve;

DROP TABLE IF EXISTS panem_evolve.cities CASCADE;
CREATE TABLE panem_evolve.cities (
    id   INTEGER,
    name VARCHAR(100)
);

-- ── Fulfilment centres (city → P360 org/store mapping) ──────────
CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.fulfilment_centres CASCADE;
CREATE TABLE analytics.fulfilment_centres (
    city_id               INTEGER,
    email                 VARCHAR(200),
    p360_organisation_id  VARCHAR(50),
    p360_store_id         VARCHAR(50)
);

-- ── Order management ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS order_management_systems_evolve;

DROP TABLE IF EXISTS order_management_systems_evolve.items CASCADE;
CREATE TABLE order_management_systems_evolve.items (
    id              INTEGER,
    vertical        VARCHAR(50),
    state           VARCHAR(30),
    catalog_item_id INTEGER          -- used for SALE vertical product lookup
);

DROP TABLE IF EXISTS order_management_systems_evolve.attachments CASCADE;
CREATE TABLE order_management_systems_evolve.attachments (
    id              INTEGER,
    vertical        VARCHAR(50),
    state           VARCHAR(30),
    catalog_item_id INTEGER
);

DROP TABLE IF EXISTS order_management_systems_evolve.value_added_services CASCADE;
CREATE TABLE order_management_systems_evolve.value_added_services (
    id          INTEGER,
    entity_id   INTEGER,
    entity_type VARCHAR(50),
    vertical    VARCHAR(50),
    state       VARCHAR(30)
);

DROP TABLE IF EXISTS order_management_systems_evolve.penalty CASCADE;
CREATE TABLE order_management_systems_evolve.penalty (
    id       INTEGER,
    vertical VARCHAR(50),
    state    VARCHAR(30)
);

DROP TABLE IF EXISTS order_management_systems_evolve.plans CASCADE;
CREATE TABLE order_management_systems_evolve.plans (
    id    INTEGER,
    state VARCHAR(30)
);

DROP TABLE IF EXISTS order_management_systems_evolve.settlement_products CASCADE;
CREATE TABLE order_management_systems_evolve.settlement_products (
    vertical             VARCHAR(50),
    settlement_id        INTEGER,
    settlement_nature    VARCHAR(50),
    settlement_category  VARCHAR(50),
    product_entity_type  VARCHAR(50),
    product_entity_id    INTEGER,
    from_date            DATE,
    to_date              DATE
);

-- ── Products (SALE vertical lookup) ─────────────────────────────
CREATE SCHEMA IF NOT EXISTS plutus_evolve;

DROP TABLE IF EXISTS plutus_evolve.products CASCADE;
CREATE TABLE plutus_evolve.products (
    id              INTEGER,
    line_of_product VARCHAR(50)
);

-- ── Financial events ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS furbooks_evolve;

DROP TABLE IF EXISTS furbooks_evolve.revenue_recognitions CASCADE;
CREATE TABLE furbooks_evolve.revenue_recognitions (
    id                       INTEGER,
    city_id                  INTEGER,
    accountable_entity_id    INTEGER,
    accountable_entity_type  VARCHAR(50),
    external_reference_type  VARCHAR(50),
    recognition_type         VARCHAR(30),
    state                    VARCHAR(30),
    start_date               DATE,
    recognised_at            TIMESTAMP,
    monetary_components      VARCHAR(4000)     -- JSON string
);

DROP TABLE IF EXISTS furbooks_evolve.credit_notes CASCADE;
CREATE TABLE furbooks_evolve.credit_notes (
    id         INTEGER,
    invoice_id VARCHAR(50),
    issue_date DATE,
    data       VARCHAR(4000)                   -- JSON string
);

DROP TABLE IF EXISTS furbooks_evolve.invoice_cycles CASCADE;
CREATE TABLE furbooks_evolve.invoice_cycles (
    invoice_id              VARCHAR(50),
    city_id                 INTEGER,
    accountable_entity_id   INTEGER,
    accountable_entity_type VARCHAR(50),
    start_date              DATE
);


-- ================================================================
-- SECTION 4: Sample source data
-- ================================================================
-- Three entities are modelled:
--
--   Entity 101  RENTAL item     Bangalore  CGST 9% + SGST 9%   ₹1,000 taxable
--   Entity 201  RENTAL item     Delhi      IGST 18%            ₹2,000 taxable
--   Entity 301  UNLMTD plan     Bangalore  CGST 9% + SGST 9%   ₹500 taxable
--
-- Expected p360_staging row counts after the first refresh:
--   Entity 101 → 4 rows  (Trade Receivables DR, Revenue CR, CGST CR, SGST CR)
--   Entity 201 → 3 rows  (Trade Receivables DR, Revenue CR, IGST CR)
--   Entity 301 → 4 rows  (Trade Receivables DR, Revenue CR, CGST CR, SGST CR)
--   Total       11 rows, balanced  DR = CR = ₹4,130

-- Cities
INSERT INTO panem_evolve.cities VALUES
    (1, 'Bangalore'),
    (2, 'Delhi');

-- Fulfilment centres
INSERT INTO analytics.fulfilment_centres VALUES
    (1, 'bangalore@furlenco.com', 'ORG_BLR_001', 'STORE_BLR_001'),
    (2, 'delhi@furlenco.com',     'ORG_DEL_001', 'STORE_DEL_001');

-- Billable entities
INSERT INTO order_management_systems_evolve.items VALUES
    (101, 'FURLENCO_RENTAL', 'ACTIVE', NULL),
    (201, 'FURLENCO_RENTAL', 'ACTIVE', NULL);

INSERT INTO order_management_systems_evolve.plans VALUES
    (301, 'ACTIVE');

-- Revenue recognitions
-- monetary_components is a JSON string; json_extract_path_text reads it at runtime.
-- Format: taxableAmount, postTaxAmount, discounts[], tax.breakup.{cgst,sgst,igst}.{rate,amount}

-- Entity 101 — intrastate (Bangalore): CGST 9% + SGST 9%
--   taxable=1000, cgst=90, sgst=90, total=1180
INSERT INTO furbooks_evolve.revenue_recognitions VALUES (
    1001, 1, 101, 'ITEM', 'NORMAL', 'ACCRUAL', 'RECOGNIZED',
    '2025-07-01', '2025-07-01 06:30:00',
    '{"taxableAmount":"1000.00","postTaxAmount":"1180.00","discounts":[],'
    '"tax":{"breakup":{"cgst":{"rate":"0.09","amount":"90.00"},'
    '"sgst":{"rate":"0.09","amount":"90.00"},'
    '"igst":{"rate":"0","amount":"0"}}}}'
);

-- Entity 201 — interstate (Delhi): IGST 18%
--   taxable=2000, igst=360, total=2360
INSERT INTO furbooks_evolve.revenue_recognitions VALUES (
    1002, 2, 201, 'ITEM', 'NORMAL', 'ACCRUAL', 'RECOGNIZED',
    '2025-07-01', '2025-07-01 06:30:00',
    '{"taxableAmount":"2000.00","postTaxAmount":"2360.00","discounts":[],'
    '"tax":{"breakup":{"cgst":{"rate":"0","amount":"0"},'
    '"sgst":{"rate":"0","amount":"0"},'
    '"igst":{"rate":"0.18","amount":"360.00"}}}}'
);

-- Entity 301 — UNLMTD plan, Bangalore: CGST 9% + SGST 9%
--   taxable=500, cgst=45, sgst=45, total=590
INSERT INTO furbooks_evolve.revenue_recognitions VALUES (
    1003, 1, 301, 'PLAN', 'NORMAL', 'ACCRUAL', 'RECOGNIZED',
    '2025-07-01', '2025-07-01 06:30:00',
    '{"taxableAmount":"500.00","postTaxAmount":"590.00","discounts":[],'
    '"tax":{"breakup":{"cgst":{"rate":"0.09","amount":"45.00"},'
    '"sgst":{"rate":"0.09","amount":"45.00"},'
    '"igst":{"rate":"0","amount":"0"}}}}'
);


-- ================================================================
-- SECTION 5: Confirm stored procedures exist
-- ================================================================
-- If this returns 0 rows, run the procedure files first:
--   automation/sp_p360_staging_refresh.sql
--   automation/sp_p360_batch_runner.sql

SELECT routine_name
FROM information_schema.routines
WHERE routine_schema = 'public'
  AND routine_type   = 'PROCEDURE'
  AND routine_name LIKE 'sp_p360%';
-- Expected: sp_p360_staging_refresh, sp_p360_batch_runner, sp_p360_batch_force_commit


-- ================================================================
-- SECTION 6: Run 1 — Daily staging refresh
-- ================================================================

CALL sp_p360_staging_refresh();
-- Expected Slack stub output: "P360 staging refresh complete — 11 rows loaded on <today>"

-- Inspect staging rows
SELECT
    code_number,
    particulars,
    DR,
    CR,
    city_name,
    vertical,
    cycle_type,
    recognised_date
FROM p360_staging
ORDER BY city_name, vertical, DR DESC NULLS LAST;
-- Expected: 11 rows
-- Bangalore RENTAL : Trade Receivables DR=1180, Revenue CR=1000, CGST CR=90, SGST CR=90
-- Bangalore UNLMTD : Trade Receivables DR=590,  Revenue CR=500,  CGST CR=45, SGST CR=45
-- Delhi     RENTAL : Trade Receivables DR=2360, Revenue CR=2000, IGST CR=360

-- Freshness check (refreshed_at must be today)
SELECT
    COUNT(*)         AS total_rows,
    MAX(refreshed_at)::DATE AS refreshed_on
FROM p360_staging;


-- ================================================================
-- SECTION 7: Run 2 — First batch (all ORIGINAL rows)
-- ================================================================
-- All 11 staging rows are new — nothing yet in delta state.
-- Batch ID will be B_<today>_001.
-- All three approval checks should pass → status = COMMITTED.

CALL sp_p360_batch_runner();
-- Expected Slack stub output: "*P360 Batch B_<today>_001 committed*"

-- Audit record
SELECT
    batch_id,
    status,
    preview_rows,
    original_rows,
    restatement_rows,
    correction_delta_rows,
    total_dr,
    total_cr,
    is_balanced
FROM p360_batch_audit
ORDER BY started_at DESC
LIMIT 5;
-- Expected: status=COMMITTED, original_rows=11, total_dr=total_cr=4130, is_balanced=TRUE

-- Committed submissions
SELECT
    batch_id,
    row_type,
    code_number,
    particulars,
    DR,
    CR,
    city_name,
    vertical
FROM p360_submissions
ORDER BY batch_id, city_name, vertical, DR DESC NULLS LAST;
-- Expected: 11 rows, all row_type=ORIGINAL

-- Delta state (snapshot of what was just committed)
SELECT
    code_number,
    city_name,
    vertical,
    cum_dr,
    cum_cr,
    last_batch_id
FROM p360_delta_state
ORDER BY city_name, vertical, code_number;
-- Expected: 11 rows, cum_dr/cum_cr matching the staging amounts above


-- ================================================================
-- SECTION 8: Run 3 — Correction batch
-- ================================================================
-- Simulate a data revision: Entity 101's taxable amount increases
-- from ₹1,000 to ₹1,200.
--
-- New amounts:
--   taxable=1200, cgst=108, sgst=108, total=1416
--
-- The next batch will compare staging (new values) against delta
-- state (old values) and produce for the 4 changed rows:
--   RESTATEMENT      — silent state marker, not sent to P360
--   CORRECTION_DELTA — net difference, sent to P360
--
-- Expected CORRECTION_DELTA balance:
--   DR  = 1416 - 1180 = 236   (Trade Receivables increase)
--   CR  = (1200-1000) + (108-90) + (108-90) = 200 + 18 + 18 = 236
--   Balanced → auto-approved

UPDATE furbooks_evolve.revenue_recognitions
SET monetary_components =
    '{"taxableAmount":"1200.00","postTaxAmount":"1416.00","discounts":[],'
    '"tax":{"breakup":{"cgst":{"rate":"0.09","amount":"108.00"},'
    '"sgst":{"rate":"0.09","amount":"108.00"},'
    '"igst":{"rate":"0","amount":"0"}}}}'
WHERE id = 1001;

-- Re-run staging to pick up the revised amounts
CALL sp_p360_staging_refresh();

-- Run the second batch
CALL sp_p360_batch_runner();
-- Expected Slack stub output: "*P360 Batch B_<today>_002 committed*"

-- Audit: two batches now
SELECT
    batch_id,
    status,
    preview_rows,
    original_rows,
    restatement_rows,
    correction_delta_rows,
    total_dr,
    total_cr,
    is_balanced
FROM p360_batch_audit
ORDER BY started_at DESC
LIMIT 5;
-- Batch 002: preview_rows=8, restatement_rows=4, correction_delta_rows=4,
--            total_dr=total_cr=236, is_balanced=TRUE, status=COMMITTED

-- Submissions: batch 002 should have RESTATEMENT + CORRECTION_DELTA rows
SELECT
    batch_id,
    row_type,
    code_number,
    particulars,
    DR,
    CR
FROM p360_submissions
WHERE batch_id = (
    SELECT batch_id FROM p360_batch_audit
    ORDER BY started_at DESC LIMIT 1
)
ORDER BY row_type, code_number;

-- Updated delta state (Entity 101 values should now reflect the new amounts)
SELECT
    code_number,
    city_name,
    vertical,
    cum_dr,
    cum_cr,
    last_batch_id
FROM p360_delta_state
WHERE city_name = 'Bangalore' AND vertical = 'FURLENCO_RENTAL'
ORDER BY code_number;
-- Expected: cum_dr updated to 1416 for Trade Receivables row


-- ================================================================
-- SECTION 9: SKIPPED batch demo (manual review path)
-- ================================================================
-- How to trigger a SKIPPED result:
--
--   Option A — Force a row-count variance failure (safest):
--     1. Open sp_p360_batch_runner.sql
--     2. Change:  ROW_COUNT_VARIANCE_THRESHOLD NUMERIC := 5.0;
--        To:      ROW_COUNT_VARIANCE_THRESHOLD NUMERIC := 0.0;
--     3. Re-run sp_p360_batch_runner.sql to recreate the procedure
--     4. Run:  CALL sp_p360_batch_runner();
--     5. Check: p360_batch_audit.status = 'SKIPPED'
--               Slack message: "MANUAL REVIEW REQUIRED"
--
--   Option B — Manually inspect a SKIPPED batch and force-commit it:
--     Replace the batch_id below with the actual SKIPPED batch_id:

-- CALL sp_p360_batch_force_commit('B_YYYYMMDD_003');
-- Expected Slack stub output: "*P360 Batch B_<today>_003 committed (manually approved)*"

-- After force commit, verify:
SELECT
    batch_id,
    status,
    error_message         -- shows "Manually approved by <user>"
FROM p360_batch_audit
ORDER BY started_at DESC
LIMIT 5;


-- ================================================================
-- SECTION 10: Monitoring queries (from SCHEDULE_SETUP.md §7)
-- ================================================================

-- Full audit history
SELECT
    batch_id,
    submission_date,
    status,
    preview_rows,
    is_balanced,
    total_dr,
    total_cr,
    error_message
FROM p360_batch_audit
ORDER BY started_at DESC
LIMIT 20;

-- Batch summary view
SELECT * FROM p360_batch_summary LIMIT 5;

-- All committed submissions for a specific batch
-- SELECT * FROM p360_submissions WHERE batch_id = 'B_YYYYMMDD_001';


-- ================================================================
-- SECTION 11: Cleanup — run to reset after the demo
-- ================================================================
-- Removes all demo objects. Run this when you are done testing.
-- The stored procedures (sp_p360_*) and f_slack_notify stub are
-- NOT dropped here — re-run this script from SECTION 1 to reset them.

DROP VIEW  IF EXISTS p360_batch_summary;
DROP TABLE IF EXISTS p360_submissions         CASCADE;
DROP TABLE IF EXISTS p360_delta_state         CASCADE;
DROP TABLE IF EXISTS p360_batch_audit         CASCADE;
DROP TABLE IF EXISTS p360_batch_control       CASCADE;
DROP TABLE IF EXISTS p360_staging             CASCADE;

DROP TABLE IF EXISTS furbooks_evolve.revenue_recognitions CASCADE;
DROP TABLE IF EXISTS furbooks_evolve.credit_notes         CASCADE;
DROP TABLE IF EXISTS furbooks_evolve.invoice_cycles       CASCADE;
DROP SCHEMA IF EXISTS furbooks_evolve;

DROP TABLE IF EXISTS order_management_systems_evolve.items                CASCADE;
DROP TABLE IF EXISTS order_management_systems_evolve.attachments          CASCADE;
DROP TABLE IF EXISTS order_management_systems_evolve.value_added_services CASCADE;
DROP TABLE IF EXISTS order_management_systems_evolve.penalty              CASCADE;
DROP TABLE IF EXISTS order_management_systems_evolve.plans                CASCADE;
DROP TABLE IF EXISTS order_management_systems_evolve.settlement_products  CASCADE;
DROP SCHEMA IF EXISTS order_management_systems_evolve;

DROP TABLE IF EXISTS plutus_evolve.products CASCADE;
DROP SCHEMA IF EXISTS plutus_evolve;

DROP TABLE IF EXISTS panem_evolve.cities CASCADE;
DROP SCHEMA IF EXISTS panem_evolve;

DROP TABLE IF EXISTS analytics.fulfilment_centres CASCADE;
DROP SCHEMA IF EXISTS analytics;
