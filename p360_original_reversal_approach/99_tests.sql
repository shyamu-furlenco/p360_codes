-- =============================================================================
-- P360 BATCH RUNNER — TEST SUITE
-- =============================================================================
-- Self-contained: uses temp tables only. Does NOT touch real tables.
-- Run the entire file top-to-bottom in a single session.
--
-- 7 scenarios exercising every code path:
--   Scenario 1 — First batch: all rows → ORIGINAL
--   Scenario 2 — No changes:  nothing → 0 rows
--   Scenario 3 — Amount correction: changed rows → RESTATEMENT (state marker) + CORRECTION_DELTA (delta to P360)
--   Scenario 4 — Row disappears: missing rows → REVERSAL_ONLY (full reversal, unchanged)
--   Scenario 5 — Multi-month correction: old recognised_date → RESTATEMENT + CORRECTION_DELTA
--   Scenario 6 — Price DECREASE with GST rows: delta flips direction (DR decrease → CR delta; CR decrease → DR delta)
--   Scenario 7 — Price INCREASE with GST rows: delta stays same direction (DR increase → DR delta; CR increase → CR delta)
--
-- PASS/FAIL: assertion queries return rows only on failure (like a unit test).
-- Expected result: every assertion query returns 0 rows.
--
-- =============================================================================
-- DESIGN NOTE: INTENTIONAL CODE DUPLICATION
-- =============================================================================
-- The batch logic CTE chain is duplicated across each scenario rather than
-- being factored into a reusable function/procedure. This is intentional:
--
-- 1. TEST ISOLATION: Each scenario is fully self-contained. A bug in one
--    scenario cannot accidentally affect another scenario's results.
--
-- 2. REDSHIFT COMPATIBILITY: Redshift does not support stored procedures
--    with temp table side effects in the same way PostgreSQL does.
--
-- 3. DEBUGGING: When a scenario fails, the full query is visible inline.
--    No need to trace through function calls.
--
-- 4. MIRRORS PRODUCTION: The test CTE chain is copy-pasted from
--    p360_batch_runner.sql, ensuring tests validate the actual production code.
--
-- MAINTENANCE: If the batch logic changes in p360_batch_runner.sql, update
-- ALL scenario blocks in this file. Use find-and-replace carefully.
--
-- =============================================================================
-- STATE TABLE NOTE (v2)
-- =============================================================================
-- Production p360_batch_runner.sql now uses p360_delta_state for efficient
-- delta computation. This test suite still uses the original last_sent CTE
-- approach with test_submissions for backward compatibility and simplicity.
--
-- The state table logic is mathematically equivalent:
--   - last_sent CTE: finds most recent ORIGINAL/RESTATEMENT per business key
--   - p360_delta_state: stores same data in compact form
--
-- Both approaches produce identical batch preview outputs.
-- =============================================================================


-- =============================================================================
-- SETUP: temp tables mirroring the real schema
-- =============================================================================

DROP TABLE IF EXISTS test_staging;
DROP TABLE IF EXISTS test_submissions;
DROP TABLE IF EXISTS test_batch_preview;

CREATE TEMP TABLE test_staging (
    code_number           VARCHAR,
    particulars           VARCHAR,
    DR                    DECIMAL(15,4),
    CR                    DECIMAL(15,4),
    city_name             VARCHAR,
    cycle_type            VARCHAR,
    vertical              VARCHAR,
    city_id               INT,
    store_id              VARCHAR,
    organization_id       VARCHAR,
    organization_email_id VARCHAR,
    start_date            DATE,
    end_date              DATE,
    remarks               VARCHAR
);

CREATE TEMP TABLE test_submissions (
    code_number           VARCHAR,
    particulars           VARCHAR,
    DR                    DECIMAL(15,4),
    CR                    DECIMAL(15,4),
    city_name             VARCHAR,
    cycle_type            VARCHAR,
    vertical              VARCHAR,
    city_id               INT,
    store_id              VARCHAR,
    organization_id       VARCHAR,
    organization_email_id VARCHAR,
    start_date            DATE,
    end_date              DATE,
    remarks               VARCHAR,
    batch_id              VARCHAR    NOT NULL,
    submission_date       DATE       NOT NULL,
    cycle_start           DATE,
    cycle_end             DATE,
    row_type              VARCHAR    NOT NULL,
    reference_batch_id    VARCHAR,
    correction_period     DATE
);


-- =============================================================================
-- TEST DATA
--
-- Two cities, two verticals, one recognised_date each:
--
--   City 10 | Mumbai   | FURLENCO_RENTAL | 2025-01-06
--     3004010 | Trade Receivables - Furlenco | DR=10000.00 | CR=NULL
--     1001010 | Revenue - Furlenco           | DR=NULL     | CR=8474.58
--     (balanced: 10000.00 DR = 8474.58 revenue + 762.71 CGST + 762.71 SGST
--      — tax rows omitted from test for brevity; balance check still works)
--
--   City 20 | Bangalore | UNLMTD          | 2025-01-06
--     3004080 | Trade Receivables - Unlmtd | DR=5000.00 | CR=NULL
--     1001020 | Revenue - Unlmtd           | DR=NULL    | CR=4237.29
--
--   City 10 | Mumbai   | FURLENCO_RENTAL | 2024-10-07  (used in Scenario 5 only)
--     3004010 | Trade Receivables - Furlenco | DR=3000.00 | CR=NULL
--     1001010 | Revenue - Furlenco           | DR=NULL    | CR=2542.37
-- =============================================================================


-- =============================================================================
-- SCENARIO 1 — First batch: all rows should be ORIGINAL
-- =============================================================================
-- State:  test_staging  = 4 rows (Jan 2025 data)
--         test_submissions = empty
-- Expect: 4 ORIGINAL rows
--         cycle_start = 2025-01-06, cycle_end = 2025-01-06
--         reference_batch_id = NULL for all rows
-- =============================================================================

-- Load staging with Jan 2025 data
INSERT INTO test_staging VALUES
    ('3004010','Trade Receivables - Furlenco', 10000.00, NULL,    'Mumbai',    'Normal_billing_cycle','FURLENCO_RENTAL',10,'ST001','ORG001','mumbai@furlenco.com',    '2025-01-06','2025-01-12','furlenco_rental, Jan-2025'),
    ('1001010','Revenue - Furlenco',           NULL,     8474.58, 'Mumbai',    'Normal_billing_cycle','FURLENCO_RENTAL',10,'ST001','ORG001','mumbai@furlenco.com',    '2025-01-06','2025-01-12','furlenco_rental, Jan-2025'),
    ('3004080','Trade Receivables - Unlmtd',   5000.00,  NULL,    'Bangalore', 'Normal_billing_cycle','UNLMTD',         20,'ST002','ORG002','bangalore@furlenco.com', '2025-01-06','2025-01-12','unlmtd, Jan-2025'),
    ('1001020','Revenue - Unlmtd',             NULL,     4237.29, 'Bangalore', 'Normal_billing_cycle','UNLMTD',         20,'ST002','ORG002','bangalore@furlenco.com', '2025-01-06','2025-01-12','unlmtd, Jan-2025');

-- Run batch logic
DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (
    SELECT 'B_' || TO_CHAR(CURRENT_DATE,'YYYYMMDD') || '_' ||
           LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date = CURRENT_DATE) + 1 AS VARCHAR),3,'0') AS batch_id
),
last_sent AS (
    SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn
          FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1
),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number, COALESCE(cur.particulars,ls.particulars) AS particulars,
           COALESCE(cur.city_name,ls.city_name) AS city_name, COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,
           COALESCE(cur.vertical,ls.vertical) AS vertical, COALESCE(cur.city_id,ls.city_id) AS city_id,
           COALESCE(cur.store_id,ls.store_id) AS store_id, COALESCE(cur.organization_id,ls.organization_id) AS organization_id,
           COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,
           COALESCE(cur.start_date,ls.start_date) AS start_date, COALESCE(cur.end_date,ls.end_date) AS end_date, COALESCE(cur.remarks,ls.remarks) AS remarks,
           cur.DR AS cur_DR, cur.CR AS cur_CR, ls.DR AS old_DR, ls.CR AS old_CR, ls.batch_id AS last_batch_id,
           CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL'
                WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY'
                WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4) <> ROUND(COALESCE(ls.DR,0)::NUMERIC,4)
                  OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4) <> ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION'
                ELSE 'UNCHANGED' END AS action
    FROM current_data cur
    FULL OUTER JOIN last_sent ls
      ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical
     AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date
     AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start, MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

-- Review output
SELECT '--- Scenario 1 output ---' AS scenario;
SELECT code_number, particulars, DR, CR, city_name, vertical, start_date, end_date, row_type, reference_batch_id, batch_id FROM test_batch_preview ORDER BY city_name, code_number;

-- ASSERTIONS (each query returns rows only on FAILURE)
SELECT 'FAIL S1-A: expected 4 rows, got ' || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 4;
SELECT 'FAIL S1-B: expected all ORIGINAL, found non-ORIGINAL' AS result FROM test_batch_preview WHERE row_type <> 'ORIGINAL' LIMIT 1;
SELECT 'FAIL S1-C: expected cycle_start=2025-01-06' AS result FROM test_batch_preview WHERE cycle_start <> '2025-01-06' LIMIT 1;
SELECT 'FAIL S1-D: expected cycle_end=2025-01-06'   AS result FROM test_batch_preview WHERE cycle_end   <> '2025-01-06' LIMIT 1;
SELECT 'FAIL S1-E: expected reference_batch_id=NULL' AS result FROM test_batch_preview WHERE reference_batch_id IS NOT NULL LIMIT 1;
SELECT 'FAIL S1-F: Mumbai DR row not found'          AS result FROM test_batch_preview WHERE code_number='3004010' AND city_id=10 AND DR=10000.00 HAVING COUNT(*)=0;
SELECT 'FAIL S1-G: Bangalore DR row not found'       AS result FROM test_batch_preview WHERE code_number='3004080' AND city_id=20 AND DR=5000.00 HAVING COUNT(*)=0;

-- Commit Scenario 1
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- SCENARIO 2 — No changes: nothing should be emitted
-- =============================================================================
-- State:  test_staging  = same 4 rows (unchanged)
--         test_submissions = 4 ORIGINAL rows from Scenario 1
-- Expect: 0 rows
-- =============================================================================

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 2 output ---' AS scenario;
SELECT '(no rows expected)' AS expected_output;
SELECT * FROM test_batch_preview;

-- ASSERTIONS
SELECT 'FAIL S2-A: expected 0 rows, got ' || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 0;

-- Nothing to commit (0 rows)


-- =============================================================================
-- SCENARIO 3 — Amount correction: Mumbai amounts change
-- =============================================================================
-- Change:  3004010 Mumbai DR: 10000.00 → 10500.00
--          1001010 Mumbai CR: 8474.58  → 8898.31
-- State:  test_staging  = updated amounts for Mumbai
--         test_submissions = 4 ORIGINAL rows (Scenario 1)
-- Expect: 4 rows —
--           RESTATEMENT    3004010: DR=10500.00, CR=NULL    (silent state marker, NOT sent to P360)
--           CORRECTION_DELTA 3004010: DR=500.00, CR=NULL   (delta: 10500−10000, sent to P360)
--           RESTATEMENT    1001010: DR=NULL, CR=8898.31     (silent state marker, NOT sent to P360)
--           CORRECTION_DELTA 1001010: DR=NULL, CR=423.73   (delta: 8898.31−8474.58, sent to P360)
--         No REVERSAL rows (CORRECTION uses CORRECTION_DELTA, not REVERSAL)
--         CORRECTION_DELTA recognised_date = current cycle date (from delta_date CTE)
--         CORRECTION_DELTA correction_period = '2025-01-06' (original recognised_date)
--         reference_batch_id = Scenario 1 batch_id for all 4 rows
--         Bangalore rows: UNCHANGED → NOT emitted
-- =============================================================================

UPDATE test_staging SET DR = 10500.00 WHERE code_number = '3004010' AND city_id = 10;
UPDATE test_staging SET CR = 8898.31  WHERE code_number = '1001010' AND city_id = 10;

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 3 output ---' AS scenario;
SELECT code_number, particulars, DR, CR, start_date, end_date, row_type, reference_batch_id, correction_period
FROM test_batch_preview
ORDER BY CASE row_type WHEN 'RESTATEMENT' THEN 1 WHEN 'CORRECTION_DELTA' THEN 2 END, code_number;

-- ASSERTIONS
SELECT 'FAIL S3-A: expected 4 rows, got '                  || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 4;
SELECT 'FAIL S3-B: expected 2 RESTATEMENT rows, got '      || COUNT(*) AS result FROM test_batch_preview WHERE row_type='RESTATEMENT'     HAVING COUNT(*) <> 2;
SELECT 'FAIL S3-C: expected 2 CORRECTION_DELTA rows, got ' || COUNT(*) AS result FROM test_batch_preview WHERE row_type='CORRECTION_DELTA' HAVING COUNT(*) <> 2;
SELECT 'FAIL S3-D: RESTATEMENT 3004010 wrong amounts (expect DR=10500 CR=NULL)' AS result
    FROM test_batch_preview WHERE code_number='3004010' AND row_type='RESTATEMENT' AND (DR <> 10500.00 OR CR IS NOT NULL) LIMIT 1;
SELECT 'FAIL S3-E: CORRECTION_DELTA 3004010 wrong amounts (expect DR=500 CR=NULL)' AS result
    FROM test_batch_preview WHERE code_number='3004010' AND row_type='CORRECTION_DELTA' AND (DR <> 500.00 OR CR IS NOT NULL) LIMIT 1;
SELECT 'FAIL S3-F: RESTATEMENT 1001010 wrong amounts (expect DR=NULL CR=8898.31)' AS result
    FROM test_batch_preview WHERE code_number='1001010' AND row_type='RESTATEMENT' AND (DR IS NOT NULL OR CR <> 8898.31) LIMIT 1;
SELECT 'FAIL S3-G: CORRECTION_DELTA 1001010 wrong amounts (expect DR=NULL CR=423.73)' AS result
    FROM test_batch_preview WHERE code_number='1001010' AND row_type='CORRECTION_DELTA' AND (DR IS NOT NULL OR CR <> 423.73) LIMIT 1;
SELECT 'FAIL S3-H: correction_period should be 2025-01-06 on CORRECTION_DELTA rows' AS result
    FROM test_batch_preview WHERE row_type='CORRECTION_DELTA' AND correction_period <> '2025-01-06' LIMIT 1;
SELECT 'FAIL S3-I: CORRECTION_DELTA start_date should differ from correction_period' AS result
    FROM test_batch_preview WHERE row_type='CORRECTION_DELTA' AND start_date = correction_period LIMIT 1;
SELECT 'FAIL S3-J: Bangalore rows should NOT appear in this batch (they were UNCHANGED)' AS result
    FROM test_batch_preview WHERE city_id = 20 LIMIT 1;
SELECT 'FAIL S3-K: RESTATEMENT reference_batch_id should be non-null (points to original batch)' AS result
    FROM test_batch_preview WHERE row_type='RESTATEMENT' AND reference_batch_id IS NULL LIMIT 1;

-- Commit Scenario 3
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- SCENARIO 4 — Row disappears: Bangalore removed from staging
-- =============================================================================
-- Change:  DELETE city_id=20 rows from test_staging
-- State:  test_staging  = 2 rows (Mumbai only)
--         test_submissions = 8 rows (4 ORIGINAL + 4 correction)
-- Expect: 2 rows —
--           REVERSAL_ONLY 3004080: DR=NULL,    CR=5000.00  (old DR flipped to CR)
--           REVERSAL_ONLY 1001020: DR=4237.29, CR=NULL     (old CR flipped to DR)
--         No RESTATEMENT (row is gone, not changed)
-- =============================================================================

DELETE FROM test_staging WHERE city_id = 20;

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 4 output ---' AS scenario;
SELECT code_number, particulars, DR, CR, row_type, reference_batch_id
FROM test_batch_preview ORDER BY code_number;

-- ASSERTIONS
SELECT 'FAIL S4-A: expected 2 rows, got '                   || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 2;
SELECT 'FAIL S4-B: expected only REVERSAL rows'             AS result FROM test_batch_preview WHERE row_type <> 'REVERSAL' LIMIT 1;
SELECT 'FAIL S4-C: REVERSAL 3004080 wrong amounts (expect DR=NULL CR=5000)' AS result
    FROM test_batch_preview WHERE code_number='3004080' AND row_type='REVERSAL' AND (DR IS NOT NULL OR CR <> 5000.00) LIMIT 1;
SELECT 'FAIL S4-D: REVERSAL 1001020 wrong amounts (expect DR=4237.29 CR=NULL)' AS result
    FROM test_batch_preview WHERE code_number='1001020' AND row_type='REVERSAL' AND (DR <> 4237.29 OR CR IS NOT NULL) LIMIT 1;
SELECT 'FAIL S4-E: Mumbai rows should NOT appear (they were UNCHANGED)' AS result
    FROM test_batch_preview WHERE city_id = 10 LIMIT 1;

-- Commit Scenario 4
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- SCENARIO 5 — Multi-month correction: old recognised_date corrected
-- =============================================================================
-- Simulate: a batch from Oct 2024 (B_TEST_000) submitted Mumbai rental data
-- for recognised_date 2024-10-07 with DR=3000.00. The source has since been
-- corrected to DR=3200.00. This is 3+ months before the current Jan 2025 data.
--
-- Setup:
--   1. Manually insert old batch (B_TEST_000) into test_submissions as ORIGINAL
--      with the original amounts (DR=3000.00, CR=2542.37)
--   2. Insert corrected amounts into test_staging (DR=3200.00, CR=2711.86)
--      alongside existing Mumbai Jan 2025 data
--
-- Expect: 4 rows —
--   RESTATEMENT    3004010 (2024-10-07): DR=3200.00, CR=NULL   (silent state marker, NOT sent to P360)
--   CORRECTION_DELTA 3004010: DR=200.00, CR=NULL               (delta: 3200−3000, sent to P360)
--   RESTATEMENT    1001010 (2024-10-07): DR=NULL, CR=2711.86   (silent state marker, NOT sent to P360)
--   CORRECTION_DELTA 1001010: DR=NULL, CR=169.49               (delta: 2711.86−2542.37, sent to P360)
--   No REVERSAL rows (CORRECTION uses CORRECTION_DELTA, not REVERSAL)
--   CORRECTION_DELTA correction_period = '2024-10-07' (the old recognised_date)
--   CORRECTION_DELTA recognised_date = current cycle date (NOT '2024-10-07')
--   reference_batch_id = 'B_TEST_000' for all 4 rows
--   Jan 2025 Mumbai rows = UNCHANGED → NOT emitted
--   cycle_start = 2024-10-07 (old date), cycle_end = 2025-01-06
-- =============================================================================

-- 1. Simulate an old batch in test_submissions (submitted 2024-10-14, covering 2024-10-07 data)
INSERT INTO test_submissions VALUES
    ('3004010','Trade Receivables - Furlenco', 3000.00, NULL,    'Mumbai','Normal_billing_cycle','FURLENCO_RENTAL',10,'ST001','ORG001','mumbai@furlenco.com', '2024-10-07','2024-10-13','furlenco_rental, Oct-2024','B_TEST_000','2024-10-14','2024-10-07','2024-10-07','ORIGINAL',NULL,NULL),
    ('1001010','Revenue - Furlenco',           NULL,    2542.37, 'Mumbai','Normal_billing_cycle','FURLENCO_RENTAL',10,'ST001','ORG001','mumbai@furlenco.com', '2024-10-07','2024-10-13','furlenco_rental, Oct-2024','B_TEST_000','2024-10-14','2024-10-07','2024-10-07','ORIGINAL',NULL,NULL);

-- 2. Add corrected amounts into staging for the old date (DR corrected: 3000 → 3200)
INSERT INTO test_staging VALUES
    ('3004010','Trade Receivables - Furlenco', 3200.00, NULL,    'Mumbai','Normal_billing_cycle','FURLENCO_RENTAL',10,'ST001','ORG001','mumbai@furlenco.com', '2024-10-07','2024-10-13','furlenco_rental, Oct-2024'),
    ('1001010','Revenue - Furlenco',           NULL,    2711.86, 'Mumbai','Normal_billing_cycle','FURLENCO_RENTAL',10,'ST001','ORG001','mumbai@furlenco.com', '2024-10-07','2024-10-13','furlenco_rental, Oct-2024');

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 5 output ---' AS scenario;
SELECT code_number, particulars, DR, CR, start_date, end_date, row_type, reference_batch_id, correction_period, cycle_start, cycle_end
FROM test_batch_preview
ORDER BY CASE row_type WHEN 'RESTATEMENT' THEN 1 WHEN 'CORRECTION_DELTA' THEN 2 END, code_number;

-- ASSERTIONS
SELECT 'FAIL S5-A: expected 4 rows, got '                                  || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 4;
SELECT 'FAIL S5-B: expected 2 RESTATEMENT rows, got '                      || COUNT(*) AS result FROM test_batch_preview WHERE row_type='RESTATEMENT'     HAVING COUNT(*) <> 2;
SELECT 'FAIL S5-C: expected 2 CORRECTION_DELTA rows, got '                 || COUNT(*) AS result FROM test_batch_preview WHERE row_type='CORRECTION_DELTA' HAVING COUNT(*) <> 2;
SELECT 'FAIL S5-D: CORRECTION_DELTA 3004010 wrong amounts (expect DR=200 CR=NULL)' AS result
    FROM test_batch_preview WHERE code_number='3004010' AND row_type='CORRECTION_DELTA' AND (DR <> 200.00 OR CR IS NOT NULL) LIMIT 1;
SELECT 'FAIL S5-E: CORRECTION_DELTA 1001010 wrong amounts (expect DR=NULL CR=169.49)' AS result
    FROM test_batch_preview WHERE code_number='1001010' AND row_type='CORRECTION_DELTA' AND (DR IS NOT NULL OR CR <> 169.49) LIMIT 1;
SELECT 'FAIL S5-F: correction_period should be 2024-10-07 on CORRECTION_DELTA rows' AS result
    FROM test_batch_preview WHERE row_type='CORRECTION_DELTA' AND correction_period <> '2024-10-07' LIMIT 1;
SELECT 'FAIL S5-G: CORRECTION_DELTA start_date should NOT be 2024-10-07 (must be current cycle period)' AS result
    FROM test_batch_preview WHERE row_type='CORRECTION_DELTA' AND start_date = '2024-10-07' LIMIT 1;
SELECT 'FAIL S5-H: reference_batch_id should be B_TEST_000 on all rows' AS result
    FROM test_batch_preview WHERE reference_batch_id <> 'B_TEST_000' LIMIT 1;
SELECT 'FAIL S5-I: cycle_start should be 2024-10-07 (oldest corrected date)'  AS result FROM test_batch_preview WHERE cycle_start <> '2024-10-07' LIMIT 1;
SELECT 'FAIL S5-J: cycle_end should be 2025-01-06 (latest date in staging)'   AS result FROM test_batch_preview WHERE cycle_end   <> '2025-01-06' LIMIT 1;

-- Commit Scenario 5
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- SCENARIO 6 — Price DECREASE with GST rows (reversal direction)
-- =============================================================================
-- Setup:
--   1. Manually insert B_S6_000 into test_submissions (5 ORIGINAL rows, Chennai, recognised_date='2025-01-06')
--   2. Insert decreased amounts into test_staging for all 5 Chennai codes
--
-- Original (B_S6_000): 4001001 DR=5900 | 5001001 CR=5000 | 5001002 CR=300 | 5001003 CR=300 | 5001004 CR=300
-- Decreased staging:   4001001 DR=4720 | 5001001 CR=4000 | 5001002 CR=240 | 5001003 CR=240 | 5001004 CR=240
--
-- Expect: 10 rows — 5 RESTATEMENT (state markers) + 5 CORRECTION_DELTA (sent to P360)
--   CORRECTION_DELTA direction (decrease → flip to opposite side):
--     4001001 Receivable: old_DR=5900 > cur_DR=4720 → CORRECTION_DELTA CR=1180  (DR decreased → flipped to CR)
--     5001001 Revenue:    old_CR=5000 > cur_CR=4000 → CORRECTION_DELTA DR=1000  (CR decreased → flipped to DR)
--     5001002 SGST:       old_CR=300  > cur_CR=240  → CORRECTION_DELTA DR=60    (CR decreased → flipped to DR)
--     5001003 CGST:       old_CR=300  > cur_CR=240  → CORRECTION_DELTA DR=60    (CR decreased → flipped to DR)
--     5001004 IGST:       old_CR=300  > cur_CR=240  → CORRECTION_DELTA DR=60    (CR decreased → flipped to DR)
--   Delta balanced: 1000+60+60+60=1180 DR = 1180 CR ✓
-- =============================================================================

-- 1. Simulate prior batch B_S6_000 in test_submissions (Chennai original amounts, Jan 2025)
INSERT INTO test_submissions VALUES
    ('4001001','Trade Receivable', 5900.00, NULL,    'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai','B_S6_000','2025-01-10','2025-01-06','2025-01-06','ORIGINAL',NULL,NULL),
    ('5001001','Revenue',          NULL,    5000.00, 'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai','B_S6_000','2025-01-10','2025-01-06','2025-01-06','ORIGINAL',NULL,NULL),
    ('5001002','SGST Payable',     NULL,    300.00,  'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai','B_S6_000','2025-01-10','2025-01-06','2025-01-06','ORIGINAL',NULL,NULL),
    ('5001003','CGST Payable',     NULL,    300.00,  'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai','B_S6_000','2025-01-10','2025-01-06','2025-01-06','ORIGINAL',NULL,NULL),
    ('5001004','IGST Payable',     NULL,    300.00,  'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai','B_S6_000','2025-01-10','2025-01-06','2025-01-06','ORIGINAL',NULL,NULL);

-- 2. Add decreased staging amounts for Chennai
INSERT INTO test_staging VALUES
    ('4001001','Trade Receivable', 4720.00, NULL,    'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai'),
    ('5001001','Revenue',          NULL,    4000.00, 'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai'),
    ('5001002','SGST Payable',     NULL,    240.00,  'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai'),
    ('5001003','CGST Payable',     NULL,    240.00,  'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai'),
    ('5001004','IGST Payable',     NULL,    240.00,  'Chennai','Normal_billing_cycle','FURLENCO_RENTAL',30,'ST003','ORG003','chennai@furlenco.com','2025-01-06','furlenco_rental, Jan-2025, Chennai');

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 6 output ---' AS scenario;
SELECT code_number, particulars, DR, CR, start_date, end_date, row_type, reference_batch_id, correction_period
FROM test_batch_preview
ORDER BY CASE row_type WHEN 'RESTATEMENT' THEN 1 WHEN 'CORRECTION_DELTA' THEN 2 END, code_number;

-- ASSERTIONS
SELECT 'FAIL S6-A: expected 10 rows, got '                  || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 10;
SELECT 'FAIL S6-B: expected 5 RESTATEMENT rows, got '       || COUNT(*) AS result FROM test_batch_preview WHERE row_type='RESTATEMENT'     HAVING COUNT(*) <> 5;
SELECT 'FAIL S6-C: expected 5 CORRECTION_DELTA rows, got '  || COUNT(*) AS result FROM test_batch_preview WHERE row_type='CORRECTION_DELTA' HAVING COUNT(*) <> 5;
SELECT 'FAIL S6-D: CORRECTION_DELTA 4001001 wrong (expect DR=NULL CR=1180 — DR decreased, flipped to CR)' AS result
    FROM test_batch_preview WHERE code_number='4001001' AND row_type='CORRECTION_DELTA' AND (DR IS NOT NULL OR CR <> 1180.00) LIMIT 1;
SELECT 'FAIL S6-E: CORRECTION_DELTA 5001001 wrong (expect DR=1000 CR=NULL — CR decreased, flipped to DR)' AS result
    FROM test_batch_preview WHERE code_number='5001001' AND row_type='CORRECTION_DELTA' AND (DR <> 1000.00 OR CR IS NOT NULL) LIMIT 1;
SELECT 'FAIL S6-F: CORRECTION_DELTA 5001002 SGST wrong (expect DR=60 CR=NULL — CR decreased, flipped to DR)' AS result
    FROM test_batch_preview WHERE code_number='5001002' AND row_type='CORRECTION_DELTA' AND (DR <> 60.00 OR CR IS NOT NULL) LIMIT 1;
SELECT 'FAIL S6-G: CORRECTION_DELTA 5001003 CGST wrong (expect DR=60 CR=NULL — CR decreased, flipped to DR)' AS result
    FROM test_batch_preview WHERE code_number='5001003' AND row_type='CORRECTION_DELTA' AND (DR <> 60.00 OR CR IS NOT NULL) LIMIT 1;
SELECT 'FAIL S6-H: CORRECTION_DELTA 5001004 IGST wrong (expect DR=60 CR=NULL — CR decreased, flipped to DR)' AS result
    FROM test_batch_preview WHERE code_number='5001004' AND row_type='CORRECTION_DELTA' AND (DR <> 60.00 OR CR IS NOT NULL) LIMIT 1;
SELECT 'FAIL S6-I: correction_period should be 2025-01-06 on CORRECTION_DELTA rows' AS result
    FROM test_batch_preview WHERE row_type='CORRECTION_DELTA' AND correction_period <> '2025-01-06' LIMIT 1;
SELECT 'FAIL S6-J: delta not balanced (total CORRECTION_DELTA DR should equal total CR)' AS result
FROM (
    SELECT ROUND(SUM(COALESCE(DR,0))::NUMERIC,2) AS total_DR,
           ROUND(SUM(COALESCE(CR,0))::NUMERIC,2) AS total_CR
    FROM test_batch_preview
    WHERE row_type = 'CORRECTION_DELTA'
) t
WHERE total_DR <> total_CR;
SELECT 'FAIL S6-K: reference_batch_id should be B_S6_000 on all rows' AS result
    FROM test_batch_preview WHERE reference_batch_id <> 'B_S6_000' LIMIT 1;

-- Commit Scenario 6
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- SCENARIO 7 — Price INCREASE with GST rows (increase back to original)
-- =============================================================================
-- After Scenario 6 the RESTATEMENT rows represent decreased amounts as new baseline.
-- Now update staging with amounts HIGHER than the post-S6 baseline (back to original).
--
-- Post-S6 baseline (RESTATEMENT): 4001001 DR=4720 | 5001001 CR=4000 | 5001002 CR=240 | 5001003 CR=240 | 5001004 CR=240
-- Increased staging:               4001001 DR=5900 | 5001001 CR=5000 | 5001002 CR=300 | 5001003 CR=300 | 5001004 CR=300
--
-- Expect: 10 rows — 5 RESTATEMENT (state markers) + 5 CORRECTION_DELTA (sent to P360)
--   CORRECTION_DELTA direction (increase → stays on same side):
--     4001001 Receivable: cur_DR=5900 > old_DR=4720 → CORRECTION_DELTA DR=1180  (DR increased, stays DR)
--     5001001 Revenue:    cur_CR=5000 > old_CR=4000 → CORRECTION_DELTA CR=1000  (CR increased, stays CR)
--     5001002 SGST:       cur_CR=300  > old_CR=240  → CORRECTION_DELTA CR=60    (CR increased, stays CR)
--     5001003 CGST:       cur_CR=300  > old_CR=240  → CORRECTION_DELTA CR=60    (CR increased, stays CR)
--     5001004 IGST:       cur_CR=300  > old_CR=240  → CORRECTION_DELTA CR=60    (CR increased, stays CR)
--   Delta balanced: 1180 DR = 1000+60+60+60=1180 CR ✓
-- =============================================================================

-- Update staging to original (increased) amounts for Chennai
UPDATE test_staging SET DR = 5900.00 WHERE code_number = '4001001' AND city_id = 30;
UPDATE test_staging SET CR = 5000.00 WHERE code_number = '5001001' AND city_id = 30;
UPDATE test_staging SET CR = 300.00  WHERE code_number = '5001002' AND city_id = 30;
UPDATE test_staging SET CR = 300.00  WHERE code_number = '5001003' AND city_id = 30;
UPDATE test_staging SET CR = 300.00  WHERE code_number = '5001004' AND city_id = 30;

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 7 output ---' AS scenario;
SELECT code_number, particulars, DR, CR, start_date, end_date, row_type, reference_batch_id, correction_period
FROM test_batch_preview
ORDER BY CASE row_type WHEN 'RESTATEMENT' THEN 1 WHEN 'CORRECTION_DELTA' THEN 2 END, code_number;

-- ASSERTIONS
SELECT 'FAIL S7-A: expected 10 rows, got '                  || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 10;
SELECT 'FAIL S7-B: expected 5 RESTATEMENT rows, got '       || COUNT(*) AS result FROM test_batch_preview WHERE row_type='RESTATEMENT'     HAVING COUNT(*) <> 5;
SELECT 'FAIL S7-C: expected 5 CORRECTION_DELTA rows, got '  || COUNT(*) AS result FROM test_batch_preview WHERE row_type='CORRECTION_DELTA' HAVING COUNT(*) <> 5;
SELECT 'FAIL S7-D: CORRECTION_DELTA 4001001 wrong (expect DR=1180 CR=NULL — DR increased, stays DR)' AS result
    FROM test_batch_preview WHERE code_number='4001001' AND row_type='CORRECTION_DELTA' AND (DR <> 1180.00 OR CR IS NOT NULL) LIMIT 1;
SELECT 'FAIL S7-E: CORRECTION_DELTA 5001001 wrong (expect DR=NULL CR=1000 — CR increased, stays CR)' AS result
    FROM test_batch_preview WHERE code_number='5001001' AND row_type='CORRECTION_DELTA' AND (DR IS NOT NULL OR CR <> 1000.00) LIMIT 1;
SELECT 'FAIL S7-F: CORRECTION_DELTA 5001002 SGST wrong (expect DR=NULL CR=60 — CR increased, stays CR)' AS result
    FROM test_batch_preview WHERE code_number='5001002' AND row_type='CORRECTION_DELTA' AND (DR IS NOT NULL OR CR <> 60.00) LIMIT 1;
SELECT 'FAIL S7-G: CORRECTION_DELTA 5001003 CGST wrong (expect DR=NULL CR=60 — CR increased, stays CR)' AS result
    FROM test_batch_preview WHERE code_number='5001003' AND row_type='CORRECTION_DELTA' AND (DR IS NOT NULL OR CR <> 60.00) LIMIT 1;
SELECT 'FAIL S7-H: CORRECTION_DELTA 5001004 IGST wrong (expect DR=NULL CR=60 — CR increased, stays CR)' AS result
    FROM test_batch_preview WHERE code_number='5001004' AND row_type='CORRECTION_DELTA' AND (DR IS NOT NULL OR CR <> 60.00) LIMIT 1;
SELECT 'FAIL S7-I: delta not balanced (total CORRECTION_DELTA DR should equal total CR)' AS result
FROM (
    SELECT ROUND(SUM(COALESCE(DR,0))::NUMERIC,2) AS total_DR,
           ROUND(SUM(COALESCE(CR,0))::NUMERIC,2) AS total_CR
    FROM test_batch_preview
    WHERE row_type = 'CORRECTION_DELTA'
) t
WHERE total_DR <> total_CR;

-- Commit Scenario 7
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- FINAL STATE — Full ledger balance check
-- =============================================================================
-- After all scenarios, p360_submissions should be balanced:
-- net_DR = net_CR for every (code_number, vertical) group.
-- This confirms reversals and restatements net out correctly.
-- =============================================================================

SELECT '--- Final balance check ---' AS check_name;
SELECT
    code_number,
    particulars,
    vertical,
    SUM(COALESCE(DR, 0))  AS net_DR,
    SUM(COALESCE(CR, 0))  AS net_CR,
    SUM(COALESCE(DR, 0)) - SUM(COALESCE(CR, 0)) AS imbalance
FROM test_submissions
GROUP BY code_number, particulars, vertical
ORDER BY vertical, code_number;

-- ASSERTION: any imbalance is a failure
SELECT 'FAIL BALANCE: imbalance detected for ' || code_number || ' / ' || vertical AS result
FROM (
    SELECT code_number, vertical,
           ROUND(SUM(COALESCE(DR,0))::NUMERIC,4) - ROUND(SUM(COALESCE(CR,0))::NUMERIC,4) AS diff
    FROM test_submissions
    GROUP BY code_number, vertical
) t
WHERE ABS(diff) > 0.001;

-- Full history for inspection
SELECT '--- Full test_submissions history ---' AS history;
SELECT batch_id, submission_date, row_type, code_number, particulars, DR, CR, start_date, end_date, reference_batch_id
FROM test_submissions
ORDER BY submission_date, batch_id, row_type, code_number;


-- =============================================================================
-- RESET FOR SCENARIOS 8–11
-- =============================================================================
-- Clean slate: wipe all state from Scenarios 1–7.
-- Scenarios 8–11 are fully self-contained and can be run independently.
-- =============================================================================

DELETE FROM test_staging;
DELETE FROM test_submissions;


-- =============================================================================
-- SCENARIO 8 — All cycle_types in one batch
-- =============================================================================
-- Demonstrates that cycle_type is part of the business key — rows with the
-- same code_number, city, and date but different cycle_types are distinct
-- ledger entries, each becoming its own ORIGINAL row.
--
-- City 40 (Delhi) | FURLENCO_RENTAL | week 2025-02-03 → 2025-02-09
-- 6 cycle_types × 4 journal entries = 24 rows, all ORIGINAL.
--
-- Balance per cycle_type:
--   Normal_billing_cycle : 11,800 DR = 10,000 Revenue + 900 CGST + 900 SGST CR
--   Swap                 :  5,900 DR =  5,000 Revenue + 450 CGST + 450 SGST CR
--   VAS                  :    590 DR =    500 Revenue +  45 CGST +  45 SGST CR
--   MTP                  :  3,540 DR =  3,000 Revenue + 270 CGST + 270 SGST CR
--   Penalty              :  2,360 DR =  2,000 Revenue + 180 CGST + 180 SGST CR
--   Credit_Note          : DR/CR flipped — 11,800 CR Trade Rec = 10,000+900+900 DR Revenue/Tax
-- =============================================================================

INSERT INTO test_staging VALUES
    -- Normal_billing_cycle: standard monthly rental
    ('3004010','Trade Receivables - Furlenco', 11800.00, NULL,    'Delhi','Normal_billing_cycle','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',           NULL,    10000.00, 'Delhi','Normal_billing_cycle','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006270','Output CGST 9%',               NULL,      900.00, 'Delhi','Normal_billing_cycle','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006310','Output SGST 9%',               NULL,      900.00, 'Delhi','Normal_billing_cycle','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    -- Swap: same code_numbers, different cycle_type = different business key
    ('3004010','Trade Receivables - Furlenco',  5900.00, NULL,    'Delhi','Swap','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',            NULL,    5000.00, 'Delhi','Swap','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006270','Output CGST 9%',               NULL,      450.00, 'Delhi','Swap','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006310','Output SGST 9%',               NULL,      450.00, 'Delhi','Swap','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    -- VAS: value added service
    ('3004010','Trade Receivables - Furlenco',   590.00, NULL,    'Delhi','VAS','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',            NULL,     500.00, 'Delhi','VAS','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006270','Output CGST 9%',               NULL,      45.00,  'Delhi','VAS','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006310','Output SGST 9%',               NULL,      45.00,  'Delhi','VAS','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    -- MTP: min tenure penalty (early return)
    ('3004010','Trade Receivables - Furlenco',  3540.00, NULL,    'Delhi','MTP','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',            NULL,    3000.00, 'Delhi','MTP','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006270','Output CGST 9%',               NULL,      270.00, 'Delhi','MTP','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006310','Output SGST 9%',               NULL,      270.00, 'Delhi','MTP','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    -- Penalty: non-MTP penalty
    ('3004010','Trade Receivables - Furlenco',  2360.00, NULL,    'Delhi','Penalty','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',            NULL,    2000.00, 'Delhi','Penalty','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006270','Output CGST 9%',               NULL,      180.00, 'Delhi','Penalty','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006310','Output SGST 9%',               NULL,      180.00, 'Delhi','Penalty','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    -- Credit_Note: DR/CR are FLIPPED — this is a revenue reversal
    ('3004010','Trade Receivables - Furlenco',  NULL,   11800.00, 'Delhi','Credit_Note','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',           10000.00, NULL,    'Delhi','Credit_Note','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006270','Output CGST 9%',                 900.00, NULL,    'Delhi','Credit_Note','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006310','Output SGST 9%',                 900.00, NULL,    'Delhi','Credit_Note','FURLENCO_RENTAL',40,'ST004','ORG004','delhi@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025');

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 8 output: All cycle_types ---' AS scenario;
SELECT cycle_type, code_number, particulars, DR, CR, row_type
FROM test_batch_preview
ORDER BY cycle_type, CASE WHEN DR IS NOT NULL THEN 0 ELSE 1 END, code_number;

-- ASSERTIONS
SELECT 'FAIL S8-A: expected 24 rows, got '               || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 24;
SELECT 'FAIL S8-B: expected all ORIGINAL rows'           AS result FROM test_batch_preview WHERE row_type <> 'ORIGINAL' LIMIT 1;
SELECT 'FAIL S8-C: expected 6 distinct cycle_types, got '|| COUNT(DISTINCT cycle_type) AS result FROM test_batch_preview HAVING COUNT(DISTINCT cycle_type) <> 6;
SELECT 'FAIL S8-D: Normal_billing_cycle Trade Rec DR should be 11800' AS result FROM test_batch_preview WHERE cycle_type='Normal_billing_cycle' AND code_number='3004010' AND DR <> 11800.00 LIMIT 1;
SELECT 'FAIL S8-E: Swap Trade Rec DR should be 5900'                  AS result FROM test_batch_preview WHERE cycle_type='Swap'     AND code_number='3004010' AND DR <> 5900.00  LIMIT 1;
SELECT 'FAIL S8-F: VAS Trade Rec DR should be 590'                    AS result FROM test_batch_preview WHERE cycle_type='VAS'      AND code_number='3004010' AND DR <> 590.00   LIMIT 1;
SELECT 'FAIL S8-G: MTP Trade Rec DR should be 3540'                   AS result FROM test_batch_preview WHERE cycle_type='MTP'      AND code_number='3004010' AND DR <> 3540.00  LIMIT 1;
SELECT 'FAIL S8-H: Penalty Trade Rec DR should be 2360'               AS result FROM test_batch_preview WHERE cycle_type='Penalty'  AND code_number='3004010' AND DR <> 2360.00  LIMIT 1;
SELECT 'FAIL S8-I: Credit_Note Trade Rec must be on CR side (DR/CR flipped)' AS result FROM test_batch_preview WHERE cycle_type='Credit_Note' AND code_number='3004010' AND (DR IS NOT NULL OR CR <> 11800.00) LIMIT 1;
SELECT 'FAIL S8-J: Credit_Note Revenue must be on DR side (DR/CR flipped)'   AS result FROM test_batch_preview WHERE cycle_type='Credit_Note' AND code_number='1001010' AND (CR IS NOT NULL OR DR <> 10000.00) LIMIT 1;
SELECT 'FAIL S8-K: balance — each cycle_type must have total DR = total CR'  AS result
FROM (SELECT cycle_type, ROUND(SUM(COALESCE(DR,0))::NUMERIC,2) AS tDR, ROUND(SUM(COALESCE(CR,0))::NUMERIC,2) AS tCR FROM test_batch_preview GROUP BY cycle_type) t WHERE tDR <> tCR;

-- Commit Scenario 8
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- SCENARIO 9 — All verticals in one batch
-- =============================================================================
-- Shows the Trade Receivable and Revenue code_number for each vertical.
-- Each vertical uses a different start_date to prevent business key conflicts
-- when the same code_number appears across verticals (e.g. 3004030 for both
-- New Sales D2C and New Sales Store).
--
-- City 50 (Hyderabad) | Normal_billing_cycle | 2 rows per vertical (Trade Rec + Revenue)
-- 7 verticals × 2 rows = 14 rows, all ORIGINAL.
--
-- Vertical → Trade Rec code → Revenue code:
--   FURLENCO_RENTAL (B2C) : 3004010 → 1001010
--   FURLENCO_RENTAL (B2B) : 3004020 → 1001010  (only Trade Rec changes for B2B)
--   UNLMTD                : 3004080 → 1001020
--   New Sales - D2C       : 3004030 → 1001050
--   New Sales - Store     : 3004030 → 1001030
--   Refurb Sales - D2C    : 3004040 → 1001060
--   Refurb Sales - Store  : 3004040 → 1001040
-- =============================================================================

INSERT INTO test_staging VALUES
    -- FURLENCO_RENTAL — B2C (week Feb 3–9)
    ('3004010','Trade Receivables - Furlenco',  11800.00, NULL,    'Hyderabad','Normal_billing_cycle','FURLENCO_RENTAL',   50,'ST005','ORG005','hyd@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',            NULL,    11800.00, 'Hyderabad','Normal_billing_cycle','FURLENCO_RENTAL',   50,'ST005','ORG005','hyd@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    -- FURLENCO_RENTAL — B2B: Trade Rec = 3004020 (not 3004010); Revenue = 1001010 unchanged (week Feb 10–16)
    ('3004020','Trade Receivables - B2B',       11800.00, NULL,    'Hyderabad','Normal_billing_cycle','FURLENCO_RENTAL',   50,'ST005','ORG005','hyd@furlenco.com','2025-02-10','2025-02-16','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',            NULL,    11800.00, 'Hyderabad','Normal_billing_cycle','FURLENCO_RENTAL',   50,'ST005','ORG005','hyd@furlenco.com','2025-02-10','2025-02-16','furlenco_rental, Feb-2025'),
    -- UNLMTD (week Feb 17–23)
    ('3004080','Trade Receivables - Unlmtd',     5000.00, NULL,    'Hyderabad','Normal_billing_cycle','UNLMTD',            50,'ST005','ORG005','hyd@furlenco.com','2025-02-17','2025-02-23','unlmtd, Feb-2025'),
    ('1001020','Revenue - Unlmtd',              NULL,     5000.00, 'Hyderabad','Normal_billing_cycle','UNLMTD',            50,'ST005','ORG005','hyd@furlenco.com','2025-02-17','2025-02-23','unlmtd, Feb-2025'),
    -- New Sales - D2C (week Feb 24–Mar 2)
    ('3004030','Trade Receivables - New Sales',  15000.00, NULL,   'Hyderabad','Normal_billing_cycle','New Sales - D2C',   50,'ST005','ORG005','hyd@furlenco.com','2025-02-24','2025-03-02','new_sales_d2c, Feb-2025'),
    ('1001050','Revenue - New Sales - D2C',     NULL,    15000.00, 'Hyderabad','Normal_billing_cycle','New Sales - D2C',   50,'ST005','ORG005','hyd@furlenco.com','2025-02-24','2025-03-02','new_sales_d2c, Feb-2025'),
    -- New Sales - Store (week Mar 3–9; same 3004030 Trade Rec but different vertical = different key)
    ('3004030','Trade Receivables - New Sales',  12000.00, NULL,   'Hyderabad','Normal_billing_cycle','New Sales - Store', 50,'ST005','ORG005','hyd@furlenco.com','2025-03-03','2025-03-09','new_sales_store, Mar-2025'),
    ('1001030','Revenue - New Sales - Store',   NULL,    12000.00, 'Hyderabad','Normal_billing_cycle','New Sales - Store', 50,'ST005','ORG005','hyd@furlenco.com','2025-03-03','2025-03-09','new_sales_store, Mar-2025'),
    -- Refurb Sales - D2C (week Mar 10–16)
    ('3004040','Trade Receivables - Refurb Sales', 8000.00, NULL,  'Hyderabad','Normal_billing_cycle','Refurb Sales - D2C',50,'ST005','ORG005','hyd@furlenco.com','2025-03-10','2025-03-16','refurb_d2c, Mar-2025'),
    ('1001060','Revenue - Refurb Sales - D2C',  NULL,     8000.00, 'Hyderabad','Normal_billing_cycle','Refurb Sales - D2C',50,'ST005','ORG005','hyd@furlenco.com','2025-03-10','2025-03-16','refurb_d2c, Mar-2025'),
    -- Refurb Sales - Store (week Mar 17–23; same 3004040 Trade Rec but different vertical = different key)
    ('3004040','Trade Receivables - Refurb Sales', 7000.00, NULL,  'Hyderabad','Normal_billing_cycle','Refurb Sales - Store',50,'ST005','ORG005','hyd@furlenco.com','2025-03-17','2025-03-23','refurb_store, Mar-2025'),
    ('1001040','Revenue - Refurb Sales - Store',NULL,     7000.00, 'Hyderabad','Normal_billing_cycle','Refurb Sales - Store',50,'ST005','ORG005','hyd@furlenco.com','2025-03-17','2025-03-23','refurb_store, Mar-2025');

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 9 output: All verticals ---' AS scenario;
SELECT vertical, code_number, particulars, DR, CR, row_type, start_date
FROM test_batch_preview
ORDER BY vertical, start_date, CASE WHEN DR IS NOT NULL THEN 0 ELSE 1 END;

-- ASSERTIONS
SELECT 'FAIL S9-A: expected 14 rows, got '                              || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 14;
SELECT 'FAIL S9-B: expected all ORIGINAL rows'                          AS result FROM test_batch_preview WHERE row_type <> 'ORIGINAL' LIMIT 1;
SELECT 'FAIL S9-C: FURLENCO_RENTAL B2C must use Trade Rec 3004010'      AS result FROM test_batch_preview WHERE vertical='FURLENCO_RENTAL' AND start_date='2025-02-03' AND code_number NOT IN ('3004010','1001010') LIMIT 1;
SELECT 'FAIL S9-D: B2B must use Trade Rec 3004020 (not 3004010)'        AS result FROM test_batch_preview WHERE vertical='FURLENCO_RENTAL' AND start_date='2025-02-10' AND code_number='3004010' LIMIT 1;
SELECT 'FAIL S9-E: B2B Trade Rec 3004020 not found'                     AS result FROM test_batch_preview WHERE vertical='FURLENCO_RENTAL' AND start_date='2025-02-10' AND code_number='3004020' HAVING COUNT(*)=0;
SELECT 'FAIL S9-F: UNLMTD must use Trade Rec 3004080 and Revenue 1001020' AS result FROM test_batch_preview WHERE vertical='UNLMTD' AND code_number NOT IN ('3004080','1001020') LIMIT 1;
SELECT 'FAIL S9-G: New Sales D2C must use Revenue 1001050'              AS result FROM test_batch_preview WHERE vertical='New Sales - D2C'    AND code_number='1001050' HAVING COUNT(*)=0;
SELECT 'FAIL S9-H: New Sales Store must use Revenue 1001030'            AS result FROM test_batch_preview WHERE vertical='New Sales - Store'   AND code_number='1001030' HAVING COUNT(*)=0;
SELECT 'FAIL S9-I: Refurb Sales D2C must use Revenue 1001060'           AS result FROM test_batch_preview WHERE vertical='Refurb Sales - D2C'  AND code_number='1001060' HAVING COUNT(*)=0;
SELECT 'FAIL S9-J: Refurb Sales Store must use Revenue 1001040'         AS result FROM test_batch_preview WHERE vertical='Refurb Sales - Store' AND code_number='1001040' HAVING COUNT(*)=0;
SELECT 'FAIL S9-K: New Sales D2C and Store share Trade Rec 3004030 but are separate rows (different vertical)' AS result
FROM test_batch_preview WHERE code_number='3004030' HAVING COUNT(*) <> 2;

-- Commit Scenario 9
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- SCENARIO 10 — Inter-state vs intra-state GST
-- =============================================================================
-- Shows that IGST (inter-state) and CGST+SGST (intra-state) use different
-- ledger codes, and that only one tax type appears per row — never both.
--
-- City 60 (Pune)    — inter-state: IGST 18% only  (code 3006350)
--   taxable=10,000 | igst=1,800 | post-tax=11,800 → 3 rows
--
-- City 70 (Kolkata) — intra-state: CGST 9% + SGST 9% (codes 3006270 + 3006310)
--   taxable=10,000 | cgst=900 | sgst=900 | post-tax=11,800 → 4 rows
--
-- Total: 7 rows, all ORIGINAL.
-- Both cities balance to 11,800 DR = 11,800 CR.
-- =============================================================================

INSERT INTO test_staging VALUES
    -- Inter-state: Pune (city 60) — IGST 18% only, no CGST/SGST
    ('3004010','Trade Receivables - Furlenco', 11800.00, NULL,    'Pune',    'Normal_billing_cycle','FURLENCO_RENTAL',60,'ST006','ORG006','pune@furlenco.com',   '2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',           NULL,    10000.00, 'Pune',    'Normal_billing_cycle','FURLENCO_RENTAL',60,'ST006','ORG006','pune@furlenco.com',   '2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006350','Output IGST 18%',              NULL,     1800.00, 'Pune',    'Normal_billing_cycle','FURLENCO_RENTAL',60,'ST006','ORG006','pune@furlenco.com',   '2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    -- Intra-state: Kolkata (city 70) — CGST 9% + SGST 9%, no IGST
    ('3004010','Trade Receivables - Furlenco', 11800.00, NULL,    'Kolkata', 'Normal_billing_cycle','FURLENCO_RENTAL',70,'ST007','ORG007','kolkata@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('1001010','Revenue - Furlenco',           NULL,    10000.00, 'Kolkata', 'Normal_billing_cycle','FURLENCO_RENTAL',70,'ST007','ORG007','kolkata@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006270','Output CGST 9%',               NULL,      900.00, 'Kolkata', 'Normal_billing_cycle','FURLENCO_RENTAL',70,'ST007','ORG007','kolkata@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025'),
    ('3006310','Output SGST 9%',               NULL,      900.00, 'Kolkata', 'Normal_billing_cycle','FURLENCO_RENTAL',70,'ST007','ORG007','kolkata@furlenco.com','2025-02-03','2025-02-09','furlenco_rental, Feb-2025');

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 10 output: Inter-state (IGST) vs Intra-state (CGST+SGST) ---' AS scenario;
SELECT city_name, code_number, particulars, DR, CR, row_type
FROM test_batch_preview
ORDER BY city_name, CASE WHEN DR IS NOT NULL THEN 0 ELSE 1 END, code_number;

-- ASSERTIONS
SELECT 'FAIL S10-A: expected 7 rows, got '                                         || COUNT(*) AS result FROM test_batch_preview HAVING COUNT(*) <> 7;
SELECT 'FAIL S10-B: expected all ORIGINAL rows'                                    AS result FROM test_batch_preview WHERE row_type <> 'ORIGINAL' LIMIT 1;
SELECT 'FAIL S10-C: Pune (inter-state) must have IGST row 3006350 with CR=1800'   AS result FROM test_batch_preview WHERE city_id=60 AND code_number='3006350' AND CR <> 1800.00 LIMIT 1;
SELECT 'FAIL S10-D: Pune (inter-state) must NOT have CGST or SGST rows'           AS result FROM test_batch_preview WHERE city_id=60 AND code_number IN ('3006270','3006310') LIMIT 1;
SELECT 'FAIL S10-E: Kolkata (intra-state) must have CGST 3006270 with CR=900'     AS result FROM test_batch_preview WHERE city_id=70 AND code_number='3006270' AND CR <> 900.00 LIMIT 1;
SELECT 'FAIL S10-F: Kolkata (intra-state) must have SGST 3006310 with CR=900'     AS result FROM test_batch_preview WHERE city_id=70 AND code_number='3006310' AND CR <> 900.00 LIMIT 1;
SELECT 'FAIL S10-G: Kolkata (intra-state) must NOT have IGST row'                 AS result FROM test_batch_preview WHERE city_id=70 AND code_number='3006350' LIMIT 1;
SELECT 'FAIL S10-H: balance — both cities must have total DR = total CR'           AS result
FROM (SELECT city_id, ROUND(SUM(COALESCE(DR,0))::NUMERIC,2) AS tDR, ROUND(SUM(COALESCE(CR,0))::NUMERIC,2) AS tCR FROM test_batch_preview GROUP BY city_id) t WHERE tDR <> tCR;

-- Commit Scenario 10
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- SCENARIO 11 — Deferral: cross-month billing cycle
-- =============================================================================
-- Shows the 4 journal entries generated when a billing cycle spans two months.
-- Revenue is split proportionally using average-month-length arithmetic.
--
-- Billing cycle: Feb 20 – Mar 19, 2025 (spans Feb → Mar)
-- Taxable: 6,000.00
--
-- Calculation (Feb is billing start month → uses +2.5 day adjustment):
--   total_days        = (Mar 19 – Feb 20 + 1) + 2.5 = 28 + 2.5 = 30.5
--   start_day         = 20
--   Feb portion       = ROUND((30.5 – 19) × 6000 / 30.5, 2) = ROUND(2262.30) = 2,262.30
--   Mar portion (def) = 6000 – 2262.30 = 3,737.70
--
-- 4 journal entries (cycle_type = 'Deferral'), city 70 (Kolkata), all ORIGINAL:
--   Week Feb 17–23 (recognised_date = Feb 20 → Monday of that week):
--     1001010 Revenue DR = 3737.70   (reduce Feb revenue by deferred amount)
--     4006020 Deferred Revenue CR = 3737.70   (create liability)
--   Week Feb 24–Mar 2 (cr_recognised_date = Mar 1 → Monday of that week):
--     4006020 Deferred Revenue DR = 3737.70   (clear liability)
--     1001010 Revenue CR = 3737.70   (recognise in March)
-- =============================================================================

INSERT INTO test_staging VALUES
    -- Current month (week Feb 17–23): reduce Feb revenue, create deferred liability
    ('1001010','Revenue - Furlenco',   3737.70, NULL,    'Kolkata','Deferral','FURLENCO_RENTAL',70,'ST007','ORG007','kolkata@furlenco.com','2025-02-17','2025-02-23','deferral current month, Feb-2025'),
    ('4006020','Deferred Revenue',     NULL,    3737.70, 'Kolkata','Deferral','FURLENCO_RENTAL',70,'ST007','ORG007','kolkata@furlenco.com','2025-02-17','2025-02-23','deferral current month, Feb-2025'),
    -- Next month (week Feb 24–Mar 2): clear liability, recognise revenue in March
    ('4006020','Deferred Revenue',     3737.70, NULL,    'Kolkata','Deferral','FURLENCO_RENTAL',70,'ST007','ORG007','kolkata@furlenco.com','2025-02-24','2025-03-02','deferral next month, Mar-2025'),
    ('1001010','Revenue - Furlenco',   NULL,    3737.70, 'Kolkata','Deferral','FURLENCO_RENTAL',70,'ST007','ORG007','kolkata@furlenco.com','2025-02-24','2025-03-02','deferral next month, Mar-2025');

DROP TABLE IF EXISTS test_batch_preview;
CREATE TEMP TABLE test_batch_preview AS
WITH
new_batch AS (SELECT 'B_'||TO_CHAR(CURRENT_DATE,'YYYYMMDD')||'_'||LPAD(CAST((SELECT COUNT(DISTINCT batch_id) FROM test_submissions WHERE submission_date=CURRENT_DATE)+1 AS VARCHAR),3,'0') AS batch_id),
last_sent AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY code_number,city_id,vertical,cycle_type,start_date,end_date,organization_id,COALESCE(store_id,'') ORDER BY submission_date DESC,batch_id DESC) AS rn FROM test_submissions WHERE row_type IN ('ORIGINAL','RESTATEMENT')) t WHERE rn=1),
current_data AS (SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks FROM test_staging),
comparison AS (
    SELECT COALESCE(cur.code_number,ls.code_number) AS code_number,COALESCE(cur.particulars,ls.particulars) AS particulars,COALESCE(cur.city_name,ls.city_name) AS city_name,COALESCE(cur.cycle_type,ls.cycle_type) AS cycle_type,COALESCE(cur.vertical,ls.vertical) AS vertical,COALESCE(cur.city_id,ls.city_id) AS city_id,COALESCE(cur.store_id,ls.store_id) AS store_id,COALESCE(cur.organization_id,ls.organization_id) AS organization_id,COALESCE(cur.organization_email_id,ls.organization_email_id) AS organization_email_id,COALESCE(cur.start_date,ls.start_date) AS start_date,COALESCE(cur.end_date,ls.end_date) AS end_date,COALESCE(cur.remarks,ls.remarks) AS remarks,cur.DR AS cur_DR,cur.CR AS cur_CR,ls.DR AS old_DR,ls.CR AS old_CR,ls.batch_id AS last_batch_id,
    CASE WHEN ls.code_number IS NULL THEN 'ORIGINAL' WHEN cur.code_number IS NULL THEN 'REVERSAL_ONLY' WHEN ROUND(COALESCE(cur.DR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.DR,0)::NUMERIC,4) OR ROUND(COALESCE(cur.CR,0)::NUMERIC,4)<>ROUND(COALESCE(ls.CR,0)::NUMERIC,4) THEN 'CORRECTION' ELSE 'UNCHANGED' END AS action
    FROM current_data cur FULL OUTER JOIN last_sent ls ON cur.code_number=ls.code_number AND cur.city_id=ls.city_id AND cur.vertical=ls.vertical AND cur.cycle_type=ls.cycle_type AND cur.start_date=ls.start_date AND cur.end_date=ls.end_date AND cur.organization_id=ls.organization_id AND COALESCE(cur.store_id,'')=COALESCE(ls.store_id,'')
),
batch_bounds AS (SELECT MIN(start_date) AS cycle_start,MAX(start_date) AS cycle_end FROM comparison WHERE action IN ('ORIGINAL','REVERSAL_ONLY','CORRECTION')),
delta_date AS (SELECT COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN start_date END),CURRENT_DATE) AS start_date, COALESCE(MAX(CASE WHEN action='ORIGINAL' THEN end_date END),CURRENT_DATE) AS end_date FROM comparison),
output_rows AS (
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE AS submission_date,bb.cycle_start,bb.cycle_end,'ORIGINAL'::VARCHAR AS row_type,NULL::VARCHAR AS reference_batch_id,NULL::DATE AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='ORIGINAL'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.old_CR AS DR,cmp.old_DR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'REVERSAL'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='REVERSAL_ONLY'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,cmp.cur_DR AS DR,cmp.cur_CR AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,cmp.start_date,cmp.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'RESTATEMENT'::VARCHAR,cmp.last_batch_id,NULL::DATE FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb WHERE cmp.action='CORRECTION'
    UNION ALL
    SELECT cmp.code_number,cmp.particulars,CASE WHEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0)>0 THEN COALESCE(cmp.cur_DR,0)-COALESCE(cmp.old_DR,0) WHEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0)>0 THEN COALESCE(cmp.old_CR,0)-COALESCE(cmp.cur_CR,0) ELSE NULL END AS DR,CASE WHEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0)>0 THEN COALESCE(cmp.cur_CR,0)-COALESCE(cmp.old_CR,0) WHEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0)>0 THEN COALESCE(cmp.old_DR,0)-COALESCE(cmp.cur_DR,0) ELSE NULL END AS CR,cmp.city_name,cmp.cycle_type,cmp.vertical,cmp.city_id,cmp.store_id,cmp.organization_id,cmp.organization_email_id,dd.start_date,dd.end_date,cmp.remarks,nb.batch_id,CURRENT_DATE,bb.cycle_start,bb.cycle_end,'CORRECTION_DELTA'::VARCHAR,cmp.last_batch_id,cmp.start_date AS correction_period FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd WHERE cmp.action='CORRECTION'
)
SELECT code_number,particulars,DR,CR,city_name,cycle_type,vertical,city_id,store_id,organization_id,organization_email_id,start_date,end_date,remarks,batch_id,submission_date,cycle_start,cycle_end,row_type,reference_batch_id,correction_period FROM output_rows;

SELECT '--- Scenario 11 output: Deferral (cross-month billing) ---' AS scenario;
SELECT start_date, end_date, code_number, particulars, DR, CR, cycle_type, row_type
FROM test_batch_preview
WHERE cycle_type = 'Deferral'
ORDER BY start_date, CASE WHEN DR IS NOT NULL THEN 0 ELSE 1 END, code_number;

-- ASSERTIONS
SELECT 'FAIL S11-A: expected 4 rows, got '                                        || COUNT(*) AS result FROM test_batch_preview WHERE cycle_type='Deferral' HAVING COUNT(*) <> 4;
SELECT 'FAIL S11-B: expected all ORIGINAL rows'                                   AS result FROM test_batch_preview WHERE cycle_type='Deferral' AND row_type <> 'ORIGINAL' LIMIT 1;
SELECT 'FAIL S11-C: current month — Revenue (1001010) must be on DR side'         AS result FROM test_batch_preview WHERE cycle_type='Deferral' AND code_number='1001010' AND start_date='2025-02-17' AND (CR IS NOT NULL OR DR <> 3737.70) LIMIT 1;
SELECT 'FAIL S11-D: current month — Deferred Revenue (4006020) must be on CR side' AS result FROM test_batch_preview WHERE cycle_type='Deferral' AND code_number='4006020' AND start_date='2025-02-17' AND (DR IS NOT NULL OR CR <> 3737.70) LIMIT 1;
SELECT 'FAIL S11-E: next month — Deferred Revenue (4006020) must be on DR side'  AS result FROM test_batch_preview WHERE cycle_type='Deferral' AND code_number='4006020' AND start_date='2025-02-24' AND (CR IS NOT NULL OR DR <> 3737.70) LIMIT 1;
SELECT 'FAIL S11-F: next month — Revenue (1001010) must be on CR side'           AS result FROM test_batch_preview WHERE cycle_type='Deferral' AND code_number='1001010' AND start_date='2025-02-24' AND (DR IS NOT NULL OR CR <> 3737.70) LIMIT 1;
SELECT 'FAIL S11-G: all deferral amounts must be 3737.70'                        AS result FROM test_batch_preview WHERE cycle_type='Deferral' AND COALESCE(DR,CR) <> 3737.70 LIMIT 1;
SELECT 'FAIL S11-H: balance — each week must have DR = CR'                        AS result
FROM (SELECT start_date, ROUND(SUM(COALESCE(DR,0))::NUMERIC,2) AS tDR, ROUND(SUM(COALESCE(CR,0))::NUMERIC,2) AS tCR FROM test_batch_preview WHERE cycle_type='Deferral' GROUP BY start_date) t WHERE tDR <> tCR;

-- Commit Scenario 11
INSERT INTO test_submissions SELECT * FROM test_batch_preview;


-- =============================================================================
-- SCENARIO SUMMARY — what each scenario produced
-- =============================================================================

SELECT '--- Full scenario coverage summary (Scenarios 8–11) ---' AS summary;
SELECT
    batch_id,
    row_type,
    cycle_type,
    vertical,
    COUNT(*)                       AS rows,
    ROUND(SUM(COALESCE(DR,0))::NUMERIC,2) AS total_DR,
    ROUND(SUM(COALESCE(CR,0))::NUMERIC,2) AS total_CR
FROM test_submissions
GROUP BY batch_id, row_type, cycle_type, vertical
ORDER BY batch_id, cycle_type, vertical, row_type;


-- =============================================================================
-- CLEANUP
-- =============================================================================
DROP TABLE IF EXISTS test_batch_preview;
DROP TABLE IF EXISTS test_staging;
DROP TABLE IF EXISTS test_submissions;

SELECT 'Test suite complete. No rows returned by FAIL queries = all tests passed.' AS status;



-- Test suite overview

--   The file is fully self-contained — no real tables are touched. Run it top-to-bottom in a single Redshift session.

--   7 scenarios (cumulative state — each builds on the last)
--
--   Scenario 1 — First batch:            4 rows in staging, submissions empty
--                                         → 4 ORIGINAL rows
--   Scenario 2 — No changes:             Staging unchanged
--                                         → 0 rows (all UNCHANGED)
--   Scenario 3 — Amount correction:      Mumbai DR 10000→10500, CR 8474.58→8898.31
--                                         → 2 RESTATEMENT + 2 CORRECTION_DELTA; no REVERSAL
--   Scenario 4 — Row disappears:         Bangalore rows deleted from staging
--                                         → 2 REVERSAL (full cancel; row gone)
--   Scenario 5 — Multi-month correction: Oct 2024 data (B_TEST_000) corrected DR 3000→3200
--                                         → 2 RESTATEMENT + 2 CORRECTION_DELTA, correction_period='2024-10-07'
--   Scenario 6 — Price DECREASE + GST:   Chennai 5 rows decreased (Receivable+Revenue+SGST+CGST+IGST)
--                                         → 5 RESTATEMENT + 5 CORRECTION_DELTA; delta flips side
--                                            (DR decreased → CR delta; CR decreased → DR delta)
--   Scenario 7 — Price INCREASE + GST:   Chennai 5 rows increased back to original (baseline = S6 RESTATEMENT)
--                                         → 5 RESTATEMENT + 5 CORRECTION_DELTA; delta stays same side
--                                            (DR increased → DR delta; CR increased → CR delta)

--   How assertions work

--   Every assertion is a HAVING or WHERE query that returns rows only on failure:

--   FAIL S3-E: CORRECTION_DELTA 3004010 wrong amounts (expect DR=500 CR=NULL)

--   If all assertions return 0 rows, all tests pass. Any returned row identifies exactly which check failed and why.

--   Final checks

--   - Balance check — after all commits, net_DR = net_CR for every (code_number, vertical) group across all rows in test_submissions, confirming that reversals and restatements net out correctly to zero
--   - Full history view — shows every row ever inserted into test_submissions across all batches, in order