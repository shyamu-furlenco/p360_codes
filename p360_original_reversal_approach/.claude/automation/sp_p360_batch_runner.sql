-- =============================================================================
-- STORED PROCEDURES: sp_p360_batch_runner + sp_p360_batch_force_commit
--
-- sp_p360_batch_runner
--   Runs the full batch workflow (Steps 0–3) unattended.
--   Auto-approves batches that pass all three threshold checks; sends a Slack
--   review alert when a check fails instead of committing.
--
-- sp_p360_batch_force_commit(p_batch_id)
--   Human-override for batches stuck in SKIPPED state.
--   Re-generates the preview from live staging and commits, bypassing thresholds.
--
-- Mode: NONATOMIC — required so DDL (CREATE TEMP TABLE) and explicit COMMIT /
--       ROLLBACK can be used together inside the procedure.
--
-- Approval thresholds (change here to tune):
--   ROW_COUNT_VARIANCE_THRESHOLD  NUMERIC := 5.0;   -- ±% vs 30-day avg
--   CORRECTION_RATE_THRESHOLD     NUMERIC := 20.0;  -- % of P360 rows
--
-- Scheduled at: configure to your batch cadence (e.g., weekly Monday 1 AM UTC)
--   Cron example: 0 1 ? * 2 *
--
-- Manual run: CALL sp_p360_batch_runner();
-- =============================================================================


-- =============================================================================
-- sp_p360_batch_runner — full automated batch
-- =============================================================================

CREATE OR REPLACE PROCEDURE sp_p360_batch_runner()
NONATOMIC
AS $$
DECLARE
    -- Tunable thresholds
    ROW_COUNT_VARIANCE_THRESHOLD  NUMERIC := 5.0;   -- percent
    CORRECTION_RATE_THRESHOLD     NUMERIC := 20.0;  -- percent

    -- Working variables
    v_batch_id            VARCHAR(20);
    v_is_balanced         BOOLEAN;
    v_preview_rows        INTEGER;
    v_restatement_rows    INTEGER;
    v_correction_rows     INTEGER;
    v_total_dr            NUMERIC(15,4);
    v_total_cr            NUMERIC(15,4);
    v_avg_recent          NUMERIC(10,2);
    v_p360_rows           INTEGER;
    v_variance_pct        NUMERIC(6,2);
    v_correction_pct      NUMERIC(6,2);
    v_pre_insert_count    INTEGER;
    v_state_dr            NUMERIC(15,4);
    v_state_cr            NUMERIC(15,4);
    v_approval_ok         BOOLEAN := TRUE;
    v_stop_reason         VARCHAR(500) := '';
    v_msg                 VARCHAR(2000);

BEGIN

    -- =========================================================================
    -- STEP 0 — ALLOCATE BATCH ID (ATOMIC)
    -- UPDATE + CREATE TEMP TABLE are two separate statements, but the CREATE
    -- (DDL) auto-commits the UPDATE, making the allocation durable before
    -- anything else happens.
    -- =========================================================================

    DROP TABLE IF EXISTS p360_current_batch;

    -- Atomically advance the sequence counter
    UPDATE p360_batch_control
    SET
        last_batch_seq  = CASE
                              WHEN last_batch_date = CURRENT_DATE THEN last_batch_seq + 1
                              ELSE 1
                          END,
        last_batch_date = CURRENT_DATE,
        updated_at      = CURRENT_TIMESTAMP
    WHERE control_key = 'BATCH_SEQ';

    -- Materialise the allocated batch_id in a temp table so all subsequent
    -- CTEs can reference it with  SELECT batch_id FROM p360_current_batch
    -- (DDL auto-commits the UPDATE above)
    CREATE TEMP TABLE p360_current_batch AS
    SELECT
        'B_' || TO_CHAR(last_batch_date, 'YYYYMMDD') || '_' ||
        LPAD(CAST(last_batch_seq AS VARCHAR), 3, '0') AS batch_id
    FROM p360_batch_control
    WHERE control_key = 'BATCH_SEQ';

    -- Read into a variable for procedural use
    SELECT batch_id INTO v_batch_id FROM p360_current_batch;

    -- =========================================================================
    -- STEP 1 — GENERATE PREVIEW
    -- Full delta-based CTE — identical logic to p360_batch_runner.sql Step 1.
    -- Safe to re-run: DROP TABLE IF EXISTS ensures a clean slate.
    -- =========================================================================

    DROP TABLE IF EXISTS p360_batch_preview;

    CREATE TEMP TABLE p360_batch_preview AS
    WITH

    new_batch AS (
        SELECT batch_id FROM p360_current_batch
    ),

    current_state AS (
        SELECT
            code_number, particulars, cum_dr AS DR, cum_cr AS CR,
            city_name, cycle_type, vertical, city_id,
            store_id, organization_id, organization_email_id,
            recognised_date, remarks,
            last_batch_id AS batch_id
        FROM p360_delta_state
    ),

    current_data AS (
        SELECT
            code_number, particulars, DR, CR,
            city_name, cycle_type, vertical, city_id,
            store_id, organization_id, organization_email_id,
            recognised_date, remarks
        FROM p360_staging
    ),

    comparison AS (
        SELECT
            COALESCE(cur.code_number,           st.code_number)           AS code_number,
            COALESCE(cur.particulars,           st.particulars)           AS particulars,
            COALESCE(cur.city_name,             st.city_name)             AS city_name,
            COALESCE(cur.cycle_type,            st.cycle_type)            AS cycle_type,
            COALESCE(cur.vertical,              st.vertical)              AS vertical,
            COALESCE(cur.city_id,               st.city_id)               AS city_id,
            COALESCE(cur.store_id,              st.store_id)              AS store_id,
            COALESCE(cur.organization_id,       st.organization_id)       AS organization_id,
            COALESCE(cur.organization_email_id, st.organization_email_id) AS organization_email_id,
            COALESCE(cur.recognised_date,       st.recognised_date)       AS recognised_date,
            COALESCE(cur.remarks,               st.remarks)               AS remarks,

            cur.DR   AS cur_DR,
            cur.CR   AS cur_CR,
            st.DR    AS old_DR,
            st.CR    AS old_CR,
            st.batch_id AS last_batch_id,

            CASE
                WHEN st.code_number IS NULL
                    THEN 'ORIGINAL'
                WHEN cur.code_number IS NULL
                    THEN 'REVERSAL_ONLY'
                WHEN ROUND(COALESCE(cur.DR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.DR, 0)::NUMERIC, 4)
                  OR ROUND(COALESCE(cur.CR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.CR, 0)::NUMERIC, 4)
                    THEN 'CORRECTION'
                ELSE 'UNCHANGED'
            END AS action

        FROM current_data cur
        FULL OUTER JOIN current_state st
          ON cur.code_number     = st.code_number
         AND cur.city_id         = st.city_id
         AND cur.vertical        = st.vertical
         AND cur.cycle_type      = st.cycle_type
         AND cur.recognised_date = st.recognised_date
         AND cur.organization_id = st.organization_id
         AND COALESCE(cur.store_id, '') = COALESCE(st.store_id, '')
    ),

    batch_bounds AS (
        SELECT
            MIN(recognised_date) AS cycle_start,
            MAX(recognised_date) AS cycle_end
        FROM comparison
        WHERE action IN ('ORIGINAL', 'REVERSAL_ONLY', 'CORRECTION')
    ),

    delta_date AS (
        SELECT COALESCE(
            MAX(CASE WHEN action = 'ORIGINAL' THEN recognised_date END),
            CURRENT_DATE
        ) AS recognised_date
        FROM comparison
    ),

    output_rows AS (

        -- ORIGINAL
        SELECT
            cmp.code_number, cmp.particulars,
            cmp.cur_DR  AS DR,
            cmp.cur_CR  AS CR,
            cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
            cmp.store_id, cmp.organization_id, cmp.organization_email_id,
            cmp.recognised_date, cmp.remarks,
            nb.batch_id,
            CURRENT_DATE         AS submission_date,
            bb.cycle_start,
            bb.cycle_end,
            'ORIGINAL'::VARCHAR  AS row_type,
            NULL::VARCHAR        AS reference_batch_id,
            NULL::DATE           AS correction_period
        FROM comparison cmp
        CROSS JOIN new_batch nb
        CROSS JOIN batch_bounds bb
        WHERE cmp.action = 'ORIGINAL'

        UNION ALL

        -- REVERSAL
        SELECT
            cmp.code_number, cmp.particulars,
            cmp.old_CR  AS DR,
            cmp.old_DR  AS CR,
            cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
            cmp.store_id, cmp.organization_id, cmp.organization_email_id,
            cmp.recognised_date, cmp.remarks,
            nb.batch_id,
            CURRENT_DATE          AS submission_date,
            bb.cycle_start,
            bb.cycle_end,
            'REVERSAL'::VARCHAR   AS row_type,
            cmp.last_batch_id     AS reference_batch_id,
            NULL::DATE            AS correction_period
        FROM comparison cmp
        CROSS JOIN new_batch nb
        CROSS JOIN batch_bounds bb
        WHERE cmp.action = 'REVERSAL_ONLY'

        UNION ALL

        -- RESTATEMENT (silent state marker — not sent to P360)
        SELECT
            cmp.code_number, cmp.particulars,
            cmp.cur_DR  AS DR,
            cmp.cur_CR  AS CR,
            cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
            cmp.store_id, cmp.organization_id, cmp.organization_email_id,
            cmp.recognised_date, cmp.remarks,
            nb.batch_id,
            CURRENT_DATE             AS submission_date,
            bb.cycle_start,
            bb.cycle_end,
            'RESTATEMENT'::VARCHAR   AS row_type,
            cmp.last_batch_id        AS reference_batch_id,
            NULL::DATE               AS correction_period
        FROM comparison cmp
        CROSS JOIN new_batch nb
        CROSS JOIN batch_bounds bb
        WHERE cmp.action = 'CORRECTION'

        UNION ALL

        -- CORRECTION_DELTA (net change — sent to P360)
        SELECT
            cmp.code_number, cmp.particulars,
            CASE
                WHEN COALESCE(cmp.cur_DR, 0) - COALESCE(cmp.old_DR, 0) > 0
                    THEN COALESCE(cmp.cur_DR, 0) - COALESCE(cmp.old_DR, 0)
                WHEN COALESCE(cmp.old_CR, 0) - COALESCE(cmp.cur_CR, 0) > 0
                    THEN COALESCE(cmp.old_CR, 0) - COALESCE(cmp.cur_CR, 0)
                ELSE NULL
            END AS DR,
            CASE
                WHEN COALESCE(cmp.cur_CR, 0) - COALESCE(cmp.old_CR, 0) > 0
                    THEN COALESCE(cmp.cur_CR, 0) - COALESCE(cmp.old_CR, 0)
                WHEN COALESCE(cmp.old_DR, 0) - COALESCE(cmp.cur_DR, 0) > 0
                    THEN COALESCE(cmp.old_DR, 0) - COALESCE(cmp.cur_DR, 0)
                ELSE NULL
            END AS CR,
            cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
            cmp.store_id, cmp.organization_id, cmp.organization_email_id,
            dd.recognised_date, cmp.remarks,
            nb.batch_id,
            CURRENT_DATE                  AS submission_date,
            bb.cycle_start,
            bb.cycle_end,
            'CORRECTION_DELTA'::VARCHAR   AS row_type,
            cmp.last_batch_id             AS reference_batch_id,
            cmp.recognised_date           AS correction_period
        FROM comparison cmp
        CROSS JOIN new_batch nb
        CROSS JOIN batch_bounds bb
        CROSS JOIN delta_date dd
        WHERE cmp.action = 'CORRECTION'
    )

    SELECT
        code_number, particulars, DR, CR,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        recognised_date, remarks,
        batch_id, submission_date, cycle_start, cycle_end,
        row_type, reference_batch_id, correction_period
    FROM output_rows;

    -- =========================================================================
    -- STEP 1.5 — LOG AUDIT RECORD (PENDING status before approval decision)
    -- =========================================================================

    INSERT INTO p360_batch_audit (
        batch_id, submission_date, cycle_start, cycle_end,
        staging_rows, preview_rows, original_rows, reversal_rows,
        restatement_rows, correction_delta_rows,
        total_dr, total_cr, is_balanced, started_at, status
    )
    SELECT
        batch_id,
        CURRENT_DATE,
        MIN(cycle_start),
        MAX(cycle_end),
        (SELECT COUNT(*) FROM p360_staging),
        COUNT(*),
        SUM(CASE WHEN row_type = 'ORIGINAL'          THEN 1 ELSE 0 END),
        SUM(CASE WHEN row_type = 'REVERSAL'          THEN 1 ELSE 0 END),
        SUM(CASE WHEN row_type = 'RESTATEMENT'       THEN 1 ELSE 0 END),
        SUM(CASE WHEN row_type = 'CORRECTION_DELTA'  THEN 1 ELSE 0 END),
        SUM(COALESCE(DR, 0)),
        SUM(COALESCE(CR, 0)),
        ABS(SUM(COALESCE(DR, 0)) - SUM(COALESCE(CR, 0))) < 0.01,
        CURRENT_TIMESTAMP,
        'PENDING'
    FROM p360_batch_preview
    GROUP BY batch_id;

    -- Commit the audit row so it is visible even if we later ROLLBACK Step 2
    COMMIT;

    -- =========================================================================
    -- Read audit fields for the approval engine
    -- =========================================================================

    SELECT is_balanced, preview_rows, restatement_rows, correction_delta_rows,
           total_dr, total_cr
    INTO   v_is_balanced, v_preview_rows, v_restatement_rows, v_correction_rows,
           v_total_dr, v_total_cr
    FROM p360_batch_audit
    WHERE batch_id = v_batch_id;

    -- P360-facing rows = total preview − silent RESTATEMENT markers
    v_p360_rows := COALESCE(v_preview_rows, 0) - COALESCE(v_restatement_rows, 0);

    -- =========================================================================
    -- APPROVAL CHECK 1: Must be balanced (DR ≈ CR within 0.01)
    -- =========================================================================
    IF NOT COALESCE(v_is_balanced, FALSE) THEN
        v_approval_ok := FALSE;
        v_stop_reason := v_stop_reason
            || 'IMBALANCED (DR=' || v_total_dr || ' CR=' || v_total_cr || '). ';
    END IF;

    -- =========================================================================
    -- APPROVAL CHECK 2: Row count must be within ±ROW_COUNT_VARIANCE_THRESHOLD%
    --                   of the 30-day committed average
    -- =========================================================================
    SELECT AVG(preview_rows - COALESCE(restatement_rows, 0))
    INTO   v_avg_recent
    FROM   p360_batch_audit
    WHERE  submission_date >= CURRENT_DATE - 30
      AND  status = 'COMMITTED';

    IF v_avg_recent IS NOT NULL AND v_avg_recent > 0 THEN
        v_variance_pct := ABS(v_p360_rows - v_avg_recent) / v_avg_recent * 100;
        IF v_variance_pct > ROW_COUNT_VARIANCE_THRESHOLD THEN
            v_approval_ok := FALSE;
            v_stop_reason := v_stop_reason
                || 'ROW_COUNT_VARIANCE=' || ROUND(v_variance_pct, 1)
                || '% (threshold ' || ROW_COUNT_VARIANCE_THRESHOLD || '%). ';
        END IF;
    END IF;

    -- =========================================================================
    -- APPROVAL CHECK 3: Correction delta rate must be ≤ CORRECTION_RATE_THRESHOLD%
    -- =========================================================================
    IF v_p360_rows > 0 THEN
        v_correction_pct :=
            COALESCE(v_correction_rows, 0)::NUMERIC / v_p360_rows * 100;
        IF v_correction_pct > CORRECTION_RATE_THRESHOLD THEN
            v_approval_ok := FALSE;
            v_stop_reason := v_stop_reason
                || 'CORRECTION_RATE=' || ROUND(v_correction_pct, 1)
                || '% (threshold ' || CORRECTION_RATE_THRESHOLD || '%). ';
        END IF;
    END IF;

    -- =========================================================================
    -- APPROVAL GATE — skip commit and alert if any check failed
    -- =========================================================================
    IF NOT v_approval_ok THEN
        UPDATE p360_batch_audit
        SET    status        = 'SKIPPED',
               error_message = 'Auto-approval failed: ' || v_stop_reason
        WHERE  batch_id = v_batch_id;

        COMMIT;   -- persist SKIPPED status

        v_msg := '*P360 Batch ' || v_batch_id || ' — MANUAL REVIEW REQUIRED*'
              || chr(10) || 'Reason: '    || v_stop_reason
              || chr(10) || 'P360 rows: ' || v_p360_rows
              || chr(10) || 'DR: '        || v_total_dr || '  CR: ' || v_total_cr
              || chr(10) || 'To commit manually: CALL sp_p360_batch_force_commit('''
              ||            v_batch_id || ''');';
        PERFORM f_slack_notify(v_msg);
        RETURN;
    END IF;

    -- =========================================================================
    -- STEP 2 — COMMIT
    -- Mark IN_PROGRESS, lock table, insert, merge state, update audit, commit.
    -- All DML below is in one implicit transaction; COMMIT makes it atomic.
    -- =========================================================================

    UPDATE p360_batch_audit
    SET    status       = 'IN_PROGRESS',
           committed_at = CURRENT_TIMESTAMP
    WHERE  batch_id = v_batch_id
      AND  status   = 'PENDING';

    -- Exclusive lock prevents concurrent batch commits
    LOCK TABLE p360_submissions;

    -- Capture row count before INSERT (defensive; freshly-allocated batch_id
    -- should always return 0, but idempotent check below handles retries)
    SELECT COUNT(*) INTO v_pre_insert_count
    FROM   p360_submissions
    WHERE  batch_id = v_batch_id;

    -- Idempotent INSERT: skip rows already in submissions for this batch_id
    INSERT INTO p360_submissions (
        code_number, particulars, DR, CR, city_name, cycle_type, vertical,
        city_id, store_id, organization_id, organization_email_id,
        recognised_date, remarks, batch_id, submission_date,
        cycle_start, cycle_end, row_type, reference_batch_id, correction_period
    )
    SELECT
        p.code_number, p.particulars, p.DR, p.CR, p.city_name, p.cycle_type, p.vertical,
        p.city_id, p.store_id, p.organization_id, p.organization_email_id,
        p.recognised_date, p.remarks, p.batch_id, p.submission_date,
        p.cycle_start, p.cycle_end, p.row_type, p.reference_batch_id, p.correction_period
    FROM p360_batch_preview p
    WHERE NOT EXISTS (
        SELECT 1 FROM p360_submissions s
        WHERE s.batch_id        = p.batch_id
          AND s.code_number     = p.code_number
          AND s.city_id         = p.city_id
          AND s.vertical        = p.vertical
          AND s.cycle_type      = p.cycle_type
          AND s.recognised_date = p.recognised_date
          AND s.organization_id = p.organization_id
          AND COALESCE(s.store_id, '') = COALESCE(p.store_id, '')
          AND s.row_type        = p.row_type
    );

    -- ── State merge ──────────────────────────────────────────────────────────

    -- Step 2a: Remove reversed rows from state
    DELETE FROM p360_delta_state
    WHERE (code_number, city_id, vertical, cycle_type, recognised_date,
           organization_id, COALESCE(store_id, '')) IN (
        SELECT code_number, city_id, vertical, cycle_type, recognised_date,
               organization_id, COALESCE(store_id, '')
        FROM p360_batch_preview
        WHERE row_type = 'REVERSAL'
    );

    -- Step 2b: Update existing state rows for ORIGINAL and RESTATEMENT
    UPDATE p360_delta_state
    SET
        cum_dr                = COALESCE(p.DR, 0),
        cum_cr                = COALESCE(p.CR, 0),
        particulars           = p.particulars,
        city_name             = p.city_name,
        organization_email_id = p.organization_email_id,
        remarks               = p.remarks,
        last_batch_id         = p.batch_id,
        updated_at            = CURRENT_TIMESTAMP
    FROM (
        SELECT DISTINCT code_number, city_id, vertical, cycle_type, recognised_date,
               organization_id, store_id, DR, CR, particulars, city_name,
               organization_email_id, remarks, batch_id
        FROM p360_batch_preview
        WHERE row_type IN ('ORIGINAL', 'RESTATEMENT')
    ) p
    WHERE p360_delta_state.code_number     = p.code_number
      AND p360_delta_state.city_id         = p.city_id
      AND p360_delta_state.vertical        = p.vertical
      AND p360_delta_state.cycle_type      = p.cycle_type
      AND p360_delta_state.recognised_date = p.recognised_date
      AND p360_delta_state.organization_id = p.organization_id
      AND COALESCE(p360_delta_state.store_id, '') = COALESCE(p.store_id, '');

    -- Step 2c: Insert new state rows for ORIGINAL rows not yet in state
    INSERT INTO p360_delta_state (
        code_number, city_id, vertical, cycle_type, recognised_date,
        organization_id, store_id, particulars, city_name,
        organization_email_id, remarks, cum_dr, cum_cr,
        last_batch_id, created_at, updated_at
    )
    SELECT
        p.code_number, p.city_id, p.vertical, p.cycle_type, p.recognised_date,
        p.organization_id, COALESCE(p.store_id, ''), p.particulars, p.city_name,
        p.organization_email_id, p.remarks, COALESCE(p.DR, 0), COALESCE(p.CR, 0),
        p.batch_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    FROM p360_batch_preview p
    WHERE p.row_type = 'ORIGINAL'
      AND NOT EXISTS (
          SELECT 1 FROM p360_delta_state st
          WHERE st.code_number     = p.code_number
            AND st.city_id         = p.city_id
            AND st.vertical        = p.vertical
            AND st.cycle_type      = p.cycle_type
            AND st.recognised_date = p.recognised_date
            AND st.organization_id = p.organization_id
            AND COALESCE(st.store_id, '') = COALESCE(p.store_id, '')
      );

    -- Update audit with actual committed row count and final status
    UPDATE p360_batch_audit
    SET
        committed_rows = (
            SELECT COUNT(*) FROM p360_submissions
            WHERE batch_id = v_batch_id
        ) - v_pre_insert_count,
        committed_at   = CURRENT_TIMESTAMP,
        status         = 'COMMITTED'
    WHERE batch_id = v_batch_id
      AND status   = 'IN_PROGRESS';

    COMMIT;   -- atomically persist submissions + state merge + audit update

    -- =========================================================================
    -- STEP 3 — VERIFY (post-commit balance check on state table)
    -- =========================================================================

    SELECT SUM(cum_dr), SUM(cum_cr)
    INTO   v_state_dr, v_state_cr
    FROM   p360_delta_state;

    -- =========================================================================
    -- Success notification
    -- =========================================================================
    v_msg := '*P360 Batch ' || v_batch_id || ' committed*'
          || chr(10) || 'P360 rows: ' || v_p360_rows
          || chr(10) || 'DR: '        || v_total_dr || '  CR: ' || v_total_cr
          || chr(10) || 'State balance — DR: ' || COALESCE(v_state_dr::VARCHAR, 'n/a')
          ||            '  CR: '              || COALESCE(v_state_cr::VARCHAR, 'n/a')
          || chr(10) || 'Date: '      || CURRENT_DATE;
    PERFORM f_slack_notify(v_msg);

    -- Clean up temp tables
    DROP TABLE IF EXISTS p360_batch_preview;
    DROP TABLE IF EXISTS p360_current_batch;

EXCEPTION WHEN OTHERS THEN
    -- Redshift has already rolled back the current (uncommitted) transaction.
    -- We start fresh here to persist the failure record and alert.
    UPDATE p360_batch_audit
    SET    status        = 'FAILED',
           error_message = LEFT(SQLERRM, 1000),
           committed_at  = CURRENT_TIMESTAMP
    WHERE  batch_id = v_batch_id
      AND  status  IN ('PENDING', 'IN_PROGRESS');

    COMMIT;   -- persist FAILED status

    v_msg := '*P360 BATCH FAILED: ' || COALESCE(v_batch_id, '(unallocated)') || '*'
          || chr(10) || 'Error: '  || SQLERRM
          || chr(10) || 'State table is consistent (auto-rollback executed).'
          || chr(10) || 'To retry: CALL sp_p360_batch_runner();';
    PERFORM f_slack_notify(v_msg);

    DROP TABLE IF EXISTS p360_batch_preview;
    DROP TABLE IF EXISTS p360_current_batch;

    RAISE;    -- re-raise so the scheduled query records FAILED status
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- sp_p360_batch_force_commit — human-approved override for SKIPPED batches
--
-- Usage:
--   CALL sp_p360_batch_force_commit('B_20260221_001');
--
-- What it does:
--   1. Verifies the batch is in SKIPPED status (safety guard)
--   2. Restores p360_current_batch temp table with the given batch_id
--   3. Re-generates p360_batch_preview from live staging (idempotent query)
--   4. Runs Step 2 commit (identical logic to sp_p360_batch_runner)
--   5. Notifies Slack with "manually approved" label
--
-- The approver's username is recorded in the audit error_message field.
-- =============================================================================

CREATE OR REPLACE PROCEDURE sp_p360_batch_force_commit(p_batch_id VARCHAR(20))
NONATOMIC
AS $$
DECLARE
    v_status           VARCHAR(20);
    v_preview_rows     INTEGER;
    v_restatement_rows INTEGER;
    v_p360_rows        INTEGER;
    v_total_dr         NUMERIC(15,4);
    v_total_cr         NUMERIC(15,4);
    v_pre_insert_count INTEGER;
    v_state_dr         NUMERIC(15,4);
    v_state_cr         NUMERIC(15,4);
    v_msg              VARCHAR(2000);

BEGIN

    -- ── Safety guard: only operate on SKIPPED batches ────────────────────────
    SELECT status INTO v_status
    FROM   p360_batch_audit
    WHERE  batch_id = p_batch_id
    LIMIT  1;

    IF v_status IS NULL THEN
        RAISE EXCEPTION 'Batch % not found in p360_batch_audit', p_batch_id;
    END IF;

    IF v_status <> 'SKIPPED' THEN
        RAISE EXCEPTION
            'Batch % has status %. Force commit only works on SKIPPED batches.',
            p_batch_id, v_status;
    END IF;

    -- ── Restore p360_current_batch so Step 1 CTEs can reference it ───────────
    DROP TABLE IF EXISTS p360_current_batch;
    CREATE TEMP TABLE p360_current_batch AS
    SELECT p_batch_id AS batch_id;

    -- ── Re-generate preview from live staging (identical Step 1 CTE) ─────────
    DROP TABLE IF EXISTS p360_batch_preview;

    CREATE TEMP TABLE p360_batch_preview AS
    WITH

    new_batch AS (
        SELECT batch_id FROM p360_current_batch
    ),

    current_state AS (
        SELECT
            code_number, particulars, cum_dr AS DR, cum_cr AS CR,
            city_name, cycle_type, vertical, city_id,
            store_id, organization_id, organization_email_id,
            recognised_date, remarks,
            last_batch_id AS batch_id
        FROM p360_delta_state
    ),

    current_data AS (
        SELECT
            code_number, particulars, DR, CR,
            city_name, cycle_type, vertical, city_id,
            store_id, organization_id, organization_email_id,
            recognised_date, remarks
        FROM p360_staging
    ),

    comparison AS (
        SELECT
            COALESCE(cur.code_number,           st.code_number)           AS code_number,
            COALESCE(cur.particulars,           st.particulars)           AS particulars,
            COALESCE(cur.city_name,             st.city_name)             AS city_name,
            COALESCE(cur.cycle_type,            st.cycle_type)            AS cycle_type,
            COALESCE(cur.vertical,              st.vertical)              AS vertical,
            COALESCE(cur.city_id,               st.city_id)               AS city_id,
            COALESCE(cur.store_id,              st.store_id)              AS store_id,
            COALESCE(cur.organization_id,       st.organization_id)       AS organization_id,
            COALESCE(cur.organization_email_id, st.organization_email_id) AS organization_email_id,
            COALESCE(cur.recognised_date,       st.recognised_date)       AS recognised_date,
            COALESCE(cur.remarks,               st.remarks)               AS remarks,
            cur.DR   AS cur_DR,
            cur.CR   AS cur_CR,
            st.DR    AS old_DR,
            st.CR    AS old_CR,
            st.batch_id AS last_batch_id,
            CASE
                WHEN st.code_number IS NULL
                    THEN 'ORIGINAL'
                WHEN cur.code_number IS NULL
                    THEN 'REVERSAL_ONLY'
                WHEN ROUND(COALESCE(cur.DR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.DR, 0)::NUMERIC, 4)
                  OR ROUND(COALESCE(cur.CR, 0)::NUMERIC, 4) <> ROUND(COALESCE(st.CR, 0)::NUMERIC, 4)
                    THEN 'CORRECTION'
                ELSE 'UNCHANGED'
            END AS action
        FROM current_data cur
        FULL OUTER JOIN current_state st
          ON cur.code_number     = st.code_number
         AND cur.city_id         = st.city_id
         AND cur.vertical        = st.vertical
         AND cur.cycle_type      = st.cycle_type
         AND cur.recognised_date = st.recognised_date
         AND cur.organization_id = st.organization_id
         AND COALESCE(cur.store_id, '') = COALESCE(st.store_id, '')
    ),

    batch_bounds AS (
        SELECT MIN(recognised_date) AS cycle_start, MAX(recognised_date) AS cycle_end
        FROM comparison
        WHERE action IN ('ORIGINAL', 'REVERSAL_ONLY', 'CORRECTION')
    ),

    delta_date AS (
        SELECT COALESCE(MAX(CASE WHEN action = 'ORIGINAL' THEN recognised_date END), CURRENT_DATE) AS recognised_date
        FROM comparison
    ),

    output_rows AS (

        SELECT
            cmp.code_number, cmp.particulars,
            cmp.cur_DR AS DR, cmp.cur_CR AS CR,
            cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
            cmp.store_id, cmp.organization_id, cmp.organization_email_id,
            cmp.recognised_date, cmp.remarks,
            nb.batch_id, CURRENT_DATE AS submission_date, bb.cycle_start, bb.cycle_end,
            'ORIGINAL'::VARCHAR AS row_type, NULL::VARCHAR AS reference_batch_id, NULL::DATE AS correction_period
        FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb
        WHERE cmp.action = 'ORIGINAL'

        UNION ALL

        SELECT
            cmp.code_number, cmp.particulars,
            cmp.old_CR AS DR, cmp.old_DR AS CR,
            cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
            cmp.store_id, cmp.organization_id, cmp.organization_email_id,
            cmp.recognised_date, cmp.remarks,
            nb.batch_id, CURRENT_DATE AS submission_date, bb.cycle_start, bb.cycle_end,
            'REVERSAL'::VARCHAR AS row_type, cmp.last_batch_id AS reference_batch_id, NULL::DATE AS correction_period
        FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb
        WHERE cmp.action = 'REVERSAL_ONLY'

        UNION ALL

        SELECT
            cmp.code_number, cmp.particulars,
            cmp.cur_DR AS DR, cmp.cur_CR AS CR,
            cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
            cmp.store_id, cmp.organization_id, cmp.organization_email_id,
            cmp.recognised_date, cmp.remarks,
            nb.batch_id, CURRENT_DATE AS submission_date, bb.cycle_start, bb.cycle_end,
            'RESTATEMENT'::VARCHAR AS row_type, cmp.last_batch_id AS reference_batch_id, NULL::DATE AS correction_period
        FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb
        WHERE cmp.action = 'CORRECTION'

        UNION ALL

        SELECT
            cmp.code_number, cmp.particulars,
            CASE
                WHEN COALESCE(cmp.cur_DR, 0) - COALESCE(cmp.old_DR, 0) > 0 THEN COALESCE(cmp.cur_DR, 0) - COALESCE(cmp.old_DR, 0)
                WHEN COALESCE(cmp.old_CR, 0) - COALESCE(cmp.cur_CR, 0) > 0 THEN COALESCE(cmp.old_CR, 0) - COALESCE(cmp.cur_CR, 0)
                ELSE NULL
            END AS DR,
            CASE
                WHEN COALESCE(cmp.cur_CR, 0) - COALESCE(cmp.old_CR, 0) > 0 THEN COALESCE(cmp.cur_CR, 0) - COALESCE(cmp.old_CR, 0)
                WHEN COALESCE(cmp.old_DR, 0) - COALESCE(cmp.cur_DR, 0) > 0 THEN COALESCE(cmp.old_DR, 0) - COALESCE(cmp.cur_DR, 0)
                ELSE NULL
            END AS CR,
            cmp.city_name, cmp.cycle_type, cmp.vertical, cmp.city_id,
            cmp.store_id, cmp.organization_id, cmp.organization_email_id,
            dd.recognised_date, cmp.remarks,
            nb.batch_id, CURRENT_DATE AS submission_date, bb.cycle_start, bb.cycle_end,
            'CORRECTION_DELTA'::VARCHAR AS row_type, cmp.last_batch_id AS reference_batch_id, cmp.recognised_date AS correction_period
        FROM comparison cmp CROSS JOIN new_batch nb CROSS JOIN batch_bounds bb CROSS JOIN delta_date dd
        WHERE cmp.action = 'CORRECTION'
    )

    SELECT
        code_number, particulars, DR, CR,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        recognised_date, remarks,
        batch_id, submission_date, cycle_start, cycle_end,
        row_type, reference_batch_id, correction_period
    FROM output_rows;

    -- Compute totals for notification
    SELECT
        COUNT(*) - SUM(CASE WHEN row_type = 'RESTATEMENT' THEN 1 ELSE 0 END),
        SUM(CASE WHEN row_type = 'RESTATEMENT' THEN 1 ELSE 0 END),
        SUM(COALESCE(DR, 0)),
        SUM(COALESCE(CR, 0))
    INTO v_preview_rows, v_restatement_rows, v_total_dr, v_total_cr
    FROM p360_batch_preview;

    v_p360_rows := v_preview_rows - v_restatement_rows;

    -- ── Step 2: Commit ────────────────────────────────────────────────────────

    UPDATE p360_batch_audit
    SET    status        = 'IN_PROGRESS',
           error_message = 'Manually approved by ' || CURRENT_USER
                        || ' at ' || CURRENT_TIMESTAMP,
           committed_at  = CURRENT_TIMESTAMP
    WHERE  batch_id = p_batch_id
      AND  status   = 'SKIPPED';

    LOCK TABLE p360_submissions;

    SELECT COUNT(*) INTO v_pre_insert_count
    FROM   p360_submissions
    WHERE  batch_id = p_batch_id;

    INSERT INTO p360_submissions (
        code_number, particulars, DR, CR, city_name, cycle_type, vertical,
        city_id, store_id, organization_id, organization_email_id,
        recognised_date, remarks, batch_id, submission_date,
        cycle_start, cycle_end, row_type, reference_batch_id, correction_period
    )
    SELECT
        p.code_number, p.particulars, p.DR, p.CR, p.city_name, p.cycle_type, p.vertical,
        p.city_id, p.store_id, p.organization_id, p.organization_email_id,
        p.recognised_date, p.remarks, p.batch_id, p.submission_date,
        p.cycle_start, p.cycle_end, p.row_type, p.reference_batch_id, p.correction_period
    FROM p360_batch_preview p
    WHERE NOT EXISTS (
        SELECT 1 FROM p360_submissions s
        WHERE s.batch_id        = p.batch_id
          AND s.code_number     = p.code_number
          AND s.city_id         = p.city_id
          AND s.vertical        = p.vertical
          AND s.cycle_type      = p.cycle_type
          AND s.recognised_date = p.recognised_date
          AND s.organization_id = p.organization_id
          AND COALESCE(s.store_id, '') = COALESCE(p.store_id, '')
          AND s.row_type        = p.row_type
    );

    DELETE FROM p360_delta_state
    WHERE (code_number, city_id, vertical, cycle_type, recognised_date,
           organization_id, COALESCE(store_id, '')) IN (
        SELECT code_number, city_id, vertical, cycle_type, recognised_date,
               organization_id, COALESCE(store_id, '')
        FROM p360_batch_preview
        WHERE row_type = 'REVERSAL'
    );

    UPDATE p360_delta_state
    SET
        cum_dr                = COALESCE(p.DR, 0),
        cum_cr                = COALESCE(p.CR, 0),
        particulars           = p.particulars,
        city_name             = p.city_name,
        organization_email_id = p.organization_email_id,
        remarks               = p.remarks,
        last_batch_id         = p.batch_id,
        updated_at            = CURRENT_TIMESTAMP
    FROM (
        SELECT DISTINCT code_number, city_id, vertical, cycle_type, recognised_date,
               organization_id, store_id, DR, CR, particulars, city_name,
               organization_email_id, remarks, batch_id
        FROM p360_batch_preview
        WHERE row_type IN ('ORIGINAL', 'RESTATEMENT')
    ) p
    WHERE p360_delta_state.code_number     = p.code_number
      AND p360_delta_state.city_id         = p.city_id
      AND p360_delta_state.vertical        = p.vertical
      AND p360_delta_state.cycle_type      = p.cycle_type
      AND p360_delta_state.recognised_date = p.recognised_date
      AND p360_delta_state.organization_id = p.organization_id
      AND COALESCE(p360_delta_state.store_id, '') = COALESCE(p.store_id, '');

    INSERT INTO p360_delta_state (
        code_number, city_id, vertical, cycle_type, recognised_date,
        organization_id, store_id, particulars, city_name,
        organization_email_id, remarks, cum_dr, cum_cr,
        last_batch_id, created_at, updated_at
    )
    SELECT
        p.code_number, p.city_id, p.vertical, p.cycle_type, p.recognised_date,
        p.organization_id, COALESCE(p.store_id, ''), p.particulars, p.city_name,
        p.organization_email_id, p.remarks, COALESCE(p.DR, 0), COALESCE(p.CR, 0),
        p.batch_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    FROM p360_batch_preview p
    WHERE p.row_type = 'ORIGINAL'
      AND NOT EXISTS (
          SELECT 1 FROM p360_delta_state st
          WHERE st.code_number     = p.code_number
            AND st.city_id         = p.city_id
            AND st.vertical        = p.vertical
            AND st.cycle_type      = p.cycle_type
            AND st.recognised_date = p.recognised_date
            AND st.organization_id = p.organization_id
            AND COALESCE(st.store_id, '') = COALESCE(p.store_id, '')
      );

    UPDATE p360_batch_audit
    SET
        committed_rows = (
            SELECT COUNT(*) FROM p360_submissions WHERE batch_id = p_batch_id
        ) - v_pre_insert_count,
        committed_at   = CURRENT_TIMESTAMP,
        status         = 'COMMITTED'
    WHERE batch_id = p_batch_id
      AND status   = 'IN_PROGRESS';

    COMMIT;

    -- ── Step 3: Post-commit state balance ─────────────────────────────────────
    SELECT SUM(cum_dr), SUM(cum_cr) INTO v_state_dr, v_state_cr
    FROM   p360_delta_state;

    v_msg := '*P360 Batch ' || p_batch_id || ' committed (manually approved)*'
          || chr(10) || 'Approved by: ' || CURRENT_USER
          || chr(10) || 'P360 rows: '   || v_p360_rows
          || chr(10) || 'DR: '          || v_total_dr || '  CR: ' || v_total_cr
          || chr(10) || 'State balance — DR: ' || COALESCE(v_state_dr::VARCHAR, 'n/a')
          ||            '  CR: '               || COALESCE(v_state_cr::VARCHAR, 'n/a')
          || chr(10) || 'Date: '        || CURRENT_DATE;
    PERFORM f_slack_notify(v_msg);

    DROP TABLE IF EXISTS p360_batch_preview;
    DROP TABLE IF EXISTS p360_current_batch;

EXCEPTION WHEN OTHERS THEN
    UPDATE p360_batch_audit
    SET    status        = 'FAILED',
           error_message = 'Force commit failed: ' || LEFT(SQLERRM, 900),
           committed_at  = CURRENT_TIMESTAMP
    WHERE  batch_id = p_batch_id
      AND  status  IN ('SKIPPED', 'IN_PROGRESS');

    COMMIT;

    v_msg := '*P360 FORCE COMMIT FAILED: ' || p_batch_id || '*'
          || chr(10) || 'Error: '       || SQLERRM
          || chr(10) || 'Approved by: ' || CURRENT_USER
          || chr(10) || 'State table is consistent (auto-rollback executed).';
    PERFORM f_slack_notify(v_msg);

    DROP TABLE IF EXISTS p360_batch_preview;
    DROP TABLE IF EXISTS p360_current_batch;

    RAISE;
END;
$$ LANGUAGE plpgsql;
