-- =============================================================================
-- P360 BATCH CONTROL — DDL
-- Atomic batch_id sequence table. Prevents race conditions when multiple
-- sessions run p360_batch_runner.sql concurrently.
-- Run once to create the table, then seed with initial row.
-- =============================================================================

CREATE TABLE p360_batch_control (
    control_key          VARCHAR(50)   PRIMARY KEY DEFAULT 'BATCH_SEQ',
    last_batch_date      DATE          NOT NULL,
    last_batch_seq       INT           NOT NULL DEFAULT 0,
    updated_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- Seed the control row (run once after CREATE TABLE)
INSERT INTO p360_batch_control (control_key, last_batch_date, last_batch_seq)
VALUES ('BATCH_SEQ', CURRENT_DATE, 0);

-- =============================================================================
-- USAGE (in p360_batch_runner.sql):
--
-- Instead of:
--   SELECT 'B_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || '_' ||
--          LPAD((SELECT COUNT(DISTINCT batch_id)...) + 1, 3, '0') AS batch_id
--
-- Use atomic UPDATE...RETURNING pattern:
--
--   UPDATE p360_batch_control
--   SET
--       last_batch_seq = CASE
--           WHEN last_batch_date = CURRENT_DATE THEN last_batch_seq + 1
--           ELSE 1
--       END,
--       last_batch_date = CURRENT_DATE,
--       updated_at = CURRENT_TIMESTAMP
--   WHERE control_key = 'BATCH_SEQ'
--   RETURNING 'B_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || '_' ||
--             LPAD(CAST(last_batch_seq AS VARCHAR), 3, '0') AS batch_id;
--
-- This guarantees unique batch_id even under concurrent execution.
-- =============================================================================
