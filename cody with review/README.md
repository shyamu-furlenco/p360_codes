# P360 Auto Correction System — Usage Guide

## Overview

This system automates the detection and correction of P360 journal entries. It compares the current state of source data against everything previously submitted, and generates the exact rows to send — including reversals and restatements for any corrections, regardless of how far back the recognised date falls.

```
Source tables (Redshift)
        ↓  [daily]
  p360_staging_refresh.sql   →   p360_staging   (daily snapshot)
                                        ↓  [batch day]
                               p360_batch_runner.sql
                                        ↓
                               p360_batch_preview  (review)
                                        ↓  [after review]
                               p360_submissions    (permanent ledger)
                                        ↓
                               Send output to P360 team
```

---

## Files

| File | Purpose | Run |
|------|---------|-----|
| `p360_submissions_ddl.sql` | Creates the permanent submission tracking table | Once (setup) |
| `p360_staging_ddl.sql` | Creates the daily staging snapshot table | Once (setup) |
| `p360_staging_refresh.sql` | Truncates and reloads staging from source | Daily (scheduled) |
| `p360_batch_runner.sql` | Detects new/changed rows and generates output | Batch day |
| `p360_final_view_claude.sql` | Source view — **do not modify** | Reference only |

---

## Setup (run once)

Run these two DDL files in order. They only need to be run once to create the tables.

```sql
-- 1. Create the permanent tracking table
\i p360_submissions_ddl.sql

-- 2. Create the daily staging table
\i p360_staging_ddl.sql
```

**Then run the staging refresh once** to populate the staging table with current source data before your first batch:

```sql
\i p360_staging_refresh.sql
```

> **Note:** If the tables already exist, running the DDL files again will error. Use `CREATE TABLE IF NOT EXISTS` or skip if already set up.

---

## Daily Operation — Staging Refresh

**File:** `p360_staging_refresh.sql`
**Schedule:** Run every day (cron / Redshift scheduler / Airflow DAG)
**What it does:** Deletes all rows from `p360_staging` and re-inserts the full current output of the source view logic.

```sql
\i p360_staging_refresh.sql
```

This ensures `p360_staging` always reflects the latest state of source data — including any backdated corrections to revenue recognitions or credit notes.

**To confirm the refresh succeeded:**
```sql
SELECT MAX(refreshed_at) AS last_refresh, COUNT(*) AS row_count
FROM p360_staging;
```
`last_refresh` should match today's date.

**What happens if the refresh fails?**
The script uses `DELETE FROM` (not `TRUNCATE`) inside a transaction. If the `INSERT` fails, the `DELETE` is rolled back automatically — staging retains its previous data and no rows are lost.

---

## Batch Day Operation — Generating the Batch

**File:** `p360_batch_runner.sql`
**Schedule:** Run weekly, monthly, or on-demand for corrections
**What it does:** Compares all rows in `p360_staging` against everything previously submitted in `p360_submissions`, then generates the rows to send.

The batch runner follows a three-step workflow:

---

### Step 0 — Pre-check (staging freshness)

Before running anything, verify staging was refreshed today:

```sql
SELECT MAX(refreshed_at) AS last_refresh, COUNT(*) AS staging_rows
FROM p360_staging;
```

If `last_refresh` is not today, run `p360_staging_refresh.sql` first.

---

### Step 1 — Preview

Run the `CREATE TEMP TABLE` block (lines 29–249 of `p360_batch_runner.sql`). This materialises all rows for the batch into a temporary table called `p360_batch_preview`.

```sql
DROP TABLE IF EXISTS p360_batch_preview;

CREATE TEMP TABLE p360_batch_preview AS
WITH new_batch AS (...), ...
SELECT ... FROM output_rows;
```

Then review the output:

```sql
-- All rows for P360 (excludes silent RESTATEMENT state markers)
SELECT *
FROM p360_batch_preview
WHERE row_type <> 'RESTATEMENT'
ORDER BY
    CASE row_type WHEN 'ORIGINAL' THEN 1 WHEN 'REVERSAL' THEN 2 WHEN 'CORRECTION_DELTA' THEN 3 END,
    city_name, recognised_date, cycle_type, vertical, code_number;

-- Summary counts and totals by row_type — P360-facing rows only
SELECT row_type, COUNT(*) AS row_count,
       SUM(COALESCE(DR, 0)) AS total_DR,
       SUM(COALESCE(CR, 0)) AS total_CR
FROM p360_batch_preview
WHERE row_type <> 'RESTATEMENT'
GROUP BY row_type ORDER BY 1;
```

**What each `row_type` means:**

| row_type | Sent to P360? | Meaning | DR / CR |
|----------|---------------|---------|---------|
| `ORIGINAL` | Yes | New row, never sent before | Amounts from staging |
| `REVERSAL` | Yes | Cancels a row that disappeared from source (REVERSAL_ONLY) | Old CR → DR, old DR → CR |
| `CORRECTION_DELTA` | Yes | Incremental change for a corrected row | Delta (new − old) on the changed side |
| `RESTATEMENT` | **No** (state marker) | Records the full new amount after a CORRECTION so future batches have the correct baseline | New amounts from staging |

For **CORRECTION** (amounts changed), the batch produces:
- One `RESTATEMENT` row (stored in `p360_submissions`, never sent) — preserves baseline for future comparisons
- One `CORRECTION_DELTA` row (sent to P360) — carries only the net change, with `correction_period` set to the original `recognised_date`

For **REVERSAL_ONLY** (row disappeared), a full `REVERSAL` is still sent — no partial delta is possible.

> `p360_batch_preview` is safe to re-run. The `DROP TABLE IF EXISTS` at the top ensures a clean slate each time.
> The batch_id shown (e.g. `B_20250319_001`) does not increment until after the INSERT in Step 2 — re-running Step 1 on the same day always previews the same batch_id.

---

### Step 2 — Commit

Only run this after reviewing Step 1 output and confirming it is correct.

```sql
BEGIN;

INSERT INTO p360_submissions (
    code_number, particulars, DR, CR, city_name, cycle_type, vertical,
    city_id, store_id, organization_id, organization_email_id,
    recognised_date, remarks, batch_id, submission_date,
    cycle_start, cycle_end, row_type, reference_batch_id, correction_period
)
SELECT
    code_number, particulars, DR, CR, city_name, cycle_type, vertical,
    city_id, store_id, organization_id, organization_email_id,
    recognised_date, remarks, batch_id, submission_date,
    cycle_start, cycle_end, row_type, reference_batch_id, correction_period
FROM p360_batch_preview;

COMMIT;

DROP TABLE IF EXISTS p360_batch_preview;
```

> **Note:** ALL rows from `p360_batch_preview` are inserted — including the silent `RESTATEMENT` state markers. This is intentional: the `last_sent` CTE in future batches reads `RESTATEMENT` rows from `p360_submissions` as the new baseline for corrections.

After committing, the batch_id counter increments. Running Step 1 again on the same day will generate `_002`.

---

### Step 3 — Verify (balance check)

After the INSERT, confirm every ledger group is balanced (DR = CR):

```sql
SELECT code_number, particulars, city_name, cycle_type, vertical,
       SUM(COALESCE(DR, 0)) AS net_DR,
       SUM(COALESCE(CR, 0)) AS net_CR
FROM p360_submissions
GROUP BY code_number, particulars, city_name, cycle_type, vertical
ORDER BY city_name, vertical;
```

`net_DR` should equal `net_CR` for every row. Any imbalance indicates a missing reversal or restatement pair.

---

## How Corrections Are Detected

The batch runner compares `p360_staging` (current source) against the most recent `ORIGINAL` or `RESTATEMENT` row per business key in `p360_submissions`.

**Business key** (what makes a row unique):
```
code_number + city_id + vertical + cycle_type + recognised_date + organization_id + store_id
```

**Detection logic (FULL OUTER JOIN, no date restriction):**

| Situation | Action | Rows sent to P360 | Rows silently stored |
|-----------|--------|-------------------|---------------------|
| Row in staging, not in submissions | `ORIGINAL` | 1 ORIGINAL | — |
| Row in submissions, not in staging (disappeared from source) | `REVERSAL_ONLY` | 1 REVERSAL (full flip) | — |
| Row in both, but DR or CR differs | `CORRECTION` | 1 CORRECTION_DELTA (delta only) | 1 RESTATEMENT (new baseline) |
| Row in both, amounts match | `UNCHANGED` | 0 rows (skipped) | — |

The `CORRECTION_DELTA` row carries `correction_period` = the original `recognised_date` and uses the current cycle date as its own `recognised_date`.

**There is no date lookback limit.** A correction to a `recognised_date` from 6 months ago is detected and handled in the same batch as new current-month entries.

---

## Batch ID Format

Batch IDs are auto-generated:

```
B_YYYYMMDD_NNN
```

Examples: `B_20250319_001`, `B_20250319_002`

The `NNN` counter increments each time a batch is committed (`INSERT INTO p360_submissions`) on the same day. Multiple batches on the same day are supported.

The `reference_batch_id` column on `REVERSAL`, `RESTATEMENT`, and `CORRECTION_DELTA` rows points back to the `batch_id` of the row being corrected.

The `correction_period` column (DATE) is non-NULL only on `CORRECTION_DELTA` rows — it records the original `recognised_date` of the entry being corrected, so recipients know which period the delta applies to.

---

## Querying Submission History

**All rows for a specific batch:**
```sql
SELECT * FROM p360_submissions
WHERE batch_id = 'B_20250319_001'
ORDER BY row_type, city_name, recognised_date;
```

**Latest submitted value per business key (current state of the ledger):**
```sql
SELECT code_number, city_id, vertical, cycle_type, recognised_date,
       organization_id, store_id, DR, CR, batch_id, submission_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY code_number, city_id, vertical, cycle_type,
                         recognised_date, organization_id, COALESCE(store_id,'')
            ORDER BY submission_date DESC, batch_id DESC
        ) AS rn
    FROM p360_submissions
    WHERE row_type IN ('ORIGINAL', 'RESTATEMENT')
) t
WHERE rn = 1;
```

**Full correction history for a specific key:**
```sql
SELECT batch_id, submission_date, row_type, DR, CR, reference_batch_id, correction_period
FROM p360_submissions
WHERE code_number     = '1001010'
  AND city_id         = 42
  AND vertical        = 'FURLENCO_RENTAL'
  AND cycle_type      = 'Normal_billing_cycle'
  AND recognised_date = '2024-11-04'
ORDER BY submission_date, batch_id;
```

**All CORRECTION_DELTA rows (what was actually sent to P360 for corrections):**
```sql
SELECT batch_id, submission_date, code_number, particulars,
       recognised_date, correction_period, DR, CR, reference_batch_id
FROM p360_submissions
WHERE row_type = 'CORRECTION_DELTA'
ORDER BY submission_date DESC, batch_id, code_number;
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `p360_batch_preview` is empty | Staging not refreshed, or all rows unchanged | Run pre-check query; if staging is stale, re-run `p360_staging_refresh.sql` |
| Unexpected CORRECTION rows | Float rounding in source | Review — amounts within 0.0001 are treated as equal; larger differences are genuine corrections |
| REVERSAL with no paired RESTATEMENT | Row deleted from source (not a correction) | Expected — row disappeared, only a reversal is needed |
| Balance check shows net_DR ≠ net_CR | Rows from an old manual batch inserted without reversal pairs | Investigate using the correction history query above |
| `last_refresh` is stale | Daily refresh job failed | Check scheduler logs; re-run `p360_staging_refresh.sql` manually |
| Same batch_id on two separate runs | Step 1 was re-run without committing in between | Normal — batch_id only increments after a committed INSERT |

---

## File Dependency Map

```
p360_final_view_claude.sql   (source — read only)
         │
         │  CTE chain copied verbatim
         ▼
p360_staging_refresh.sql  ──→  p360_staging
                                     │
                                     │  FULL OUTER JOIN
                                     ▼
p360_batch_runner.sql  ──────→  p360_batch_preview  (temp)
         │                           │
         │  also reads               │  INSERT after review
         ▼                           ▼
p360_submissions  ◄──────────────────┘
(permanent ledger)
```
