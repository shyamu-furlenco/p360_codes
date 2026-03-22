# P360 Scenario Coverage

**Purpose:** List of all scenarios handled by the P360 integration — how each is currently handled and key discussion points for Finance.
**Prepared for:** Monday Finance Connect
**Date:** 22 March 2026

---

> **Common base assumption across examples:**
> City: Bangalore | Vertical: FURLENCO_RENTAL | Taxable: ₹10,000 | CGST 9%: ₹900 | SGST 9%: ₹900 | Post-tax: ₹11,800 | Week: Apr 7–13, 2025

---

## 1. Data Lifecycle Scenarios

---

### 1.1 ORIGINAL — First Submission

**Trigger:** A business key appears in staging but has no entry in the state table. First time this period's data is ever submitted.

**Example:** Week of Apr 7–13 is being submitted for the first time (batch `B_20250413_001`).

**What P360 receives:**

| code_number | particulars | DR | CR | row_type |
|---|---|---|---|---|
| 3004010 | Trade Receivables - Furlenco | 11,800 | — | ORIGINAL |
| 1001010 | Revenue - Furlenco | — | 10,000 | ORIGINAL |
| 3006270 | Output CGST 9% | — | 900 | ORIGINAL |
| 3006310 | Output SGST 9% | — | 900 | ORIGINAL |

**DR total = CR total = ₹11,800 ✓ (balanced)**

---

### 1.2 CORRECTION — Amounts Changed Retroactively

**Trigger:** Same business key was submitted in a prior batch, but staging now shows different amounts (Furbooks corrected a recognition retroactively).

**Example:** After batch `B_20250413_001`, taxable revised from ₹10,000 → ₹12,000.

**What P360 already has (from prior batch):**

| code_number | DR | CR |
|---|---|---|
| 3004010 | 11,800 | — |
| 1001010 | — | 10,000 |
| 3006270 | — | 900 |
| 3006310 | — | 900 |

**Current staging (new truth):**

| code_number | DR | CR |
|---|---|---|
| 3004010 | 14,160 | — |
| 1001010 | — | 12,000 |
| 3006270 | — | 1,080 |
| 3006310 | — | 1,080 |

**What P360 receives (CORRECTION_DELTA = new − old):**

| code_number | particulars | DR | CR | row_type |
|---|---|---|---|---|
| 3004010 | Trade Receivables - Furlenco | 2,360 | — | CORRECTION_DELTA |
| 1001010 | Revenue - Furlenco | — | 2,000 | CORRECTION_DELTA |
| 3006270 | Output CGST 9% | — | 180 | CORRECTION_DELTA |
| 3006310 | Output SGST 9% | — | 180 | CORRECTION_DELTA |

**What is stored internally only (NOT sent to P360):**

| code_number | DR | CR | row_type |
|---|---|---|---|
| 3004010 | 14,160 | — | RESTATEMENT |
| 1001010 | — | 12,000 | RESTATEMENT |
| 3006270 | — | 1,080 | RESTATEMENT |
| 3006310 | — | 1,080 | RESTATEMENT |

> The RESTATEMENT is what updates the internal state table — so the next correction also computes its delta correctly.

---

### 1.2b CORRECTION — Amounts Decreased (Sign Flip)

**Example:** Taxable revised downward from ₹10,000 → ₹8,000.

| code_number | Old (P360 has) | New (staging) | CORRECTION_DELTA sent |
|---|---|---|---|
| 3004010 | DR 11,800 | DR 9,440 | −2,360 → flipped → **CR 2,360** |
| 1001010 | CR 10,000 | CR 8,000 | −2,000 → flipped → **DR 2,000** |
| 3006270 | CR 900 | CR 720 | −180 → flipped → **DR 180** |
| 3006310 | CR 900 | CR 720 | −180 → flipped → **DR 180** |

> **Finance note:** P360 only accepts positive amounts. A decrease on one side is always submitted as a positive amount on the opposite side.

---

### 1.3 REVERSAL — Row Disappeared from Source

**Trigger:** A business key exists in the state table (was submitted) but is no longer present in staging. Typically: an entity cancelled retroactively after it was already submitted.

**Example:** Bangalore Rental week Apr 7–13 was submitted. The item is then cancelled in SMS and no longer appears in staging.

**What P360 receives:**

| code_number | particulars | DR | CR | row_type |
|---|---|---|---|---|
| 3004010 | Trade Receivables - Furlenco | — | 11,800 | REVERSAL |
| 1001010 | Revenue - Furlenco | 10,000 | — | REVERSAL |
| 3006270 | Output CGST 9% | 900 | — | REVERSAL |
| 3006310 | Output SGST 9% | 900 | — | REVERSAL |

> Old DR → becomes CR. Old CR → becomes DR. Fully cancels the original entry in P360.

---

### 1.4 UNCHANGED — No Change

**Trigger:** Business key is in both staging and state, amounts match exactly.

**What happens:** Zero rows emitted. Nothing sent to P360. Silent skip.

---

### 1.5 Multiple Corrections Across Batches

| Batch | Submitted as | DR (Trade Rec) | CR (Revenue) |
|---|---|---|---|
| B_20250413_001 | ORIGINAL | 11,800 | 10,000 |
| B_20250420_001 | CORRECTION_DELTA | +2,360 (DR) | +2,000 (CR) — now 12,000 total |
| B_20250427_001 | CORRECTION_DELTA | 2,360 flipped to CR | 2,000 flipped to DR — back to 10,000 |

> Each delta is computed against the latest internal state — never against the original. Corrections never compound.

---

## 2. Revenue Type Scenarios

---

### 2.1 Normal Billing Cycle

Standard monthly rental.

| code_number | particulars | DR | CR | cycle_type |
|---|---|---|---|---|
| 3004010 | Trade Receivables - Furlenco | 11,800 | — | Normal_billing_cycle |
| 1001010 | Revenue - Furlenco | — | 10,000 | Normal_billing_cycle |
| 3006270 | Output CGST 9% | — | 900 | Normal_billing_cycle |
| 3006310 | Output SGST 9% | — | 900 | Normal_billing_cycle |

---

### 2.2 Swap

Same journal entry structure as Normal, but `cycle_type = Swap`. Identified when `external_reference_type = 'SWAP'` on the revenue recognition in Furbooks.

| code_number | particulars | DR | CR | cycle_type |
|---|---|---|---|---|
| 3004010 | Trade Receivables - Furlenco | 5,900 | — | Swap |
| 1001010 | Revenue - Furlenco | — | 5,000 | Swap |
| 3006270 | Output CGST 9% | — | 450 | Swap |
| 3006310 | Output SGST 9% | — | 450 | Swap |

---

### 2.3 VAS (Value Added Service)

Same journal structure; `cycle_type = VAS`. Identified when `entity_type = VALUE_ADDED_SERVICE` and not a Swap.

| code_number | particulars | DR | CR | cycle_type |
|---|---|---|---|---|
| 3004010 | Trade Receivables - Furlenco | 590 | — | VAS |
| 1001010 | Revenue - Furlenco | — | 500 | VAS |
| 3006270 | Output CGST 9% | — | 45 | VAS |
| 3006310 | Output SGST 9% | — | 45 | VAS |

---

### 2.4 MTP (Min Tenure Penalty)

Triggered by early return before minimum tenure. Identified via:
- `external_reference_type IN ('RETURN', 'PLAN_CANCELLATION')`, OR
- `settlement_category = 'MIN_TENURE_PENALTY'`

| code_number | particulars | DR | CR | cycle_type |
|---|---|---|---|---|
| 3004010 | Trade Receivables - Furlenco | 3,540 | — | MTP |
| 1001010 | Revenue - Furlenco | — | 3,000 | MTP |
| 3006270 | Output CGST 9% | — | 270 | MTP |
| 3006310 | Output SGST 9% | — | 270 | MTP |

---

### 2.5 Penalty (Non-MTP)

`entity_type = PENALTY` and not an MTP trigger. Same journal structure, `cycle_type = Penalty`.

---

### 2.6 Credit Note — via Invalidated Invoice Cycle

**Trigger:** An invoice cycle is INVALIDATED with `revenue_recognition_type = DEFERRAL`. A credit note exists for that invoice. Typically happens when Furbooks reverses a billing cycle.

| code_number | particulars | DR | CR | cycle_type |
|---|---|---|---|---|
| 3004010 | Trade Receivables - Furlenco | — | 11,800 | Credit_Note |
| 1001010 | Revenue - Furlenco | 10,000 | — | Credit_Note |
| 3006270 | Output CGST 9% | 900 | — | Credit_Note |
| 3006310 | Output SGST 9% | 900 | — | Credit_Note |

> DR/CR are flipped relative to normal revenue — it is a reversal of revenue.

---

### 2.7 Credit Note — via Outstanding Settlement

**Trigger:** A credit note is linked to an outstanding settlement (e.g., used to settle an outstanding balance).

Same journal structure as 2.6. The difference is the data source: amounts come from `outstanding_settlements.monetary_components` (not from `invoice_cycles`).

---

### 2.8 Deferral — Cross-Month Billing Cycle

**Trigger:** Billing cycle spans two calendar months. Revenue is split proportionally: current month gets its share, the rest is deferred.

**Example:** Billing cycle Mar 28 – Apr 27. Taxable = ₹6,000.

**Calculation:**
- Billing start day = 28
- Average total days in cycle (March, 31-day month) = 30.5
- Current month days (average) = 30.5 − (28 − 1) = 3.5
- **March portion** = ROUND(3.5 × 6,000 / 30.5, 2) = **₹688.52**
- **April portion (deferred)** = 6,000 − 688.52 = **₹5,311.48**

**4 journal entries sent to P360:**

*In March (week of recognised date):*

| code_number | particulars | DR | CR | Note |
|---|---|---|---|---|
| 1001010 | Revenue - Furlenco | 5,311.48 | — | Reduce March revenue by deferred portion |
| 4006020 | Deferred Revenue | — | 5,311.48 | Create deferred revenue liability |

*In April (week of first day of April):*

| code_number | particulars | DR | CR | Note |
|---|---|---|---|---|
| 4006020 | Deferred Revenue | 5,311.48 | — | Clear the liability |
| 1001010 | Revenue - Furlenco | — | 5,311.48 | Recognise in April |

> **Net effect:** March books ₹688.52, April books ₹5,311.48. Total = ₹6,000 ✓

---

## 3. Vertical / Business Line Scenarios

---

### 3.1 Rental (B2C)

| Trade Receivable Code | Revenue Code | Vertical |
|---|---|---|
| 3004010 | 1001010 | FURLENCO_RENTAL |

---

### 3.2 B2B Customer

**Trigger:** Customer's display ID matches the B2B list. Trade Receivable ledger overrides to 3004020 regardless of vertical.

**Normal (B2C) customer:**

| code_number | particulars | DR |
|---|---|---|
| 3004010 | Trade Receivables - Furlenco | 11,800 |

**Same customer if B2B:**

| code_number | particulars | DR |
|---|---|---|
| **3004020** | Trade Receivables - B2B | 11,800 |

> Revenue code (1001010), CGST, SGST codes remain unchanged.

> **Open item:** B2B customer list is currently hardcoded (single ID). Finance to share updated B2B ledger.

---

### 3.3 UNLMTD

| Trade Receivable Code | Revenue Code | Vertical |
|---|---|---|
| 3004080 | 1001020 | UNLMTD |

---

### 3.4 – 3.7 Sales Verticals

`vertical` is derived from `product.line_of_product` + `order.source`:

| Scenario | Trigger | Trade Rec Code | Revenue Code |
|---|---|---|---|
| New Sales D2C | BUY_NEW + ANDROID/IOS/MWEB/WEB | 3004030 | 1001050 |
| New Sales Store | BUY_NEW + OFFLINE_STORE | 3004030 | 1001030 |
| Refurb Sales D2C | BUY_REFURBISHED + ANDROID/IOS/MWEB/WEB | 3004040 | 1001060 |
| Refurb Sales Store | BUY_REFURBISHED + OFFLINE_STORE | 3004040 | 1001040 |

---

## 4. Missing / Incomplete Data Scenarios

---

### 4.1 Entity Has No FC Mapping — Silently Dropped

**Trigger:** Entity's delivery address has a pincode not configured in the FC mapping table → `dispatch_fc_id = NULL` → no `organization_id` → row is excluded from P360.

**Example:** Entity in a new city not yet configured in FC mapping. It has revenue recognitions in Furbooks. Those amounts exist but produce `organization_id = NULL` — and are silently dropped before reaching P360.

**Finance discussion point:** There is currently no alert or report for this. These amounts are invisible. Should we build a report showing what was excluded?

---

### 4.2 Missing GST Rate — Back-Calculated

**Trigger:** Revenue recognition has `cgst_rate = 0` or NULL but `cgst_amount` is non-zero.

**How handled:** Rate is back-calculated from amounts:

```
cgst_rate = ROUND(cgst_amount × 100 / taxable_amount) / 100
```

**Example:**
- `cgst_rate = 0`, `cgst_amount = 900`, `taxable_amount = 10,000`
- Back-calculated: `ROUND(900 × 100 / 10,000) / 100 = 0.09` → maps to ledger 3006270 (Output CGST 9%)

If both rate and amount are 0: no CGST journal entry is emitted (correct behaviour).

---

### 4.3 Stale Staging Data

**Trigger:** Staging was not refreshed before the batch run.

**Effect:** Batch compares stale data vs state → may emit zero rows or incorrect deltas.

**Recommended action:** Always confirm `MAX(refreshed_at) = today` on the staging table before running any batch.

---

### 4.4 B2B Customer List Hardcoded

**Current state:** B2B customer list is a single hardcoded ID in the source view.

**Risk:** Any new B2B customer not in this list will use the wrong trade receivable ledger (3004010 instead of 3004020).

**Action item:** Finance to share the updated B2B ledger so this can be made table-driven.

---

## 5. GST Type Scenarios

---

### 5.1 Intra-State — CGST + SGST

Customer and fulfilment centre in the same state (e.g., Bangalore customer, Bangalore FC).

| code_number | particulars | DR | CR |
|---|---|---|---|
| 3004010 | Trade Receivables | 11,800 | — |
| 1001010 | Revenue | — | 10,000 |
| 3006270 | Output CGST 9% | — | 900 |
| 3006310 | Output SGST 9% | — | 900 |

---

### 5.2 Inter-State — IGST Only

Customer in one state, fulfilment centre in another (e.g., Delhi customer, Bangalore FC).

| code_number | particulars | DR | CR |
|---|---|---|---|
| 3004010 | Trade Receivables | 11,800 | — |
| 1001010 | Revenue | — | 10,000 |
| 3006350 | Output IGST 18% | — | 1,800 |

> CGST and SGST rows are not emitted when their amounts are zero.

---

### 5.3 GST Rate → Ledger Code Mapping

| Tax | Rate | Code |
|---|---|---|
| CGST | 2.5% | 3006250 |
| CGST | 6% | 3006260 |
| CGST | 9% | 3006270 |
| CGST | 14% | 3006280 |
| SGST | 2.5% | 3006290 |
| SGST | 6% | 3006300 |
| SGST | 9% | 3006310 |
| SGST | 14% | 3006320 |
| IGST | 5% | 3006330 |
| IGST | 12% | 3006340 |
| IGST | 18% | 3006350 |
| IGST | 28% | 3006360 |

---

## 6. Correction Accounting Convention

P360 does not accept negative amounts. When an amount decreases, the delta is submitted as a positive on the opposite side.

| Situation | What changed | CORRECTION_DELTA sent |
|---|---|---|
| Revenue increased | CR increased by ₹X | CR ₹X |
| Revenue decreased | CR decreased by ₹X | DR ₹X (flipped) |
| Trade Rec increased | DR increased by ₹X | DR ₹X |
| Trade Rec decreased | DR decreased by ₹X | CR ₹X (flipped) |

**Finance discussion point:** Confirm this sign convention matches what P360's import expects.

