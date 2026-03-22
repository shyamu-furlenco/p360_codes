-- =============================================================================
-- STORED PROCEDURE: sp_p360_staging_refresh
-- Wraps the daily staging refresh (p360_staging_refresh.sql) so it can be
-- scheduled via Redshift Scheduled Queries and post Slack alerts on
-- success or failure.
--
-- Mode: NONATOMIC — allows the explicit COMMIT inside the DELETE+INSERT block
--       and lets the EXCEPTION handler commit the failure notification even
--       after a rollback.
--
-- Scheduled at: 2:00 AM IST (8:30 PM UTC previous day)
--   Cron: 0 20 * * ? *   (adjust if IST offset differs for your region)
--
-- Manual run: CALL sp_p360_staging_refresh();
-- =============================================================================

CREATE OR REPLACE PROCEDURE sp_p360_staging_refresh()
NONATOMIC
AS $$
DECLARE
    v_row_count     INTEGER;
    v_refreshed_at  TIMESTAMP;
    v_msg           VARCHAR(1000);
BEGIN

    -- =========================================================================
    -- Staging refresh — identical logic to p360_staging_refresh.sql.
    -- DELETE + INSERT are in the same implicit transaction; COMMIT makes it
    -- atomic (if INSERT fails, the DELETE is rolled back too).
    -- =========================================================================

    DELETE FROM p360_staging;

    INSERT INTO p360_staging (
        code_number, particulars, DR, CR,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        recognised_date, remarks
    )

    WITH

    filtered_entities AS (

        -- RENTAL: Items
        SELECT i.id AS entity_id, 'ITEM' AS entity_type, i.vertical
        FROM order_management_systems_evolve.items AS i
        WHERE i.vertical = 'FURLENCO_RENTAL'
          AND i.state <> 'CANCELLED'

        UNION ALL

        -- RENTAL: Attachments
        SELECT a.id AS entity_id, 'ATTACHMENT' AS entity_type, a.vertical
        FROM order_management_systems_evolve.attachments AS a
        WHERE a.vertical = 'FURLENCO_RENTAL'
          AND a.state <> 'CANCELLED'

        UNION ALL

        -- RENTAL: VAS linked to Items
        SELECT vas.id AS entity_id, 'VALUE_ADDED_SERVICE' AS entity_type, i.vertical
        FROM order_management_systems_evolve.value_added_services AS vas
        JOIN order_management_systems_evolve.items AS i
          ON vas.entity_id = i.id AND vas.entity_type = 'ITEM'
        WHERE i.vertical = 'FURLENCO_RENTAL'
          AND i.state <> 'CANCELLED'
          AND vas.state <> 'CANCELLED'

        UNION ALL

        -- RENTAL: VAS linked to Attachments
        SELECT vas.id AS entity_id, 'VALUE_ADDED_SERVICE' AS entity_type, a.vertical
        FROM order_management_systems_evolve.value_added_services AS vas
        JOIN order_management_systems_evolve.attachments AS a
          ON vas.entity_id = a.id AND vas.entity_type = 'ATTACHMENT'
        WHERE a.vertical = 'FURLENCO_RENTAL'
          AND a.state <> 'CANCELLED'
          AND vas.state <> 'CANCELLED'

        UNION ALL

        -- RENTAL: Penalties
        SELECT p.id AS entity_id, 'PENALTY' AS entity_type, p.vertical
        FROM order_management_systems_evolve.penalty AS p
        WHERE p.vertical = 'FURLENCO_RENTAL'
          AND p.state <> 'CANCELLED'

        UNION ALL

        -- SALE: Items
        SELECT
            i.id AS entity_id,
            'ITEM' AS entity_type,
            CASE
                WHEN p.line_of_product = 'BUY_NEW'          THEN 'FURLENCO_SALE'
                WHEN p.line_of_product = 'BUY_REFURBISHED'  THEN 'FURLENCO_REFURB_SALE'
            END AS vertical
        FROM order_management_systems_evolve.items AS i
        JOIN plutus_evolve.products AS p ON i.catalog_item_id = p.id
        WHERE i.vertical = 'FURLENCO_SALE'
          AND i.state <> 'CANCELLED'
          AND p.line_of_product IN ('BUY_REFURBISHED', 'BUY_NEW')

        UNION ALL

        -- SALE: Attachments
        SELECT
            a.id AS entity_id,
            'ATTACHMENT' AS entity_type,
            CASE
                WHEN p.line_of_product = 'BUY_NEW'          THEN 'FURLENCO_SALE'
                WHEN p.line_of_product = 'BUY_REFURBISHED'  THEN 'FURLENCO_REFURB_SALE'
            END AS vertical
        FROM order_management_systems_evolve.attachments AS a
        JOIN plutus_evolve.products AS p ON a.catalog_item_id = p.id
        WHERE a.vertical = 'FURLENCO_SALE'
          AND a.state <> 'CANCELLED'
          AND p.line_of_product IN ('BUY_REFURBISHED', 'BUY_NEW')

        UNION ALL

        -- UNLMTD: Plans
        SELECT p.id AS entity_id, 'PLAN' AS entity_type, 'UNLMTD' AS vertical
        FROM order_management_systems_evolve.plans AS p
        WHERE p.state <> 'CANCELLED'

        UNION ALL

        -- UNLMTD: VAS linked to Plans
        SELECT vas.id AS entity_id, 'VALUE_ADDED_SERVICE' AS entity_type, 'UNLMTD' AS vertical
        FROM order_management_systems_evolve.value_added_services AS vas
        JOIN order_management_systems_evolve.plans AS p
          ON vas.entity_id = p.id AND vas.entity_type = 'PLAN'
        WHERE p.state <> 'CANCELLED'
          AND vas.state <> 'CANCELLED'

        UNION ALL

        -- UNLMTD: Penalties
        SELECT p.id AS entity_id, 'PENALTY' AS entity_type, p.vertical
        FROM order_management_systems_evolve.penalty AS p
        WHERE p.vertical = 'UNLMTD'
          AND p.state <> 'CANCELLED'
    ),

    settlements AS (
        SELECT
            vertical, settlement_id, settlement_nature, settlement_category,
            product_entity_type, product_entity_id, from_date, to_date
        FROM order_management_systems_evolve.settlement_products
    ),

    financial_events AS (

        -- Part A: Revenue Recognitions
        SELECT
            rr.city_id,
            fe.vertical,
            rr.accountable_entity_id,
            CASE
                WHEN rr.external_reference_type = 'SWAP'                                          THEN 'Swap'
                WHEN fe.entity_type = 'VALUE_ADDED_SERVICE' AND rr.external_reference_type <> 'SWAP' THEN 'VAS'
                WHEN rr.external_reference_type IN ('RETURN', 'PLAN_CANCELLATION')
                  OR stl.settlement_category = 'MIN_TENURE_PENALTY'                               THEN 'MTP'
                WHEN fe.entity_type = 'PENALTY'                                                   THEN 'Penalty'
                WHEN rr.recognition_type IN ('DEFERRAL', 'ACCRUAL')                               THEN 'Normal_billing_cycle'
                ELSE rr.accountable_entity_type
            END AS cycle_type,
            CASE WHEN fe.vertical IN ('FURLENCO_SALE', 'FURLENCO_REFURB_SALE')
                 THEN rr.start_date
                 ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes')
            END AS recognised_date,
            json_extract_path_text(rr.monetary_components, 'taxableAmount')::DECIMAL(15,4)              AS taxable_amount,
            json_extract_path_text(rr.monetary_components, 'postTaxAmount')::DECIMAL(15,4)              AS post_tax_amount,
            CASE
                WHEN json_extract_path_text(rr.monetary_components, 'discounts', '0', 'code') = 'NCEMI' THEN json_extract_path_text(rr.monetary_components, 'discounts', '0', 'amount')::DECIMAL(15,4)
                WHEN json_extract_path_text(rr.monetary_components, 'discounts', '1', 'code') = 'NCEMI' THEN json_extract_path_text(rr.monetary_components, 'discounts', '1', 'amount')::DECIMAL(15,4)
                WHEN json_extract_path_text(rr.monetary_components, 'discounts', '2', 'code') = 'NCEMI' THEN json_extract_path_text(rr.monetary_components, 'discounts', '2', 'amount')::DECIMAL(15,4)
                WHEN json_extract_path_text(rr.monetary_components, 'discounts', '3', 'code') = 'NCEMI' THEN json_extract_path_text(rr.monetary_components, 'discounts', '3', 'amount')::DECIMAL(15,4)
                ELSE NULL
            END AS ncemi_amount,
            json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'cgst', 'rate')   AS cgst_rate,
            json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'sgst', 'rate')   AS sgst_rate,
            json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'igst', 'rate')   AS igst_rate,
            json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'cgst', 'amount')::DECIMAL(15,4) AS cgst_amount,
            json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'sgst', 'amount')::DECIMAL(15,4) AS sgst_amount,
            json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'igst', 'amount')::DECIMAL(15,4) AS igst_amount,
            CASE WHEN EXTRACT(DOW FROM (CASE WHEN fe.vertical IN ('FURLENCO_SALE', 'FURLENCO_REFURB_SALE') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE) = 0
                 THEN (CASE WHEN fe.vertical IN ('FURLENCO_SALE', 'FURLENCO_REFURB_SALE') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE - 6
                 ELSE (CASE WHEN fe.vertical IN ('FURLENCO_SALE', 'FURLENCO_REFURB_SALE') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE - EXTRACT(DOW FROM (CASE WHEN fe.vertical IN ('FURLENCO_SALE', 'FURLENCO_REFURB_SALE') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE)::INTEGER + 1
            END AS week_start_date,
            CASE WHEN EXTRACT(DOW FROM (CASE WHEN fe.vertical IN ('FURLENCO_SALE', 'FURLENCO_REFURB_SALE') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE) = 0
                 THEN (CASE WHEN fe.vertical IN ('FURLENCO_SALE', 'FURLENCO_REFURB_SALE') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE
                 ELSE (CASE WHEN fe.vertical IN ('FURLENCO_SALE', 'FURLENCO_REFURB_SALE') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE - EXTRACT(DOW FROM (CASE WHEN fe.vertical IN ('FURLENCO_SALE', 'FURLENCO_REFURB_SALE') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE)::INTEGER + 7
            END AS week_end_date
        FROM furbooks_evolve.revenue_recognitions AS rr
        JOIN filtered_entities fe
          ON rr.accountable_entity_id = fe.entity_id
         AND rr.accountable_entity_type = fe.entity_type
        LEFT JOIN (SELECT * FROM settlements WHERE settlement_category = 'MIN_TENURE_PENALTY') AS stl
          ON stl.product_entity_id = rr.accountable_entity_id
         AND stl.product_entity_type = rr.accountable_entity_type
        WHERE rr.state NOT IN ('CANCELLED', 'INVALIDATED')
          AND rr.start_date >= 'April 01, 2024'

        UNION ALL

        -- Part B: Credit Notes
        SELECT DISTINCT
            ic.city_id,
            fe.vertical,
            cn.id AS accountable_entity_id,
            'Credit_Note'::VARCHAR AS cycle_type,
            cn.issue_date AS recognised_date,
            json_extract_path_text(cn.data, 'totalTaxableAmount')::DECIMAL(15,4) AS taxable_amount,
            json_extract_path_text(cn.data, 'totalAmount')::DECIMAL(15,4)        AS post_tax_amount,
            NULL::DECIMAL(15,4) AS ncemi_amount,
            CASE
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'componentType') = 'CGST' THEN (json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'percentage')::DECIMAL(15,4) / 100)::VARCHAR
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'componentType') = 'CGST' THEN (json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'percentage')::DECIMAL(15,4) / 100)::VARCHAR
            END AS cgst_rate,
            CASE
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'componentType') = 'SGST' THEN (json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'percentage')::DECIMAL(15,4) / 100)::VARCHAR
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'componentType') = 'SGST' THEN (json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'percentage')::DECIMAL(15,4) / 100)::VARCHAR
            END AS sgst_rate,
            CASE
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'componentType') = 'IGST' THEN (json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'percentage')::DECIMAL(15,4) / 100)::VARCHAR
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'componentType') = 'IGST' THEN (json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'percentage')::DECIMAL(15,4) / 100)::VARCHAR
            END AS igst_rate,
            CASE
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'componentType') = 'CGST' THEN json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'amount')::DECIMAL(15,4)
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'componentType') = 'CGST' THEN json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'amount')::DECIMAL(15,4)
            END AS cgst_amount,
            CASE
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'componentType') = 'SGST' THEN json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'amount')::DECIMAL(15,4)
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'componentType') = 'SGST' THEN json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'amount')::DECIMAL(15,4)
            END AS sgst_amount,
            CASE
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'componentType') = 'IGST' THEN json_extract_path_text(cn.data, 'taxDisplayComponents', '0', 'amount')::DECIMAL(15,4)
                WHEN json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'componentType') = 'IGST' THEN json_extract_path_text(cn.data, 'taxDisplayComponents', '1', 'amount')::DECIMAL(15,4)
            END AS igst_amount,
            CASE WHEN EXTRACT(DOW FROM cn.issue_date::DATE) = 0
                 THEN cn.issue_date::DATE - 6
                 ELSE cn.issue_date::DATE - EXTRACT(DOW FROM cn.issue_date::DATE)::INTEGER + 1
            END AS week_start_date,
            CASE WHEN EXTRACT(DOW FROM cn.issue_date::DATE) = 0
                 THEN cn.issue_date::DATE
                 ELSE cn.issue_date::DATE - EXTRACT(DOW FROM cn.issue_date::DATE)::INTEGER + 7
            END AS week_end_date
        FROM furbooks_evolve.credit_notes cn
        JOIN furbooks_evolve.invoice_cycles ic ON cn.invoice_id = ic.invoice_id
        JOIN filtered_entities fe
          ON ic.accountable_entity_id = fe.entity_id
         AND ic.accountable_entity_type = fe.entity_type
        WHERE ic.start_date >= 'April 01, 2024'
    ),

    gst_fix AS (
        SELECT
            city_id, vertical, accountable_entity_id, cycle_type, recognised_date,
            taxable_amount, post_tax_amount, ncemi_amount,
            cgst_rate, sgst_rate,
            COALESCE(
                NULLIF(igst_rate, 0)::DECIMAL(15,4),
                (ROUND((igst_amount::DECIMAL(15,4) * 100) / NULLIF(taxable_amount, 0)::DECIMAL(15,4)) / 100)::DECIMAL(15,4)
            ) AS igst_rate,
            cgst_amount, sgst_amount, igst_amount,
            week_start_date, week_end_date
        FROM financial_events
    ),

    agg_view AS (
        SELECT
            c.name AS city_name,
            ev.city_id,
            fc.email,
            fc.p360_organisation_id,
            fc.p360_store_id,
            ev.vertical,
            ev.recognised_date,
            ev.cycle_type,
            CASE
                WHEN ev.cgst_rate::DECIMAL(15,4) = 0.025 THEN 'Output CGST 2.5%'
                WHEN ev.cgst_rate::DECIMAL(15,4) = 0.06  THEN 'Output CGST 6%'
                WHEN ev.cgst_rate::DECIMAL(15,4) = 0.09  THEN 'Output CGST 9%'
                WHEN ev.cgst_rate::DECIMAL(15,4) = 0.14  THEN 'Output CGST 14%'
            END AS cgst_ledger_name,
            CASE
                WHEN ev.cgst_rate::DECIMAL(15,4) = 0.025 THEN '3006250'
                WHEN ev.cgst_rate::DECIMAL(15,4) = 0.06  THEN '3006260'
                WHEN ev.cgst_rate::DECIMAL(15,4) = 0.09  THEN '3006270'
                WHEN ev.cgst_rate::DECIMAL(15,4) = 0.14  THEN '3006280'
            END AS cgst_ledger_code,
            CASE
                WHEN ev.sgst_rate::DECIMAL(15,4) = 0.025 THEN 'Output SGST 2.5%'
                WHEN ev.sgst_rate::DECIMAL(15,4) = 0.06  THEN 'Output SGST 6%'
                WHEN ev.sgst_rate::DECIMAL(15,4) = 0.09  THEN 'Output SGST 9%'
                WHEN ev.sgst_rate::DECIMAL(15,4) = 0.14  THEN 'Output SGST 14%'
            END AS sgst_ledger_name,
            CASE
                WHEN ev.sgst_rate::DECIMAL(15,4) = 0.025 THEN '3006290'
                WHEN ev.sgst_rate::DECIMAL(15,4) = 0.06  THEN '3006300'
                WHEN ev.sgst_rate::DECIMAL(15,4) = 0.09  THEN '3006310'
                WHEN ev.sgst_rate::DECIMAL(15,4) = 0.14  THEN '3006320'
            END AS sgst_ledger_code,
            CASE
                WHEN ev.igst_rate::DECIMAL(15,4) = 0.05 THEN 'Output IGST 5%'
                WHEN ev.igst_rate::DECIMAL(15,4) = 0.12 THEN 'Output IGST 12%'
                WHEN ev.igst_rate::DECIMAL(15,4) = 0.18 THEN 'Output IGST 18%'
                WHEN ev.igst_rate::DECIMAL(15,4) = 0.28 THEN 'Output IGST 28%'
            END AS igst_ledger_name,
            CASE
                WHEN ev.igst_rate::DECIMAL(15,4) = 0.05 THEN '3006330'
                WHEN ev.igst_rate::DECIMAL(15,4) = 0.12 THEN '3006340'
                WHEN ev.igst_rate::DECIMAL(15,4) = 0.18 THEN '3006350'
                WHEN ev.igst_rate::DECIMAL(15,4) = 0.28 THEN '3006360'
            END AS igst_ledger_code,
            SUM(ev.taxable_amount + COALESCE(ev.ncemi_amount, 0))                        AS sum_of_taxable_amount,
            SUM(COALESCE(ev.cgst_amount, 0))                                              AS sum_of_cgst_amount,
            SUM(COALESCE(ev.sgst_amount, 0))                                              AS sum_of_sgst_amount,
            SUM(COALESCE(ev.igst_amount, 0))                                              AS sum_of_igst_amount,
            SUM(COALESCE(ev.post_tax_amount, 0) + COALESCE(ev.ncemi_amount, 0))           AS sum_of_total
        FROM gst_fix ev
        LEFT JOIN (
            SELECT city_id, email, p360_organisation_id, p360_store_id
            FROM analytics.fulfilment_centres
            WHERE p360_organisation_id IS NOT NULL
        ) AS fc ON ev.city_id = fc.city_id
        LEFT JOIN panem_evolve.cities AS c ON c.id = ev.city_id
        GROUP BY
            c.name, ev.city_id, fc.email, fc.p360_organisation_id, fc.p360_store_id,
            ev.vertical, recognised_date, ev.cycle_type,
            cgst_ledger_name, cgst_ledger_code,
            sgst_ledger_name, sgst_ledger_code,
            igst_ledger_name, igst_ledger_code
    ),

    unpivoted_data AS (

        -- Trade Receivables (DR)
        SELECT
            city_name, city_id, email AS organization_email_id,
            p360_store_id AS store_id, p360_organisation_id AS organization_id,
            vertical, cycle_type, recognised_date,
            CASE
                WHEN vertical = 'FURLENCO_RENTAL'      THEN '3004010'
                WHEN vertical = 'UNLMTD'               THEN '3004080'
                WHEN vertical = 'FURLENCO_SALE'         THEN '3004030'
                WHEN vertical = 'FURLENCO_REFURB_SALE'  THEN '3004040'
                ELSE '3004010'
            END AS code_number,
            CASE
                WHEN vertical = 'FURLENCO_RENTAL'      THEN 'Trade Receivables - Furlenco'
                WHEN vertical = 'UNLMTD'               THEN 'Trade Receivables - Unlmtd'
                WHEN vertical = 'FURLENCO_SALE'         THEN 'Trade Receivables - New Sales'
                WHEN vertical = 'FURLENCO_REFURB_SALE'  THEN 'Trade Receivables - Refurb Sales'
                ELSE 'Trade Receivables - Furlenco'
            END AS particulars,
            sum_of_total::DECIMAL(15,4) AS DR, NULL::DECIMAL(15,4) AS CR,
            1 AS row_order, 0 AS sub_order
        FROM agg_view

        UNION ALL

        -- Revenue (CR)
        SELECT
            city_name, city_id, email, p360_store_id, p360_organisation_id,
            vertical, cycle_type, recognised_date,
            CASE
                WHEN vertical = 'FURLENCO_RENTAL'      THEN '1001010'
                WHEN vertical = 'UNLMTD'               THEN '1001020'
                WHEN vertical = 'FURLENCO_SALE'         THEN '1001030'
                WHEN vertical = 'FURLENCO_REFURB_SALE'  THEN '1001040'
                ELSE '1001010'
            END AS code_number,
            CASE
                WHEN vertical = 'FURLENCO_RENTAL'      THEN 'Revenue - Furlenco'
                WHEN vertical = 'UNLMTD'               THEN 'Revenue - Unlmtd'
                WHEN vertical = 'FURLENCO_SALE'         THEN 'Revenue - New Sales - D2C'
                WHEN vertical = 'FURLENCO_REFURB_SALE'  THEN 'Revenue - Refurb Sales - D2C'
                ELSE 'Revenue - Furlenco'
            END AS particulars,
            NULL::DECIMAL(15,4) AS DR, sum_of_taxable_amount::DECIMAL(15,4) AS CR,
            1 AS row_order, 1 AS sub_order
        FROM agg_view

        UNION ALL

        -- Output CGST (CR)
        SELECT
            city_name, city_id, email, p360_store_id, p360_organisation_id,
            vertical, cycle_type, recognised_date,
            cgst_ledger_code AS code_number, cgst_ledger_name AS particulars,
            NULL::DECIMAL(15,4) AS DR, sum_of_cgst_amount::DECIMAL(15,4) AS CR,
            1 AS row_order, 2 AS sub_order
        FROM agg_view
        WHERE sum_of_cgst_amount > 0

        UNION ALL

        -- Output SGST (CR)
        SELECT
            city_name, city_id, email, p360_store_id, p360_organisation_id,
            vertical, cycle_type, recognised_date,
            sgst_ledger_code AS code_number, sgst_ledger_name AS particulars,
            NULL::DECIMAL(15,4) AS DR, sum_of_sgst_amount::DECIMAL(15,4) AS CR,
            1 AS row_order, 3 AS sub_order
        FROM agg_view
        WHERE sum_of_sgst_amount > 0

        UNION ALL

        -- Output IGST (CR)
        SELECT
            city_name, city_id, email, p360_store_id, p360_organisation_id,
            vertical, cycle_type, recognised_date,
            igst_ledger_code AS code_number, igst_ledger_name AS particulars,
            NULL::DECIMAL(15,4) AS DR, sum_of_igst_amount::DECIMAL(15,4) AS CR,
            1 AS row_order, 4 AS sub_order
        FROM agg_view
        WHERE sum_of_igst_amount > 0
    )

    SELECT
        code_number,
        particulars,
        SUM(DR) AS DR,
        SUM(CR) AS CR,
        city_name,
        cycle_type,
        vertical,
        city_id,
        store_id,
        organization_id,
        organization_email_id,
        recognised_date,
        LOWER(vertical) || ', ' || TO_CHAR(recognised_date, 'Mon-YYYY') AS remarks
    FROM unpivoted_data
    GROUP BY
        code_number, particulars,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        recognised_date,
        row_order, sub_order;

    -- Commit the DELETE + INSERT atomically before validating
    COMMIT;

    -- =========================================================================
    -- Validate freshness: refreshed_at must equal today (set by DEFAULT on insert)
    -- =========================================================================
    SELECT COUNT(*), MAX(refreshed_at)
    INTO v_row_count, v_refreshed_at
    FROM p360_staging;

    IF v_refreshed_at::DATE <> CURRENT_DATE THEN
        RAISE EXCEPTION 'Stale refresh detected: refreshed_at=%, expected %',
            v_refreshed_at, CURRENT_DATE;
    END IF;

    -- =========================================================================
    -- Success notification
    -- =========================================================================
    v_msg := 'P360 staging refresh complete — '
          || v_row_count || ' rows loaded on ' || CURRENT_DATE;
    PERFORM f_slack_notify(v_msg);

EXCEPTION WHEN OTHERS THEN
    -- Current uncommitted transaction is automatically rolled back by Redshift.
    -- Post the failure alert in a fresh transaction so it always reaches Slack.
    v_msg := 'P360 STAGING FAILED on ' || CURRENT_DATE || ': ' || SQLERRM;
    PERFORM f_slack_notify(v_msg);
    COMMIT;   -- commit the Slack call
    RAISE;    -- re-raise so the scheduled query shows FAILED status
END;
$$ LANGUAGE plpgsql;
