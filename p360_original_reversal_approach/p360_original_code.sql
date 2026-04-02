

-- =============================================================================
-- P360 FINAL VIEW (Combined) - Rental + Sale + UNLMTD
-- =============================================================================
-- Merges rental_view_v2.sql, sale_view.sql, unlmtd_view.sql into one query.
-- Structure: 5 CTEs → final SELECT
--   1. filtered_entities  : All billable entities across all verticals
--                           (includes dispatch_fc_id via pincode lookup)
--   2. settlements        : Settlement products (for MTP identification)
--   3. financial_events   : Revenue recognitions UNION ALL Credit notes
--   4. agg_view           : Aggregation by city/week/cycle/ledger
--   5. unpivoted_data     : DR/CR journal entry format
-- =============================================================================

WITH

-- =============================================================================
-- PERIOD CONFIG
-- Change ONE value here to switch aggregation granularity for the P360 output.
--   'day'     → each recognised_date is its own row
--   'week'    → Mon–Sun  (Redshift DATE_TRUNC('week') starts on Monday)
--   'month'   → calendar month
--   'quarter' → calendar quarter
--   'year'    → calendar year
-- =============================================================================
period_config AS (
    SELECT 'week'::VARCHAR AS period_type   -- ← change only this value
),

-- =============================================================================
-- B2B USERS — single source of truth for B2B customer IDs
-- =============================================================================
b2b_users AS (
    SELECT 'FUR17641677857' as fur_id 
),

-- =============================================================================
-- Step 1: All billable entities across Rental, Sale, and UNLMTD
--         dispatch_fc_id resolved via: entity → snapshotted_addresses.pincode
--         → wmsl_evolve.fc_configuration_for_pincodes → dispatch_fc_id
-- =============================================================================
filtered_entities AS (

    -- -------------------------------------------------------------------------
    -- RENTAL: Items
    -- -------------------------------------------------------------------------
    SELECT i.id AS entity_id, 'ITEM' AS entity_type, i.vertical,
        fc_pin.dispatch_fc_id,
        FALSE AS is_b2b
    FROM order_management_systems_evolve.items AS i
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON i.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON i.vertical = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE i.vertical = 'FURLENCO_RENTAL'
      AND i.state <> 'CANCELLED'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- RENTAL: Attachments
    -- -------------------------------------------------------------------------
    SELECT a.id AS entity_id, 'ATTACHMENT' AS entity_type, a.vertical,
        fc_pin.dispatch_fc_id,
        FALSE AS is_b2b
    FROM order_management_systems_evolve.attachments AS a
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON a.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON a.vertical = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE a.vertical = 'FURLENCO_RENTAL'
      AND a.state <> 'CANCELLED'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- RENTAL: VAS linked to Items
    -- -------------------------------------------------------------------------
    SELECT vas.id AS entity_id, 'VALUE_ADDED_SERVICE' AS entity_type, i.vertical,
        fc_pin.dispatch_fc_id,
        FALSE AS is_b2b
    FROM order_management_systems_evolve.value_added_services AS vas
    JOIN order_management_systems_evolve.items AS i
      ON vas.entity_id = i.id AND vas.entity_type = 'ITEM'
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON i.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON i.vertical = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE i.vertical = 'FURLENCO_RENTAL'
      AND i.state <> 'CANCELLED'
      AND vas.state <> 'CANCELLED'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- RENTAL: VAS linked to Attachments
    -- -------------------------------------------------------------------------
    SELECT vas.id AS entity_id, 'VALUE_ADDED_SERVICE' AS entity_type, a.vertical,
        fc_pin.dispatch_fc_id,
        FALSE AS is_b2b
    FROM order_management_systems_evolve.value_added_services AS vas
    JOIN order_management_systems_evolve.attachments AS a
      ON vas.entity_id = a.id AND vas.entity_type = 'ATTACHMENT'
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON a.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON a.vertical = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE a.vertical = 'FURLENCO_RENTAL'
      AND a.state <> 'CANCELLED'
      AND vas.state <> 'CANCELLED'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- RENTAL: Penalties (parent may be Item or Attachment)
    -- -------------------------------------------------------------------------
    SELECT pen.id AS entity_id, 'PENALTY' AS entity_type, pen.vertical,
        COALESCE(fc_i.dispatch_fc_id, fc_a.dispatch_fc_id) AS dispatch_fc_id,
        FALSE AS is_b2b
    FROM order_management_systems_evolve.penalty AS pen
    LEFT JOIN order_management_systems_evolve.items AS i
        ON pen.product_entity_id = i.id AND pen.product_entity_type = 'ITEM'
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa_i
        ON i.snapshotted_delivery_address_id = sa_i.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_i
        ON i.vertical = fc_i.vertical AND sa_i.pincode = fc_i.pincode
    LEFT JOIN order_management_systems_evolve.attachments AS a
        ON pen.product_entity_id = a.id AND pen.product_entity_type = 'ATTACHMENT'
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa_a
        ON a.snapshotted_delivery_address_id = sa_a.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_a
        ON a.vertical = fc_a.vertical AND sa_a.pincode = fc_a.pincode
    WHERE pen.vertical = 'FURLENCO_RENTAL'
      AND pen.state <> 'CANCELLED'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- SALE: Items (vertical derived from product line_of_product)
    -- -------------------------------------------------------------------------
    SELECT
        i.id AS entity_id,
        'ITEM' AS entity_type,
        CASE
            WHEN (json_extract_path_text(ord.user_details,'displayId') IN (SELECT fur_id FROM b2b_users)) THEN 'Sale-B2B'
            WHEN p.line_of_product = 'BUY_NEW'         AND ord.source IN ('ANDROID','IOS','MWEB','WEB') THEN 'New Sales - D2C'
            WHEN p.line_of_product = 'BUY_NEW'         AND ord.source = 'OFFLINE_STORE'                 THEN 'New Sales - Store'
            WHEN p.line_of_product = 'BUY_REFURBISHED' AND ord.source IN ('ANDROID','IOS','MWEB','WEB') THEN 'Refurb Sales - D2C'
            WHEN p.line_of_product = 'BUY_REFURBISHED' AND ord.source = 'OFFLINE_STORE'                 THEN 'Refurb Sales - Store'
        END AS vertical,
        fc_pin.dispatch_fc_id,
        (json_extract_path_text(ord.user_details,'displayId') IN (SELECT fur_id FROM b2b_users)) AS is_b2b
    FROM order_management_systems_evolve.items AS i
    JOIN plutus_evolve.products AS p ON i.catalog_item_id = p.id
    JOIN order_management_systems_evolve.orders AS ord ON i.order_id = ord.id
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON i.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON i.vertical = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE i.vertical = 'FURLENCO_SALE'
      AND i.state <> 'CANCELLED'
      AND p.line_of_product IN ('BUY_REFURBISHED', 'BUY_NEW')

    UNION ALL

    -- -------------------------------------------------------------------------
    -- SALE: Attachments (vertical derived from product line_of_product)
    -- -------------------------------------------------------------------------
    SELECT
        a.id AS entity_id,
        'ATTACHMENT' AS entity_type,
        CASE
            WHEN (json_extract_path_text(ord.user_details,'displayId') IN (SELECT fur_id FROM b2b_users)) THEN 'Sale-B2B'
            WHEN p.line_of_product = 'BUY_NEW'         AND ord.source IN ('ANDROID','IOS','MWEB','WEB') THEN 'New Sales - D2C'
            WHEN p.line_of_product = 'BUY_NEW'         AND ord.source = 'OFFLINE_STORE'                 THEN 'New Sales - Store'
            WHEN p.line_of_product = 'BUY_REFURBISHED' AND ord.source IN ('ANDROID','IOS','MWEB','WEB') THEN 'Refurb Sales - D2C'
            WHEN p.line_of_product = 'BUY_REFURBISHED' AND ord.source = 'OFFLINE_STORE'                 THEN 'Refurb Sales - Store'
        END AS vertical,
        fc_pin.dispatch_fc_id,
        (json_extract_path_text(ord.user_details,'displayId') IN (SELECT fur_id FROM b2b_users)) AS is_b2b
    FROM order_management_systems_evolve.attachments AS a
    JOIN plutus_evolve.products AS p ON a.catalog_item_id = p.id
    JOIN order_management_systems_evolve.orders AS ord ON a.order_id = ord.id
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON a.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON a.vertical = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE a.vertical = 'FURLENCO_SALE'
      AND a.state <> 'CANCELLED'
      AND p.line_of_product IN ('BUY_REFURBISHED', 'BUY_NEW')

    UNION ALL

    -- -------------------------------------------------------------------------
    -- SALE: VAS linked to Items
    -- -------------------------------------------------------------------------
    SELECT
        vas.id AS entity_id,
        'VALUE_ADDED_SERVICE' AS entity_type,
        CASE
            WHEN (json_extract_path_text(ord.user_details,'displayId') IN (SELECT fur_id FROM b2b_users)) THEN 'Sale-B2B'
            WHEN p.line_of_product = 'BUY_NEW'         AND ord.source IN ('ANDROID','IOS','MWEB','WEB') THEN 'New Sales - D2C'
            WHEN p.line_of_product = 'BUY_NEW'         AND ord.source = 'OFFLINE_STORE'                 THEN 'New Sales - Store'
            WHEN p.line_of_product = 'BUY_REFURBISHED' AND ord.source IN ('ANDROID','IOS','MWEB','WEB') THEN 'Refurb Sales - D2C'
            WHEN p.line_of_product = 'BUY_REFURBISHED' AND ord.source = 'OFFLINE_STORE'                 THEN 'Refurb Sales - Store'
        END AS vertical,
        fc_pin.dispatch_fc_id,
        (json_extract_path_text(ord.user_details,'displayId') IN (SELECT fur_id FROM b2b_users)) AS is_b2b
    FROM order_management_systems_evolve.value_added_services AS vas
    JOIN order_management_systems_evolve.items AS i
      ON vas.entity_id = i.id AND vas.entity_type = 'ITEM'
    JOIN plutus_evolve.products AS p ON i.catalog_item_id = p.id
    JOIN order_management_systems_evolve.orders AS ord ON i.order_id = ord.id
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON i.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON i.vertical = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE i.vertical = 'FURLENCO_SALE'
      AND i.state <> 'CANCELLED'
      AND p.line_of_product IN ('BUY_REFURBISHED', 'BUY_NEW')
      AND vas.state <> 'CANCELLED'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- SALE: VAS linked to Attachments
    -- -------------------------------------------------------------------------
    SELECT
        vas.id AS entity_id,
        'VALUE_ADDED_SERVICE' AS entity_type,
        CASE
            WHEN (json_extract_path_text(ord.user_details,'displayId') IN (SELECT fur_id FROM b2b_users)) THEN 'Sale-B2B'
            WHEN p.line_of_product = 'BUY_NEW'         AND ord.source IN ('ANDROID','IOS','MWEB','WEB') THEN 'New Sales - D2C'
            WHEN p.line_of_product = 'BUY_NEW'         AND ord.source = 'OFFLINE_STORE'                 THEN 'New Sales - Store'
            WHEN p.line_of_product = 'BUY_REFURBISHED' AND ord.source IN ('ANDROID','IOS','MWEB','WEB') THEN 'Refurb Sales - D2C'
            WHEN p.line_of_product = 'BUY_REFURBISHED' AND ord.source = 'OFFLINE_STORE'                 THEN 'Refurb Sales - Store'
        END AS vertical,
        fc_pin.dispatch_fc_id,
        (json_extract_path_text(ord.user_details,'displayId') IN (SELECT fur_id FROM b2b_users)) AS is_b2b
    FROM order_management_systems_evolve.value_added_services AS vas
    JOIN order_management_systems_evolve.attachments AS a
      ON vas.entity_id = a.id AND vas.entity_type = 'ATTACHMENT'
    JOIN plutus_evolve.products AS p ON a.catalog_item_id = p.id
    JOIN order_management_systems_evolve.orders AS ord ON a.order_id = ord.id
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON a.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON a.vertical = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE a.vertical = 'FURLENCO_SALE'
      AND a.state <> 'CANCELLED'
      AND p.line_of_product IN ('BUY_REFURBISHED', 'BUY_NEW')
      AND vas.state <> 'CANCELLED'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- UNLMTD: Plans
    -- -------------------------------------------------------------------------
    SELECT p.id AS entity_id, 'PLAN' AS entity_type, 'UNLMTD' AS vertical,
        fc_pin.dispatch_fc_id,
        FALSE AS is_b2b
    FROM order_management_systems_evolve.plans AS p
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON p.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON 'UNLMTD' = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE p.state <> 'CANCELLED'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- UNLMTD: VAS linked to Plans
    -- -------------------------------------------------------------------------
    SELECT vas.id AS entity_id, 'VALUE_ADDED_SERVICE' AS entity_type, 'UNLMTD' AS vertical,
        fc_pin.dispatch_fc_id,
        FALSE AS is_b2b
    FROM order_management_systems_evolve.value_added_services AS vas
    JOIN order_management_systems_evolve.plans AS p
      ON vas.entity_id = p.id AND vas.entity_type = 'PLAN'
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON p.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON 'UNLMTD' = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE p.state <> 'CANCELLED'
      AND vas.state <> 'CANCELLED'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- UNLMTD: Penalties (parent is Plan)
    -- -------------------------------------------------------------------------
    SELECT pen.id AS entity_id, 'PENALTY' AS entity_type, pen.vertical,
        fc_pin.dispatch_fc_id,
        FALSE AS is_b2b
    FROM order_management_systems_evolve.penalty AS pen
    LEFT JOIN order_management_systems_evolve.plans AS p
        ON pen.product_entity_id = p.id AND pen.product_entity_type = 'PLAN'
    LEFT JOIN order_management_systems_evolve.snapshotted_addresses AS sa
        ON p.snapshotted_delivery_address_id = sa.id
    LEFT JOIN wmsl_evolve.fc_configuration_for_pincodes AS fc_pin
        ON 'UNLMTD' = fc_pin.vertical AND sa.pincode = fc_pin.pincode
    WHERE pen.vertical = 'UNLMTD'
      AND pen.state <> 'CANCELLED'
),

-- =============================================================================
-- Step 2: Settlement products (for MTP identification — used by Rental & UNLMTD)
-- =============================================================================
settlements AS (
    SELECT
        vertical,
        settlement_id,
        settlement_nature,
        settlement_category,
        product_entity_type,
        product_entity_id,
        from_date,
        to_date
    FROM order_management_systems_evolve.settlement_products
)

-- =============================================================================
-- Step 3: Financial events — Revenue Recognitions + Credit Notes
-- =============================================================================
,financial_events as (

    -- Part A: Revenue Recognitions (unified across all verticals)
    SELECT
        rr.city_id,
        fe.vertical,
        rr.accountable_entity_id,

        -- cycle_type: combined logic covering Rental, Sale, and UNLMTD
        CASE
            WHEN rr.external_reference_type = 'SWAP'  THEN 'Swap'
            WHEN fe.entity_type = 'VALUE_ADDED_SERVICE' AND rr.external_reference_type <> 'SWAP' THEN 'VAS'
            WHEN rr.external_reference_type IN ('RETURN', 'PLAN_CANCELLATION') OR stl.settlement_category = 'MIN_TENURE_PENALTY' THEN 'MTP'
            WHEN fe.entity_type = 'PENALTY' THEN 'Penalty'
            WHEN fe.entity_type not in ('PENALTY','VALUE_ADDED_SERVICE') THEN 'Normal_billing_cycle'
            ELSE rr.accountable_entity_type
        END AS cycle_type,

        -- recognised_date: Sale uses start_date only; Rental/UNLMTD use LEAST logic
        CASE WHEN fe.vertical IN ('New Sales - D2C', 'New Sales - Store', 'Refurb Sales - D2C', 'Refurb Sales - Store', 'Sale-B2B')
             THEN rr.start_date
             ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes')
        END AS recognised_date,

        -- Amounts
        json_extract_path_text(rr.monetary_components, 'taxableAmount')::FLOAT AS taxable_amount,
        json_extract_path_text(rr.monetary_components, 'postTaxAmount')::FLOAT AS post_tax_amount,

        -- NCEMI discount (scans positions 0-3 in JSON array)
        CASE
            WHEN json_extract_path_text(rr.monetary_components, 'discounts', '0', 'code') = 'NCEMI' THEN json_extract_path_text(rr.monetary_components, 'discounts', '0', 'amount')::FLOAT
            WHEN json_extract_path_text(rr.monetary_components, 'discounts', '1', 'code') = 'NCEMI' THEN json_extract_path_text(rr.monetary_components, 'discounts', '1', 'amount')::FLOAT
            WHEN json_extract_path_text(rr.monetary_components, 'discounts', '2', 'code') = 'NCEMI' THEN json_extract_path_text(rr.monetary_components, 'discounts', '2', 'amount')::FLOAT
            WHEN json_extract_path_text(rr.monetary_components, 'discounts', '3', 'code') = 'NCEMI' THEN json_extract_path_text(rr.monetary_components, 'discounts', '3', 'amount')::FLOAT
            ELSE NULL
        END AS ncemi_amount,

        -- Tax rates
        json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'cgst', 'rate') AS cgst_rate,
        json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'sgst', 'rate') AS sgst_rate,
        json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'igst', 'rate') AS igst_rate,

        -- Tax amounts
        json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'cgst', 'amount')::FLOAT AS cgst_amount,
        json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'sgst', 'amount')::FLOAT AS sgst_amount,
        json_extract_path_text(rr.monetary_components, 'tax', 'breakup', 'igst', 'amount')::FLOAT AS igst_amount,

        -- Billing cycle dates (for deferral calculation)
        rr.start_date AS billing_start_date,
        rr.end_date   AS billing_end_date,

        -- Week boundaries (Monday-Sunday) derived from recognised_date
        CASE WHEN EXTRACT(DOW FROM (CASE WHEN fe.vertical IN ('New Sales - D2C', 'New Sales - Store', 'Refurb Sales - D2C', 'Refurb Sales - Store', 'Sale-B2B') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE) = 0
             THEN (CASE WHEN fe.vertical IN ('New Sales - D2C', 'New Sales - Store', 'Refurb Sales - D2C', 'Refurb Sales - Store', 'Sale-B2B') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE - 6
             ELSE (CASE WHEN fe.vertical IN ('New Sales - D2C', 'New Sales - Store', 'Refurb Sales - D2C', 'Refurb Sales - Store', 'Sale-B2B') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE - EXTRACT(DOW FROM (CASE WHEN fe.vertical IN ('New Sales - D2C', 'New Sales - Store', 'Refurb Sales - D2C', 'Refurb Sales - Store', 'Sale-B2B') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE)::INTEGER + 1
        END AS week_start_date,
        CASE WHEN EXTRACT(DOW FROM (CASE WHEN fe.vertical IN ('New Sales - D2C', 'New Sales - Store', 'Refurb Sales - D2C', 'Refurb Sales - Store', 'Sale-B2B') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE) = 0
             THEN (CASE WHEN fe.vertical IN ('New Sales - D2C', 'New Sales - Store', 'Refurb Sales - D2C', 'Refurb Sales - Store', 'Sale-B2B') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE
             ELSE (CASE WHEN fe.vertical IN ('New Sales - D2C', 'New Sales - Store', 'Refurb Sales - D2C', 'Refurb Sales - Store', 'Sale-B2B') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE - EXTRACT(DOW FROM (CASE WHEN fe.vertical IN ('New Sales - D2C', 'New Sales - Store', 'Refurb Sales - D2C', 'Refurb Sales - Store', 'Sale-B2B') THEN rr.start_date ELSE LEAST(rr.start_date, rr.recognised_at + INTERVAL '330 minutes') END)::DATE)::INTEGER + 7
        END AS week_end_date,
        fe.dispatch_fc_id,
        fe.is_b2b

    FROM furbooks_evolve.revenue_recognitions AS rr
    JOIN filtered_entities fe
      ON rr.accountable_entity_id = fe.entity_id
     AND rr.accountable_entity_type = fe.entity_type
    LEFT JOIN (SELECT settlement_id, product_entity_id, product_entity_type, settlement_category FROM settlements WHERE settlement_category = 'MIN_TENURE_PENALTY') AS stl
      ON stl.product_entity_id = rr.accountable_entity_id
     AND stl.product_entity_type = rr.accountable_entity_type
     AND stl.settlement_id = rr.external_reference_id
     AND rr.external_reference_type = 'SETTLEMENT'
    WHERE rr.state NOT IN ('CANCELLED', 'INVALIDATED')
    AND rr.start_date >= 'April 01, 2024'

    UNION ALL

    -- Part B1: Credit Notes via Invoice Cycles (INVALIDATED + DEFERRAL)
    SELECT DISTINCT
        ic.city_id,
        fe.vertical,
        cn.id AS accountable_entity_id,
        'Credit_Note'::VARCHAR AS cycle_type,
        cn.issue_date AS recognised_date,

        json_extract_path_text(ic.monetary_components, 'taxableAmount')::FLOAT          AS taxable_amount,
        json_extract_path_text(ic.monetary_components, 'postTaxAmount')::FLOAT          AS post_tax_amount,
        NULL::FLOAT AS ncemi_amount,

        json_extract_path_text(ic.monetary_components, 'tax', 'breakup', 'cgst', 'rate')   AS cgst_rate,
        json_extract_path_text(ic.monetary_components, 'tax', 'breakup', 'sgst', 'rate')   AS sgst_rate,
        json_extract_path_text(ic.monetary_components, 'tax', 'breakup', 'igst', 'rate')   AS igst_rate,

        json_extract_path_text(ic.monetary_components, 'tax', 'breakup', 'cgst', 'amount')::FLOAT AS cgst_amount,
        json_extract_path_text(ic.monetary_components, 'tax', 'breakup', 'sgst', 'amount')::FLOAT AS sgst_amount,
        json_extract_path_text(ic.monetary_components, 'tax', 'breakup', 'igst', 'amount')::FLOAT AS igst_amount,

        NULL::DATE AS billing_start_date,
        NULL::DATE AS billing_end_date,

        CASE WHEN EXTRACT(DOW FROM cn.issue_date::DATE) = 0
             THEN cn.issue_date::DATE - 6
             ELSE cn.issue_date::DATE - EXTRACT(DOW FROM cn.issue_date::DATE)::INTEGER + 1
        END AS week_start_date,
        CASE WHEN EXTRACT(DOW FROM cn.issue_date::DATE) = 0
             THEN cn.issue_date::DATE
             ELSE cn.issue_date::DATE - EXTRACT(DOW FROM cn.issue_date::DATE)::INTEGER + 7
        END AS week_end_date,
        fe.dispatch_fc_id,
        fe.is_b2b

    FROM furbooks_evolve.invoice_cycles AS ic
    JOIN furbooks_evolve.credit_notes AS cn ON cn.invoice_id = ic.invoice_id
    JOIN filtered_entities fe
        ON ic.accountable_entity_id = fe.entity_id
       AND ic.accountable_entity_type = fe.entity_type
    WHERE ic.state = 'INVALIDATED'
      AND ic.revenue_recognition_type = 'DEFERRAL'
      AND ic.start_date >= 'April 01, 2024'

    UNION ALL

    -- Part B2: Credit Notes via Outstanding Settlements
    SELECT DISTINCT
        os.city_id,
        fe.vertical,
        cn.id AS accountable_entity_id,
        'Credit_Note'::VARCHAR AS cycle_type,
        cn.issue_date AS recognised_date,

        json_extract_path_text(os.monetary_components, 'taxableAmount')::FLOAT          AS taxable_amount,
        json_extract_path_text(os.monetary_components, 'postTaxAmount')::FLOAT          AS post_tax_amount,
        NULL::FLOAT AS ncemi_amount,

        json_extract_path_text(os.monetary_components, 'tax', 'breakup', 'cgst', 'rate')   AS cgst_rate,
        json_extract_path_text(os.monetary_components, 'tax', 'breakup', 'sgst', 'rate')   AS sgst_rate,
        json_extract_path_text(os.monetary_components, 'tax', 'breakup', 'igst', 'rate')   AS igst_rate,

        json_extract_path_text(os.monetary_components, 'tax', 'breakup', 'cgst', 'amount')::FLOAT AS cgst_amount,
        json_extract_path_text(os.monetary_components, 'tax', 'breakup', 'sgst', 'amount')::FLOAT AS sgst_amount,
        json_extract_path_text(os.monetary_components, 'tax', 'breakup', 'igst', 'amount')::FLOAT AS igst_amount,

        NULL::DATE AS billing_start_date,
        NULL::DATE AS billing_end_date,

        CASE WHEN EXTRACT(DOW FROM cn.issue_date::DATE) = 0
             THEN cn.issue_date::DATE - 6
             ELSE cn.issue_date::DATE - EXTRACT(DOW FROM cn.issue_date::DATE)::INTEGER + 1
        END AS week_start_date,
        CASE WHEN EXTRACT(DOW FROM cn.issue_date::DATE) = 0
             THEN cn.issue_date::DATE
             ELSE cn.issue_date::DATE - EXTRACT(DOW FROM cn.issue_date::DATE)::INTEGER + 7
        END AS week_end_date,
        fe.dispatch_fc_id,
        fe.is_b2b

    FROM furbooks_evolve.credit_notes AS cn
    JOIN furbooks_evolve.outstanding_settlements AS os
        ON cn.id = os.credit_note_id
       AND os.credit_note_id IS NOT NULL
    JOIN filtered_entities fe
        ON os.accountable_entity_id = fe.entity_id
       AND os.accountable_entity_type = fe.entity_type
    WHERE cn.issue_date >= 'April 01, 2024'
)

, gst_fix as (

SELECT city_id, vertical, accountable_entity_id, cycle_type, recognised_date,
 	taxable_amount, post_tax_amount, ncemi_amount,
 	COALESCE(NULLIF(cgst_rate,0)::float, (ROUND((cgst_amount::float * 100) / NULLIF(taxable_amount,0)::float)/100)::float) as cgst_rate,
 	COALESCE(NULLIF(sgst_rate,0)::float, (ROUND((sgst_amount::float * 100) / NULLIF(taxable_amount,0)::float)/100)::float) as sgst_rate,
 	COALESCE(NULLIF(igst_rate,0)::float, (ROUND((igst_amount::float * 100) / NULLIF(taxable_amount,0)::float)/100)::float) as igst_rate,
 	cgst_amount, sgst_amount, igst_amount, week_start_date, week_end_date, dispatch_fc_id,
 	billing_start_date, billing_end_date,
 	is_b2b
 FROM financial_events
 )

-- =============================================================================
-- Step 4a: Deferral calculation — cross-month billing cycle correction
--          Identifies Normal_billing_cycle rows that span two calendar months
--          and computes the next-month portion of post_tax revenue.
-- =============================================================================
, deferral_calc AS (
  SELECT
    city_id, vertical, dispatch_fc_id,
    is_b2b,
    recognised_date                  AS dr_recognised_date,
    DATE_TRUNC('month', billing_end_date) AS cr_recognised_date,
    EXTRACT(DAY FROM billing_start_date) AS start_day,
    -- total_days: calendar days in the billing cycle, adjusted for avg month length
    CASE
      WHEN EXTRACT(MONTH FROM billing_start_date) = 2           THEN (billing_end_date - billing_start_date + 1 + 2.5)
      WHEN EXTRACT(MONTH FROM billing_start_date) IN (4,6,9,11) THEN (billing_end_date - billing_start_date + 1 + 0.5)
      ELSE                                                            (billing_end_date - billing_start_date + 1 - 0.5)
    END AS total_days,
    DATEADD(day, -1, DATEADD(month, 1, DATE_TRUNC('month', billing_start_date))) AS current_month_end,
    post_tax_amount, ncemi_amount, taxable_amount
  FROM gst_fix
  WHERE cycle_type = 'Normal_billing_cycle'
    AND billing_end_date IS NOT NULL
    AND billing_end_date > DATEADD(day, -1, DATEADD(month, 1, DATE_TRUNC('month', billing_start_date)))
)

, deferral_amounts AS (
  SELECT
    city_id, vertical, dispatch_fc_id,
    is_b2b,
    dr_recognised_date,
    cr_recognised_date,
    -- next_month_post_tax: total minus current-month portion (avoids dual rounding)
    (taxable_amount + COALESCE(ncemi_amount, 0))
      - ROUND((30.5 - (start_day - 1)) * (taxable_amount + COALESCE(ncemi_amount, 0)) / total_days, 2)
      AS next_month_taxable_amount
  FROM deferral_calc
  WHERE total_days > 0
)

, deferral_agg AS (
  SELECT
    c.name AS city_name,
    da.city_id,
    fc.email,
    fc.p360_organisation_id,
    fc.p360_store_id,
    da.vertical,
    da.is_b2b,
    da.dr_recognised_date AS recognised_date,
    da.cr_recognised_date,
    SUM(da.next_month_taxable_amount) AS next_month_taxable_amount
  FROM deferral_amounts da
  LEFT JOIN analytics.wmsl_fulfilment_centres AS fc
      ON fc.id = da.dispatch_fc_id AND fc.p360_organisation_id IS NOT NULL
  LEFT JOIN panem_evolve.cities AS c ON c.id = da.city_id
  WHERE da.next_month_taxable_amount <> 0
  GROUP BY c.name, da.city_id, fc.email, fc.p360_organisation_id, fc.p360_store_id,
           da.vertical, da.is_b2b, da.dr_recognised_date, da.cr_recognised_date
)

, agg_view AS (
    SELECT
        c.name AS city_name,
        ev.city_id,
        fc.email,
        fc.p360_organisation_id,
        fc.p360_store_id,
        ev.vertical,
        ev.is_b2b,
        ev.recognised_date,
        ev.cycle_type,

        -- Ledger names/codes derived from tax rate
        CASE
            WHEN ev.cgst_rate::FLOAT = 0.025 THEN 'Output CGST 2.5%'
            WHEN ev.cgst_rate::FLOAT = 0.06  THEN 'Output CGST 6%'
            WHEN ev.cgst_rate::FLOAT = 0.09  THEN 'Output CGST 9%'
            WHEN ev.cgst_rate::FLOAT = 0.14  THEN 'Output CGST 14%'
        END AS cgst_ledger_name,
        CASE
            WHEN ev.cgst_rate::FLOAT = 0.025 THEN '3006250'
            WHEN ev.cgst_rate::FLOAT = 0.06  THEN '3006260'
            WHEN ev.cgst_rate::FLOAT = 0.09  THEN '3006270'
            WHEN ev.cgst_rate::FLOAT = 0.14  THEN '3006280'
        END AS cgst_ledger_code,
        CASE
            WHEN ev.sgst_rate::FLOAT = 0.025 THEN 'Output SGST 2.5%'
            WHEN ev.sgst_rate::FLOAT = 0.06  THEN 'Output SGST 6%'
            WHEN ev.sgst_rate::FLOAT = 0.09  THEN 'Output SGST 9%'
            WHEN ev.sgst_rate::FLOAT = 0.14  THEN 'Output SGST 14%'
        END AS sgst_ledger_name,
        CASE
            WHEN ev.sgst_rate::FLOAT = 0.025 THEN '3006290'
            WHEN ev.sgst_rate::FLOAT = 0.06  THEN '3006300'
            WHEN ev.sgst_rate::FLOAT = 0.09  THEN '3006310'
            WHEN ev.sgst_rate::FLOAT = 0.14  THEN '3006320'
        END AS sgst_ledger_code,
        CASE
            WHEN ev.igst_rate::FLOAT = 0.05 THEN 'Output IGST 5%'
            WHEN ev.igst_rate::FLOAT = 0.12 THEN 'Output IGST 12%'
            WHEN ev.igst_rate::FLOAT = 0.18 THEN 'Output IGST 18%'
            WHEN ev.igst_rate::FLOAT = 0.28 THEN 'Output IGST 28%'
        END AS igst_ledger_name,
        CASE
            WHEN ev.igst_rate::FLOAT = 0.05 THEN '3006330'
            WHEN ev.igst_rate::FLOAT = 0.12 THEN '3006340'
            WHEN ev.igst_rate::FLOAT = 0.18 THEN '3006350'
            WHEN ev.igst_rate::FLOAT = 0.28 THEN '3006360'
        END AS igst_ledger_code,

        -- Aggregated amounts
        SUM(ev.taxable_amount + COALESCE(ev.ncemi_amount, 0)) AS sum_of_taxable_amount,
        SUM(COALESCE(ev.cgst_amount, 0)) AS sum_of_cgst_amount,
        SUM(COALESCE(ev.sgst_amount, 0)) AS sum_of_sgst_amount,
        SUM(COALESCE(ev.igst_amount, 0)) AS sum_of_igst_amount,
        SUM(COALESCE(ev.post_tax_amount, 0) + COALESCE(ev.ncemi_amount, 0)) AS sum_of_total

    FROM gst_fix ev
    LEFT JOIN analytics.wmsl_fulfilment_centres AS fc
        ON fc.id = ev.dispatch_fc_id
       AND fc.p360_organisation_id IS NOT NULL
    LEFT JOIN panem_evolve.cities AS c ON c.id = ev.city_id
    GROUP BY
        c.name, ev.city_id, ev.dispatch_fc_id, fc.email, fc.p360_organisation_id, fc.p360_store_id,
        ev.vertical, ev.is_b2b, recognised_date,
        ev.cycle_type,
        cgst_ledger_name, cgst_ledger_code,
        sgst_ledger_name, sgst_ledger_code,
        igst_ledger_name, igst_ledger_code
),

-- =============================================================================
-- Step 5: Unpivot into DR/CR journal entries
-- =============================================================================
unpivoted_data AS (
    -- Trade Receivables (DR)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, cycle_type, recognised_date,
        CASE
            WHEN is_b2b = TRUE                                                            THEN '3004020'
            WHEN vertical = 'FURLENCO_RENTAL'                                             THEN '3004010'
            WHEN vertical = 'UNLMTD'                                                      THEN '3004080'
            WHEN vertical IN ('New Sales - D2C', 'New Sales - Store')                     THEN '3004030'
            WHEN vertical IN ('Refurb Sales - D2C', 'Refurb Sales - Store')               THEN '3004040'
            ELSE '0000000'
        END AS code_number,
        CASE
            WHEN is_b2b = TRUE                                                            THEN 'Trade Receivables - B2B'
            WHEN vertical = 'FURLENCO_RENTAL'                                             THEN 'Trade Receivables - Furlenco'
            WHEN vertical = 'UNLMTD'                                                      THEN 'Trade Receivables - Unlmtd'
            WHEN vertical IN ('New Sales - D2C', 'New Sales - Store')                     THEN 'Trade Receivables - New Sales'
            WHEN vertical IN ('Refurb Sales - D2C', 'Refurb Sales - Store')               THEN 'Trade Receivables - Refurb Sales'
            ELSE 'Trade Receivables - Unknown'
        END AS particulars,
        CASE WHEN cycle_type = 'Credit_Note' THEN NULL::FLOAT ELSE sum_of_total::FLOAT END AS DR,
        CASE WHEN cycle_type = 'Credit_Note' THEN sum_of_total::FLOAT ELSE NULL::FLOAT END AS CR,
        1 AS row_order, 0 AS sub_order
    FROM agg_view

    UNION ALL

    -- Revenue (CR)
    SELECT
        city_name, city_id, email, p360_store_id, p360_organisation_id,
        vertical, cycle_type, recognised_date,
        CASE
            WHEN is_b2b = TRUE                      THEN '1001150'
            WHEN vertical = 'FURLENCO_RENTAL'       THEN '1001010'
            WHEN vertical = 'UNLMTD'                THEN '1001020'
            WHEN vertical = 'New Sales - D2C'       THEN '1001050'
            WHEN vertical = 'New Sales - Store'     THEN '1001030'
            WHEN vertical = 'Refurb Sales - D2C'    THEN '1001060'
            WHEN vertical = 'Refurb Sales - Store'  THEN '1001040'
            ELSE '0000000'
        END AS code_number,
        CASE
            WHEN is_b2b = TRUE                      THEN 'Revenue - B2B Sales'
            WHEN vertical = 'FURLENCO_RENTAL'       THEN 'Revenue - Furlenco'
            WHEN vertical = 'UNLMTD'                THEN 'Revenue - Unlmtd'
            WHEN vertical = 'New Sales - D2C'       THEN 'Revenue - New Sales - D2C'
            WHEN vertical = 'New Sales - Store'     THEN 'Revenue - New Sales - Store'
            WHEN vertical = 'Refurb Sales - D2C'    THEN 'Revenue - Refurb Sales - D2C'
            WHEN vertical = 'Refurb Sales - Store'  THEN 'Revenue - Refurb Sales - Store'
            ELSE 'Revenue - Unknown'
        END AS particulars,
        CASE WHEN cycle_type = 'Credit_Note' THEN sum_of_taxable_amount::FLOAT ELSE NULL::FLOAT END AS DR,
        CASE WHEN cycle_type = 'Credit_Note' THEN NULL::FLOAT ELSE sum_of_taxable_amount::FLOAT END AS CR,
        1 AS row_order, 1 AS sub_order
    FROM agg_view

    UNION ALL

    -- Output CGST (CR)
    SELECT
        city_name, city_id, email, p360_store_id, p360_organisation_id,
        vertical, cycle_type, recognised_date,
        cgst_ledger_code AS code_number, cgst_ledger_name AS particulars,
        CASE WHEN cycle_type = 'Credit_Note' THEN sum_of_cgst_amount::FLOAT ELSE NULL::FLOAT END AS DR,
        CASE WHEN cycle_type = 'Credit_Note' THEN NULL::FLOAT ELSE sum_of_cgst_amount::FLOAT END AS CR,
        1 AS row_order, 2 AS sub_order
    FROM agg_view
    WHERE sum_of_cgst_amount > 0

    UNION ALL

    -- Output SGST (CR)
    SELECT
        city_name, city_id, email, p360_store_id, p360_organisation_id,
        vertical, cycle_type, recognised_date,
        sgst_ledger_code AS code_number, sgst_ledger_name AS particulars,
        CASE WHEN cycle_type = 'Credit_Note' THEN sum_of_sgst_amount::FLOAT ELSE NULL::FLOAT END AS DR,
        CASE WHEN cycle_type = 'Credit_Note' THEN NULL::FLOAT ELSE sum_of_sgst_amount::FLOAT END AS CR,
        1 AS row_order, 3 AS sub_order
    FROM agg_view
    WHERE sum_of_sgst_amount > 0

    UNION ALL

    -- Output IGST (CR)
    SELECT
        city_name, city_id, email, p360_store_id, p360_organisation_id,
        vertical, cycle_type, recognised_date,
        igst_ledger_code AS code_number, igst_ledger_name AS particulars,
        CASE WHEN cycle_type = 'Credit_Note' THEN sum_of_igst_amount::FLOAT ELSE NULL::FLOAT END AS DR,
        CASE WHEN cycle_type = 'Credit_Note' THEN NULL::FLOAT ELSE sum_of_igst_amount::FLOAT END AS CR,
        1 AS row_order, 4 AS sub_order
    FROM agg_view
    WHERE sum_of_igst_amount > 0

    UNION ALL

    -- B2B Reversal: CR Trade Receivables - B2B (reverse the DR from normal entry)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, cycle_type, recognised_date,
        '3004020' AS code_number,
        'Trade Receivables - B2B' AS particulars,
        NULL::FLOAT AS DR, sum_of_total::FLOAT AS CR,
        1 AS row_order, 5 AS sub_order
    FROM agg_view
    WHERE is_b2b = TRUE
      AND cycle_type <> 'Credit_Note'

    UNION ALL

    -- B2B Reversal: DR Revenue - B2B Sales (reverse the CR from normal entry)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, cycle_type, recognised_date,
        '1001150' AS code_number,
        'Revenue - B2B Sales' AS particulars,
        sum_of_taxable_amount::FLOAT AS DR, NULL::FLOAT AS CR,
        1 AS row_order, 6 AS sub_order
    FROM agg_view
    WHERE is_b2b = TRUE
      AND cycle_type <> 'Credit_Note'

    UNION ALL

    -- B2B Reversal: DR CGST (reverse the CR from normal entry)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, cycle_type, recognised_date,
        cgst_ledger_code AS code_number, cgst_ledger_name AS particulars,
        sum_of_cgst_amount::FLOAT AS DR, NULL::FLOAT AS CR,
        1 AS row_order, 7 AS sub_order
    FROM agg_view
    WHERE is_b2b = TRUE
      AND cycle_type <> 'Credit_Note'
      AND sum_of_cgst_amount > 0

    UNION ALL

    -- B2B Reversal: DR SGST (reverse the CR from normal entry)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, cycle_type, recognised_date,
        sgst_ledger_code AS code_number, sgst_ledger_name AS particulars,
        sum_of_sgst_amount::FLOAT AS DR, NULL::FLOAT AS CR,
        1 AS row_order, 8 AS sub_order
    FROM agg_view
    WHERE is_b2b = TRUE
      AND cycle_type <> 'Credit_Note'
      AND sum_of_sgst_amount > 0

    UNION ALL

    -- B2B Reversal: DR IGST (reverse the CR from normal entry)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, cycle_type, recognised_date,
        igst_ledger_code AS code_number, igst_ledger_name AS particulars,
        sum_of_igst_amount::FLOAT AS DR, NULL::FLOAT AS CR,
        1 AS row_order, 9 AS sub_order
    FROM agg_view
    WHERE is_b2b = TRUE
      AND cycle_type <> 'Credit_Note'
      AND sum_of_igst_amount > 0

    UNION ALL

    -- Deferral: Current month — DR Revenue (reduce current month revenue)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, 'Deferral' AS cycle_type, recognised_date,
        CASE
            WHEN vertical = 'Sale-B2B'              THEN '1001150'
            WHEN vertical = 'FURLENCO_RENTAL'       THEN '1001010'
            WHEN vertical = 'UNLMTD'                THEN '1001020'
            WHEN vertical = 'New Sales - D2C'       THEN '1001050'
            WHEN vertical = 'New Sales - Store'     THEN '1001030'
            WHEN vertical = 'Refurb Sales - D2C'    THEN '1001060'
            WHEN vertical = 'Refurb Sales - Store'  THEN '1001040'
            ELSE '0000000'
        END AS code_number,
        CASE
            WHEN vertical = 'Sale-B2B'              THEN 'Revenue - B2B Sales'
            WHEN vertical = 'FURLENCO_RENTAL'       THEN 'Revenue - Furlenco'
            WHEN vertical = 'UNLMTD'                THEN 'Revenue - Unlmtd'
            WHEN vertical = 'New Sales - D2C'       THEN 'Revenue - New Sales - D2C'
            WHEN vertical = 'New Sales - Store'     THEN 'Revenue - New Sales - Store'
            WHEN vertical = 'Refurb Sales - D2C'    THEN 'Revenue - Refurb Sales - D2C'
            WHEN vertical = 'Refurb Sales - Store'  THEN 'Revenue - Refurb Sales - Store'
            ELSE 'Revenue - Unknown'
        END AS particulars,
        next_month_taxable_amount AS DR, NULL::FLOAT AS CR,
        2 AS row_order, 0 AS sub_order
    FROM deferral_agg

    UNION ALL

    -- Deferral: Current month — CR Deferred Revenue (create liability)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, 'Deferral' AS cycle_type, recognised_date,
        '4006020' AS code_number,
        'Deferred Revenue' AS particulars,
        NULL::FLOAT AS DR, next_month_taxable_amount AS CR,
        2 AS row_order, 1 AS sub_order
    FROM deferral_agg

    UNION ALL

    -- Deferral: Next month — DR Deferred Revenue (clear liability)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, 'Deferral' AS cycle_type, cr_recognised_date AS recognised_date,
        '4006020' AS code_number,
        'Deferred Revenue' AS particulars,
        next_month_taxable_amount AS DR, NULL::FLOAT AS CR,
        2 AS row_order, 2 AS sub_order
    FROM deferral_agg

    UNION ALL

    -- Deferral: Next month — CR Revenue (recognize deferred revenue)
    SELECT
        city_name, city_id, email AS organization_email_id,
        p360_store_id AS store_id, p360_organisation_id AS organization_id,
        vertical, 'Deferral' AS cycle_type, cr_recognised_date AS recognised_date,
        CASE
            WHEN vertical = 'Sale-B2B'              THEN '1001150'
            WHEN vertical = 'FURLENCO_RENTAL'       THEN '1001010'
            WHEN vertical = 'UNLMTD'                THEN '1001020'
            WHEN vertical = 'New Sales - D2C'       THEN '1001050'
            WHEN vertical = 'New Sales - Store'     THEN '1001030'
            WHEN vertical = 'Refurb Sales - D2C'    THEN '1001060'
            WHEN vertical = 'Refurb Sales - Store'  THEN '1001040'
            ELSE '0000000'
        END AS code_number,
        CASE
            WHEN vertical = 'Sale-B2B'              THEN 'Revenue - B2B Sales'
            WHEN vertical = 'FURLENCO_RENTAL'       THEN 'Revenue - Furlenco'
            WHEN vertical = 'UNLMTD'                THEN 'Revenue - Unlmtd'
            WHEN vertical = 'New Sales - D2C'       THEN 'Revenue - New Sales - D2C'
            WHEN vertical = 'New Sales - Store'     THEN 'Revenue - New Sales - Store'
            WHEN vertical = 'Refurb Sales - D2C'    THEN 'Revenue - Refurb Sales - D2C'
            WHEN vertical = 'Refurb Sales - Store'  THEN 'Revenue - Refurb Sales - Store'
            ELSE 'Revenue - Unknown'
        END AS particulars,
        NULL::FLOAT AS DR, next_month_taxable_amount AS CR,
        2 AS row_order, 3 AS sub_order
    FROM deferral_agg
)

-- =============================================================================
-- Final output — period-bucketed (controlled by period_config CTE above)
-- =============================================================================
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
    start_date,
    end_date,
    LOWER(vertical) || ', ' || TO_CHAR(start_date, 'Mon-YYYY') AS remarks
FROM (
    SELECT
        code_number, particulars, DR, CR,
        city_name, cycle_type, vertical, city_id,
        store_id, organization_id, organization_email_id,
        CASE pc.period_type
            WHEN 'day'     THEN recognised_date
            WHEN 'week'    THEN DATE_TRUNC('week', recognised_date)::DATE
            WHEN 'month'   THEN DATE_TRUNC('month', recognised_date)::DATE
            WHEN 'quarter' THEN DATE_TRUNC('quarter', recognised_date)::DATE
            WHEN 'year'    THEN DATE_TRUNC('year', recognised_date)::DATE
        END AS start_date,
        CASE pc.period_type
            WHEN 'day'     THEN recognised_date
            WHEN 'week'    THEN DATE_TRUNC('week', recognised_date)::DATE + 6
            WHEN 'month'   THEN DATEADD(day, -1, DATEADD(month,   1, DATE_TRUNC('month',   recognised_date)::DATE))
            WHEN 'quarter' THEN DATEADD(day, -1, DATEADD(month,   3, DATE_TRUNC('quarter', recognised_date)::DATE))
            WHEN 'year'    THEN DATEADD(day, -1, DATEADD(year,    1, DATE_TRUNC('year',    recognised_date)::DATE))
        END AS end_date,
        row_order,
        sub_order
    FROM unpivoted_data
    CROSS JOIN period_config pc
) bucketed
GROUP BY
    code_number,
    particulars,
    city_name,
    cycle_type,
    vertical,
    city_id,
    store_id,
    organization_id,
    organization_email_id,
    start_date,
    end_date,
    row_order,
    sub_order
ORDER BY
    city_name,
    start_date,
    cycle_type,
    vertical,
    row_order,
    sub_order
