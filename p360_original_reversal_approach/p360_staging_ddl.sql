-- =============================================================================
-- P360 STAGING — DDL
-- Daily snapshot of all current source data. Truncated and reloaded each
-- day by p360_staging_refresh.sql.
-- Run once to create the table.
-- =============================================================================

CREATE TABLE p360_staging (

    -- -------------------------------------------------------------------------
    -- Output columns (mirror p360_final_view_claude.sql final SELECT)
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
SORTKEY (code_number, city_id, vertical, cycle_type, start_date);  -- aligns with batch_runner FULL OUTER JOIN key (also joins on COALESCE(organization_id,'') and COALESCE(store_id,''))
