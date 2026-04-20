-- ============================================================
-- P&C Insurance Pipeline — Gold Transformation Scripts
-- All 5 Gold tables
-- ============================================================

USE gold_db;
GO

-- ============================================================
-- 1. dim_coverage — Full load
-- ============================================================

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_coverage')
    DROP EXTERNAL TABLE dim_coverage;

CREATE EXTERNAL TABLE dim_coverage
WITH (
    LOCATION    = 'gold/dim_coverage/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS
SELECT
    coverage_id, coverage_name, line_of_business, coverage_type,
    min_limit, max_limit, min_deductible, max_deductible,
    effective_date, is_active,
    CAST(GETDATE() AS DATE) AS gold_load_date
FROM OPENROWSET(
    BULK 'silver/coverage/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) WITH (
    coverage_id        VARCHAR(10),
    coverage_name      VARCHAR(100),
    line_of_business   VARCHAR(20),
    coverage_type      VARCHAR(30),
    min_limit          DECIMAL(15,2),
    max_limit          DECIMAL(15,2),
    min_deductible     DECIMAL(15,2),
    max_deductible     DECIMAL(15,2),
    effective_date     DATE,
    is_active          INT,
    gold_load_date     DATE
) AS silver_coverage;
GO

-- ============================================================
-- 2. dim_agent — Full load
-- ============================================================

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_agent')
    DROP EXTERNAL TABLE dim_agent;

CREATE EXTERNAL TABLE dim_agent
WITH (
    LOCATION    = 'gold/dim_agent/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS
SELECT
    agent_id, agent_first_name, agent_last_name, agent_email, agent_phone,
    agency_name, agency_city, agency_state, agency_zip, license_number,
    line_of_business, is_active, hire_date, termination_date,
    CAST(GETDATE() AS DATE) AS gold_load_date
FROM OPENROWSET(
    BULK 'silver/agent/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) WITH (
    agent_id           VARCHAR(10),
    agent_first_name   VARCHAR(50),
    agent_last_name    VARCHAR(50),
    agent_email        VARCHAR(100),
    agent_phone        VARCHAR(30),
    agency_name        VARCHAR(100),
    agency_city        VARCHAR(50),
    agency_state       VARCHAR(2),
    agency_zip         VARCHAR(10),
    license_number     VARCHAR(20),
    line_of_business   VARCHAR(50),
    is_active          INT,
    hire_date          DATE,
    termination_date   DATE,
    silver_load_date   DATE
) AS silver_agent;
GO

-- ============================================================
-- 3. dim_customer — SCD Type 1 (merge/swap)
-- ============================================================

-- Step 1: Merge
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_customer_tmp')
    DROP EXTERNAL TABLE dim_customer_tmp;

CREATE EXTERNAL TABLE dim_customer_tmp
WITH (
    LOCATION    = 'gold/dim_customer_tmp/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS
WITH combined AS (
    SELECT
        CAST(customer_id       COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS customer_id,
        CAST(first_name        COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS first_name,
        CAST(last_name         COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS last_name,
        CAST(email             COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(100)) AS email,
        CAST(phone             COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(30))  AS phone,
        date_of_birth,
        CAST(gender            COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(5))   AS gender,
        CAST(address_street    COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(100)) AS address_street,
        CAST(address_suite     COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS address_suite,
        CAST(address_city      COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS address_city,
        CAST(address_state     COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(2))   AS address_state,
        CAST(zip_code          COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(10))  AS zip_code,
        CAST(marital_status    COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS marital_status,
        CAST(occupation        COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS occupation,
        credit_score,
        CAST(risk_tier         COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS risk_tier,
        CAST(preferred_contact COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS preferred_contact,
        customer_since, created_date, modified_date, gold_load_date
    FROM dim_customer

    UNION ALL

    SELECT
        customer_id, first_name, last_name, email, phone, date_of_birth,
        gender, address_street, address_suite, address_city, address_state,
        zip_code, marital_status, occupation, credit_score, risk_tier,
        preferred_contact, customer_since, created_date, modified_date,
        CAST(GETDATE() AS DATE) AS gold_load_date
    FROM OPENROWSET(
        BULK 'silver/customer/*.parquet',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (
        customer_id       VARCHAR(20), first_name  VARCHAR(50),  last_name   VARCHAR(50),
        email             VARCHAR(100), phone      VARCHAR(30),  date_of_birth DATE,
        gender            VARCHAR(5),  address_street VARCHAR(100), address_suite VARCHAR(50),
        address_city      VARCHAR(50), address_state VARCHAR(2),  zip_code    VARCHAR(10),
        marital_status    VARCHAR(20), occupation  VARCHAR(50),  credit_score INT,
        risk_tier         VARCHAR(20), preferred_contact VARCHAR(20), customer_since DATE,
        source_system     VARCHAR(50), created_date DATE,        modified_date DATE,
        silver_load_date  DATE
    ) AS sc
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY modified_date DESC) AS rn
    FROM combined WHERE customer_id IS NOT NULL
)
SELECT
    customer_id, first_name, last_name, email, phone, date_of_birth,
    gender, address_street, address_suite, address_city, address_state,
    zip_code, marital_status, occupation, credit_score, risk_tier,
    preferred_contact, customer_since, created_date, modified_date, gold_load_date
FROM deduped WHERE rn = 1;
GO

-- Step 2: Drop and recreate
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_customer')
    DROP EXTERNAL TABLE dim_customer;

CREATE EXTERNAL TABLE dim_customer
WITH (
    LOCATION    = 'gold/dim_customer/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS
SELECT
    customer_id, first_name, last_name, email, phone, date_of_birth,
    gender, address_street, address_suite, address_city, address_state,
    zip_code, marital_status, occupation, credit_score, risk_tier,
    preferred_contact, customer_since, created_date, modified_date, gold_load_date
FROM OPENROWSET(
    BULK 'gold/dim_customer_tmp/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) WITH (
    customer_id       VARCHAR(20), first_name  VARCHAR(50),  last_name   VARCHAR(50),
    email             VARCHAR(100), phone      VARCHAR(30),  date_of_birth DATE,
    gender            VARCHAR(5),  address_street VARCHAR(100), address_suite VARCHAR(50),
    address_city      VARCHAR(50), address_state VARCHAR(2),  zip_code    VARCHAR(10),
    marital_status    VARCHAR(20), occupation  VARCHAR(50),  credit_score INT,
    risk_tier         VARCHAR(20), preferred_contact VARCHAR(20), customer_since DATE,
    created_date      DATE,        modified_date DATE,        gold_load_date DATE
) AS final;
GO

-- ============================================================
-- 4. dim_policy — SCD Type 2 (merge/swap)
-- ============================================================

-- Step 1: Merge
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_policy_tmp')
    DROP EXTERNAL TABLE dim_policy_tmp;

CREATE EXTERNAL TABLE dim_policy_tmp
WITH (
    LOCATION    = 'gold/dim_policy_tmp/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS
WITH silver_pol AS (
    SELECT * FROM OPENROWSET(
        BULK 'silver/policy/*.parquet',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (
        policy_id VARCHAR(15), policy_number VARCHAR(30), customer_id VARCHAR(15),
        agent_id VARCHAR(10), coverage_id VARCHAR(10), line_of_business VARCHAR(20),
        policy_status VARCHAR(20), effective_date DATE, expiration_date DATE,
        policy_term_months INT, annual_premium DECIMAL(15,2), monthly_premium DECIMAL(15,2),
        deductible DECIMAL(15,2), coverage_limit DECIMAL(15,2), payment_plan VARCHAR(20),
        cancellation_date DATE, cancellation_reason VARCHAR(50), underwriting_score INT,
        risk_state VARCHAR(2), created_date DATE, modified_date DATE, silver_load_date DATE
    ) AS sp
),
gold_pol AS (
    SELECT * FROM OPENROWSET(
        BULK 'gold/dim_policy/*.parquet',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (
        policy_id VARCHAR(15), policy_number VARCHAR(30), customer_id VARCHAR(15),
        agent_id VARCHAR(10), coverage_id VARCHAR(10), line_of_business VARCHAR(20),
        policy_status VARCHAR(20), effective_date DATE, expiration_date DATE,
        policy_term_months INT, annual_premium DECIMAL(15,2), monthly_premium DECIMAL(15,2),
        deductible DECIMAL(15,2), coverage_limit DECIMAL(15,2), payment_plan VARCHAR(20),
        cancellation_date DATE, cancellation_reason VARCHAR(50), underwriting_score INT,
        risk_state VARCHAR(2), scd_start_date DATE, scd_end_date DATE, is_current INT,
        gold_load_date DATE
    ) AS gp
),
historical AS (
    SELECT policy_id, policy_number, customer_id, agent_id, coverage_id,
        line_of_business, policy_status, effective_date, expiration_date,
        policy_term_months, annual_premium, monthly_premium, deductible,
        coverage_limit, payment_plan, cancellation_date, cancellation_reason,
        underwriting_score, risk_state, scd_start_date, scd_end_date, is_current, gold_load_date
    FROM gold_pol WHERE is_current = 0
),
unchanged AS (
    SELECT g.policy_id, g.policy_number, g.customer_id, g.agent_id, g.coverage_id,
        g.line_of_business, g.policy_status, g.effective_date, g.expiration_date,
        g.policy_term_months, g.annual_premium, g.monthly_premium, g.deductible,
        g.coverage_limit, g.payment_plan, g.cancellation_date, g.cancellation_reason,
        g.underwriting_score, g.risk_state, g.scd_start_date, g.scd_end_date, g.is_current, g.gold_load_date
    FROM gold_pol g INNER JOIN silver_pol s ON g.policy_id = s.policy_id
    WHERE g.is_current = 1
    AND g.annual_premium = s.annual_premium
    AND g.policy_status  = s.policy_status
    AND g.coverage_limit = s.coverage_limit
),
expired AS (
    SELECT g.policy_id, g.policy_number, g.customer_id, g.agent_id, g.coverage_id,
        g.line_of_business, g.policy_status, g.effective_date, g.expiration_date,
        g.policy_term_months, g.annual_premium, g.monthly_premium, g.deductible,
        g.coverage_limit, g.payment_plan, g.cancellation_date, g.cancellation_reason,
        g.underwriting_score, g.risk_state, g.scd_start_date,
        CAST(GETDATE() AS DATE) AS scd_end_date,
        CAST(0 AS INT) AS is_current, g.gold_load_date
    FROM gold_pol g INNER JOIN silver_pol s ON g.policy_id = s.policy_id
    WHERE g.is_current = 1
    AND (g.annual_premium != s.annual_premium OR g.policy_status != s.policy_status OR g.coverage_limit != s.coverage_limit)
),
new_records AS (
    SELECT s.policy_id, s.policy_number, s.customer_id, s.agent_id, s.coverage_id,
        s.line_of_business, s.policy_status, s.effective_date, s.expiration_date,
        s.policy_term_months, s.annual_premium, s.monthly_premium, s.deductible,
        s.coverage_limit, s.payment_plan, s.cancellation_date, s.cancellation_reason,
        s.underwriting_score, s.risk_state,
        CAST(GETDATE() AS DATE) AS scd_start_date,
        CAST('9999-12-31' AS DATE) AS scd_end_date,
        CAST(1 AS INT) AS is_current,
        CAST(GETDATE() AS DATE) AS gold_load_date
    FROM silver_pol s
    LEFT JOIN gold_pol g ON s.policy_id = g.policy_id AND g.is_current = 1
    WHERE g.policy_id IS NULL
    OR (g.annual_premium != s.annual_premium OR g.policy_status != s.policy_status OR g.coverage_limit != s.coverage_limit)
)
SELECT * FROM historical UNION ALL SELECT * FROM unchanged UNION ALL SELECT * FROM expired UNION ALL SELECT * FROM new_records;
GO

-- Step 2: Drop and recreate
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_policy')
    DROP EXTERNAL TABLE dim_policy;

CREATE EXTERNAL TABLE dim_policy
WITH (
    LOCATION    = 'gold/dim_policy/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS
SELECT policy_id, policy_number, customer_id, agent_id, coverage_id,
    line_of_business, policy_status, effective_date, expiration_date,
    policy_term_months, annual_premium, monthly_premium, deductible,
    coverage_limit, payment_plan, cancellation_date, cancellation_reason,
    underwriting_score, risk_state, scd_start_date, scd_end_date, is_current, gold_load_date
FROM OPENROWSET(
    BULK 'gold/dim_policy_tmp/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) WITH (
    policy_id VARCHAR(15), policy_number VARCHAR(30), customer_id VARCHAR(15),
    agent_id VARCHAR(10), coverage_id VARCHAR(10), line_of_business VARCHAR(20),
    policy_status VARCHAR(20), effective_date DATE, expiration_date DATE,
    policy_term_months INT, annual_premium DECIMAL(15,2), monthly_premium DECIMAL(15,2),
    deductible DECIMAL(15,2), coverage_limit DECIMAL(15,2), payment_plan VARCHAR(20),
    cancellation_date DATE, cancellation_reason VARCHAR(50), underwriting_score INT,
    risk_state VARCHAR(2), scd_start_date DATE, scd_end_date DATE, is_current INT,
    gold_load_date DATE
) AS final;
GO

-- ============================================================
-- 5. fact_claims — SCD Type 2 (merge/swap)
-- ============================================================

-- Step 1: Merge
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'fact_claims_tmp')
    DROP EXTERNAL TABLE fact_claims_tmp;

CREATE EXTERNAL TABLE fact_claims_tmp
WITH (
    LOCATION    = 'gold/fact_claims_tmp/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS
WITH silver_claims AS (
    SELECT
        c.claim_id, c.claim_number, c.policy_id, p.customer_id, p.agent_id, p.coverage_id,
        c.peril_type, c.claim_status, c.reserve_amount, c.settlement_amount,
        c.loss_date, c.fnol_date, c.reported_date, c.close_date, c.adjuster_id,
        c.subrogation_flag, c.fraud_flag, c.cat_event_name, c.litigation_flag,
        c.fnol_lag_days, c.is_orphaned_claim
    FROM OPENROWSET(
        BULK 'silver/claims/*.parquet',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (
        claim_id VARCHAR(15), claim_number VARCHAR(25), policy_id VARCHAR(15),
        peril_type VARCHAR(50), claim_status VARCHAR(20), reserve_amount DECIMAL(15,2),
        settlement_amount DECIMAL(15,2), loss_date DATE, fnol_date DATE,
        reported_date DATE, close_date DATE, adjuster_id VARCHAR(10),
        subrogation_flag VARCHAR(1), fraud_flag VARCHAR(1), cat_event_name VARCHAR(50),
        litigation_flag VARCHAR(1), fnol_lag_days INT, is_orphaned_claim VARCHAR(1),
        modified_date DATE, silver_load_date DATE
    ) AS c
    LEFT JOIN OPENROWSET(
        BULK 'silver/policy/*.parquet',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (policy_id VARCHAR(15), customer_id VARCHAR(15), agent_id VARCHAR(10), coverage_id VARCHAR(10)) AS p
    ON c.policy_id = p.policy_id
),
gold_claims AS (
    SELECT * FROM OPENROWSET(
        BULK 'gold/fact_claims/*.parquet',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (
        claim_id VARCHAR(15), claim_number VARCHAR(25), policy_id VARCHAR(15),
        customer_id VARCHAR(15), agent_id VARCHAR(10), coverage_id VARCHAR(10),
        peril_type VARCHAR(50), claim_status VARCHAR(20), reserve_amount DECIMAL(15,2),
        settlement_amount DECIMAL(15,2), loss_date DATE, fnol_date DATE,
        reported_date DATE, close_date DATE, adjuster_id VARCHAR(10),
        subrogation_flag VARCHAR(1), fraud_flag VARCHAR(1), cat_event_name VARCHAR(50),
        litigation_flag VARCHAR(1), fnol_lag_days INT, is_orphaned_claim VARCHAR(1),
        scd_start_date DATE, scd_end_date DATE, is_current INT, gold_load_date DATE
    ) AS gc
),
historical AS (
    SELECT claim_id, claim_number, policy_id, customer_id, agent_id, coverage_id,
        peril_type, claim_status, reserve_amount, settlement_amount, loss_date, fnol_date,
        reported_date, close_date, adjuster_id, subrogation_flag, fraud_flag, cat_event_name,
        litigation_flag, fnol_lag_days, is_orphaned_claim, scd_start_date, scd_end_date, is_current, gold_load_date
    FROM gold_claims WHERE is_current = 0
),
unchanged AS (
    SELECT g.claim_id, g.claim_number, g.policy_id, g.customer_id, g.agent_id, g.coverage_id,
        g.peril_type, g.claim_status, g.reserve_amount, g.settlement_amount, g.loss_date, g.fnol_date,
        g.reported_date, g.close_date, g.adjuster_id, g.subrogation_flag, g.fraud_flag, g.cat_event_name,
        g.litigation_flag, g.fnol_lag_days, g.is_orphaned_claim, g.scd_start_date, g.scd_end_date, g.is_current, g.gold_load_date
    FROM gold_claims g INNER JOIN silver_claims s ON g.claim_id = s.claim_id
    WHERE g.is_current = 1
    AND g.claim_status = s.claim_status
    AND ISNULL(g.reserve_amount, 0)    = ISNULL(s.reserve_amount, 0)
    AND ISNULL(g.settlement_amount, 0) = ISNULL(s.settlement_amount, 0)
    AND ISNULL(g.fraud_flag, 'N')      = ISNULL(s.fraud_flag, 'N')
),
expired AS (
    SELECT g.claim_id, g.claim_number, g.policy_id, g.customer_id, g.agent_id, g.coverage_id,
        g.peril_type, g.claim_status, g.reserve_amount, g.settlement_amount, g.loss_date, g.fnol_date,
        g.reported_date, g.close_date, g.adjuster_id, g.subrogation_flag, g.fraud_flag, g.cat_event_name,
        g.litigation_flag, g.fnol_lag_days, g.is_orphaned_claim, g.scd_start_date,
        CAST(GETDATE() AS DATE) AS scd_end_date, CAST(0 AS INT) AS is_current, g.gold_load_date
    FROM gold_claims g INNER JOIN silver_claims s ON g.claim_id = s.claim_id
    WHERE g.is_current = 1
    AND (g.claim_status != s.claim_status
        OR ISNULL(g.reserve_amount, 0)    != ISNULL(s.reserve_amount, 0)
        OR ISNULL(g.settlement_amount, 0) != ISNULL(s.settlement_amount, 0)
        OR ISNULL(g.fraud_flag, 'N')      != ISNULL(s.fraud_flag, 'N'))
),
new_records AS (
    SELECT s.claim_id, s.claim_number, s.policy_id, s.customer_id, s.agent_id, s.coverage_id,
        s.peril_type, s.claim_status, s.reserve_amount, s.settlement_amount, s.loss_date, s.fnol_date,
        s.reported_date, s.close_date, s.adjuster_id, s.subrogation_flag, s.fraud_flag, s.cat_event_name,
        s.litigation_flag, s.fnol_lag_days, s.is_orphaned_claim,
        CAST(GETDATE() AS DATE) AS scd_start_date,
        CAST('9999-12-31' AS DATE) AS scd_end_date,
        CAST(1 AS INT) AS is_current,
        CAST(GETDATE() AS DATE) AS gold_load_date
    FROM silver_claims s
    LEFT JOIN gold_claims g ON s.claim_id = g.claim_id AND g.is_current = 1
    WHERE g.claim_id IS NULL
    OR (g.claim_status != s.claim_status
        OR ISNULL(g.reserve_amount, 0)    != ISNULL(s.reserve_amount, 0)
        OR ISNULL(g.settlement_amount, 0) != ISNULL(s.settlement_amount, 0)
        OR ISNULL(g.fraud_flag, 'N')      != ISNULL(s.fraud_flag, 'N'))
)
SELECT * FROM historical UNION ALL SELECT * FROM unchanged UNION ALL SELECT * FROM expired UNION ALL SELECT * FROM new_records;
GO

-- Step 2: Drop and recreate
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'fact_claims')
    DROP EXTERNAL TABLE fact_claims;

CREATE EXTERNAL TABLE fact_claims
WITH (
    LOCATION    = 'gold/fact_claims/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS
SELECT claim_id, claim_number, policy_id, customer_id, agent_id, coverage_id,
    peril_type, claim_status, reserve_amount, settlement_amount, loss_date, fnol_date,
    reported_date, close_date, adjuster_id, subrogation_flag, fraud_flag, cat_event_name,
    litigation_flag, fnol_lag_days, is_orphaned_claim, scd_start_date, scd_end_date, is_current, gold_load_date
FROM OPENROWSET(
    BULK 'gold/fact_claims_tmp/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) WITH (
    claim_id VARCHAR(15), claim_number VARCHAR(25), policy_id VARCHAR(15),
    customer_id VARCHAR(15), agent_id VARCHAR(10), coverage_id VARCHAR(10),
    peril_type VARCHAR(50), claim_status VARCHAR(20), reserve_amount DECIMAL(15,2),
    settlement_amount DECIMAL(15,2), loss_date DATE, fnol_date DATE,
    reported_date DATE, close_date DATE, adjuster_id VARCHAR(10),
    subrogation_flag VARCHAR(1), fraud_flag VARCHAR(1), cat_event_name VARCHAR(50),
    litigation_flag VARCHAR(1), fnol_lag_days INT, is_orphaned_claim VARCHAR(1),
    scd_start_date DATE, scd_end_date DATE, is_current INT, gold_load_date DATE
) AS final;
GO

-- ── Verify all Gold tables ────────────────────────────────
SELECT 'dim_coverage'  AS table_name, COUNT(*) AS row_count FROM dim_coverage
UNION ALL SELECT 'dim_agent',    COUNT(*) FROM dim_agent
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_policy',   COUNT(*) FROM dim_policy
UNION ALL SELECT 'fact_claims',  COUNT(*) FROM fact_claims;
GO
