-- ============================================================
-- P&C Insurance Pipeline — Silver Policy
-- Incremental load — merge/swap pattern
-- Watermark read from pipeline_watermark on-prem SQL
-- ============================================================

USE silver_db;
GO

-- ── Step 1: Drop tmp if exists ────────────────────────────
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'silver_policy_tmp')
    DROP EXTERNAL TABLE silver_policy_tmp;

-- ── Step 2: Merge existing Silver + new Bronze → tmp ──────
-- NOTE: In pipeline this script uses dynamic content with watermark variable
-- Replace 'WATERMARK_VALUE' with actual watermark date when running manually
CREATE EXTERNAL TABLE silver_policy_tmp
WITH (
    LOCATION    = 'silver/policy_tmp/',
    DATA_SOURCE = silver_adls,
    FILE_FORMAT = ff_parquet
)
AS
WITH policy_raw AS (
    SELECT
        CAST(policy_id           COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(15))  AS policy_id,
        CAST(policy_number       COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(30))  AS policy_number,
        CAST(customer_id         COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(15))  AS customer_id,
        CAST(agent_id            COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(10))  AS agent_id,
        CAST(coverage_id         COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(10))  AS coverage_id,
        CAST(line_of_business    COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS line_of_business,
        CAST(policy_status       COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS policy_status,
        effective_date,
        expiration_date,
        policy_term_months,
        annual_premium,
        monthly_premium,
        deductible,
        coverage_limit,
        CAST(payment_plan        COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS payment_plan,
        cancellation_date,
        CAST(cancellation_reason COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS cancellation_reason,
        underwriting_score,
        CAST(risk_state          COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(2))   AS risk_state,
        created_date,
        modified_date,
        silver_load_date
    FROM silver_policy

    UNION ALL

    SELECT
        CAST(policy_id           AS VARCHAR(15))   AS policy_id,
        CAST(policy_number       AS VARCHAR(30))   AS policy_number,
        CAST(customer_id         AS VARCHAR(15))   AS customer_id,
        CAST(agent_id            AS VARCHAR(10))   AS agent_id,
        CAST(coverage_id         AS VARCHAR(10))   AS coverage_id,
        CAST(line_of_business    AS VARCHAR(20))   AS line_of_business,
        CAST(policy_status       AS VARCHAR(20))   AS policy_status,
        CASE WHEN TRIM(effective_date)   = '' THEN NULL ELSE CAST(effective_date   AS DATE) END AS effective_date,
        CASE WHEN TRIM(expiration_date)  = '' THEN NULL ELSE CAST(expiration_date  AS DATE) END AS expiration_date,
        CAST(policy_term_months  AS INT)           AS policy_term_months,
        CAST(annual_premium      AS DECIMAL(15,2)) AS annual_premium,
        CAST(monthly_premium     AS DECIMAL(15,2)) AS monthly_premium,
        CAST(deductible          AS DECIMAL(15,2)) AS deductible,
        CAST(coverage_limit      AS DECIMAL(15,2)) AS coverage_limit,
        CAST(payment_plan        AS VARCHAR(20))   AS payment_plan,
        CASE WHEN TRIM(cancellation_date)   = '' THEN NULL ELSE CAST(cancellation_date   AS DATE) END AS cancellation_date,
        CASE WHEN TRIM(cancellation_reason) = '' THEN NULL ELSE CAST(cancellation_reason AS VARCHAR(50)) END AS cancellation_reason,
        CASE
            WHEN underwriting_score IS NULL THEN NULL
            WHEN UPPER(TRIM(underwriting_score)) IN (N'N/A', N'UNKNOWN', '') THEN NULL
            ELSE CAST(CAST(underwriting_score AS FLOAT) AS INT)
        END AS underwriting_score,
        CAST(risk_state          AS VARCHAR(2))    AS risk_state,
        CASE WHEN TRIM(created_date)  = '' THEN NULL ELSE CAST(created_date  AS DATE) END AS created_date,
        CASE WHEN TRIM(modified_date) = '' THEN NULL ELSE CAST(modified_date AS DATE) END AS modified_date,
        CAST(GETDATE() AS DATE)                    AS silver_load_date
    FROM OPENROWSET(
        BULK 'bronze/policy/*.csv',
        DATA_SOURCE = 'silver_adls',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) WITH (
        policy_id            VARCHAR(15)  COLLATE Latin1_General_100_BIN2_UTF8,
        policy_number        VARCHAR(30)  COLLATE Latin1_General_100_BIN2_UTF8,
        customer_id          VARCHAR(15)  COLLATE Latin1_General_100_BIN2_UTF8,
        agent_id             VARCHAR(10)  COLLATE Latin1_General_100_BIN2_UTF8,
        coverage_id          VARCHAR(10)  COLLATE Latin1_General_100_BIN2_UTF8,
        line_of_business     VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        policy_status        VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        effective_date       VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        expiration_date      VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        policy_term_months   VARCHAR(5)   COLLATE Latin1_General_100_BIN2_UTF8,
        annual_premium       VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        monthly_premium      VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        deductible           VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        coverage_limit       VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        payment_plan         VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        cancellation_date    VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        cancellation_reason  VARCHAR(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        underwriting_score   VARCHAR(10)  COLLATE Latin1_General_100_BIN2_UTF8,
        risk_state           VARCHAR(2)   COLLATE Latin1_General_100_BIN2_UTF8,
        created_date         VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        modified_date        VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8
    ) AS raw
    WHERE CAST(modified_date AS DATETIME) > '1900-01-01' -- Replace with watermark value
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY policy_id
            ORDER BY modified_date DESC
        ) AS rn
    FROM policy_raw
    WHERE policy_id IS NOT NULL
      AND TRIM(policy_id) != ''
)
SELECT
    policy_id, policy_number, customer_id, agent_id, coverage_id,
    line_of_business, policy_status, effective_date, expiration_date,
    policy_term_months, annual_premium, monthly_premium, deductible,
    coverage_limit, payment_plan, cancellation_date, cancellation_reason,
    underwriting_score, risk_state, created_date, modified_date,
    silver_load_date
FROM deduped
WHERE rn = 1;
GO

-- ── Step 3: Drop silver_policy ────────────────────────────
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'silver_policy')
    DROP EXTERNAL TABLE silver_policy;
GO

-- ── Step 4: Recreate silver_policy from tmp ───────────────
CREATE EXTERNAL TABLE silver_policy
WITH (
    LOCATION    = 'silver/policy/',
    DATA_SOURCE = silver_adls,
    FILE_FORMAT = ff_parquet
)
AS
SELECT
    policy_id, policy_number, customer_id, agent_id, coverage_id,
    line_of_business, policy_status, effective_date, expiration_date,
    policy_term_months, annual_premium, monthly_premium, deductible,
    coverage_limit, payment_plan, cancellation_date, cancellation_reason,
    underwriting_score, risk_state, created_date, modified_date,
    silver_load_date
FROM OPENROWSET(
    BULK 'silver/policy_tmp/*.parquet',
    DATA_SOURCE = 'silver_adls',
    FORMAT = 'PARQUET'
) WITH (
    policy_id            VARCHAR(15),
    policy_number        VARCHAR(30),
    customer_id          VARCHAR(15),
    agent_id             VARCHAR(10),
    coverage_id          VARCHAR(10),
    line_of_business     VARCHAR(20),
    policy_status        VARCHAR(20),
    effective_date       DATE,
    expiration_date      DATE,
    policy_term_months   INT,
    annual_premium       DECIMAL(15,2),
    monthly_premium      DECIMAL(15,2),
    deductible           DECIMAL(15,2),
    coverage_limit       DECIMAL(15,2),
    payment_plan         VARCHAR(20),
    cancellation_date    DATE,
    cancellation_reason  VARCHAR(50),
    underwriting_score   INT,
    risk_state           VARCHAR(2),
    created_date         DATE,
    modified_date        DATE,
    silver_load_date     DATE
) AS final;
GO

SELECT COUNT(*) AS row_count FROM silver_policy;
GO
