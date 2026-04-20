-- ============================================================
-- P&C Insurance Pipeline — Bronze Validation Scripts
-- Run before Silver transformation
-- Returns check_status: PASS / FAIL
-- ============================================================

USE silver_db;
GO

-- ── Validate Bronze Coverage ──────────────────────────────
SELECT
    'coverage' AS table_name,
    COUNT(*) AS row_count,
    SUM(CASE WHEN coverage_id IS NULL OR TRIM(coverage_id) = '' THEN 1 ELSE 0 END) AS null_pk,
    COUNT(*) - COUNT(DISTINCT coverage_id) AS duplicate_pk,
    SUM(CASE WHEN coverage_name IS NULL OR TRIM(coverage_name) = '' THEN 1 ELSE 0 END) AS null_coverage_name,
    CASE
        WHEN COUNT(*) = 0 THEN 'FAIL'
        WHEN SUM(CASE WHEN coverage_id IS NULL OR TRIM(coverage_id) = '' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS check_status
FROM OPENROWSET(
    BULK 'bronze/coverage/*.csv',
    DATA_SOURCE = 'silver_adls',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) WITH (
    coverage_id   VARCHAR(10)  COLLATE Latin1_General_100_BIN2_UTF8,
    coverage_name VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8
) AS raw;
GO

-- ── Validate Bronze Agent ─────────────────────────────────
SELECT
    'agent' AS table_name,
    COUNT(*) AS row_count,
    SUM(CASE WHEN agent_id IS NULL OR TRIM(agent_id) = '' THEN 1 ELSE 0 END) AS null_pk,
    COUNT(*) - COUNT(DISTINCT agent_id) AS duplicate_pk,
    SUM(CASE WHEN agent_first_name IS NULL OR TRIM(agent_first_name) = '' THEN 1 ELSE 0 END) AS null_first_name,
    CASE
        WHEN COUNT(*) = 0 THEN 'FAIL'
        WHEN SUM(CASE WHEN agent_id IS NULL OR TRIM(agent_id) = '' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS check_status
FROM OPENROWSET(
    BULK 'bronze/agent/*.csv',
    DATA_SOURCE = 'silver_adls',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) WITH (
    agent_id         VARCHAR(10) COLLATE Latin1_General_100_BIN2_UTF8,
    agent_first_name VARCHAR(50) COLLATE Latin1_General_100_BIN2_UTF8
) AS raw;
GO

-- ── Validate Bronze Customer ──────────────────────────────
SELECT
    'customer' AS table_name,
    COUNT(*) AS row_count,
    SUM(CASE WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 1 ELSE 0 END) AS null_pk,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_pk,
    SUM(CASE WHEN first_name IS NULL OR TRIM(first_name) = '' THEN 1 ELSE 0 END) AS null_first_name,
    SUM(CASE WHEN email IS NULL OR TRIM(email) = '' THEN 1 ELSE 0 END) AS null_email,
    CASE
        WHEN COUNT(*) = 0 THEN 'FAIL'
        WHEN SUM(CASE WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS check_status
FROM OPENROWSET(
    BULK 'bronze/customer/*.json',
    DATA_SOURCE = 'silver_adls',
    FORMAT = 'CSV',
    PARSER_VERSION = '1.0',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE = '0x0b',
    ROWTERMINATOR = '0x0b'
) WITH (jsonDoc NVARCHAR(MAX)) AS raw_json
CROSS APPLY OPENJSON(jsonDoc)
WITH (
    customer_id  VARCHAR(20)  N'$.id',
    first_name   VARCHAR(50)  N'$.firstName',
    email        VARCHAR(100) N'$.email'
);
GO

-- ── Validate Bronze Policy ────────────────────────────────
SELECT
    'policy' AS table_name,
    COUNT(*) AS row_count,
    SUM(CASE WHEN policy_id IS NULL OR TRIM(policy_id) = '' THEN 1 ELSE 0 END) AS null_pk,
    COUNT(*) - COUNT(DISTINCT policy_id) AS duplicate_pk,
    SUM(CASE WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN annual_premium IS NULL OR TRIM(annual_premium) = '' THEN 1 ELSE 0 END) AS null_annual_premium,
    CASE
        WHEN COUNT(*) = 0 THEN 'FAIL'
        WHEN SUM(CASE WHEN policy_id IS NULL OR TRIM(policy_id) = '' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
        WHEN SUM(CASE WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS check_status
FROM OPENROWSET(
    BULK 'bronze/policy/*.csv',
    DATA_SOURCE = 'silver_adls',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) WITH (
    policy_id      VARCHAR(15) COLLATE Latin1_General_100_BIN2_UTF8,
    customer_id    VARCHAR(15) COLLATE Latin1_General_100_BIN2_UTF8,
    annual_premium VARCHAR(20) COLLATE Latin1_General_100_BIN2_UTF8
) AS raw;
GO

-- ── Validate Bronze Claims ────────────────────────────────
SELECT
    'claims' AS table_name,
    COUNT(*) AS row_count,
    SUM(CASE WHEN claim_id IS NULL OR TRIM(claim_id) = '' THEN 1 ELSE 0 END) AS null_pk,
    COUNT(*) - COUNT(DISTINCT claim_id) AS duplicate_pk,
    SUM(CASE WHEN policy_id IS NULL OR TRIM(policy_id) = '' THEN 1 ELSE 0 END) AS null_policy_id,
    SUM(CASE WHEN loss_date IS NULL OR TRIM(loss_date) = '' THEN 1 ELSE 0 END) AS null_loss_date,
    CASE
        WHEN COUNT(*) = 0 THEN 'FAIL'
        WHEN SUM(CASE WHEN claim_id IS NULL OR TRIM(claim_id) = '' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS check_status
FROM OPENROWSET(
    BULK 'bronze/claims/*.csv',
    DATA_SOURCE = 'silver_adls',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) WITH (
    claim_id  VARCHAR(15) COLLATE Latin1_General_100_BIN2_UTF8,
    policy_id VARCHAR(15) COLLATE Latin1_General_100_BIN2_UTF8,
    loss_date VARCHAR(20) COLLATE Latin1_General_100_BIN2_UTF8
) AS raw;
GO
