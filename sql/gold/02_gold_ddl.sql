-- ============================================================
-- P&C Insurance Pipeline — Gold DDL
-- Creates empty external tables for all Gold layer tables
-- Run as part of pl_init_gold pipeline
-- ============================================================

USE gold_db;

-- ── dim_coverage ──────────────────────────────────────────
CREATE EXTERNAL TABLE dim_coverage
WITH (
    LOCATION    = 'gold/dim_coverage/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS SELECT
    CAST(NULL AS VARCHAR(10))   AS coverage_id,
    CAST(NULL AS VARCHAR(100))  AS coverage_name,
    CAST(NULL AS VARCHAR(20))   AS line_of_business,
    CAST(NULL AS VARCHAR(30))   AS coverage_type,
    CAST(NULL AS DECIMAL(15,2)) AS min_limit,
    CAST(NULL AS DECIMAL(15,2)) AS max_limit,
    CAST(NULL AS DECIMAL(15,2)) AS min_deductible,
    CAST(NULL AS DECIMAL(15,2)) AS max_deductible,
    CAST(NULL AS DATE)          AS effective_date,
    CAST(NULL AS INT)           AS is_active,
    CAST(NULL AS DATE)          AS gold_load_date
WHERE 1 = 0;

-- ── dim_agent ─────────────────────────────────────────────
CREATE EXTERNAL TABLE dim_agent
WITH (
    LOCATION    = 'gold/dim_agent/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS SELECT
    CAST(NULL AS VARCHAR(10))   AS agent_id,
    CAST(NULL AS VARCHAR(50))   AS agent_first_name,
    CAST(NULL AS VARCHAR(50))   AS agent_last_name,
    CAST(NULL AS VARCHAR(100))  AS agent_email,
    CAST(NULL AS VARCHAR(30))   AS agent_phone,
    CAST(NULL AS VARCHAR(100))  AS agency_name,
    CAST(NULL AS VARCHAR(50))   AS agency_city,
    CAST(NULL AS VARCHAR(2))    AS agency_state,
    CAST(NULL AS VARCHAR(10))   AS agency_zip,
    CAST(NULL AS VARCHAR(20))   AS license_number,
    CAST(NULL AS VARCHAR(50))   AS line_of_business,
    CAST(NULL AS INT)           AS is_active,
    CAST(NULL AS DATE)          AS hire_date,
    CAST(NULL AS DATE)          AS termination_date,
    CAST(NULL AS DATE)          AS gold_load_date
WHERE 1 = 0;

-- ── dim_customer (SCD Type 1) ─────────────────────────────
CREATE EXTERNAL TABLE dim_customer
WITH (
    LOCATION    = 'gold/dim_customer/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS SELECT
    CAST(NULL AS VARCHAR(20))  AS customer_id,
    CAST(NULL AS VARCHAR(50))  AS first_name,
    CAST(NULL AS VARCHAR(50))  AS last_name,
    CAST(NULL AS VARCHAR(100)) AS email,
    CAST(NULL AS VARCHAR(30))  AS phone,
    CAST(NULL AS DATE)         AS date_of_birth,
    CAST(NULL AS VARCHAR(5))   AS gender,
    CAST(NULL AS VARCHAR(100)) AS address_street,
    CAST(NULL AS VARCHAR(50))  AS address_suite,
    CAST(NULL AS VARCHAR(50))  AS address_city,
    CAST(NULL AS VARCHAR(2))   AS address_state,
    CAST(NULL AS VARCHAR(10))  AS zip_code,
    CAST(NULL AS VARCHAR(20))  AS marital_status,
    CAST(NULL AS VARCHAR(50))  AS occupation,
    CAST(NULL AS INT)          AS credit_score,
    CAST(NULL AS VARCHAR(20))  AS risk_tier,
    CAST(NULL AS VARCHAR(20))  AS preferred_contact,
    CAST(NULL AS DATE)         AS customer_since,
    CAST(NULL AS DATE)         AS created_date,
    CAST(NULL AS DATE)         AS modified_date,
    CAST(NULL AS DATE)         AS gold_load_date
WHERE 1 = 0;

-- ── dim_policy (SCD Type 2) ───────────────────────────────
CREATE EXTERNAL TABLE dim_policy
WITH (
    LOCATION    = 'gold/dim_policy/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS SELECT
    CAST(NULL AS VARCHAR(15))   AS policy_id,
    CAST(NULL AS VARCHAR(30))   AS policy_number,
    CAST(NULL AS VARCHAR(15))   AS customer_id,
    CAST(NULL AS VARCHAR(10))   AS agent_id,
    CAST(NULL AS VARCHAR(10))   AS coverage_id,
    CAST(NULL AS VARCHAR(20))   AS line_of_business,
    CAST(NULL AS VARCHAR(20))   AS policy_status,
    CAST(NULL AS DATE)          AS effective_date,
    CAST(NULL AS DATE)          AS expiration_date,
    CAST(NULL AS INT)           AS policy_term_months,
    CAST(NULL AS DECIMAL(15,2)) AS annual_premium,
    CAST(NULL AS DECIMAL(15,2)) AS monthly_premium,
    CAST(NULL AS DECIMAL(15,2)) AS deductible,
    CAST(NULL AS DECIMAL(15,2)) AS coverage_limit,
    CAST(NULL AS VARCHAR(20))   AS payment_plan,
    CAST(NULL AS DATE)          AS cancellation_date,
    CAST(NULL AS VARCHAR(50))   AS cancellation_reason,
    CAST(NULL AS INT)           AS underwriting_score,
    CAST(NULL AS VARCHAR(2))    AS risk_state,
    CAST(NULL AS DATE)          AS scd_start_date,
    CAST(NULL AS DATE)          AS scd_end_date,
    CAST(NULL AS INT)           AS is_current,
    CAST(NULL AS DATE)          AS gold_load_date
WHERE 1 = 0;

-- ── fact_claims (SCD Type 2) ──────────────────────────────
CREATE EXTERNAL TABLE fact_claims
WITH (
    LOCATION    = 'gold/fact_claims/',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = gold_ff_parquet
)
AS SELECT
    CAST(NULL AS VARCHAR(15))   AS claim_id,
    CAST(NULL AS VARCHAR(25))   AS claim_number,
    CAST(NULL AS VARCHAR(15))   AS policy_id,
    CAST(NULL AS VARCHAR(15))   AS customer_id,
    CAST(NULL AS VARCHAR(10))   AS agent_id,
    CAST(NULL AS VARCHAR(10))   AS coverage_id,
    CAST(NULL AS VARCHAR(50))   AS peril_type,
    CAST(NULL AS VARCHAR(20))   AS claim_status,
    CAST(NULL AS DECIMAL(15,2)) AS reserve_amount,
    CAST(NULL AS DECIMAL(15,2)) AS settlement_amount,
    CAST(NULL AS DATE)          AS loss_date,
    CAST(NULL AS DATE)          AS fnol_date,
    CAST(NULL AS DATE)          AS reported_date,
    CAST(NULL AS DATE)          AS close_date,
    CAST(NULL AS VARCHAR(10))   AS adjuster_id,
    CAST(NULL AS VARCHAR(1))    AS subrogation_flag,
    CAST(NULL AS VARCHAR(1))    AS fraud_flag,
    CAST(NULL AS VARCHAR(50))   AS cat_event_name,
    CAST(NULL AS VARCHAR(1))    AS litigation_flag,
    CAST(NULL AS INT)           AS fnol_lag_days,
    CAST(NULL AS VARCHAR(1))    AS is_orphaned_claim,
    CAST(NULL AS DATE)          AS scd_start_date,
    CAST(NULL AS DATE)          AS scd_end_date,
    CAST(NULL AS INT)           AS is_current,
    CAST(NULL AS DATE)          AS gold_load_date
WHERE 1 = 0;

PRINT 'Gold DDL completed — 5 tables created';
GO
