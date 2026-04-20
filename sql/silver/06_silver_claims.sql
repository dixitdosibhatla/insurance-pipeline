-- ============================================================
-- P&C Insurance Pipeline — Silver Claims
-- Incremental load — merge/swap pattern
-- Watermark read from pipeline_watermark on-prem SQL
-- ============================================================

USE silver_db;
GO

-- ── Step 1: Drop tmp if exists ────────────────────────────
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'silver_claims_tmp')
    DROP EXTERNAL TABLE silver_claims_tmp;

-- ── Step 2: Merge existing Silver + new Bronze → tmp ──────
CREATE EXTERNAL TABLE silver_claims_tmp
WITH (
    LOCATION    = 'silver/claims_tmp/',
    DATA_SOURCE = silver_adls,
    FILE_FORMAT = ff_parquet
)
AS
WITH claims_raw AS (
    SELECT
        CAST(claim_id          COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(15))  AS claim_id,
        CAST(claim_number      COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(25))  AS claim_number,
        CAST(policy_id         COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(15))  AS policy_id,
        loss_date,
        fnol_date,
        reported_date,
        CAST(peril_type        COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS peril_type,
        CAST(loss_description  COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(200)) AS loss_description,
        CAST(claim_status      COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS claim_status,
        reserve_amount,
        settlement_amount,
        close_date,
        CAST(adjuster_id       COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(10))  AS adjuster_id,
        CAST(adjuster_notes    COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(300)) AS adjuster_notes,
        CAST(subrogation_flag  COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(1))   AS subrogation_flag,
        CAST(fraud_flag        COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(1))   AS fraud_flag,
        CAST(cat_event_name    COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS cat_event_name,
        CAST(litigation_flag   COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(1))   AS litigation_flag,
        fnol_lag_days,
        CAST(is_orphaned_claim COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(1))   AS is_orphaned_claim,
        created_date,
        modified_date,
        silver_load_date
    FROM silver_claims

    UNION ALL

    SELECT
        CAST(claim_id          AS VARCHAR(15))  AS claim_id,
        CAST(claim_number      AS VARCHAR(25))  AS claim_number,
        CAST(policy_id         AS VARCHAR(15))  AS policy_id,
        CASE WHEN TRIM(loss_date)     = '' THEN NULL ELSE CAST(loss_date     AS DATE) END AS loss_date,
        CASE WHEN TRIM(fnol_date)     = '' THEN NULL ELSE CAST(fnol_date     AS DATE) END AS fnol_date,
        CASE WHEN TRIM(reported_date) = '' THEN NULL ELSE CAST(reported_date AS DATE) END AS reported_date,
        CAST(peril_type        AS VARCHAR(50))  AS peril_type,
        CAST(loss_description  AS VARCHAR(200)) AS loss_description,
        CAST(claim_status      AS VARCHAR(20))  AS claim_status,
        CASE
            WHEN reserve_amount IS NULL THEN NULL
            WHEN UPPER(TRIM(reserve_amount)) IN (N'N/A', N'UNKNOWN', '') THEN NULL
            ELSE CAST(reserve_amount AS DECIMAL(15,2))
        END AS reserve_amount,
        CASE
            WHEN settlement_amount IS NULL THEN NULL
            WHEN TRIM(settlement_amount) = '' THEN NULL
            ELSE CAST(settlement_amount AS DECIMAL(15,2))
        END AS settlement_amount,
        CASE WHEN TRIM(close_date)  = '' THEN NULL ELSE CAST(close_date  AS DATE) END AS close_date,
        CASE WHEN TRIM(adjuster_id) = '' THEN NULL ELSE CAST(adjuster_id AS VARCHAR(10)) END AS adjuster_id,
        CASE WHEN TRIM(adjuster_notes) = '' THEN NULL ELSE CAST(adjuster_notes AS VARCHAR(300)) END AS adjuster_notes,
        CAST(subrogation_flag  AS VARCHAR(1))   AS subrogation_flag,
        CAST(fraud_flag        AS VARCHAR(1))   AS fraud_flag,
        CASE WHEN TRIM(cat_event_name) = '' THEN NULL ELSE CAST(cat_event_name AS VARCHAR(50)) END AS cat_event_name,
        CAST(litigation_flag   AS VARCHAR(1))   AS litigation_flag,
        CASE
            WHEN TRIM(loss_date) = '' OR TRIM(fnol_date) = '' THEN NULL
            ELSE DATEDIFF(DAY, CAST(loss_date AS DATE), CAST(fnol_date AS DATE))
        END AS fnol_lag_days,
        CASE WHEN policy_id LIKE 'POL9%' THEN 'Y' ELSE 'N' END AS is_orphaned_claim,
        CASE WHEN TRIM(created_date)  = '' THEN NULL ELSE CAST(created_date  AS DATE) END AS created_date,
        CASE WHEN TRIM(modified_date) = '' THEN NULL ELSE CAST(modified_date AS DATE) END AS modified_date,
        CAST(GETDATE() AS DATE) AS silver_load_date
    FROM OPENROWSET(
        BULK 'bronze/claims/*.csv',
        DATA_SOURCE = 'silver_adls',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) WITH (
        claim_id           VARCHAR(15)  COLLATE Latin1_General_100_BIN2_UTF8,
        claim_number       VARCHAR(25)  COLLATE Latin1_General_100_BIN2_UTF8,
        policy_id          VARCHAR(15)  COLLATE Latin1_General_100_BIN2_UTF8,
        loss_date          VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        fnol_date          VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        reported_date      VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        peril_type         VARCHAR(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        loss_description   VARCHAR(200) COLLATE Latin1_General_100_BIN2_UTF8,
        claim_status       VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        reserve_amount     VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        settlement_amount  VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        close_date         VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        adjuster_id        VARCHAR(10)  COLLATE Latin1_General_100_BIN2_UTF8,
        adjuster_notes     VARCHAR(300) COLLATE Latin1_General_100_BIN2_UTF8,
        subrogation_flag   VARCHAR(1)   COLLATE Latin1_General_100_BIN2_UTF8,
        fraud_flag         VARCHAR(1)   COLLATE Latin1_General_100_BIN2_UTF8,
        cat_event_name     VARCHAR(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        litigation_flag    VARCHAR(1)   COLLATE Latin1_General_100_BIN2_UTF8,
        created_date       VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        modified_date      VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8
    ) AS raw
    WHERE CAST(modified_date AS DATETIME) > '1900-01-01' -- Replace with watermark value
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY claim_id
            ORDER BY modified_date DESC
        ) AS rn
    FROM claims_raw
    WHERE claim_id IS NOT NULL
      AND TRIM(claim_id) != ''
)
SELECT
    claim_id, claim_number, policy_id, loss_date, fnol_date,
    reported_date, peril_type, loss_description, claim_status,
    reserve_amount, settlement_amount, close_date, adjuster_id,
    adjuster_notes, subrogation_flag, fraud_flag, cat_event_name,
    litigation_flag, fnol_lag_days, is_orphaned_claim,
    created_date, modified_date, silver_load_date
FROM deduped
WHERE rn = 1;
GO

-- ── Step 3: Drop silver_claims ────────────────────────────
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'silver_claims')
    DROP EXTERNAL TABLE silver_claims;
GO

-- ── Step 4: Recreate silver_claims from tmp ───────────────
CREATE EXTERNAL TABLE silver_claims
WITH (
    LOCATION    = 'silver/claims/',
    DATA_SOURCE = silver_adls,
    FILE_FORMAT = ff_parquet
)
AS
SELECT
    claim_id, claim_number, policy_id, loss_date, fnol_date,
    reported_date, peril_type, loss_description, claim_status,
    reserve_amount, settlement_amount, close_date, adjuster_id,
    adjuster_notes, subrogation_flag, fraud_flag, cat_event_name,
    litigation_flag, fnol_lag_days, is_orphaned_claim,
    created_date, modified_date, silver_load_date
FROM OPENROWSET(
    BULK 'silver/claims_tmp/*.parquet',
    DATA_SOURCE = 'silver_adls',
    FORMAT = 'PARQUET'
) WITH (
    claim_id           VARCHAR(15),
    claim_number       VARCHAR(25),
    policy_id          VARCHAR(15),
    loss_date          DATE,
    fnol_date          DATE,
    reported_date      DATE,
    peril_type         VARCHAR(50),
    loss_description   VARCHAR(200),
    claim_status       VARCHAR(20),
    reserve_amount     DECIMAL(15,2),
    settlement_amount  DECIMAL(15,2),
    close_date         DATE,
    adjuster_id        VARCHAR(10),
    adjuster_notes     VARCHAR(300),
    subrogation_flag   VARCHAR(1),
    fraud_flag         VARCHAR(1),
    cat_event_name     VARCHAR(50),
    litigation_flag    VARCHAR(1),
    fnol_lag_days      INT,
    is_orphaned_claim  VARCHAR(1),
    created_date       DATE,
    modified_date      DATE,
    silver_load_date   DATE
) AS final;
GO

SELECT COUNT(*) AS row_count FROM silver_claims;
GO
