-- ============================================================
-- P&C Insurance Pipeline — Silver Customer
-- Incremental load — merge/swap pattern
-- Reads watermark from silver/watermark/watermark.json
-- ============================================================

USE silver_db;
GO

-- ── Step 1: Drop tmp if exists ────────────────────────────
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'silver_customer_tmp')
    DROP EXTERNAL TABLE silver_customer_tmp;

-- ── Step 2: Merge existing Silver + new Bronze → tmp ──────
CREATE EXTERNAL TABLE silver_customer_tmp
WITH (
    LOCATION    = 'silver/customer_tmp/',
    DATA_SOURCE = silver_adls,
    FILE_FORMAT = ff_parquet
)
AS
WITH customer_raw AS (
    -- Existing Silver customer data
    SELECT
        CAST(customer_id       COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS customer_id,
        CAST(first_name        COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS first_name,
        CAST(last_name         COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS last_name,
        CAST(email             COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(100)) AS email,
        CAST(phone             COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(30))  AS phone,
        CAST(date_of_birth AS VARCHAR(20))                                            AS date_of_birth,
        CAST(gender            COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(5))   AS gender,
        CAST(address_street    COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(100)) AS address_street,
        CAST(address_suite     COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS address_suite,
        CAST(address_city      COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS address_city,
        CAST(address_state     COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(2))   AS address_state,
        CAST(zip_code          COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(10))  AS zip_code,
        CAST(marital_status    COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS marital_status,
        CAST(occupation        COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS occupation,
        CAST(credit_score AS VARCHAR(10))                                             AS credit_score,
        CAST(risk_tier         COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS risk_tier,
        CAST(preferred_contact COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(20))  AS preferred_contact,
        CAST(customer_since AS VARCHAR(20))                                           AS customer_since,
        CAST(source_system     COLLATE Latin1_General_100_BIN2_UTF8 AS VARCHAR(50))  AS source_system,
        CAST(created_date AS VARCHAR(20))                                             AS created_date,
        CAST(modified_date AS VARCHAR(20))                                            AS modified_date,
        CAST(silver_load_date AS VARCHAR(20))                                         AS silver_load_date
    FROM silver_customer

    UNION ALL

    -- New Bronze records filtered by watermark
    SELECT *
    FROM OPENROWSET(
        BULK 'bronze/customer/customer.json',
        DATA_SOURCE = 'silver_adls',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0b'
    ) WITH (jsonDoc NVARCHAR(MAX)) AS raw_json
    CROSS APPLY OPENJSON(jsonDoc)
    WITH (
        customer_id       VARCHAR(20)  N'$.id',
        first_name        VARCHAR(50)  N'$.firstName',
        last_name         VARCHAR(50)  N'$.lastName',
        email             VARCHAR(100) N'$.email',
        phone             VARCHAR(30)  N'$.phone',
        date_of_birth     VARCHAR(20)  N'$.dateOfBirth',
        gender            VARCHAR(5)   N'$.gender',
        address_street    VARCHAR(100) N'$.address.street',
        address_suite     VARCHAR(50)  N'$.address.suite',
        address_city      VARCHAR(50)  N'$.address.city',
        address_state     VARCHAR(2)   N'$.address.state',
        zip_code          VARCHAR(10)  N'$.address.zipcode',
        marital_status    VARCHAR(20)  N'$.profile.maritalStatus',
        occupation        VARCHAR(50)  N'$.profile.occupation',
        credit_score      VARCHAR(10)  N'$.profile.creditScore',
        risk_tier         VARCHAR(20)  N'$.profile.riskTier',
        preferred_contact VARCHAR(20)  N'$.profile.preferredContact',
        customer_since    VARCHAR(20)  N'$.profile.customerSince',
        source_system     VARCHAR(50)  N'$.meta.sourceSystem',
        created_date      VARCHAR(20)  N'$.meta.createdDate',
        modified_date     VARCHAR(20)  N'$.meta.modifiedDate'
    )
    WHERE CAST(modified_date AS DATETIME) > (
        SELECT TOP 1 CAST(last_load_date AS DATETIME)
        FROM OPENROWSET(
            BULK 'silver/watermark/watermark.json',
            DATA_SOURCE = 'silver_adls',
            FORMAT = 'CSV',
            FIELDTERMINATOR = '0x0b',
            FIELDQUOTE = '0x0b',
            ROWTERMINATOR = '0x0b'
        ) WITH (jsonDoc NVARCHAR(MAX)) AS wm_raw
        CROSS APPLY OPENJSON(jsonDoc)
        WITH (
            table_name     VARCHAR(50) N'$.table_name',
            last_load_date VARCHAR(30) N'$.last_load_date'
        )
        WHERE table_name = N'silver_customer'
    )
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY modified_date DESC
        ) AS rn
    FROM customer_raw
    WHERE customer_id IS NOT NULL
      AND TRIM(customer_id) != ''
)
SELECT
    CAST(customer_id      AS VARCHAR(20))  AS customer_id,
    CAST(first_name       AS VARCHAR(50))  AS first_name,
    CAST(last_name        AS VARCHAR(50))  AS last_name,
    CAST(email            AS VARCHAR(100)) AS email,
    CASE
        WHEN phone IS NULL OR TRIM(phone) = '' THEN NULL
        ELSE
            '(' +
            SUBSTRING(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ',''),
                LEN(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ','')) - 9, 3
            ) + ') ' +
            SUBSTRING(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ',''),
                LEN(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ','')) - 6, 3
            ) + '-' +
            SUBSTRING(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ',''),
                LEN(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ','')) - 3, 4
            )
    END AS phone,
    CASE WHEN TRIM(date_of_birth) = '' THEN NULL ELSE CAST(date_of_birth AS DATE) END AS date_of_birth,
    CAST(gender           AS VARCHAR(5))             AS gender,
    CAST(address_street   AS VARCHAR(100))           AS address_street,
    CAST(address_suite    AS VARCHAR(50))            AS address_suite,
    UPPER(LEFT(TRIM(address_city), 1)) +
    LOWER(SUBSTRING(TRIM(address_city), 2, LEN(address_city)))                        AS address_city,
    UPPER(TRIM(address_state))                                                         AS address_state,
    CASE
        WHEN zip_code IS NULL OR TRIM(zip_code) = '' THEN NULL
        ELSE LEFT(REPLACE(REPLACE(zip_code, '-', ''), ' ', ''), 5)
    END                                                                                AS zip_code,
    CAST(marital_status   AS VARCHAR(20))            AS marital_status,
    CAST(occupation       AS VARCHAR(50))            AS occupation,
    CASE
        WHEN credit_score IS NULL THEN NULL
        WHEN UPPER(TRIM(credit_score)) IN (N'N/A', N'UNKNOWN', N'NULL', '') THEN NULL
        ELSE CAST(credit_score AS INT)
    END                                                                                AS credit_score,
    CAST(risk_tier        AS VARCHAR(20))            AS risk_tier,
    CAST(preferred_contact AS VARCHAR(20))           AS preferred_contact,
    CASE WHEN TRIM(customer_since) = '' THEN NULL ELSE CAST(customer_since AS DATE) END AS customer_since,
    CAST(source_system    AS VARCHAR(50))            AS source_system,
    CASE WHEN TRIM(created_date)  = '' THEN NULL ELSE CAST(created_date  AS DATE) END AS created_date,
    CASE WHEN TRIM(modified_date) = '' THEN NULL ELSE CAST(modified_date AS DATE) END AS modified_date,
    CAST(GETDATE() AS DATE)                                                            AS silver_load_date
FROM deduped
WHERE rn = 1;
GO

-- ── Step 3: Drop silver_customer ──────────────────────────
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'silver_customer')
    DROP EXTERNAL TABLE silver_customer;
GO

-- ── Step 4: Recreate silver_customer from tmp ─────────────
CREATE EXTERNAL TABLE silver_customer
WITH (
    LOCATION    = 'silver/customer/',
    DATA_SOURCE = silver_adls,
    FILE_FORMAT = ff_parquet
)
AS
SELECT
    customer_id, first_name, last_name, email, phone,
    date_of_birth, gender, address_street, address_suite,
    address_city, address_state, zip_code, marital_status,
    occupation, credit_score, risk_tier, preferred_contact,
    customer_since, source_system, created_date, modified_date,
    silver_load_date
FROM OPENROWSET(
    BULK 'silver/customer_tmp/*.parquet',
    DATA_SOURCE = 'silver_adls',
    FORMAT = 'PARQUET'
) WITH (
    customer_id       VARCHAR(20),
    first_name        VARCHAR(50),
    last_name         VARCHAR(50),
    email             VARCHAR(100),
    phone             VARCHAR(30),
    date_of_birth     DATE,
    gender            VARCHAR(5),
    address_street    VARCHAR(100),
    address_suite     VARCHAR(50),
    address_city      VARCHAR(50),
    address_state     VARCHAR(2),
    zip_code          VARCHAR(10),
    marital_status    VARCHAR(20),
    occupation        VARCHAR(50),
    credit_score      INT,
    risk_tier         VARCHAR(20),
    preferred_contact VARCHAR(20),
    customer_since    DATE,
    source_system     VARCHAR(50),
    created_date      DATE,
    modified_date     DATE,
    silver_load_date  DATE
) AS final;
GO

SELECT COUNT(*) AS row_count FROM silver_customer;
GO
