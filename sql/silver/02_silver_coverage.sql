-- ============================================================
-- P&C Insurance Pipeline — Silver Coverage
-- Full load — delete folder before running
-- ============================================================

USE silver_db;
GO

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'silver_coverage')
    DROP EXTERNAL TABLE silver_coverage;

CREATE EXTERNAL TABLE silver_coverage
WITH (
    LOCATION    = 'silver/coverage/',
    DATA_SOURCE = silver_adls,
    FILE_FORMAT = ff_parquet
)
AS
SELECT
    CAST(coverage_id              AS VARCHAR(10))   AS coverage_id,
    CAST(coverage_name            AS VARCHAR(100))  AS coverage_name,
    CAST(line_of_business         AS VARCHAR(20))   AS line_of_business,
    CAST(coverage_type            AS VARCHAR(30))   AS coverage_type,
    CAST(min_limit                AS DECIMAL(15,2)) AS min_limit,
    CAST(max_limit                AS DECIMAL(15,2)) AS max_limit,
    CAST(min_deductible           AS DECIMAL(15,2)) AS min_deductible,
    CAST(max_deductible           AS DECIMAL(15,2)) AS max_deductible,
    effective_date,
    is_active,
    CAST(duck_creek_coverage_code AS VARCHAR(20))   AS duck_creek_coverage_code,
    created_date,
    modified_date,
    CAST(GETDATE() AS DATE)                         AS silver_load_date
FROM OPENROWSET(
    BULK 'bronze/coverage/*.csv',
    DATA_SOURCE = 'silver_adls',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) WITH (
    coverage_id              VARCHAR(10)   COLLATE Latin1_General_100_BIN2_UTF8,
    coverage_name            VARCHAR(100)  COLLATE Latin1_General_100_BIN2_UTF8,
    line_of_business         VARCHAR(20)   COLLATE Latin1_General_100_BIN2_UTF8,
    coverage_type            VARCHAR(30)   COLLATE Latin1_General_100_BIN2_UTF8,
    min_limit                DECIMAL(15,2),
    max_limit                DECIMAL(15,2),
    min_deductible           DECIMAL(15,2),
    max_deductible           DECIMAL(15,2),
    effective_date           DATE,
    is_active                BIT,
    duck_creek_coverage_code VARCHAR(20)   COLLATE Latin1_General_100_BIN2_UTF8,
    created_date             DATE,
    modified_date            DATE
) AS coverage_raw;
GO

SELECT COUNT(*) AS row_count FROM silver_coverage;
GO
