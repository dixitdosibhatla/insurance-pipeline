-- ============================================================
-- P&C Insurance Pipeline — Silver Agent
-- Full load — delete folder before running
-- ============================================================

USE silver_db;
GO

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'silver_agent')
    DROP EXTERNAL TABLE silver_agent;

CREATE EXTERNAL TABLE silver_agent
WITH (
    LOCATION    = 'silver/agent/',
    DATA_SOURCE = silver_adls,
    FILE_FORMAT = ff_parquet
)
AS
WITH agent_raw AS (
    SELECT *
    FROM OPENROWSET(
        BULK 'bronze/agent/*.csv',
        DATA_SOURCE = 'silver_adls',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) WITH (
        agent_id               VARCHAR(10)  COLLATE Latin1_General_100_BIN2_UTF8,
        agent_first_name       VARCHAR(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        agent_last_name        VARCHAR(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        agent_email            VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8,
        agent_phone            VARCHAR(30)  COLLATE Latin1_General_100_BIN2_UTF8,
        agency_name            VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8,
        agency_city            VARCHAR(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        agency_state           VARCHAR(2)   COLLATE Latin1_General_100_BIN2_UTF8,
        agency_zip             VARCHAR(10)  COLLATE Latin1_General_100_BIN2_UTF8,
        license_number         VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        licensed_states        VARCHAR(200) COLLATE Latin1_General_100_BIN2_UTF8,
        line_of_business       VARCHAR(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        hire_date              VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        termination_date       VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        is_active              VARCHAR(5)   COLLATE Latin1_General_100_BIN2_UTF8,
        ytd_policies_written   VARCHAR(10)  COLLATE Latin1_General_100_BIN2_UTF8,
        ytd_premium_written    VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        created_date           VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        modified_date          VARCHAR(20)  COLLATE Latin1_General_100_BIN2_UTF8
    ) AS raw
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY agent_id
            ORDER BY modified_date DESC
        ) AS rn
    FROM agent_raw
    WHERE agent_id IS NOT NULL
      AND TRIM(agent_id) != ''
),
cleaned AS (
    SELECT
        CAST(agent_id           AS VARCHAR(10))  AS agent_id,
        CAST(agent_first_name   AS VARCHAR(50))  AS agent_first_name,
        CAST(agent_last_name    AS VARCHAR(50))  AS agent_last_name,
        CAST(agent_email        AS VARCHAR(100)) AS agent_email,
        CASE
            WHEN agent_phone IS NULL OR TRIM(agent_phone) = '' THEN NULL
            ELSE
                '(' +
                SUBSTRING(
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        agent_phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ',''),
                    LEN(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        agent_phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ','')) - 9, 3
                ) + ') ' +
                SUBSTRING(
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        agent_phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ',''),
                    LEN(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        agent_phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ','')) - 6, 3
                ) + '-' +
                SUBSTRING(
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        agent_phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ',''),
                    LEN(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        agent_phone,' ',''),'-',''),'.',''),'+',''),'(',''),')',''),' ','')) - 3, 4
                )
        END AS agent_phone,
        CAST(agency_name        AS VARCHAR(100)) AS agency_name,
        CAST(agency_city        AS VARCHAR(50))  AS agency_city,
        CAST(agency_state       AS VARCHAR(2))   AS agency_state,
        CAST(agency_zip         AS VARCHAR(10))  AS agency_zip,
        CAST(license_number     AS VARCHAR(20))  AS license_number,
        CAST(licensed_states    AS VARCHAR(200)) AS licensed_states,
        CAST(line_of_business   AS VARCHAR(50))  AS line_of_business,
        CASE WHEN TRIM(hire_date)        = '' THEN NULL ELSE CAST(hire_date        AS DATE) END AS hire_date,
        CASE WHEN TRIM(termination_date) = '' THEN NULL ELSE CAST(termination_date AS DATE) END AS termination_date,
        CASE WHEN UPPER(TRIM(is_active)) = 'TRUE' THEN 1 ELSE 0 END                            AS is_active,
        CAST(ytd_policies_written AS INT)           AS ytd_policies_written,
        CAST(ytd_premium_written  AS DECIMAL(15,2)) AS ytd_premium_written,
        CASE WHEN TRIM(created_date)  = '' THEN NULL ELSE CAST(created_date  AS DATE) END AS created_date,
        CASE WHEN TRIM(modified_date) = '' THEN NULL ELSE CAST(modified_date AS DATE) END AS modified_date,
        CAST(GETDATE() AS DATE) AS silver_load_date
    FROM deduped
    WHERE rn = 1
)
SELECT * FROM cleaned;
GO

SELECT COUNT(*) AS row_count FROM silver_agent;
GO
