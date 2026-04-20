-- ============================================================
-- P&C Insurance Pipeline — Silver Database Setup
-- Run in Synapse Serverless SQL Pool
-- ============================================================

-- Create silver_db
CREATE DATABASE silver_db;
GO

USE silver_db;
GO

-- Master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'SilverLayer@pncinsurance1';
GO

-- Credential
CREATE DATABASE SCOPED CREDENTIAL silver_credential
WITH IDENTITY = 'Managed Identity';
GO

-- External data source
CREATE EXTERNAL DATA SOURCE silver_adls
WITH (
    LOCATION   = 'https://adlspncinsurance.dfs.core.windows.net',
    CREDENTIAL = silver_credential
);
GO

-- File formats
CREATE EXTERNAL FILE FORMAT ff_csv
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2,
        USE_TYPE_DEFAULT = FALSE
    )
);
GO

CREATE EXTERNAL FILE FORMAT ff_parquet
WITH (FORMAT_TYPE = PARQUET);
GO

CREATE EXTERNAL FILE FORMAT ff_json
WITH (FORMAT_TYPE = JSON);
GO

-- Verify
SELECT name FROM sys.external_data_sources;
SELECT name FROM sys.external_file_formats;
GO

PRINT 'Silver database setup completed successfully';
GO
