-- ============================================================
-- P&C Insurance Pipeline — Gold Database Setup
-- Run in Synapse Serverless SQL Pool
-- ============================================================

CREATE DATABASE gold_db;
GO

USE gold_db;
GO

-- Master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'GoldLayer@pncinsurance1';
GO

-- Credential
CREATE DATABASE SCOPED CREDENTIAL gold_credential
WITH IDENTITY = 'Managed Identity';
GO

-- External data source
CREATE EXTERNAL DATA SOURCE gold_adls
WITH (
    LOCATION   = 'https://adlspncinsurance.dfs.core.windows.net',
    CREDENTIAL = gold_credential
);
GO

-- Parquet file format
CREATE EXTERNAL FILE FORMAT gold_ff_parquet
WITH (FORMAT_TYPE = PARQUET);
GO

-- Verify
SELECT name, location FROM sys.external_data_sources;
SELECT name FROM sys.external_file_formats;
GO

PRINT 'Gold database setup completed successfully';
GO
