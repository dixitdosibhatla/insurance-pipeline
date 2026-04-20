-- ============================================================
-- PROJECT   : P&C Insurance Data Pipeline
-- SCRIPT    : DML - Bulk Insert (On-Premises SQL Server)
-- SERVER    : SQL Server Express 16.0.1000
-- DATABASE  : PNCINSURANCE
-- AUTHOR    : Dathathreya Dixit Dosibhatla
-- DATE      : 2024
-- ============================================================
-- SOURCE    : <Your Source>
-- DELIMITER : Pipe (|) — avoids comma conflicts in data
-- ============================================================

USE PNCINSURANCE;
GO

SET NOCOUNT ON;
GO

-- ============================================================
-- 1. COVERAGE
-- ============================================================
PRINT 'Loading coverage...';

BULK INSERT dbo.coverage
FROM <Your Source>
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = '|',
    ROWTERMINATOR   = '0x0a',
    TABLOCK
);
GO

-- ============================================================
-- 2. AGENT
-- ============================================================
PRINT 'Loading agent...';

BULK INSERT dbo.agent
FROM <Your Source>
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = '|',
    ROWTERMINATOR   = '0x0a',
    TABLOCK
);
GO

-- ============================================================
-- 3. POLICY
-- ============================================================
PRINT 'Loading policy...';

BULK INSERT dbo.policy
FROM <Your Source>
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = '|',
    ROWTERMINATOR   = '0x0a',
    TABLOCK
);
GO

-- ============================================================
-- 4. CLAIMS
-- ============================================================
PRINT 'Loading claims...';

BULK INSERT dbo.claims
FROM <Your Source>
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = '|',
    ROWTERMINATOR   = '0x0a',
    TABLOCK
);
GO

-- ============================================================
-- VERIFY ROW COUNTS
-- ============================================================
PRINT 'Verifying row counts...';

SELECT 'coverage' AS table_name, COUNT(*) AS row_count FROM dbo.coverage
UNION ALL
SELECT 'agent',   COUNT(*) FROM dbo.agent
UNION ALL
SELECT 'policy',  COUNT(*) FROM dbo.policy
UNION ALL
SELECT 'claims',  COUNT(*) FROM dbo.claims;
GO

PRINT 'Bulk load completed successfully';
GO
