-- ============================================================
-- P&C Insurance Pipeline — Watermark Setup + Stored Procedures
-- Database: PNCINSURANCE
-- ============================================================

USE PNCINSURANCE;
GO

-- ── Initial watermark values ──────────────────────────────
INSERT INTO dbo.pipeline_watermark VALUES ('customer',       '1900-01-01');
INSERT INTO dbo.pipeline_watermark VALUES ('policy',         '1900-01-01');
INSERT INTO dbo.pipeline_watermark VALUES ('claims',         '1900-01-01');
INSERT INTO dbo.pipeline_watermark VALUES ('silver_customer','1900-01-01');
INSERT INTO dbo.pipeline_watermark VALUES ('silver_policy',  '1900-01-01');
INSERT INTO dbo.pipeline_watermark VALUES ('silver_claims',  '1900-01-01');
GO

-- ── Bronze watermark SP (1 param — calculates MAX from source) ──
IF OBJECT_ID('dbo.usp_update_watermark', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_update_watermark;
GO

CREATE PROCEDURE dbo.usp_update_watermark
    @table_name VARCHAR(100)
AS
BEGIN
    UPDATE dbo.pipeline_watermark
    SET    last_load_date = GETDATE()
    WHERE  table_name = @table_name;
END
GO

-- ── Silver watermark SP (2 params — receives MAX date from pipeline) ──
IF OBJECT_ID('dbo.usp_update_silver_watermark', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_update_silver_watermark;
GO

CREATE PROCEDURE dbo.usp_update_silver_watermark
    @table_name     VARCHAR(100),
    @last_load_date DATETIME
AS
BEGIN
    UPDATE dbo.pipeline_watermark
    SET    last_load_date = @last_load_date
    WHERE  table_name = @table_name;
END
GO

-- ── Verify ────────────────────────────────────────────────
SELECT * FROM dbo.pipeline_watermark;
GO

PRINT 'Watermark setup completed successfully';
GO
