USE PNCINSURANCE;
GO

-- Drop if exists then recreate
IF OBJECT_ID('dbo.usp_update_watermark', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_update_watermark;
GO

CREATE PROCEDURE dbo.usp_update_watermark
    @table_name VARCHAR(100)
AS
BEGIN
    DECLARE @max_date DATETIME

    IF @table_name = 'policy'
        SELECT @max_date = MAX(modified_date) FROM dbo.policy
    ELSE IF @table_name = 'claims'
        SELECT @max_date = MAX(modified_date) FROM dbo.claims

    UPDATE dbo.pipeline_watermark
    SET    last_load_date = @max_date
    WHERE  table_name = @table_name
END
GO

-- Verify
EXEC sp_helptext 'dbo.usp_update_watermark';
GO