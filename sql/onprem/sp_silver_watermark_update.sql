USE PNCINSURANCE;
GO

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

-- Test
EXEC dbo.usp_update_silver_watermark 'silver_customer', '2024-01-01';
SELECT * FROM dbo.pipeline_watermark;

-- Reset
EXEC dbo.usp_update_silver_watermark 'silver_customer', '1900-01-01';
SELECT * FROM dbo.pipeline_watermark;
GO