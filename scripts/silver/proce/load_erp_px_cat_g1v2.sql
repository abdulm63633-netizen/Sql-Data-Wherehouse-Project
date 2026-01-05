
CREATE OR ALTER PROCEDURE silver.load_erp_px_cat_g1v2
AS 
BEGIN 
    DECLARE @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
    SET @start_time = GETDATE()

    -- Added Print for Truncating
    PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2

    -- Added Print for Inserting
    PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance,
        Record_source
    )
    SELECT 
        id,
        cat,
        subcat,
        maintenance,
        'ERP SYSTEM' AS Record_source
    FROM bronze.erp_px_cat_g1v2

    DECLARE @cnt INT;
    SELECT @cnt = COUNT(*) FROM silver.erp_px_cat_g1v2;

    PRINT 'Silver load completed. Rows: ' + CAST(@cnt AS VARCHAR)

    SET @end_time = GETDATE() 
    PRINT '>> Loading Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS nvarchar) + ' Second'

END
GO

-- Execution Command
EXEC silver.load_erp_px_cat_g1v2;
