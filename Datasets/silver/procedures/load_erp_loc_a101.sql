USE DataWhereHourse;
GO

CREATE OR ALTER PROCEDURE silver.load_erp_loc_a101
AS 
BEGIN 
    -- 1. Declare and Initialize Timing Variables
    DECLARE @start_time datetime, @end_time datetime;
    SET @start_time = GETDATE();

    -- 2. Print and Execute Truncate
    PRINT '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    -- 3. Print and Execute Insert with Data Mapping Logic
    PRINT '>> Inserting Data into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry,
        Record_source
    )
    SELECT 
        -- Standardizing ID by removing dashes
        REPLACE(cid, '-', '') AS cid,
        -- Mapping country codes to full names
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unknown'
            ELSE TRIM(cntry) 
        END AS cntry,
        'ERP SYSTEM' AS Record_source
    FROM bronze.erp_loc_a101;

    -- 4. Calculate and Print Duration
    SET @end_time = GETDATE(); 
    PRINT '>> Loading Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS nvarchar) + ' Seconds';

    -- 5. Final Row Count Log
    DECLARE @cnt INT;
    SELECT @cnt = COUNT(*) FROM silver.erp_loc_a101;
    PRINT 'Silver load completed. Rows: ' + CAST(@cnt AS VARCHAR);
END
GO

-- Execution Command (Corrected to match procedure name)
EXEC silver.load_erp_loc_a101;
