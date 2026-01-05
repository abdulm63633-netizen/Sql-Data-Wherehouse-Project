


CREATE OR ALTER PROCEDURE silver.load_erp_cust_az12
AS 
BEGIN 
    -- 1. Declare and Initialize Timing Variables
    DECLARE @start_time datetime, @end_time datetime;
    SET @start_time = GETDATE();

    -- 2. Print and Execute Truncate
    PRINT '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    -- 3. Print and Execute Insert with Data Cleansing Logic
    PRINT '>> Inserting Data into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 ( 
        cid,
        bdate,
        gen,
        Record_source
    )
    SELECT 
        -- Removing 'NAS' prefix to match CRM customer IDs for future integration
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
            ELSE cid
        END AS cid,
        -- Data Quality: Preventing future birthdates
        CASE 
            WHEN bdate > GETDATE() THEN NULL
            ELSE bdate 
        END AS bdate,
        -- Standardizing Gender labels
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'Unknown'
        END AS gen,
        'ERP SYSTEM' AS Record_source
    FROM bronze.erp_cust_az12;

    -- 4. Calculate and Print Duration
    SET @end_time = GETDATE(); 
    PRINT '>> Loading Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS nvarchar) + ' Seconds';

    -- 5. Final Row Count Log
    DECLARE @cnt INT;
    SELECT @cnt = COUNT(*) FROM silver.erp_cust_az12;
    PRINT 'Silver load completed. Rows: ' + CAST(@cnt AS VARCHAR);
END
GO

-- Execution Command
EXEC silver.load_erp_cust_az12;
