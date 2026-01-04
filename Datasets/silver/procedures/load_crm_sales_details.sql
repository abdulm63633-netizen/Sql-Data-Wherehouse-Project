


CREATE OR ALTER PROCEDURE silver.load_crm_sales_details
AS
BEGIN 
    -- 1. Declare and Initialize Timing Variables
    DECLARE @start_time datetime, @end_time datetime;
    SET @start_time = GETDATE();

    -- 2. Print and Execute Truncate
    PRINT '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    -- 3. Print and Execute Insert with Advanced Data Quality Logic
    PRINT '>> Inserting Data into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,   
        sls_prd_key,   
        sls_cust_id,            
        sls_order_dt,           
        sls_ship_dt,            
        sls_due_dt,             
        sls_sales,            
        sls_quantity,            
        sls_price,
        Record_source
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Date Cleansing: Converting numeric YYYYMMDD to DATE
        CASE 
            WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
            ELSE TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR), 102)
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
            ELSE TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR), 102)
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
            ELSE TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR), 102)
        END AS sls_due_dt,
        -- Sales Cleansing: Calculating Sales if NULL or Zero
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        -- Price Cleansing: Calculating Price if NULL or Zero
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0) 
            ELSE sls_price
        END AS sls_price,
        'CRM SYSTEM' AS Record_source
    FROM bronze.crm_sales_details;

    -- 4. Calculate and Print Duration
    SET @end_time = GETDATE(); 
    PRINT '>> Loading Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS nvarchar) + ' Seconds';

    -- 5. Final Row Count Log
    DECLARE @cnt INT;
    SELECT @cnt = COUNT(*) FROM silver.crm_sales_details;
    PRINT 'Silver load completed. Rows: ' + CAST(@cnt AS VARCHAR);
END
GO

-- Execution
EXEC silver.load_crm_sales_details;


