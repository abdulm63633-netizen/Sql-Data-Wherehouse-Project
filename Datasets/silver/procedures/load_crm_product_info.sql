
CREATE OR ALTER PROCEDURE silver.load_crm_product_info 
AS
BEGIN 
    -- 1. Declare and Initialize Timing Variables
    DECLARE @start_time datetime, @end_time datetime;
    SET @start_time = GETDATE();

    -- 2. Print and Execute Truncate
    PRINT '>> Truncating Table: silver.crm_product_info';
    TRUNCATE TABLE silver.crm_product_info;

    -- 3. Print and Execute Insert with Data Cleansing
    PRINT '>> Inserting Data into: silver.crm_product_info';
    INSERT INTO silver.crm_product_info ( 
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        Record_source
    )
    SELECT 
        prd_id,
        -- Standardizing Category ID
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        -- Extracting Product Key
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        -- Handling missing costs
        ISNULL(prd_cost, 0) AS prd_cost,
        -- Standardizing Product Line names
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        -- Calculating end date based on the next product start date
        CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt,
        'CRM SYSTEM' AS Record_source
    FROM bronze.crm_product_info;

    -- 4. Calculate and Print Duration
    SET @end_time = GETDATE(); 
    PRINT '>> Loading Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS nvarchar) + ' Seconds';

    -- 5. Final Row Count Log
    DECLARE @cnt INT;
    SELECT @cnt = COUNT(*) FROM silver.crm_product_info;
    PRINT 'Silver load completed. Rows: ' + CAST(@cnt AS VARCHAR);
END
GO

-- Execution
EXEC silver.load_crm_product_info;
