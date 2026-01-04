
CREATE OR ALTER PROCEDURE silver.load_crm_customer_info 
AS
BEGIN 
    -- 1. Declare and Initialize Timing Variables
    DECLARE @start_time datetime, @end_time datetime;
    SET @start_time = GETDATE();

    -- 2. Print and Execute Truncate
    PRINT '>> Truncating Table: silver.crm_customer_info';
    TRUNCATE TABLE silver.crm_customer_info;

    -- 3. Print and Execute Insert (Data Quality & De-duplication Logic)
    PRINT '>> Inserting Data into: silver.crm_customer_info';
    WITH Rnk AS (
      SELECT
        cst_id,
        cst_key,
        isnull(TRIM(cst_firstname),'Unknown') AS cst_firstname,
        isnull(TRIM(cst_lastname) ,'Unknown') AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr))  = 'F' THEN 'FEMALE'
            WHEN UPPER(TRIM(cst_gndr))  = 'M' THEN 'MALE'
            ELSE   'N/A'
        END AS cst_gndr,
        cst_create_date,
        ROW_NUMBER() OVER (
          PARTITION BY cst_id
          ORDER BY cst_create_date DESC
        ) AS rn
      FROM
        bronze.crm_customer_info
    )
    INSERT INTO silver.crm_customer_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr, 
        cst_create_date,
        record_source
    )
    SELECT 
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        'CRM_SYSTEM'  AS Record_source
     FROM Rnk 
     WHERE rn = 1; -- Ensures we only take the latest record per customer ID

    -- 5. Final Row Count Log
    DECLARE @cnt INT;
    SELECT @cnt = COUNT(*) FROM silver.crm_customer_info;
    PRINT 'Silver load completed. Rows: ' + CAST(@cnt AS VARCHAR);
    -- 4. Calculate and Print Duration
    SET @end_time = GETDATE(); 
    PRINT '>> Loading Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS nvarchar) + ' Seconds';

END 
GO

-- Execution Command
EXEC silver.load_crm_customer_info;
