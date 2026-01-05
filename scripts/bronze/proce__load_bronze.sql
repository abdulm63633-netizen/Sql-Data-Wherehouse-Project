/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY 
    SET  @batch_start_time = GETDATE();
    PRINT '================================================';
    PRINT 'Loading Bronze Layer';
    PRINT 'Batch Start Time: ' + CAST(@batch_start_time AS NVARCHAR);
    PRINT '================================================';

    PRINT '------------------------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '------------------------------------------------';

    -- Load Customer Info
    SET @start_time = GETDATE()
    PRINT '>> Truncating Table: bronze.crm_customer_info';
    TRUNCATE TABLE bronze.crm_customer_info;
    PRINT '>> Inserting Data: bronze.crm_customer_info';
    BULK INSERT bronze.crm_customer_info
    FROM 'C:\Users\khali\OneDrive\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
    SET @end_time = GETDATE();
    PRINT '   Done. Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -- Load Product Info
    SET @start_time = GETDATE()
    PRINT '>> Truncating Table: bronze.crm_product_info';
    TRUNCATE TABLE bronze.crm_product_info;
    PRINT '>> Inserting Data: bronze.crm_product_info';
    BULK INSERT bronze.crm_product_info
    FROM 'C:\Users\khali\OneDrive\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
    SET @end_time = GETDATE();
    PRINT '   Done. Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


    SET @start_time = GETDATE()
    -- Load Sales Details
    PRINT '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    PRINT '>> Inserting Data: bronze.crm_sales_details';
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\khali\OneDrive\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
    SET @end_time = GETDATE();
    PRINT '   Done. Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


    PRINT '------------------------------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '------------------------------------------------';

    -- Load ERP Location Data
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    PRINT '>> Inserting Data: bronze.erp_loc_a101';
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\khali\OneDrive\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);

    SET @end_time = GETDATE();
    PRINT '   Done. Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -- Load ERP Customer Data
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    PRINT '>> Inserting Data: bronze.erp_cust_az12';
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\khali\OneDrive\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
    SET @end_time = GETDATE();
    PRINT '   Done. Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -- Load ERP Product Category Data
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    PRINT '>> Inserting Data: bronze.erp_px_cat_g1v2';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\khali\OneDrive\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
    SET @end_time = GETDATE();
    PRINT '   Done. Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    SET @batch_end_time = GETDATE()
    PRINT '================================================';
    PRINT 'Bronze Layer Loading Completed Successfully';
    PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
    PRINT '================================================';
    END TRY 
    BEGIN CATCH  
        DECLARE @error_message NVARCHAR(4000);
            DECLARE @error_number INT;
            DECLARE @error_line INT;

            PRINT 'Bronze load FAILED';
            PRINT 'Error Number: ' + CAST(@error_number AS VARCHAR);
            PRINT 'Error Line: ' + CAST(@error_line AS VARCHAR);
            PRINT 'Error Message: ' + @error_message;

            THROW;
    END CATCH
END 
