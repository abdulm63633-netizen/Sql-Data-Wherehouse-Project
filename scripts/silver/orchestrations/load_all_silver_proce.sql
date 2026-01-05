

-- ============================================================
-- Orchestrates execution of all Silver load procedures.
-- Each Silver table has its own stored procedure; this
-- procedure controls execution order, logging, and errors.
-- ============================================================

CREATE OR ALTER PROCEDURE silver.load_silver_layer
AS 
BEGIN
	DECLARE @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
	BEGIN TRY 
			set @batch_start_time = getdate() 
			print 'loading CRM SYSTEM ...'
			EXEC silver.load_crm_customer_info;
			print('=====================================')
			EXEC silver.load_crm_product_info;
			print('=====================================')
			EXEC silver.load_crm_sales_details;
			print('=====================================')
			print 'loading CRM SYSTEM completed'

			print 'loading ERP SYSTEM ...'
			EXEC silver.load_erp_cust_az12;
			print('=====================================')
			EXEC silver.load_erp_loc_a101;
			print('=====================================')
			EXEC silver.load_erp_px_cat_g1v2;
			print 'loading ERP SYSTEM completed'

			print('=====================================')

			PRINT 'All Silver tables loaded successfully';
			set @batch_end_time  = getdate()
			PRINT '>> Loading Duration : ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS nvarchar) + ' Seconds';
			print('=====================================')

	END TRY
		BEGIN CATCH
			PRINT '❌ Silver layer load FAILED';

			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
			PRINT 'Error Message: ' + ERROR_MESSAGE();
			PRINT 'Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
			PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR);

			-- Stop execution and bubble error up
			THROW;
		END CATCH
END;


EXEC silver.load_silver_layer
