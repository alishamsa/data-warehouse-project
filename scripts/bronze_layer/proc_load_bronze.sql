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


CREATE OR ALTER PROCEDURE bronze.proc_load_bronze AS
BEGIN

    DECLARE @start_time DATETIME , @end_time DATETIME;

    BEGIN TRY 
        BEGIN TRANSACTION;

        PRINT '===============================';
        PRINT 'Loading data into bronze layer tables...';
        PRINT '===============================';

        PRINT '';
        PRINT '--------------------------------';
        PRINT 'Loading CRM tables...';
        PRINT '--------------------------------';


        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>>Truncating table: crm_cust_info';
        TRUNCATE TABLE [bronze].[crm_cust_info];
        PRINT '>>Inserting table: crm_cust_info';
        BULK INSERT [bronze].[crm_cust_info]
        FROM '../../datasets/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';


        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>>Truncating table: crm_prd_info';
        TRUNCATE TABLE [bronze].[crm_prd_info];
        PRINT '>>Inserting table: crm_prd_info';
        BULK INSERT [bronze].[crm_prd_info]
        FROM '../../datasets/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';    


        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>>Truncating table: crm_sales_details';
        TRUNCATE TABLE [bronze].[crm_sales_details];
        PRINT '>>Inserting table: crm_sales_details';
        BULK INSERT [bronze].[crm_sales_details]
        FROM '../../datasets/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';



        PRINT '';
        PRINT '--------------------------------';
        PRINT 'Loading ERP tables...';
        PRINT '--------------------------------';


        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>>Truncating table: erp_cust_az12';
        TRUNCATE TABLE [bronze].[erp_cust_az12];
        PRINT '>>Inserting table: erp_cust_az12';
        BULK INSERT [bronze].[erp_cust_az12]
        FROM '../../datasets/cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';    


        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>>Truncating table: erp_loc_a101';
        TRUNCATE TABLE [bronze].[erp_loc_a101];
        PRINT '>>Inserting table: erp_loc_a101';
        BULK INSERT [bronze].[erp_loc_a101]
        FROM '../../datasets/loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';    


        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>>Truncating table: erp_px_cat_g1v2';
        TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
        PRINT '>>Inserting table: erp_px_cat_g1v2';
        BULK INSERT [bronze].[erp_px_cat_g1v2]
        FROM '../../datasets/px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds'; 
        

        -- ✅ All successful, commit
        COMMIT TRANSACTION;
        PRINT '';
        PRINT 'All data successfully loaded into bronze tables.';
    END TRY
    BEGIN CATCH
        -- ❌ Rollback if anything fails
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT 'Error occurred during data load!';
        PRINT '====================================';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '====================================';
    END CATCH
END
