/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY

        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

        -- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info...';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting data into Table: silver.crm_cust_info...';
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, 
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            CASE UPPER(cst_marital_status)
                WHEN 'M' THEN 'Married'
                WHEN 'S' THEN 'Single'
                ELSE 'Unknown'
            END,
            CASE UPPER(cst_gndr)
                WHEN 'M' THEN 'Male'
                WHEN 'F' THEN 'Female'
                ELSE 'Unknown'
            END,
            cst_create_date
        FROM (
            SELECT 
                cst_id,
                TRIM(cst_key) AS cst_key,
                TRIM(cst_firstname) AS cst_firstname,
                TRIM(cst_lastname) AS cst_lastname,
                TRIM(cst_marital_status) AS cst_marital_status,
                TRIM(cst_gndr) AS cst_gndr,
                cst_create_date,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info
        ) AS src
        WHERE rn = 1 AND cst_id IS NOT NULL;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info...';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting data into Table: silver.crm_prd_info...';
        WITH cte_trimmed AS (
            SELECT 
                prd_id,
                TRIM(prd_key) AS prd_key,
                TRIM(prd_nm) AS prd_nm,
                TRIM(prd_cost) AS prd_cost,
                TRIM(prd_line) AS prd_line,
                prd_start_dt
            FROM bronze.crm_prd_info
            WHERE prd_id IS NOT NULL
        )
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, 
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)), '-', '_'),
            prd_nm,
            ABS(ISNULL(prd_cost, 0)),
            CASE 
                WHEN UPPER(prd_line) = 'M' THEN 'Mountain'
                WHEN UPPER(prd_line) = 'R' THEN 'Road'
                WHEN UPPER(prd_line) = 'T' THEN 'Touring'
                WHEN UPPER(prd_line) = 'S' THEN 'Other sales'
                ELSE 'Unknown'
            END,
            prd_start_dt,
            DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
        FROM cte_trimmed;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



        -- Loading silver.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details...';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting data into Table: silver.crm_sales_details...';
        WITH cte_sales AS (
            SELECT 
                sls_ord_num,
                TRIM(sls_prd_key) AS sls_prd_key,
                sls_cust_id,
                CASE WHEN LEN(sls_order_dt) = 8 THEN TRY_CAST(sls_order_dt AS DATE) END AS sls_order_dt,
                CASE WHEN LEN(sls_ship_dt) = 8 THEN TRY_CAST(sls_ship_dt AS DATE) END AS sls_ship_dt,
                CASE WHEN LEN(sls_due_dt) = 8 THEN TRY_CAST(sls_due_dt AS DATE) END AS sls_due_dt,
                ABS(CAST(sls_sales AS DECIMAL(18,2))) AS sls_sales,
                ABS(CAST(sls_quantity AS INT)) AS sls_quantity,
                ABS(CAST(sls_price AS DECIMAL(18,2))) AS sls_price
            FROM bronze.crm_sales_details
        ),
        cte_sales_final AS (
            SELECT
                sls_ord_num, sls_prd_key, sls_cust_id,
                sls_order_dt, sls_ship_dt, sls_due_dt,
                AVG(DATEDIFF(DAY, sls_order_dt, sls_ship_dt)) OVER() AS avg_ship_days,
                NULLIF(sls_sales, 0) AS sls_sales,
                NULLIF(sls_quantity, 0) AS sls_quantity,
                NULLIF(sls_price, 0) AS sls_price
            FROM cte_sales
        )
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, 
            sls_order_dt, sls_ship_dt, sls_due_dt, 
            sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            ISNULL(sls_order_dt, DATEADD(DAY, -avg_ship_days, sls_ship_dt)),
            sls_ship_dt,
            sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales != sls_quantity * sls_price
                    THEN sls_quantity * sls_price
                ELSE sls_sales
            END,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL THEN CAST(sls_sales / sls_quantity AS DECIMAL(18,2))
                ELSE sls_price
            END
        FROM cte_sales_final;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        
    	PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';


        -- Loading silver.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12...';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting data into Table: silver.erp_cust_az12...';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE CAST(bdate AS DATE) END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'Unknown'
            END
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101...';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting data into Table: silver.erp_loc_a101...';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT 
            REPLACE(cid, '-', ''),
            CASE 
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'Unknown'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2...';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting data into Table: silver.erp_px_cat_g1v2...';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

    	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END;


