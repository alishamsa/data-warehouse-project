/*===============================================================================
  Title: Bronze Layer Data Exploration & Quality Checks
  Purpose:
      This script performs data exploration and validation checks 
      on all bronze layer tables before loading them into the silver layer.

  Notes:
      - Focused on structure, completeness, and data consistency.
      - Used for exploratory assessment before cleaning & transformation.

  Database: DataWarehouse
===============================================================================*/

USE DataWarehouse;

--===============================================================================
-- Preview Bronze Tables
--===============================================================================

SELECT TOP (10) * FROM bronze.crm_cust_info;
SELECT TOP (10) * FROM bronze.crm_prd_info;
SELECT TOP (10) * FROM bronze.crm_sales_details;
SELECT TOP (10) * FROM bronze.erp_cust_az12;
SELECT TOP (10) * FROM bronze.erp_loc_a101;
SELECT TOP (10) * FROM bronze.erp_px_cat_g1v2;

--===============================================================================
-- Data Quality Checks
--===============================================================================

/*===============================================================================
  [bronze].[crm_cust_info]
===============================================================================*/

-- Inspect column structure
---------------------------------------------------------------------------------
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'crm_cust_info'
  AND TABLE_SCHEMA = 'bronze';

-- Check primary key uniqueness and nulls
---------------------------------------------------------------------------------
-- expect no rows returned
SELECT 
       cst_id, 
       COUNT(*) AS cnt
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
-- add IS NULL to check for nulls, even if it shows once.


-- Inspect problematic rows
---------------------------------------------------------------------------------

SELECT *
FROM bronze.crm_cust_info
WHERE   
    cst_id IN (
    SELECT cst_id
    FROM bronze.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1 
)
    OR cst_id IS NULL
;

-- Check for unwanted spaces
---------------------------------------------------------------------------------
-- expect no rows returned
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- standardization and  check data consistency in low cardinality columns
---------------------------------------------------------------------------------
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;


/*===============================================================================
  [bronze].[crm_prd_info]
===============================================================================*/


-- take a look at the table
---------------------------------------------------------------------------------
  SELECT *
  FROM [bronze].[crm_prd_info]
  ;

-- Inspect columns
---------------------------------------------------------------------------------
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'crm_prd_info'
  AND TABLE_SCHEMA = 'bronze';

-- Check primary key uniqueness and nulls
---------------------------------------------------------------------------------
-- expect no rows returned
SELECT prd_id, COUNT(*) AS cnt
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check cost validity
---------------------------------------------------------------------------------
-- expect no rows returned
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Review product line values
---------------------------------------------------------------------------------
SELECT DISTINCT prd_line FROM bronze.crm_prd_info;

-- Check invalid or reversed date order
---------------------------------------------------------------------------------
-- expect no rows returned
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt
ORDER BY prd_key;

-- Check for null start/end dates
---------------------------------------------------------------------------------
-- expect no rows returned
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt IS NULL OR prd_start_dt IS NULL;



-- Sample product date investigation
------------------------------------------------------------------------------------
SELECT prd_key, prd_cost, prd_start_dt, prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_key = 'AC-HE-HL-U509-B';


-- Derive next end date logic
------------------------------------------------------------------------------------
-- building the logic on the sample then generalize it in the silver load script.
-- we going to drive the end_date to next start_date -1 day for the same product.
SELECT
    prd_key,
    prd_cost,
    prd_start_dt,
    DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS derived_prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_key = 'AC-HE-HL-U509-B';


/*===============================================================================
  [bronze].[crm_sales_details]
===============================================================================*/

-- Inspect columns
------------------------------------------------------------------------------------
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
       NUMERIC_PRECISION, NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'crm_sales_details'
  AND TABLE_SCHEMA = 'bronze';

-- Check order key duplicates
------------------------------------------------------------------------------------
SELECT sls_ord_num, COUNT(*) AS cnt
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1;

-- Check for nulls in foreign keys
------------------------------------------------------------------------------------
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id IS NULL OR sls_prd_key IS NULL OR sls_ord_num IS NULL;

-- Check for unwanted spaces
------------------------------------------------------------------------------------
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key);

-- Validate FK consistency
------------------------------------------------------------------------------------
--- expect no rows returned
SELECT *
FROM bronze.crm_sales_details
WHERE NOT EXISTS (SELECT 1 FROM silver.crm_prd_info);

SELECT *
FROM bronze.crm_sales_details
WHERE NOT EXISTS (SELECT 1 FROM silver.crm_cust_info);

-- Validate date formats and logic
------------------------------------------------------------------------------------
SELECT *
FROM bronze.crm_sales_details
WHERE ISDATE(sls_order_dt) = 0 OR ISDATE(sls_ship_dt) = 0 OR ISDATE(sls_due_dt) = 0;

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NULL OR sls_ship_dt IS NULL OR sls_due_dt IS NULL;

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt < sls_order_dt 
   OR sls_due_dt < sls_order_dt 
   OR sls_due_dt < sls_ship_dt;

-- Validate date boundaries
------------------------------------------------------------------------------------
SELECT *
FROM bronze.crm_sales_details
WHERE TRY_CAST(sls_ship_dt AS DATE) < '2000-01-01'
   OR TRY_CAST(sls_due_dt AS DATE) < '2000-01-01'
   OR TRY_CAST(sls_order_dt AS DATE) < '2000-01-01'
   OR TRY_CAST(sls_ship_dt AS DATE) > GETDATE()
   OR TRY_CAST(sls_due_dt AS DATE) > GETDATE()
   OR TRY_CAST(sls_order_dt AS DATE) > GETDATE();

-- Check sales calculation logic
------------------------------------------------------------------------------------
-- negative,zero,null not allowed
-- sales = quantity * price

SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales != CAST(sls_quantity AS DECIMAL(18,2)) * CAST(sls_price AS DECIMAL(18,2))
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales = 0 OR sls_quantity = 0 OR sls_price = 0;



-- OKAY,before we doing any transformation we need to talk to expert,show it the case.
-- business roles: 
-- if sales are zero,null then use quantity * price.
-- if price are zero,null then use sales/quantity.
-- if find negative turn it to posative.
-- time to bulid tranformation logic based of there rules.

/*
Any comparison with NULL (=, !=, >, <, etc.) does not return TRUE or FALSE.
It returns UNKNOWN.

Condition sale != quantity * null→ UNKNOWN

skip the WHEN branch

Go to ELSE sale
--
if you use zero instead of null:
70!=2*0
True
run then brach 
update sale to zero which is wrong
*/


/*===============================================================================
  [bronze].[erp_cust_az12]
===============================================================================*/

-- Inspect columns
------------------------------------------------------------------------------------
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'erp_cust_az12'
  AND TABLE_SCHEMA = 'bronze';

-- Validate customer ID consistency
------------------------------------------------------------------------------------
SELECT LEN(cid) AS cid_length, COUNT(*) AS occurrence
FROM bronze.erp_cust_az12
GROUP BY LEN(cid);

-- Validate birthdate
------------------------------------------------------------------------------------
SELECT *
FROM bronze.erp_cust_az12
WHERE ISDATE(bdate) = 0;

SELECT bdate AS bdate_old,
       CASE 
            WHEN bdate > GETDATE() THEN NULL
            ELSE CAST(bdate AS DATE)
       END AS validated_bdate
FROM bronze.erp_cust_az12
WHERE CAST(bdate AS DATE) < '1925-01-01' OR CAST(bdate AS DATE) > GETDATE()
ORDER BY bdate;

-- Gender standardization
------------------------------------------------------------------------------------
SELECT DISTINCT 
    gen,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        ELSE 'Unknown'
    END AS standardized_gen
FROM bronze.erp_cust_az12;


/*===============================================================================
  [bronze].[erp_loc_a101]
===============================================================================*/

-- Inspect columns
------------------------------------------------------------------------------------
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'erp_loc_a101'
  AND TABLE_SCHEMA = 'bronze';

-- Validate customer ID format
------------------------------------------------------------------------------------
SELECT LEN(cid) AS cid_length, COUNT(*) AS occurrence
FROM bronze.erp_loc_a101
GROUP BY LEN(cid);

-- Check linkage to customer table
------------------------------------------------------------------------------------
WITH CTE_loc AS (
    SELECT REPLACE(cid, '-', '') AS cid
    FROM bronze.erp_loc_a101
)
SELECT *
FROM CTE_loc c
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.erp_cust_az12 s
    WHERE s.cid = c.cid
);

-- Standardize country names
------------------------------------------------------------------------------------
SELECT DISTINCT cntry,
       CASE 
           WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
           WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
           WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'Unknown'
           ELSE TRIM(cntry)
       END AS standardized_cntry
FROM bronze.erp_loc_a101;


/*===============================================================================
  [bronze].[erp_px_cat_g1v2]
===============================================================================*/

-- Inspect columns
------------------------------------------------------------------------------------
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'erp_px_cat_g1v2'
  AND TABLE_SCHEMA = 'bronze';

-- Validate category ID mapping
------------------------------------------------------------------------------------
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info);

-- Validate ID pattern and nulls
------------------------------------------------------------------------------------
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id NOT LIKE '__[_]__' OR id IS NULL;

/* 
Any comparison with NULL returns UNKNOWN, not TRUE or FALSE.
this means,NULL row doesn’t appear in either result (LIKE , NOT LIKE),
you have to add NULL condition if you wanna treat NULL as a “bad value”

*/

-- Check for unwanted spaces
------------------------------------------------------------------------------------
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Review data standardization
------------------------------------------------------------------------------------
SELECT cat, subcat
FROM bronze.erp_px_cat_g1v2
GROUP BY cat, subcat;

SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;

-- This table has good data quality; no cleanup required.
