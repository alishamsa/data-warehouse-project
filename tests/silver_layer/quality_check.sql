/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


-- =================================================
-- crm_cust_info
-- =================================================

-- test  DDL
-- -------------------------------------------------

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'crm_cust_info'
;

-- test data quality
-- --------------------------------------------------


SELECT * 
FROM [silver].[crm_cust_info]                 
;-- Expectation: Table populated

-- check row number(complateness)
-- ----------------

SELECT 'bronze' as layer,
        COUNT(*) AS row_num
FROM (SELECT 
       *,
       ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
     FROM [bronze].[crm_cust_info]
) AS src
WHERE rn = 1 AND cst_id IS NOT NULL
UNION ALL
SELECT 'silver' as layer,
        COUNT(*) AS row_num
FROM [silver].[crm_cust_info]
;-- expect to be equal


-- Check duplicate or NULL cst_id
----------------------------------------

select 
      cst_id,
      count(*) as cnt
FROM [silver].[crm_cust_info]
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL
;-- Expectation: No rows returned



-- check for unwanted spaces
----------------------------------------

SELECT cst_firstname
FROM [silver].[crm_cust_info]
WHERE cst_firstname != TRIM(cst_firstname)   
;-- Expectation: No rows returned


----------------------------------------

SELECT cst_gndr
FROM [silver].[crm_cust_info]
WHERE cst_gndr != TRIM(cst_gndr)
;-- Expectation: No rows returned           


-- Check standardization
----------------------------------------

SELECT DISTINCT cst_gndr
FROM [silver].[crm_cust_info]              
;-- Expectation: Values should be 'Male', 'Female', 'Unknown'

----------------------------------------

SELECT DISTINCT [cst_marital_status]
FROM [silver].[crm_cust_info]
;-- Expectation: Values should be 'Single', 'Married', 'Unknown',

-- if don't find 'unknown' means it filters 
SELECT *
FROM [bronze].[crm_cust_info]
WHERE cst_marital_status IS NULL
;

-- =================================================
-- crm_prd_info
-- =================================================


-- test  DDL
-- -------------------------------------------------

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver'
and TABLE_NAME = 'crm_prd_info'
;

-- check row number(complateness)
-- ----------------

SELECT 'bronze' as layer,
        COUNT(*) AS row_num
FROM [bronze].[crm_prd_info]
UNION ALL
SELECT 'silver' as layer,
        COUNT(*) AS row_num
FROM [silver].[crm_prd_info]
;-- expect to be equal

-- test loading quality
-- --------------------------------------------------


SELECT * 
FROM [silver].[crm_prd_info]  
;


-- Check duplicate or NULL prd_id
----------------------------------------

select 
      prd_id,
      count(*) as cnt
FROM [silver].[crm_prd_info]
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL
;--Expectation: No rows returned

-- check for unwanted spaces
----------------------------------------

SELECT prd_key
FROM [silver].[crm_prd_info]
WHERE prd_key != TRIM(prd_key)   
;--Expectation: No rows returned

----------------------------------------

SELECT prd_nm
FROM [silver].[crm_prd_info]
WHERE prd_nm != TRIM(prd_nm)   
;--Expectation: No rows returned

-- Check standardization
----------------------------------------

SELECT DISTINCT [prd_line]
FROM [silver].[crm_prd_info]
;-- Expectation: Values should be 'Mountain', 'Road', 'Touring', 'Other sales', 'Unknown'


-- referential integrity check
-- --------------------------------------

SELECT 
        prd_id,
        prd_key,
        cat_id
FROM [silver].[crm_prd_info]
WHERE cat_id NOT IN (SELECT id FROM [bronze].[erp_px_cat_g1v2] WHERE id IS NOT NULL)
;

/* 
we add 'WHERE id IS NOT NULL' condition,because comparisons with NULL become UNKNOWN →
the whole condition becomes UNKNOWN, and you get no results (even if orphans exist).
That’s why SQL developers almost always use NOT EXISTS for orphan checks.
exists doesn’t care about the actual values (even if they are NULL) —
 it only cares if rows exist.
 */

SELECT p.prd_id, p.prd_key, p.cat_id
FROM [silver].[crm_prd_info] p
WHERE NOT EXISTS (
    SELECT 1
    FROM [bronze].[erp_px_cat_g1v2] c
    WHERE p.cat_id = c.id
);


-- check for nulls or negative values in prd_cost
-- -------------------------------------------------

SELECT prd_cost
FROM [silver].[crm_prd_info]
WHERE prd_cost IS NULL OR prd_cost < 0
;-- expect no rows to be returned


-- check ordering of prd_start_dt and prd_end_dt
-- -------------------------------------------------

SELECT *
FROM [silver].[crm_prd_info]
WHERE prd_end_dt < prd_start_dt
ORDER BY prd_key
;-- expect no rows to be returned



--==========================================
-- [silver].[crm_sales_details] table
--==========================================

-- Check DDL
-- ---------

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver'
and TABLE_NAME = 'crm_prd_info'
;

-- take a look at the table
-- ------------------------------------------

SELECT * 
FROM [silver].[crm_sales_details]
;


-- check row number(complateness)
-- ----------------

SELECT 'bronze' as layer,
        COUNT(*) AS row_num
FROM [bronze].[crm_sales_details]
UNION ALL
SELECT 'silver' as layer,
        COUNT(*) AS row_num
FROM [silver].[crm_sales_details]
;-- expect to be equal

-- check consistency of sls_prd_key values with [silver].[crm_prd_info] table
-- -----------------------------------------------------------------------

-- expect no rows to be returned

SELECT * 
FROM [silver].[crm_sales_details] as s
WHERE  EXISTS (
    SELECT *
    FROM silver.crm_prd_info as P
    WHERE s.sls_prd_key =P.prd_key)
;


-- check connecting with [silver].[crm_cust_info] table
-- ---------------------------------------------------------------------------

SELECT * 
FROM [silver].[crm_sales_details] AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM [silver].[crm_cust_info] AS i
    WHERE s.sls_cust_id = i.cst_id
);-- expect no rows to be returned


-- check  date logic
-- -------------------------------------------------

SELECT *
FROM [silver].[crm_sales_details]
WHERE sls_ship_dt < sls_order_dt OR sls_due_dt < sls_order_dt OR sls_due_dt < sls_ship_dt
;-- expect no rows to be returned


-- check measures & calculation 
-- ----------------------------

 
SELECT *
FROM [silver].[crm_sales_details]
WHERE sls_sales != sls_quantity * sls_price
  OR sls_sales IS NULL OR sls_quantity IS NULL or sls_price IS NULL
  OR sls_sales = 0 OR sls_quantity = 0 or sls_price = 0
  OR sls_sales < 0 OR sls_quantity < 0 or sls_price < 0
;-- expect no rows to be returned


-- ==========================================
-- [silver].[erp_cust_az12]
-- ==========================================

-- check DDL
-- ---------

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver'
and TABLE_NAME = 'erp_cust_az12'
;

-- take a look
-- ------------

SELECT * 
FROM [silver].[erp_cust_az12]
;

-- check row number(complateness)
-- ----------------

SELECT 'bronze' as layer,
        COUNT(*) AS row_num
FROM [bronze].[erp_cust_az12]
UNION ALL
SELECT 'silver' as layer,
        COUNT(*) AS row_num
FROM [silver].[erp_cust_az12]
;-- expect to be equal


-- ckeck cid Consistency 
-- ------------------

SELECT 
    LEN(cid) as cid_length,
    COUNT(*) as occurrence
FROM [silver].[erp_cust_az12]
GROUP BY LEN(cid)
;-- expect one row to be returned

-- check date boundries
-- ---------------------

  SELECT bdate
  FROM [silver].[erp_cust_az12]
  WHERE  bdate  > GETDATE()
  ;-- expect no rows to be returned

-- check gen 
-- ----------

SELECT distinct gen 
FROM [silver].[erp_cust_az12]
;-- Expectation: Values should be 'Male', 'Female', 'Unknown'



-- ==========================================
-- [silver].[erp_loc_a101]
-- ==========================================

-- check DDL
-- ---------

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver'
and TABLE_NAME = 'erp_loc_a101'
;

-- take a look
-- ------------
SELECT *
FROM [silver].[erp_loc_a101]
;

-- check row number(complateness)
-- ----------------

SELECT 'bronze' as layer,
        COUNT(*) AS row_num
FROM [bronze].[erp_loc_a101]
UNION ALL
SELECT 'silver' as layer,
        COUNT(*) AS row_num
FROM [silver].[erp_loc_a101]
;-- expect to be equal

-- cid
-- ----

-- check length

SELECT 
    LEN(cid) as cid_length,
    COUNT(*) as occurrence
FROM [silver].[erp_loc_a101]
GROUP BY LEN(cid)
;-- expect one row to be returned

-- check if the id reliable 

SELECT *
FROM [silver].[erp_loc_a101] 
WHERE cid not in (
    SELECT cid
    FROM [silver].[erp_cust_az12] )
;--expect one row have no relationship to be returned

-- check cntry consistency
-- ------------------------

SELECT 
      distinct cntry
FROM [silver].[erp_loc_a101]
;


-- ==========================================
-- [silver].[erp_px_cat_g1v2]
-- ==========================================


-- check ddl
-- ---------

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver'
and TABLE_NAME = 'erp_px_cat_g1v2'
;


-- check data
-- -----------

SELECT *
FROM [silver].[erp_px_cat_g1v2];


-- check row number(complateness)
-- ----------------

SELECT 'bronze' as layer,
        COUNT(*) AS row_num
FROM [bronze].[erp_px_cat_g1v2]
UNION ALL
SELECT 'silver' as layer,
        COUNT(*) AS row_num
FROM [silver].[erp_px_cat_g1v2]
;-- expect to be equal

-- check connection 
-- ----------------

SELECT *
FROM [silver].[erp_px_cat_g1v2]
WHERE id  not in (select cat_id from [silver].[crm_prd_info]) 
;


-- check standardization
-- ----------------------

SELECT distinct cat
FROM [bronze].[erp_px_cat_g1v2]
;
------
SELECT distinct subcat
FROM [bronze].[erp_px_cat_g1v2]
;
------
SELECT distinct maintenance
FROM [bronze].[erp_px_cat_g1v2]
;


