/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================

-- check if cst_id got duplicate after joining(this happen in one to many relaitionship)
-- Expectation: No results 

SELECT 
    ci.cst_id,
    COUNT(*)
FROM      [silver].[crm_cust_info] AS ci 
LEFT JOIN [silver].[erp_cust_az12] AS ca
    ON    ci.cst_key=ca.cid
LEFT JOIN [silver].[erp_loc_a101]  AS la  
    ON    ci.cst_key=la.cid
GROUP BY ci.cst_id
HAVING COUNT(*) >1
;


-- check integration
-- Expectation: cst_gndr is the master,if it is empty get value from gen.
-- Expectation: no null only Unknown,Female,Male

SELECT DISTINCT 
    ci.cst_gndr,
    ca.gen,
    case 
        when ci.cst_gndr != 'Unknown' then ci.cst_gndr -- CRM is the master for customer data
        else coalesce(ca.gen,'Unknown')
    END as new_gen
FROM      [silver].[crm_cust_info] AS ci 
LEFT JOIN [silver].[erp_cust_az12] AS ca
    ON    ci.cst_key=ca.cid
LEFT JOIN [silver].[erp_loc_a101]  AS la  
    ON    ci.cst_key=la.cid
ORDER BY 1,2
;

-- check standardization
-- Expectation: three rows

SELECT 
        gender,
        COUNT(*) as count
FROM gold.dim_customers
GROUP BY gender
;


-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================

-- check category id consistency
-- expect one category id have no matching(CO_PE)

SELECT 
    p.cat_id,
    c.id
FROM [silver].[crm_prd_info]             AS p
LEFT JOIN [silver].[erp_px_cat_g1v2]     AS c
    ON p.cat_id=c.id
WHERE c.id IS NULL
;


-- check  prd_key don't get  duplicate after joining(this happen in one to many relaitionship)
-- Expectation: No results 

SELECT 
      pi.[prd_key]

FROM [silver].[crm_prd_info]           AS pi  
LEFT JOIN [silver].[erp_px_cat_g1v2]   AS pc 
    ON  pi.cat_id=pc.id
WHERE pi.prd_end_dt IS NULL                          -- filter out all historical data
GROUP BY pi.[prd_key]
HAVING COUNT(*)> 1
;


-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================


-- check foreign key integrity (Dimensions)
-- Expectation: No results,means everything matching perfectly

SELECT * 
FROM      gold.fact_sales       AS s
LEFT JOIN gold.dim_customers    AS c
    ON    s.customer_key=c.customer_key
LEFT JOIN gold.dim_products     AS p  
    ON    s.product_key=p.product_key
WHERE s.customer_key IS NULL
 OR   s.product_key  IS NULL
;

