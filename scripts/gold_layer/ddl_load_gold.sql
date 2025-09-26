/*
===============================================================================
DDL+load Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================


CREATE OR ALTER VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,  -- Surrogate key
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS firstname,
    ci.cst_lastname AS lastname,
    la.cntry AS country,
    CASE 
        WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr       -- CRM is master
        ELSE COALESCE(ca.gen, 'Unknown')
    END AS gender,
    ci.cst_marital_status AS marital_status,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la  
    ON ci.cst_key = la.cid;

GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================


CREATE OR ALTER VIEW gold.dim_products AS
(
    SELECT 
          ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_id)  AS product_key,   -- surrogate key
          pi.prd_id        AS product_id,                                            -- natural/business key
          pi.prd_key       AS product_number,
          pi.prd_nm        AS product_name,
          pi.cat_id        AS category_id,
          pc.cat           AS category,
          pc.subcat        AS subcategory,
          pc.maintenance   AS maintenance,
          pi.prd_cost      AS cost,
          pi.prd_line      AS product_line,
          pi.prd_start_dt  AS start_date         
    FROM [silver].[crm_prd_info]         AS pi
    LEFT JOIN [silver].[erp_px_cat_g1v2] AS pc
           ON pi.cat_id = pc.id
    WHERE pi.prd_end_dt IS NULL                  -- filter out all historical data,leave only active.
);

GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

CREATE OR ALTER VIEW gold.fact_sales AS
(
    SELECT
        sd.sls_ord_num          AS order_number,
        pv.product_key,
        cv.customer_key,
        sd.sls_order_dt         AS order_date,
        sd.sls_ship_dt          AS shipping_date,
        sd.sls_due_dt           AS due_date,
        sd.sls_sales            AS sales_amount,
        sd.sls_quantity         AS quantity,
        sd.sls_price            AS price
    FROM [silver].[crm_sales_details] AS sd
    LEFT JOIN gold.dim_customers AS cv
        ON sd.sls_cust_id = cv.customer_id
    LEFT JOIN gold.dim_products AS pv
        ON sd.sls_prd_key = pv.product_number
);

