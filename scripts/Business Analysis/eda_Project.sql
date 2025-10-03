/*
===============================================================================
Project   : Exploratory Data Analysis (EDA) Project
Database  : Gold Schema (dim_customers, dim_products, fact_sales)
Purpose   : Perform schema exploration, descriptive analysis, 
            key metrics calculation, magnitude insights, and ranking analysis.
===============================================================================
*/


/* ============================
   1. Database Exploration
============================ */

-- Explore All Objects in the Database
SELECT *
FROM INFORMATION_SCHEMA.TABLES;

-- Explore All Columns in the Database
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS;

-- Explore Columns for a Specific Table
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold'
  AND TABLE_NAME = 'dim_customers';

-- Preview Customer Dimension Data
SELECT *
FROM gold.dim_customers;


/* ============================
   2. Dimensions Exploration
============================ */

-- Unique Countries in Customers
SELECT DISTINCT country
FROM gold.dim_customers;

-- Product Category & Subcategory Granularity
SELECT DISTINCT 
    category,
    subcategory
FROM gold.dim_products;


/* ============================
   3. Date Exploration
============================ */

-- Order Date Range in Years and Months
SELECT
    MIN(order_date)  AS first_order,
    MAX(order_date)  AS last_order,
    DATEDIFF(YEAR,  MIN(order_date), MAX(order_date))  AS order_range_years,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date))  AS order_range_months
FROM gold.fact_sales;


/* ============================
   4. Customer Age Analysis
============================ */

-- Oldest & Youngest Customers
SELECT *
FROM (
    SELECT TOP 1
        firstname + ' ' + lastname AS Customer,
        'Oldest Customer' AS label,
        birthdate,
        DATEDIFF(YEAR, birthdate, GETDATE()) AS age
    FROM gold.dim_customers
    WHERE birthdate IS NOT NULL
    ORDER BY birthdate ASC
) AS oldest

UNION ALL

SELECT *
FROM (
    SELECT TOP 1
        firstname + ' ' + lastname AS Customer,
        'Youngest Customer' AS label,
        birthdate,
        DATEDIFF(YEAR, birthdate, GETDATE()) AS age
    FROM gold.dim_customers
    WHERE birthdate IS NOT NULL
    ORDER BY birthdate DESC
) AS youngest;


/* ============================
   5. Measures Exploration
============================ */

-- Total Sales
SELECT 
    FORMAT(SUM(sales_amount), 'C', 'en-US') AS total_sales
FROM gold.fact_sales;

-- Total Quantity Sold
SELECT 
    SUM(quantity) AS total_quantity
FROM gold.fact_sales;

-- Average Selling Price
SELECT 
    FORMAT(AVG(price),'C','en-US') AS avg_price
FROM gold.fact_sales;

-- Total Orders (raw and distinct)
SELECT COUNT(order_number)              AS total_orders
FROM gold.fact_sales;

SELECT COUNT(DISTINCT order_number)     AS distinct_total_orders
FROM gold.fact_sales;

-- Products (dimension vs sold)
SELECT COUNT(product_key)               AS total_products
FROM gold.dim_products;

SELECT COUNT(DISTINCT product_key)      AS sold_products
FROM gold.fact_sales;

-- Customers (total vs customers with orders)
SELECT COUNT(customer_key)              AS total_customers
FROM gold.dim_customers;

SELECT COUNT(DISTINCT customer_key)     AS customers_with_orders
FROM gold.fact_sales;

-- Summary Report of All Key Metrics
SELECT 'Total Sales' AS measure_name, FORMAT(SUM(sales_amount), 'C', 'en-US') AS measure_value
FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', FORMAT(AVG(price),'C','en-US')
FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity Sold', FORMAT(SUM(quantity),'N0')
FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders', FORMAT(COUNT(DISTINCT order_number),'N0')
FROM gold.fact_sales
UNION ALL
SELECT 'Total Products', FORMAT(COUNT(product_key),'N0')
FROM gold.dim_products
UNION ALL
SELECT 'Product Variety Sold', FORMAT(COUNT(DISTINCT product_key),'N0')
FROM gold.fact_sales
UNION ALL
SELECT 'Total Customers', FORMAT(COUNT(customer_key),'N0')
FROM gold.dim_customers
UNION ALL
SELECT 'Customers with Orders', FORMAT(COUNT(DISTINCT customer_key),'N0')
FROM gold.fact_sales;


/* ============================
   6. Magnitude Analysis
============================ */

-- Customers by Country
SELECT
    country,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Customers by Gender
SELECT
    gender,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- Products by Category
SELECT
    category,
    COUNT(product_id) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;

-- Average Cost by Category
SELECT
    category,
    FORMAT(AVG(cost), 'C', 'en-US') AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY AVG(cost) DESC;

-- Revenue by Category
SELECT
    p.category,
    FORMAT(SUM(f.sales_amount), 'C', 'en-US') AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
    ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY SUM(f.sales_amount) DESC;

-- Revenue by Customer
SELECT
    c.firstname + ' ' + c.lastname AS customer,
    c.customer_key,
    FORMAT(SUM(f.sales_amount), 'C', 'en-US') AS total_revenue
FROM gold.fact_sales AS f  
LEFT JOIN gold.dim_customers AS c
    ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.firstname, c.lastname
ORDER BY SUM(f.sales_amount) DESC;

-- Distribution of Sold Items by Country
SELECT
    c.country,
    SUM(f.quantity) AS sold_items_distribution
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
    ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY SUM(f.quantity) DESC;


/* ============================
   7. Ranking Analysis
============================ */

-- Top 5 Products by Revenue
SELECT TOP 5
    p.product_name,
    FORMAT(SUM(f.sales_amount), 'C', 'en-US') AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
    ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY SUM(f.sales_amount) DESC;

-- Bottom 5 Products by Revenue
SELECT TOP 5
    p.product_name,
    FORMAT(SUM(f.sales_amount), 'C', 'en-US') AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
    ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY SUM(f.sales_amount) ASC;
