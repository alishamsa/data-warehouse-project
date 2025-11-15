/* ============================================================================
📊 Data Analytics Project (SQL End-to-End Analysis)

Project Description:

This Data Analyst project is built on top of a complete Data Warehouse end-to-end 
pipeline to demonstrate how real business insights can be generated directly from a 
well-structured dimensional model using SQL.

The project uses fact and dimension tables to perform advanced analytical techniques, 
including:

- Change-Over-Time Trends (yearly, monthly, seasonality)
- Cumulative Analysis (running totals, moving averages)
- Performance Analysis (YoY comparison, variance vs. average)
- Part-to-Whole Analysis (category contribution)
- Data Segmentation (product grouping, customer segmentation)
- Final Customer Report View (combining demographics, behavior, KPIs, recency, 
  segmentation, and customer activity status)

This project demonstrates how SQL alone can be used to produce professional business 
insights—similar to the work of a Data Analyst or BI Analyst in real organizations.
=============================================================================== */


 USE DataWarehouse ;
 
-- =============================================================================== 
-- change-over-time trends
-- =============================================================================== 


-- Analyze how a measure evolves over time.
-- helps track trends and identify seasionality in your data.
-- formula: ∑ [measure] by [date dimension]
-- Insight:snapshots,How did this period perform?


-- analyze sales performance over time
-- ---------------------------------------------------------------------------------

-- over years


SELECT 
    YEAR(order_date) order_year,
    FORMAT(SUM(sales_amount), 'C', 'en-US') AS total_sales,
    COUNT(distinct customer_key) as total_customers,
    SUM(quantity)            AS total_quantity
FROM gold.fact_sales
GROUP BY YEAR(order_date)
ORDER BY order_year ASC
;

-- over month

SELECT 
    FORMAT(order_date, 'MMM') AS order_month,
    FORMAT(SUM(sales_amount), 'C', 'en-US') AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity,
    ROW_NUMBER() OVER (ORDER BY SUM(sales_amount) DESC) AS sales_rank
FROM gold.fact_sales
GROUP BY MONTH(order_date), FORMAT(order_date, 'MMM')
ORDER BY MONTH(order_date);

-- =============================================================================== 
-- cumulative analysis
-- =============================================================================== 


-- aggregating data progressively over time
-- helps to understand whether our business growing or declining
-- formula: ∑ [cumulative measure] by [date dimension]
-- example: running total sales by year , moving average by month
-- insight:story/progression,how  business is growing?

-- calculate running total sales and moving average price

SELECT 
    YEAR(order_date) AS order_year,
    FORMAT(SUM(sales_amount), 'C', 'en-US') AS total_sales,
    FORMAT(
        SUM(SUM(sales_amount)) OVER (ORDER BY YEAR(order_date)), 'C', 'en-US'
         ) AS running_total_sales,
    FORMAT(AVG(price),'C', 'en-US') as avg_price,
    FORMAT(
        AVG(AVG(price)) OVER (ORDER BY YEAR(order_date)), 'C', 'en-US'
        ) AS moving_avg_price
FROM gold.fact_sales
GROUP BY YEAR(order_date)
ORDER BY order_year ASC
;

/*
Second SUM() OVER(...)This is a window function that runs after the GROUP BY.
Default frame for cumulative functions once you add 'order by' to 'over()'is
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
*/

-- =============================================================================== 
-- performance analysis
-- =============================================================================== 

-- is the process of comparing current value to a target value.
-- helps to measure success and compare performance.
-- formula: current[measure] - target[measure]
-- example: current sales - average sales , currect year sales - previous year sales (yoy) ,current sales - lowest sales


-- task: analyze the yearly performance of products by comparing each product's sales to both is avg sales and previous year's sales.
-- --------------------------------------------------------------------------------------------------
-- requairement: two dimentions(date,product name) and one measure (sales).


WITH cte_yearly_pro_sales AS (
    SELECT
        d.product_name AS Product,
        YEAR(f.order_date) AS Order_Year,
        SUM(ISNULL(f.sales_amount, 0)) AS Sales
    FROM gold.fact_sales AS f
    LEFT JOIN gold.dim_products AS d
        ON f.product_key = d.product_key
    GROUP BY d.product_name, YEAR(f.order_date)
),
cte_cur_prev_avg_sales AS (
    SELECT 
        Product,
        Order_Year,
        Sales,
        LAG(Sales) OVER (PARTITION BY Product ORDER BY Order_Year) AS Prev_Sales,
        AVG(Sales) OVER (PARTITION BY Product) AS Avg_Sales
    FROM cte_yearly_pro_sales
)
SELECT 
    Order_Year                           AS [Year],
    Product                              AS [Product],
    FORMAT(Sales, 'C', 'en-US')          AS [Total Sales],
    FORMAT(Prev_Sales, 'C', 'en-US')     AS [Previous Sales],

    -- Year-over-Year Change
    FORMAT(Sales - Prev_Sales, 'C', 'en-US') AS [YoY Change],
    CASE 
        WHEN Sales - Prev_Sales > 0 THEN N'🟢'   -- Increase
        WHEN Sales - Prev_Sales < 0 THEN N'🔴'   -- Decline
        ELSE N'🟡'                              -- No Change
    END AS [YoY Trend],

    -- Product Average Comparison
    FORMAT(Avg_Sales, 'C', 'en-US')      AS [Average Sales],
    FORMAT(Sales - Avg_Sales, 'C', 'en-US') AS [Variance vs. Average],
    CASE 
        WHEN Sales - Avg_Sales > 0 THEN N'🟢'   -- Above Average
        WHEN Sales - Avg_Sales < 0 THEN N'🔴'   -- Below Average
        ELSE N'🟡'                              -- On Target
    END AS [Performance Trend]

FROM cte_cur_prev_avg_sales
ORDER BY Product, Order_Year
;
-- N'...' → tells SQL Server this is Unicode (nvarchar needed for emoji).

-- =============================================================================== 
-- part to whole analysis
-- =============================================================================== 

-- analyze how an individual category is contributing to the overall. 
-- helps to understand what is the most impacting category to the overall business.
-- insight:helps to understand the importance of each category.
-- formula: ([measure] / total[measure] * 100 by dimension)
-- example: (sales / total sales) * 100 by country


-- task: which categories contribute the most to overall sales
-- ---------------------------------------------------------------------------------

WITH cte_sales_by_cat AS (
    SELECT 
        d.category,
        SUM(f.sales_amount) AS total_sales
    FROM gold.fact_sales AS f
    LEFT JOIN gold.dim_products AS d
        ON f.product_key = d.product_key
    GROUP BY d.category
)
SELECT 
    category AS Category,
    FORMAT(total_sales, 'C', 'en-US') AS Total_Sales,
    FORMAT(SUM(total_sales) OVER(), 'C', 'en-US') AS Grand_Total,
    FORMAT((total_sales / SUM(total_sales) OVER()) * 100, 'N2') + '%' AS Sales_Percentage
FROM cte_sales_by_cat
;



/*

I multiplied by **`1.0`** to **force decimal (floating-point) division** 
instead of integer division.

Here’s why:

* If both `total_sales` and `SUM(total_sales) OVER()` are integers,
 SQL Server (or similar engines) will do **integer division**,
  which truncates the decimal part.

   Example: `5 / 10 = 0` (not `0.5`).

* By multiplying one side with `1.0`, SQL converts the operation to **decimal/float math**,
 giving the correct fraction.

   Example: `(5 * 1.0) / 10 = 0.5`.
*/


-- =============================================================================== 
-- Data Segmentation 
-- =============================================================================== 

-- group the data based on a specific range.
-- helps understand the correlation between two measures.by converting one measure to dimention.
-- formula: [measure] by [measure]
-- example: total number of product by sales range , total number of customers by age group


-- task: segment products into cost ranges and count how many products fall into each segment
--requirment: two measures

WITH cte_product_segments AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)

SELECT 
    cost_range,
    COUNT(product_key) AS total_products
FROM cte_product_segments
GROUP BY cost_range
ORDER BY total_products DESC
;


-- task: group customers into three segments based on thier spending brhavior.

/*

— this is a **customer segmentation** task based on two conditions:

* **Customer lifespan (months of history)** 
* **Total spending**


Logic

We have **three customer groups**:

| Segment     | Condition                                           | Description                  |
| ----------- | --------------------------------------------------- | ---------------------------- |
| **VIP**     | `months_of_history >= 12` AND `total_spent > 5000`  | Long-term, high spenders     |
| **Regular** | `months_of_history >= 12` AND `total_spent <= 5000` | Long-term, moderate spenders |
| **New**     | `months_of_history < 12`                            | New customers                |

* convert date ==> measure ==> dimention 
*/

WITH cte_customer_spending AS (
    SELECT
        customer_key,
        MIN(order_date) AS first_purchase_date,
        MAX(order_date) AS last_purchase_date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS customer_lifespan,
        SUM(sales_amount) AS total_spending
    FROM gold.fact_sales
    GROUP BY customer_key
),
cte_customer_segments AS (
    SELECT
        customer_key,
        CASE
            WHEN customer_lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN customer_lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment,
        customer_lifespan,
        total_spending
    FROM cte_customer_spending
)
SELECT
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM cte_customer_segments
GROUP BY customer_segment
ORDER BY total_customers DESC
;


/*
-- =============================================================================== 
Customer Report
-- =============================================================================== 
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/

IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE VIEW gold.report_customers AS

WITH cte_base_query AS (
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        d.customer_key,
        d.customer_number,
        CONCAT(d.firstname, ' ', d.lastname) AS customer_name,
        DATEDIFF(YEAR, d.birthdate, GETDATE()) AS age
    FROM gold.fact_sales AS f  
    LEFT JOIN gold.dim_customers AS d  
        ON f.customer_key = d.customer_key
),
cte_aggregated AS (


    SELECT
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan_months,
        DATEDIFF(MONTH, MAX(order_date), GETDATE()) AS recency_months
    FROM cte_base_query
    GROUP BY customer_key, customer_number, customer_name, age
)
SELECT
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE
        WHEN age < 18 THEN 'Under 18'
        WHEN age BETWEEN 18 AND 25 THEN '18-25'
        WHEN age BETWEEN 26 AND 35 THEN '26-35'
        WHEN age BETWEEN 36 AND 45 THEN '36-45'
        WHEN age BETWEEN 46 AND 60 THEN '46-60'
        ELSE '60+'
    END AS age_group
    ,
    CASE
            WHEN lifespan_months >= 12 AND total_sales > 5000 THEN 'VIP'
            WHEN lifespan_months >= 12 AND total_sales <= 5000 THEN 'Regular'
            ELSE 'New'
    END AS customer_segment,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    last_order_date,
    lifespan_months,
    recency_months,
    case 
        WHEN recency_months <= 1 THEN 'Active'
        WHEN recency_months BETWEEN 2 AND 6 THEN 'At Risk'
        ELSE 'Inactive'
    END AS customer_activity_status,
    -- Compuate average order value (AVO)
    CASE 
        WHEN total_sales = 0 OR total_orders = 0 THEN 0
        ELSE total_sales  / total_orders
    END AS avg_order_value,
    -- Compuate average monthly spend
    CASE 
        WHEN lifespan_months = 0 THEN total_sales
        ELSE total_sales * 1.0 / lifespan_months
    END AS avg_monthly_spend

FROM cte_aggregated
;


