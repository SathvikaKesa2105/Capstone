{{ config(materialized='view') }}
 
SELECT
 
e.role,
 
SUM(f.total_sales_amount) AS total_sales,
 
COUNT(DISTINCT f.order_id) AS total_orders
 
FROM {{ ref('fact_Sales') }} f
 
JOIN {{ ref('dim_employee') }} e
ON f.employeekey = e.employeekey
 
GROUP BY e.role
 
ORDER BY total_sales DESC