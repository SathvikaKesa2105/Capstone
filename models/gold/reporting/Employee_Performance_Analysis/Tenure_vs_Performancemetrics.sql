{{ config(materialized='view') }}
 
SELECT
 
e.full_name AS employee_name,
 
DATEDIFF(year, e.HireDate, CURRENT_DATE) AS tenure_years,
 
SUM(f.total_sales_amount) AS total_sales,
 
COUNT(DISTINCT f.order_id) AS total_orders
 
FROM {{ ref('fact_Sales') }} f
 
JOIN {{ ref('dim_employee') }} e
ON f.employeekey = e.employeekey
 
GROUP BY
e.full_name,
e.HireDate
 
ORDER BY total_sales DESC