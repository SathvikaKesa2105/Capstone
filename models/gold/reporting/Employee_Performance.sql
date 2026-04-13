{{ config(materialized='view') }}

WITH role_metrics AS (

    SELECT
        e.role,
        SUM(f.total_sales_amount) AS total_sales_by_role,
        COUNT(DISTINCT f.order_id) AS total_orders_by_role
    FROM {{ ref('fact_Sales') }} f
    JOIN {{ ref('dim_employee') }} e
        ON f.employeekey = e.employeekey
    GROUP BY e.role

),

employee_metrics AS (

    SELECT
        e.employeekey,
        e.full_name AS employee_name,
        e.role,
        DATEDIFF(year, e.HireDate, CURRENT_DATE) AS tenure_years,
        SUM(f.total_sales_amount) AS total_sales,
        COUNT(DISTINCT f.order_id) AS total_orders
    FROM {{ ref('fact_Sales') }} f
    JOIN {{ ref('dim_employee') }} e
        ON f.employeekey = e.employeekey
    GROUP BY
        e.employeekey,
        e.full_name,
        e.role,
        e.HireDate

),

employee_region_metrics AS (

    SELECT
        e.employeekey,
        e.full_name AS employee_name,
        s.region,
        SUM(f.total_sales_amount) AS total_sales_by_region,
        COUNT(DISTINCT f.order_id) AS total_orders_by_region
    FROM {{ ref('fact_Sales') }} f
    JOIN {{ ref('dim_employee') }} e
        ON f.employeekey = e.employeekey
    JOIN {{ ref('dim_store') }} s
        ON f.storekey = s.storekey
    GROUP BY
        e.employeekey,
        e.full_name,
        s.region

)

SELECT

    em.employee_name,
    em.role,
    em.tenure_years,

    em.total_sales AS employee_total_sales,
    em.total_orders AS employee_total_orders,

    rm.total_sales_by_role,
    rm.total_orders_by_role,

    erm.region,
    erm.total_sales_by_region,
    erm.total_orders_by_region

FROM employee_metrics em

LEFT JOIN role_metrics rm
    ON em.role = rm.role

LEFT JOIN employee_region_metrics erm
    ON em.employeekey = erm.employeekey

ORDER BY em.total_sales DESC