{{ config(materialized='view') }}

WITH segment_metrics AS (

    SELECT
        c.customer_segment,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.total_sales_amount) AS total_spent,
        AVG(f.total_sales_amount) AS avg_order_value
    FROM {{ ref('fact_Sales') }} f
    JOIN {{ ref('dim_customer') }} c
        ON f.customerkey = c.customerkey
    GROUP BY c.customer_segment

),

time_region_metrics AS (

    SELECT
        d.year,
        d.month,
        s.region,
        SUM(f.total_sales_amount) AS total_sales
    FROM {{ ref('fact_Sales') }} f
    JOIN {{ ref('dim_store') }} s
        ON f.storekey = s.storekey
    JOIN {{ ref('dim_date') }} d
        ON f.datekey = d.datekey
    GROUP BY
        d.year,
        d.month,
        s.region

),

top_products AS (

    SELECT
        p.product_name,
        SUM(f.quantity_sold) AS total_units_sold,
        SUM(f.total_sales_amount) AS total_sales
    FROM {{ ref('fact_Sales') }} f
    JOIN {{ ref('dim_product') }} p
        ON f.productkey = p.productkey
    GROUP BY p.product_name
    ORDER BY total_units_sold DESC
    LIMIT 10

),

category_metrics AS (

    SELECT
        p.category,
        SUM(f.total_sales_amount) AS total_sales_by_category
    FROM {{ ref('fact_Sales') }} f
    JOIN {{ ref('dim_product') }} p
        ON f.productkey = p.productkey
    GROUP BY p.category

)

SELECT

    sm.customer_segment,
    sm.total_orders,
    sm.total_spent,
    sm.avg_order_value,

    trm.year,
    trm.month,
    trm.region,
    trm.total_sales AS time_region_sales,

    tp.product_name,
    tp.total_units_sold,
    tp.total_sales AS product_sales,

    cm.category,
    cm.total_sales_by_category

FROM segment_metrics sm

CROSS JOIN time_region_metrics trm
CROSS JOIN top_products tp
CROSS JOIN category_metrics cm