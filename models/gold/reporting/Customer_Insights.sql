{{ config(materialized='view') }}

WITH customer_metrics AS (

    SELECT
        c.customerkey,
        c.full_name,
        c.customer_segment,
        SUM(f.total_sales_amount) AS customer_lifetime_value,
        COUNT(DISTINCT f.order_id) AS total_orders
    FROM {{ ref('fact_Sales') }} f
    JOIN {{ ref('dim_customer') }} c
        ON f.customerkey = c.customerkey
    GROUP BY
        c.customerkey,
        c.full_name,
        c.customer_segment

),

segment_metrics AS (

    SELECT
        c.customer_segment,
        COUNT(DISTINCT f.customerkey) AS total_customers,
        SUM(f.total_sales_amount) AS total_sales,
        AVG(f.total_sales_amount) AS avg_spend_per_order
    FROM {{ ref('fact_Sales') }} f
    JOIN {{ ref('dim_customer') }} c
        ON f.customerkey = c.customerkey
    GROUP BY c.customer_segment

),

repeat_purchase AS (

    WITH customer_orders AS (
        SELECT
            customerkey,
            COUNT(DISTINCT order_id) AS order_count
        FROM {{ ref('fact_Sales') }}
        GROUP BY customerkey
    )

    SELECT
        COUNT(CASE WHEN order_count > 1 THEN 1 END) * 100.0
        / COUNT(*) AS repeat_purchase_rate
    FROM customer_orders

)

SELECT

    cm.customerkey,
    cm.full_name,
    cm.customer_segment,
    cm.customer_lifetime_value,
    cm.total_orders,

    sm.total_customers,
    sm.total_sales,
    sm.avg_spend_per_order,

    rp.repeat_purchase_rate

FROM customer_metrics cm
LEFT JOIN segment_metrics sm
    ON cm.customer_segment = sm.customer_segment
CROSS JOIN repeat_purchase rp

ORDER BY cm.customer_lifetime_value DESC