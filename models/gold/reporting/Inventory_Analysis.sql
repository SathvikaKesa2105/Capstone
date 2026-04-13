{{ config(materialized='view') }}

SELECT

    p.product_name,

    -- Inventory Value
    SUM(f.inventoryvalue) AS total_inventory_value,

    -- Turnover Metrics
    AVG(f.stockturnoverratio) AS turnover_ratio,

    CASE
        WHEN AVG(f.stockturnoverratio) >= 0.5 THEN 'Fast Moving'
        WHEN AVG(f.stockturnoverratio) >= 0.2 THEN 'Medium Moving'
        ELSE 'Slow Moving'
    END AS product_movement,

    -- Duplicate metric from View 3 (kept for compatibility)
    AVG(f.stockturnoverratio) AS avg_stock_turnover

FROM {{ ref('fact_Inventory') }} f

JOIN {{ ref('dim_product') }} p
    ON f.productkey = p.productkey

GROUP BY p.product_name

ORDER BY total_inventory_value DESC