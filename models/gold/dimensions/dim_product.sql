SELECT

{{dbt_utils.generate_surrogate_key(['product_id']) }} AS productkey,
product_id,

product_name,
category,
subcategory,
brand,

color,
size,

unit_price,
cost_price,
stock_quantity,
reorder_level,
supplier_id
 
FROM {{ ref('silver_products') }}