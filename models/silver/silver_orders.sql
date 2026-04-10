SELECT DISTINCT
 
    TRIM(order_id) AS order_id,
    TRIM(customer_id) AS customer_id,
    TRIM(product_id) AS product_id,
    TRIM(employee_id) AS employee_id,
    TRIM(store_id) AS store_id,
    TRY_TO_NUMBER(quantity) AS quantity,
    TRY_TO_NUMBER(unit_price) AS unit_price,
    TRY_TO_NUMBER(cost_price) AS cost_price,
    TRY_TO_NUMBER(item_discount) AS discount_amount,
 
    TRY_TO_TIMESTAMP(order_date) AS order_date,
    TRY_TO_DATE(delivery_date) AS delivery_date,
    TRY_TO_DATE(estimated_delivery_date) AS estimated_delivery_date,
    TRY_TO_DATE(shipping_date) AS shipping_date,
 
    TRY_TO_NUMBER(tax_amount) AS tax_amount,
    TRY_TO_NUMBER(shipping_cost) AS shipping_cost,
    TRY_TO_NUMBER(total_amount) AS total_amount,

    quantity * unit_price AS item_total_amount,
    quantity * cost_price AS item_cost
 
FROM {{ ref('bronze_orders') }}