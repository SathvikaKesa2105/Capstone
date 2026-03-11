{% snapshot snp_product_data %}
 
{{
config(
target_database='DWH',
target_schema='Bronze',
unique_key='product_id',
strategy='timestamp',
updated_at='last_modified_date'
)
}}
 
SELECT *
FROM {{ ref('bronze_products') }}
 
QUALIFY ROW_NUMBER() OVER(
PARTITION BY product_id
ORDER BY last_modified_date DESC
)=1
 
{% endsnapshot %}



