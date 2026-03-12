{% snapshot snp_customer_data %}
 
{{
config(
target_database='DWH',
target_schema='Bronze',
unique_key='customer_id',
strategy='timestamp',
updated_at='last_modified_date'
)
}}
 
SELECT *
FROM {{ ref('bronze_customer') }} 
 
QUALIFY ROW_NUMBER() OVER(
PARTITION BY customer_id
ORDER BY last_modified_date DESC
)=1
 
{% endsnapshot %}
 