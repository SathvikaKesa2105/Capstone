{% snapshot snp_employee_data %}
 
{{
config(
target_database='DWH',
target_schema='Bronze',
unique_key='employee_id',
strategy='timestamp',
updated_at='last_modified_date'
)
}}
 
SELECT *
FROM {{ ref('bronze_employee') }}
 
QUALIFY ROW_NUMBER() OVER(
PARTITION BY employee_id
ORDER BY last_modified_date DESC
)=1
 
{% endsnapshot %}