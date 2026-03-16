{{ config(materialized='view') }}
 
SELECT
 
c.campaignname,
 
SUM(f.total_sales_influenced) AS total_sales_generated
 
FROM {{ ref('fact_MarketingPerformance') }} f
 
JOIN {{ ref('dim_MarketingCampaign') }} c
ON f.campaignkey = c.campaignkey
 
GROUP BY c.campaignname
 
ORDER BY total_sales_generated DESC