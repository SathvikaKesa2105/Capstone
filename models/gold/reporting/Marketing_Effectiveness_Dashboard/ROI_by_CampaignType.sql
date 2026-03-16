{{ config(materialized='view') }}
 
SELECT
 
c.campaigntype,
 
AVG(f.roi_percentage) AS avg_roi_percentage
 
FROM {{ ref('fact_MarketingPerformance') }} f
 
JOIN {{ ref('dim_MarketingCampaign') }} c
ON f.campaignkey = c.campaignkey
 
GROUP BY c.campaigntype
 
ORDER BY avg_roi_percentage DESC