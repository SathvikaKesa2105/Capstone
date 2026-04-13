{{ config(materialized='view') }}

WITH campaign_metrics AS (

    SELECT
        c.campaignkey,
        c.campaignname,
        c.campaigntype,
        SUM(f.new_customers_acquired) AS new_customers,
        AVG(f.repeat_purchase_rate) AS avg_repeat_purchase_rate,
        SUM(f.total_sales_influenced) AS total_sales_generated
    FROM {{ ref('fact_MarketingPerformance') }} f
    JOIN {{ ref('dim_MarketingCampaign') }} c
        ON f.campaignkey = c.campaignkey
    GROUP BY
        c.campaignkey,
        c.campaignname,
        c.campaigntype

),

campaign_type_metrics AS (

    SELECT
        c.campaigntype,
        AVG(f.roi_percentage) AS avg_roi_percentage
    FROM {{ ref('fact_MarketingPerformance') }} f
    JOIN {{ ref('dim_MarketingCampaign') }} c
        ON f.campaignkey = c.campaignkey
    GROUP BY c.campaigntype

)

SELECT

    cm.campaignname,
    cm.campaigntype,

    cm.new_customers,
    cm.avg_repeat_purchase_rate,
    cm.total_sales_generated,

    ctm.avg_roi_percentage

FROM campaign_metrics cm

LEFT JOIN campaign_type_metrics ctm
    ON cm.campaigntype = ctm.campaigntype

ORDER BY cm.new_customers DESC