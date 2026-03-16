
SELECT
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} AS campaignkey,
    campaign_id AS campaignid,
    campaign_type AS campaigntype,
    campaign_name AS campaignname,
    audience_segment AS target_audience_segment,
    budget,
    campaign_duration_days AS duration,
    expected_roi AS roi,
    start_date,
    end_date
FROM {{ ref('silver_campaign') }}