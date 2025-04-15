{{
    config(
        materialized='table',
        unique_key=['campaign_id', 'date']
    )
}}

with campaign_performance as (
    select * from {{ ref('int_campaign_performance') }}
),

campaigns as (
    select * from {{ ref('stg_campaigns') }}
),

final as (
    select
        cp.campaign_id,
        c.name as campaign_name,
        c.channel,
        cp.date,
        cp.impressions,
        cp.clicks,
        cp.conversions,
        cp.revenue,
        -- Calculate metrics
        round(cp.clicks * 100.0 / nullif(cp.impressions, 0), 2) as ctr,
        round(cp.conversions * 100.0 / nullif(cp.clicks, 0), 2) as conversion_rate,
        round(cp.revenue / nullif(cp.conversions, 0), 2) as revenue_per_conversion,
        round(cp.revenue / nullif(cp.impressions, 0), 2) as revenue_per_impression,
        cp._loaded_at
    from campaign_performance cp
    left join campaigns c
        on cp.campaign_id = c.campaign_id
)

select * from final 