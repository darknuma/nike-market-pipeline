{{
    config(
        materialized='table',
        schema='marts_marketing'
    )
}}

with campaign_data as (
    select * from {{ ref('stg_campaigns') }}
),

performance_data as (
    select * from {{ ref('int_campaign_performance') }}
),

campaign_metrics as (
    select
        c.campaign_id,
        c.campaign_name,
        c.channel,
        c.platform,
        c.start_date,
        c.end_date,
        sum(p.impressions) as total_impressions,
        sum(p.clicks) as total_clicks,
        sum(p.conversions) as total_conversions,
        sum(p.revenue) as total_revenue,
        sum(p.clicks) / nullif(sum(p.impressions), 0) as ctr,
        sum(p.conversions) / nullif(sum(p.clicks), 0) as conversion_rate,
        sum(p.revenue) / nullif(sum(p.conversions), 0) as average_order_value,
        sum(p.revenue) / nullif(sum(p.clicks), 0) as revenue_per_click,
        sum(p.revenue) / nullif(sum(p.impressions), 0) * 1000 as rpm,
        max(p._loaded_at) as _loaded_at
    from campaign_data c
    left join performance_data p on c.campaign_id = p.campaign_id
    group by 1, 2, 3, 4, 5, 6
)

select * from campaign_metrics