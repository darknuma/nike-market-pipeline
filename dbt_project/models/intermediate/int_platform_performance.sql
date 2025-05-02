{{
    config(
        materialized='view'
    )
}}

with campaign_data as (
    select * from {{ ref('stg_campaigns') }}
),

performance_data as (
    select * from {{ ref('int_campaign_performance') }}
),

platform_metrics as (
    select
        c.platform,
        sum(p.impressions) as impressions,
        sum(p.clicks) as clicks,
        sum(p.conversions) as conversions,
        sum(p.revenue) as revenue,
        sum(p.clicks) / nullif(sum(p.impressions), 0) as click_through_rate,
        sum(p.conversions) / nullif(sum(p.clicks), 0) as conversion_rate,
        sum(p.revenue) / nullif(sum(p.conversions), 0) as average_order_value,
        sum(p.revenue) / nullif(sum(p.clicks), 0) as revenue_per_click,
        max(p._loaded_at) as _loaded_at
    from campaign_data c
    join performance_data p on c.campaign_id = p.campaign_id
    group by 1
)

select * from platform_metrics