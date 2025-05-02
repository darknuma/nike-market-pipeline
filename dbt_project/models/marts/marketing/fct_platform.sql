{{
    config(
        materialized='table',
        schema='marts_marketing'
    )
}}

with platform_performance as (
    select * from {{ ref('int_platform_performance') }}
),

platform_metrics_by_date as (
    select
        cp.platform,
        date_trunc('month', p.date) as month,
        sum(p.impressions) as monthly_impressions,
        sum(p.clicks) as monthly_clicks,
        sum(p.conversions) as monthly_conversions,
        sum(p.revenue) as monthly_revenue,
        sum(p.clicks) / nullif(sum(p.impressions), 0) as monthly_ctr,
        sum(p.conversions) / nullif(sum(p.clicks), 0) as monthly_conversion_rate,
        sum(p.revenue) / nullif(sum(p.clicks), 0) as monthly_rpc,
        max(p._loaded_at) as _loaded_at
    from {{ ref('stg_campaigns') }} cp
    join {{ ref('int_campaign_performance') }} p on cp.campaign_id = p.campaign_id
    group by 1, 2
),

platform_metrics_by_channel as (
    select
        cp.platform,
        cp.channel,
        sum(p.impressions) as impressions,
        sum(p.clicks) as clicks,
        sum(p.conversions) as conversions,
        sum(p.revenue) as revenue,
        sum(p.clicks) / nullif(sum(p.impressions), 0) as ctr,
        sum(p.conversions) / nullif(sum(p.clicks), 0) as conversion_rate,
        sum(p.revenue) / nullif(sum(p.clicks), 0) as revenue_per_click,
        max(p._loaded_at) as _loaded_at
    from {{ ref('stg_campaigns') }} cp
    join {{ ref('int_campaign_performance') }} p on cp.campaign_id = p.campaign_id
    group by 1, 2
),

platform_summary as (
    select
        platform,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(conversions) as total_conversions,
        sum(revenue) as total_revenue,
        sum(clicks) / nullif(sum(impressions), 0) as overall_ctr,
        sum(conversions) / nullif(sum(clicks), 0) as overall_conversion_rate,
        sum(revenue) / nullif(sum(clicks), 0) as overall_revenue_per_click,
        sum(revenue) / nullif(sum(impressions), 0) * 1000 as overall_rpm,
        max(_loaded_at) as _loaded_at
    from platform_performance
    group by 1
)

select
    s.platform,
    s.total_impressions,
    s.total_clicks,
    s.total_conversions,
    s.total_revenue,
    s.overall_ctr,
    s.overall_conversion_rate,
    s.overall_revenue_per_click,
    s.overall_rpm,
    array_agg(distinct struct(
        c.channel,
        c.impressions,
        c.clicks,
        c.conversions,
        c.revenue,
        c.ctr,
        c.conversion_rate,
        c.revenue_per_click
    )) as channel_metrics,
    array_agg(distinct struct(
        m.month,
        m.monthly_impressions,
        m.monthly_clicks,
        m.monthly_conversions,
        m.monthly_revenue,
        m.monthly_ctr,
        m.monthly_conversion_rate,
        m.monthly_rpc
    ) order by m.month) as monthly_metrics,
    s._loaded_at
from platform_summary s
left join platform_metrics_by_channel c using (platform)
left join platform_metrics_by_date m using (platform)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10