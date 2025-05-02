{{
    config(
        materialized='table',
        schema='marts_marketing'
    )
}}

with channel_performance as (
    select * from {{ ref('int_channel_performance') }}
),

channel_metrics_by_date as (
    select
        cp.channel,
        date_trunc('month', p.date) as month,
        sum(p.impressions) as monthly_impressions,
        sum(p.clicks) as monthly_clicks,
        sum(p.conversions) as monthly_conversions,
        sum(p.revenue) as monthly_revenue,
        sum(p.clicks) / nullif(sum(p.impressions), 0) as monthly_ctr,
        sum(p.conversions) / nullif(sum(p.clicks), 0) as monthly_conversion_rate,
        sum(p.revenue) / nullif(sum(p.conversions), 0) as monthly_aov,
        sum(p.revenue) / nullif(sum(p.clicks), 0) as monthly_rpc,
        max(p._loaded_at) as _loaded_at
    from {{ ref('stg_campaigns') }} cp
    join {{ ref('int_campaign_performance') }} p on cp.campaign_id = p.campaign_id
    group by 1, 2
),

channel_summary as (
    select
        channel,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(conversions) as total_conversions,
        sum(revenue) as total_revenue,
        sum(clicks) / nullif(sum(impressions), 0) as overall_ctr,
        sum(conversions) / nullif(sum(clicks), 0) as overall_conversion_rate,
        sum(revenue) / nullif(sum(clicks), 0) as overall_revenue_per_click,
        sum(revenue) / nullif(sum(impressions), 0) * 1000 as overall_rpm,
        max(_loaded_at) as _loaded_at
    from channel_performance
    group by 1
)

select
    s.channel,
    s.total_impressions,
    s.total_clicks,
    s.total_conversions,
    s.total_revenue,
    s.overall_ctr,
    s.overall_conversion_rate,
    s.overall_revenue_per_click,
    s.overall_rpm,
    array_agg(struct(
        m.month,
        m.monthly_impressions,
        m.monthly_clicks,
        m.monthly_conversions,
        m.monthly_revenue,
        m.monthly_ctr,
        m.monthly_conversion_rate,
        m.monthly_aov,
        m.monthly_rpc
    ) order by m.month) as monthly_metrics,
    s._loaded_at
from channel_summary s
left join channel_metrics_by_date m using (channel)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10