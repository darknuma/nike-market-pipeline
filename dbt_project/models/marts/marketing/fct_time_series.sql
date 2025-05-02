{{
    config(
        materialized='table',
        schema='marts_marketing'
    )
}}

with daily_metrics as (
    select
        date,
        sum(impressions) as daily_impressions,
        sum(clicks) as daily_clicks,
        sum(conversions) as daily_conversions,
        sum(revenue) as daily_revenue,
        max(_loaded_at) as _loaded_at
    from {{ ref('int_campaign_performance') }}
    group by 1
),

weekly_metrics as (
    select
        date_trunc('week', date) as week,
        sum(daily_impressions) as weekly_impressions,
        sum(daily_clicks) as weekly_clicks,
        sum(daily_conversions) as weekly_conversions,
        sum(daily_revenue) as weekly_revenue,
        sum(daily_clicks) / nullif(sum(daily_impressions), 0) as weekly_ctr,
        sum(daily_conversions) / nullif(sum(daily_clicks), 0) as weekly_cvr,
        sum(daily_revenue) / nullif(sum(daily_clicks), 0) as weekly_rpc,
        max(_loaded_at) as _loaded_at
    from daily_metrics
    group by 1
),

monthly_metrics as (
    select
        date_trunc('month', date) as month,
        sum(daily_impressions) as monthly_impressions,
        sum(daily_clicks) as monthly_clicks,
        sum(daily_conversions) as monthly_conversions,
        sum(daily_revenue) as monthly_revenue,
        sum(daily_clicks) / nullif(sum(daily_impressions), 0) as monthly_ctr,
        sum(daily_conversions) / nullif(sum(daily_clicks), 0) as monthly_cvr,
        sum(daily_revenue) / nullif(sum(daily_clicks), 0) as monthly_rpc,
        max(_loaded_at) as _loaded_at
    from daily_metrics
    group by 1
),

daily_moving_avg as (
    select
        date,
        daily_impressions,
        daily_clicks,
        daily_conversions,
        daily_revenue,
        avg(daily_impressions) over (order by date rows between 6 preceding and current row) as impressions_7d_avg,
        avg(daily_clicks) over (order by date rows between 6 preceding and current row) as clicks_7d_avg,
        avg(daily_conversions) over (order by date rows between 6 preceding and current row) as conversions_7d_avg,
        avg(daily_revenue) over (order by date rows between 6 preceding and current row) as revenue_7d_avg,
        avg(daily_impressions) over (order by date rows between 29 preceding and current row) as impressions_30d_avg,
        avg(daily_clicks) over (order by date rows between 29 preceding and current row) as clicks_30d_avg,
        avg(daily_conversions) over (order by date rows between 29 preceding and current row) as conversions_30d_avg,
        avg(daily_revenue) over (order by date rows between 29 preceding and current row) as revenue_30d_avg,
        _loaded_at
    from daily_metrics
)

select
    d.date,
    d.daily_impressions,
    d.daily_clicks,
    d.daily_conversions,
    d.daily_revenue,
    d.impressions_7d_avg,
    d.clicks_7d_avg,
    d.conversions_7d_avg,
    d.revenue_7d_avg,
    d.impressions_30d_avg,
    d.clicks_30d_avg,
    d.conversions_30d_avg,
    d.revenue_30d_avg,
    w.week,
    w.weekly_impressions,
    w.weekly_clicks,
    w.weekly_conversions,
    w.weekly_revenue,
    w.weekly_ctr,
    w.weekly_cvr,
    w.weekly_rpc,
    m.month,
    m.monthly_impressions,
    m.monthly_clicks,
    m.monthly_conversions,
    m.monthly_revenue,
    m.monthly_ctr,
    m.monthly_cvr,
    m.monthly_rpc,
    greatest(d._loaded_at, w._loaded_at, m._loaded_at) as _loaded_at
from daily_moving_avg d
left join weekly_metrics w on date_trunc('week', d.date) = w.week
left join monthly_metrics m on date_trunc('month', d.date) = m.month
order by d.date