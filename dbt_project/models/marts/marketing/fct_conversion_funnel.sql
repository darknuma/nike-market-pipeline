{{
    config(
        materialized='table',
        schema='marts_marketing'
    )
}}

with campaign_performance as (
    select * from {{ ref('int_campaign_performance') }}
),

campaign_data as (
    select * from {{ ref('stg_campaigns') }}
),

funnel_overall as (
    select
        'All Campaigns' as segment,
        sum(p.impressions) as impressions,
        sum(p.clicks) as clicks,
        sum(p.conversions) as conversions,
        sum(p.revenue) as revenue,
        sum(p.clicks) / nullif(sum(p.impressions), 0) as impression_to_click_rate,
        sum(p.conversions) / nullif(sum(p.clicks), 0) as click_to_conversion_rate,
        sum(p.conversions) / nullif(sum(p.impressions), 0) as impression_to_conversion_rate,
        max(p._loaded_at) as _loaded_at
    from campaign_performance p
),

funnel_by_channel as (
    select
        'Channel: ' || c.channel as segment,
        sum(p.impressions) as impressions,
        sum(p.clicks) as clicks,
        sum(p.conversions) as conversions,
        sum(p.revenue) as revenue,
        sum(p.clicks) / nullif(sum(p.impressions), 0) as impression_to_click_rate,
        sum(p.conversions) / nullif(sum(p.clicks), 0) as click_to_conversion_rate,
        sum(p.conversions) / nullif(sum(p.impressions), 0) as impression_to_conversion_rate,
        max(p._loaded_at) as _loaded_at
    from campaign_performance p
    join campaign_data c on p.campaign_id = c.campaign_id
    group by 1
),

funnel_by_platform as (
    select
        'Platform: ' || c.platform as segment,
        sum(p.impressions) as impressions,
        sum(p.clicks) as clicks,
        sum(p.conversions) as conversions,
        sum(p.revenue) as revenue,
        sum(p.clicks) / nullif(sum(p.impressions), 0) as impression_to_click_rate,
        sum(p.conversions) / nullif(sum(p.clicks), 0) as click_to_conversion_rate,
        sum(p.conversions) / nullif(sum(p.impressions), 0) as impression_to_conversion_rate,
        max(p._loaded_at) as _loaded_at
    from campaign_performance p
    join campaign_data c on p.campaign_id = c.campaign_id
    group by 1
)

select * from funnel_overall
union all
select * from funnel_by_channel
union all
select * from funnel_by_platform