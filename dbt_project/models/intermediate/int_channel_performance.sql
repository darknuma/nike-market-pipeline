-- models/intermediate/int_channel_performance.sql
with campaign_data as (
  select * from {{ ref('stg_campaigns') }}
),

performance_data as (
  select * from {{ ref('int_campaign_performance') }}
),

channel_metrics as (
  select
    c.channel,
    sum(p.impressions) as impressions,
    sum(p.clicks) as clicks,
    sum(p.conversions) as conversions,
    sum(p.revenue) as revenue,
    sum(p.conversions) / nullif(sum(p.clicks), 0) as conversion_rate,
    max(p._loaded_at) as _loaded_at
  from campaign_data c
  join performance_data p on c.campaign_id = p.campaign_id
  group by 1
)

select * from channel_metrics