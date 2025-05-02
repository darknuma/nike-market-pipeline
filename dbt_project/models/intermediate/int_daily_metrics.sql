with campaign_performance as (
  select * from {{ ref('int_campaign_performance') }}
),

daily_metrics as (
  select
    date,
    sum(impressions) as daily_impressions,
    sum(clicks) as daily_clicks,
    sum(conversions) as daily_conversions,
    sum(revenue) as daily_revenue,
    sum(conversions) / nullif(sum(clicks), 0) as daily_conversion_rate,
    max(_loaded_at) as _loaded_at
  from campaign_performance
  group by 1
)

select * from daily_metrics
order by date