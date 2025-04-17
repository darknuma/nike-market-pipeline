{{
    config(
        materialized='incremental',
        unique_key=['campaign_id', 'date'],
        merge_update_columns=['impressions', 'clicks', 'conversions', 'revenue', '_loaded_at']
    )
}}

with ad_events as (
    select * from {{ ref('stg_ad_events') }}
),

conversions as (
    select * from {{ ref('stg_conversions') }}
),

daily_events as (
    select
        campaign_id,
        date(timestamp) as date,
        count(*) as total_events,
        countif(event_type = 'impression') as impressions,
        countif(event_type = 'click') as clicks,
        max(_loaded_at) as _loaded_at
    from ad_events
    group by 1, 2
),

daily_conversions as (
    select
        campaign_id,
        date(timestamp) as date,
        count(*) as conversions,
        sum(revenue) as revenue,
        max(_loaded_at) as _loaded_at
    from conversions
    group by 1, 2
)

select
    e.campaign_id,
    e.date,
    e.impressions,
    e.clicks,
    coalesce(c.conversions, 0) as conversions,
    coalesce(c.revenue, 0) as revenue,
    greatest(e._loaded_at, coalesce(c._loaded_at, e._loaded_at)) as _loaded_at
from daily_events e
left join daily_conversions c
    on e.campaign_id = c.campaign_id
    and e.date = c.date

{% if is_incremental() %}
    where e.date > (select max(date) from {{ this }})
{% endif %} 