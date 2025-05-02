{{
    config(
        materialized='table',
        schema='marts_product'
    )
}}

with product_data as (
    select * from {{ ref('stg_products') }}
),

product_performance as (
    select * from {{ ref('int_product_performance') }}
),

category_metrics as (
    select
        p.category,
        count(distinct pp.product_id) as product_count,
        sum(pp.conversions) as total_conversions,
        sum(pp.revenue) as total_revenue,
        avg(pp.revenue) as avg_revenue_per_product,
        sum(pp.revenue) / nullif(sum(pp.conversions), 0) as avg_price,
        max(pp._loaded_at) as _loaded_at
    from product_performance pp
    join product_data p on pp.product_id = p.product_id
    group by 1
),

category_time_series as (
    select
        p.category,
        date_trunc('month', c.timestamp) as month,
        count(distinct c.conversion_id) as monthly_conversions,
        sum(c.revenue) as monthly_revenue,
        max(c._loaded_at) as _loaded_at
    from product_data p
    join {{ ref('stg_conversions') }} c on p.product_id = c.product_id
    group by 1, 2
),

category_channel_performance as (
    select
        p.category,
        camp.channel,
        count(distinct c.conversion_id) as channel_conversions,
        sum(c.revenue) as channel_revenue,
        max(c._loaded_at) as _loaded_at
    from product_data p
    join {{ ref('stg_conversions') }} c on p.product_id = c.product_id
    join {{ ref('stg_campaigns') }} camp on c.campaign_id = camp.campaign_id
    group by 1, 2
),

top_products_by_category as (
    select
        p.category,
        array_agg(struct(
            pp.product_id,
            p.name as product_name,
            pp.conversions,
            pp.revenue
        ) order by pp.revenue desc limit 5) as top_products,
        max(pp._loaded_at) as _loaded_at
    from product_performance pp
    join product_data p on pp.product_id = p.product_id
    group by 1
)

select
    cm.category,
    cm.product_count,
    cm.total_conversions,
    cm.total_revenue,
    cm.avg_revenue_per_product,
    cm.avg_price,
    tp.top_products,
    array_agg(distinct struct(
        cts.month,
        cts.monthly_conversions,
        cts.monthly_revenue
    ) order by cts.month) as monthly_performance,
    array_agg(distinct struct(
        ccp.channel,
        ccp.channel_conversions,
        ccp.channel_revenue
    )) as channel_performance,
    greatest(
        cm._loaded_at,
        tp._loaded_at,
        max(cts._loaded_at),
        max(ccp._loaded_at)
    ) as _loaded_at
from category_metrics cm
join top_products_by_category tp on cm.category = tp.category
left join category_time_series cts on cm.category = cts.category
left join category_channel_performance ccp on cm.category = ccp.category
group by 1, 2, 3, 4, 5, 6, 7
order by total_revenue desc