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

product_sales_by_time as (
    select
        p.product_id,
        date_trunc('month', c.timestamp) as month,
        count(c.conversion_id) as monthly_conversions,
        sum(c.revenue) as monthly_revenue,
        max(c._loaded_at) as _loaded_at
    from product_data p
    left join {{ ref('stg_conversions') }} c on p.product_id = c.product_id
    where c.conversion_id is not null
    group by 1, 2
),

product_sales_by_channel as (
    select
        p.product_id,
        camp.channel,
        count(c.conversion_id) as channel_conversions,
        sum(c.revenue) as channel_revenue,
        max(c._loaded_at) as _loaded_at
    from product_data p
    join {{ ref('stg_conversions') }} c on p.product_id = c.product_id
    join {{ ref('stg_campaigns') }} camp on c.campaign_id = camp.campaign_id
    group by 1, 2
),

product_sales_by_platform as (
    select
        p.product_id,
        camp.platform,
        count(c.conversion_id) as platform_conversions,
        sum(c.revenue) as platform_revenue,
        max(c._loaded_at) as _loaded_at
    from product_data p
    join {{ ref('stg_conversions') }} c on p.product_id = c.product_id
    join {{ ref('stg_campaigns') }} camp on c.campaign_id = camp.campaign_id
    group by 1, 2
)

select
    pp.product_id,
    pd.name as product_name,
    pd.category,
    pp.conversions as total_conversions,
    pp.revenue as total_revenue,
    array_agg(distinct struct(
        st.month,
        st.monthly_conversions,
        st.monthly_revenue
    ) order by st.month) as monthly_sales,
    array_agg(distinct struct(
        sc.channel,
        sc.channel_conversions,
        sc.channel_revenue
    )) as sales_by_channel,
    array_agg(distinct struct(
        sp.platform,
        sp.platform_conversions,
        sp.platform_revenue
    )) as sales_by_platform,
    greatest(
        pp._loaded_at,
        max(st._loaded_at),
        max(sc._loaded_at),
        max(sp._loaded_at)
    ) as _loaded_at
from product_performance pp
join product_data pd on pp.product_id = pd.product_id
left join product_sales_by_time st on pp.product_id = st.product_id
left join product_sales_by_channel sc on pp.product_id = sc.product_id
left join product_sales_by_platform sp on pp.product_id = sp.product_id
group by 1, 2, 3, 4, 5
order by total_revenue desc