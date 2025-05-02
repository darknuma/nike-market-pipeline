with product_data as (
  select * from {{ ref('stg_products') }}
),

conversion_data as (
  select * from {{ ref('stg_conversions') }}
),

product_metrics as (
  select
    p.product_id,
    p.name,
    p.category,
    count(c.conversion_id) as conversions,
    sum(c.revenue) as revenue,
    max(c._loaded_at) as _loaded_at
  from product_data p
  left join conversion_data c on p.product_id = c.product_id
  group by 1, 2, 3
)

select * from product_metrics