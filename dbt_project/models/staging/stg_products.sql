{{
    config(
        materialized='incremental',
        unique_key='product_id',
        merge_update_columns=['name', 'category', 'price', '_loaded_at']
    )
}}

with source as (
    select * from {{ source('raw', 'products') }}
),

latest_records as (
    select 
        product_id,
        name,
        category,
        price,
        _loaded_at,
        row_number() over (partition by product_id order by _loaded_at desc) as rn
    from source
),

final as (
    select 
        product_id,
        name,
        category,
        price,
        _loaded_at
    from latest_records
    where rn = 1
)

select * from final

{% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %} 