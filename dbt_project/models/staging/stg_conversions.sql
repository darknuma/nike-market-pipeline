{{
    config(
        materialized='incremental',
        unique_key='conversion_id',
        merge_update_columns=['user_id', 'product_id', 'campaign_id', 'timestamp', 'revenue', '_loaded_at']
    )
}}

with source as (
    select * from {{ source('raw', 'conversions') }}
),

latest_records as (
    select 
        conversion_id,
        user_id,
        product_id,
        campaign_id,
        timestamp,
        revenue,
        _loaded_at,
        row_number() over (partition by conversion_id order by _loaded_at desc) as rn
    from source
),

final as (
    select 
        conversion_id,
        user_id,
        product_id,
        campaign_id,
        timestamp,
        revenue,
        _loaded_at
    from latest_records
    where rn = 1
)

select * from final

{% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %} 