{{
    config(
        materialized='incremental',
        unique_key='campaign_id',
        merge_update_columns=['name', 'channel', 'start_date', 'end_date', '_loaded_at']
    )
}}

with source as (
    select * from {{ source('raw', 'campaigns') }}
),

latest_records as (
    select 
        campaign_id,
        name,
        channel,
        start_date,
        end_date,
        _loaded_at,
        row_number() over (partition by campaign_id order by _loaded_at desc) as rn
    from source
),

final as (
    select 
        campaign_id,
        name,
        channel,
        start_date,
        end_date,
        _loaded_at
    from latest_records
    where rn = 1
)

select * from final

{% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %} 