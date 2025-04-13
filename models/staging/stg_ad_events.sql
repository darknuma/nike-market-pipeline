{{
    config(
        materialized='incremental',
        unique_key='event_id',
        merge_update_columns=['user_id', 'campaign_id', 'event_type', 'timestamp', 'platform', '_loaded_at']
    )
}}

with source as (
    select * from {{ source('raw', 'ad_events') }}
),

latest_records as (
    select 
        event_id,
        user_id,
        campaign_id,
        event_type,
        timestamp,
        platform,
        _loaded_at,
        row_number() over (partition by event_id order by _loaded_at desc) as rn
    from source
),

final as (
    select 
        event_id,
        user_id,
        campaign_id,
        event_type,
        timestamp,
        platform,
        _loaded_at
    from latest_records
    where rn = 1
)

select * from final

{% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %} 