{{
    config(
        materialized='incremental',
        unique_key='user_id',
        merge_update_columns=['name', 'email', 'segment', 'signup_date', '_loaded_at']
    )
}}

with source as (
    select * from {{ source('raw', 'users') }}
),

latest_records as (
    select 
        user_id,
        name,
        email,
        segment,
        signup_date,
        _loaded_at,
        row_number() over (partition by user_id order by _loaded_at desc) as rn
    from source
),

final as (
    select 
        user_id,
        name,
        email,
        segment,
        signup_date,
        _loaded_at
    from latest_records
    where rn = 1
)

select * from final

{% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %} 