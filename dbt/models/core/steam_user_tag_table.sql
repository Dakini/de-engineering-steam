{{
    config(
        materialized='incremental'
    )
}}

with 

source as (
    select * from {{ source('staging', 'steam_user_tag_table') }}
),

renamed as (
    select
        appid,
        tag,
        user_count
    from source
)

select * from renamed

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t
    where t.appid = renamed.appid
    and t.tag = renamed.tag
    and t.user_count = renamed.user_count
)
{% endif %}
