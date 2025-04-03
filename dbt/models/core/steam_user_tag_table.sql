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
-- Only update rows where the tag or user_count has changed or the appid is new
and (
    (appid, tag) not in (select appid, tag from {{ this }})
    or user_count != (select user_count from {{ this }} where appid = renamed.appid and tag = renamed.tag)
)
{% endif %}
