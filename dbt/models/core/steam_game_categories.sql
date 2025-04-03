{{
    config(
        materialized='incremental'
    )
}}

with 

genre_source as (
    select 
        description,
        _dlt_parent_id
    from {{ ref('stg_steam_metadata_table_test__categories') }}
),

source as (
    select 
        _dlt_id,
        appid 
    from {{ ref('stg_steam_store') }}
)

select 
    s.appid,
    g.description as category
from genre_source g
join source s
    on g._dlt_parent_id = s._dlt_id

{% if is_incremental() %}
and (
    s.appid not in (select appid from {{ this }})
    or g.value != (select category from {{ this }} where appid = s.appid)
)
{% endif %}
