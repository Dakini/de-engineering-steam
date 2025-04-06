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
    from {{ ref('stg_steam_metadata_table_test__genres') }}
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
where not exists (
    select 1
    from {{ this }} t
    where t.appid = s.appid
    and t.category = g.description
)
{% endif %}
