{{
    config(
        materialized='incremental'
    )
}}

with 

genres as (
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
    g.description as genre
from genres g
join source s
    on g._dlt_parent_id = s._dlt_id

{% if is_incremental() %}
and (
    s.appid not in (select appid from {{ this }})
    or g.description != (select genre from {{ this }} where appid = s.appid)
)
{% endif %}
