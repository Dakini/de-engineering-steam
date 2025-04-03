{{
    config(
        materialized='incremental'
    )
}}

with 

dlc_source as (
    select 
        value,
        _dlt_parent_id
    from {{ ref('stg_steam_metadata_table_test__dlc') }}
),

source as (
    select 
        _dlt_id,
        appid 
    from {{ ref('stg_steam_store') }}
)

select 
    s.appid,
    g.value as dlc
from dlc_source g
join source s
    on g._dlt_parent_id = s._dlt_id

{% if is_incremental() %}
and (
    s.appid not in (select appid from {{ this }})
    or g.value != (select dlc from {{ this }} where appid = s.appid)
)
{% endif %}
