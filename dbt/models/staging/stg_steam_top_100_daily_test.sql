{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('staging', 'steam_top_100_daily_test') }}
    where appid is not null
),

renamed as (

    select
        _dlt_id,
        appid,
        rank,
        peak_in_game,
        date_added,
    from source

)
--dbt
select * from renamed
-- dbt build --select stg_steam_top_100_daily_test.sql --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 10

{% endif %}
