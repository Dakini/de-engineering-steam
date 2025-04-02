{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select * from {{ source('staging', 'steam_store') }}

),

renamed as (

    select
        
        _dlt_id,
        name,
        appid,
        date_added,
        required_age,
        is_free,
        description,
        platforms,
        metacritic,
        recommendations,
        achievements_number,
        release_date,
        controller_support,
        english,
        release_date_year,
        release_date_month,
        release_date_day,
        reviews,
        row_number() over(partition by appid, date_added) as rn
    from source

)

select * from renamed
where rn = 1
-- dbt build --select stg_steam_top_100_daily_test.sql --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 10

{% endif %}
