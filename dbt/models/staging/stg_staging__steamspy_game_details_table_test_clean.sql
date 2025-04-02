{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select * from {{ source('staging', 'steamspy_game_details_table_test_clean') }}

),

renamed as (

    select
        
        _dlt_id
        tags,
        appid,
        name,
        date_added,
        developer,
        publisher,
        positive,
        negative,
        average_2weeks,
        median_2weeks,
        price,
        initialprice,
        discount,
        languages,
        genre,
        owners_lower_range,
        owners_upper_range,
        row_number() over(partition by appid, date_added) as rn
    from source

)

select * from renamed
where rn = 1
-- dbt build --select stg_steam_top_100_daily_test.sql --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 10

{% endif %}
