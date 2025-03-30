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

    from source

)

select * from renamed

{% if var('is_test_run', default=true) %}

  limit 10

{% endif %}
