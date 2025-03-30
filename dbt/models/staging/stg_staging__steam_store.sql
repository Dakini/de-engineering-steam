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
        reviews
    from source

)

select * from renamed
