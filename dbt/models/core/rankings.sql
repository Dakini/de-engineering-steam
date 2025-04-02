{{
    config(
        materialized='table'
    )
}}

with game_details as (
    select *
    from {{ ref('stg_staging__steamspy_game_details_table_test_clean') }}
),

top_daily_pop as (
    select *
    from {{ ref('stg_steam_top_100_daily_test') }}
)

select 
    g.appid,
    g.name,
    g.price,
    g.developer,
    t.rank,
    t.peak_in_game,
    t.date_added

from game_details g
join top_daily_pop t 
    on g.appid = t.appid
