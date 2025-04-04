{{
    config(
        materialized='table'
    )
}}

-- models/games_overview.sql
with base as (
  select
    s.appid,
    s.name,
    s.platforms,
    s.description,
    s.metacritic,
    s.recommendations, 
    s.is_free,
    s.release_date,
    s.achievements_number,
    clean.developer,
    g.category as genre,
    c.category as category,
    t.tag,
    t.user_count,
    clean.price,
    clean.publisher
  from {{ ref('stg_steam_store') }} s
  left join {{ ref('stg_steam_metadata_table_test__developers') }} d
    on s._dlt_id = d._dlt_parent_id
  left join {{ ref('steam_game_genre') }} g
    on s.appid = g.appid
  left join {{ ref('steam_game_categories') }} c
    on s.appid = c.appid
  left join {{ ref('steam_user_tag_table') }} t
    on s.appid = t.appid
  left join {{ source('staging', 'steamspy_game_details_table_test_clean') }} clean
    on s.appid = clean.appid
)

select * from base
