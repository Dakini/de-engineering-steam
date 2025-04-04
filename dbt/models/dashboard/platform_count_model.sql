-- models/platform_count_model.sql
with exploded_platforms as (
  select
    appid,
    trim(lower(platform)) as platform
  from {{ ref('stg_steam_store') }},
  unnest(
    split(
      regexp_replace(platforms, r'[\s]+', ','),
      ','
    )
  ) as platform
)

select
  platform,
  count(distinct appid) as game_count
from exploded_platforms
where platform != ''
group by platform
order by game_count desc
