-- models/aggregated_stats_by_tag.sql
with tag_rank as (
  select
    t.tag,
    count(distinct t.appid) as game_count,
    avg(r.rank) as avg_rank,
    max(r.peak_in_game) as max_peak,
    min(r.rank) as best_rank
  from {{ ref('steam_user_tag_table') }} t
  left join {{ ref('steam_top_100_daily_test') }} r
    on t.appid = r.appid
  group by t.tag
)

select * from tag_rank
