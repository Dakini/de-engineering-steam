-- models/dashboard_summary.sql
select
  a.tag,
  a.game_count,
  a.avg_rank,
  p.platform,
  p.game_count as platform_game_count
from {{ ref('aggregated_stats_by_tag') }} a
left join {{ ref('platform_count_model') }} p
  on 1=1  -- optional: could normalize/apply filters to join tags with most common platform

-- Alternatively: If tags and platforms are shown separately in dashboard, create separate sections.
