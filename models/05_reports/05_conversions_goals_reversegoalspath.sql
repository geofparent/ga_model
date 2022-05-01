with hits as(
    select * from {{ ref('03_hits') }}
),
sessions as(
    select * from {{ ref('03_sessions') }}


select
  goal_completion_location,
  goal_previous_step_1,
  goal_previous_step_2,
  goal_previous_step_3,
  count(distinct case when regexp_contains(goal_completion_location, r'/ordercompleted') then session_id else null end) as goal_1_completions,
from
  (
  select
    hits.page_pagePath as goal_completion_location,
    lag(hits.page_pagePath, 1) over (partition by sessions.fullVisitorId, sessions.visitStartTime order by hits.hitNumber asc) as goal_previous_step_1,
    lag(hits.page_pagePath, 2) over (partition by sessions.fullVisitorId, sessions.visitStartTime order by hits.hitNumber asc) as goal_previous_step_2,
    lag(hits.page_pagePath, 3) over (partition by sessions.fullVisitorId, sessions.visitStartTime order by hits.hitNumber asc) as goal_previous_step_3,
    concat(cast(sessions.fullVisitorId as string),cast(sessions.visitStartTime as string)) as session_id
  from
    sessions
    left join
    hits on sessions.session_id = hits.session_id
  )
group by
  goal_completion_location,
  goal_previous_step_1,
  goal_previous_step_2,
  goal_previous_step_3
having
  goal_completion_location not in (goal_previous_step_1, goal_previous_step_2, goal_previous_step_3) and goal_1_completions >= 1
order by
  goal_1_completions desc
