with hits as(
    select * from {{ ref('03_hits') }}
),
sessions as(
    select * from {{ ref('03_sessions') }}
),
avg_time as (
select
  page_pagePath,
  page_pageTitle,
  case when pageviews = exits then 0 else total_time_on_page / (pageviews - exits) end as avg_time_on_page
from (
  select
  page_pagePath,
  page_pageTitle,
    count(*) as pageviews,
    countif(isExit is not null) as exits,
    sum(time_on_page) as total_time_on_page
  from (
    select
      fullVisitorId,
      visitStartTime,
      page_pagePath,
      page_pageTitle,
      hit_time,
      type,
      isExit,
      case when isExit is not null then last_interaction - hit_time else next_pageview - hit_time end as time_on_page
    from (
      select
        sessions.fullVisitorId as fullVisitorId,
        sessions.visitStartTime as visitStartTime,
        hits.page_pagePath as page_pagePath,
        hits.page_pageTitle as page_pageTitle,
        hits.time / 1000 as hit_time,
        hits.type as type,
        hits.isExit as isExit,
        max(if(hits.isinteraction = true,hits.time / 1000,0)) over (partition by sessions.fullVisitorId, sessions.visitStartTime) as last_interaction,
        lead(hits.time / 1000) over (partition by sessions.fullVisitorId, sessions.visitStartTime order by hits.time / 1000) as next_pageview
      from
        hits
        LEFT JOIN
        sessions on hits.session_id = sessions.session_id
      where
        hits.type = 'PAGE'
        and sessions.totals_visits = 1))
  group by
    page_pagePath,
    page_pageTitle)
  )


select
  hits.page_pagePath as page_pagePath,
  hits.page_pageTitle as page_pageTitle,
  count(*) as pageviews,
  count(distinct concat(cast(sessions.fullVisitorId as string), cast(sessions.visitStartTime as string))) as unique_pageviews,
  avg_time_on_page,
  countif(hits.isEntrance = true) as entrances,
  countif(sessions.totals_bounces = 1) / count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))) as bounce_rate,
  countif(hits.isExit = true) / count(*) as exit_rate
from
  hits
  left JOIN
  sessions on hits.session_id = sessions.session_id
  left join avg_time on hits.page_pagePath = avg_time.page
  and hits.page_pageTitle = avg_time.page_pageTitle
where
  sessions.totals_visits = 1
  and hits.type = 'PAGE'
group by
  page_pagePath,
  avg_time_on_page,
  page_pageTitle
order by
  page_pageviews desc
