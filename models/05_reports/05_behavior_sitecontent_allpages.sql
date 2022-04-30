with hits as(
    select * from {{ ref('03_hits') }}
),
sessions as(
    select * from {{ ref('03_sessions') }}
),

avg_time as (
select
  page_pagePath as page,
  page_pageTitle,
  case when pageviews = exits then 0 else total_time_on_page / (pageviews - exits) end as avg_time_on_page
from (
  select
  page_pagePath,
  page_pageTitle,
    count(*) as pageviews,
    countif(isexit is not null) as exits,
    sum(time_on_page) as total_time_on_page
  from (
    select
      fullVisitorId,
      visitStartTime,
      page_pagePath,
      page_pageTitle,
      hit_time,
      type,
      isexit,
      case when isexit is not null then last_interaction - hit_time else next_pageview - hit_time end as time_on_page
    from (
      select
        fullVisitorId,
        visitStartTime,
        hits.page_pagePath,
        hits.page_pageTitle,
        hits.time / 1000 as hit_time,
        hits.type,
        hits.isexit,
        max(if(hits.isinteraction = true,hits.time / 1000,0)) over (partition by fullVisitorId, visitStartTime) as last_interaction,
        lead(hits.time / 1000) over (partition by fullVisitorId, visitStartTime order by hits.time / 1000) as next_pageview
      from
        hits
        LEFT JOIN
        sessions on hits.session_id = sessions.session_id
      where
        {{tableRange()}}
        and hits.type = 'PAGE'
        and sessions.totals_visits = 1))
  group by
    pagepath,
    pagetitle)
  )

select
  hits.page.pagepath as page,
  -- hits.page.pagetitle as page_title,
  count(*) as pageviews,
  count(distinct concat(cast(fullVisitorId as string), cast(visitStartTime as string))) as unique_pageviews,
  avg_time_on_page,
  countif(hits.isentrance = true) as entrances,
  countif(totals.bounces = 1) / count(distinct concat(fullVisitorId, cast(visitStartTime as string))) as bounce_rate,
  countif(hits.isexit = true) / count(*) as exit_rate
from
  hits
  left JOIN
  sessions on hits.session_id = sessions.session_id
  left join avg_time on hits.page_pagePath = avg_time.page
  and hits.page_pageTitle = avg_time.page_pageTitle
where
  {{tableRange()}}
  and sessions.totals_visits = 1
  and hits.type = 'PAGE'
group by
  page,
  avg_time_on_page
  -- ,page_title
order by
  pageviews desc
