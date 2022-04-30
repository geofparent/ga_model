with hits as(
    select * from {{ ref('03_hits') }}
),
sessions as(
    select * from {{ ref('03_sessions') }}
)

select
  hits.page_pagePath as page_pagePath,
  hits.page_pageTitle as page_pageTitle,
  count(*) as total_events,
  count(distinct concat(cast(hits.fullVisitorId as string), cast(hits.visitStartTime as string))) as unique_events,
  ifnull(sum(hits.eventInfo_eventValue),0) as event_value,
  ifnull(sum(hits.eventInfo_eventValue) / count(*),0) as avg_value
from
  hits
  LEFT JOIN
  sessions ON hits.session_id = sessions.session_id
where
  sessions.totals_visits = 1
  and type = 'EVENT'
group by
  page_pagePath
  ,page_pageTitle
order by total_events desc
