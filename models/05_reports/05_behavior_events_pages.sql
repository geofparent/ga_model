with hits as(
    select * from {{ ref('03_hits') }}
),

select
  hits.page_pagePath as page,
  -- hits.page.pagetitle as page_title,
  count(*) as total_events,
  count(distinct concat(cast(fullVisitorId as string), cast(visitStartTime as string))) as unique_events,
  ifnull(sum(hits.eventInfo_eventValue),0) as event_value,
  ifnull(sum(hits.eventInfo_eventValue) / count(*),0) as avg_value
from
  hits
where
  ### is the below total visits needed if this is unnested?
  totals.visits = 1
  and type = 'EVENT'
group by
  hits.page_pagePath
  -- ,hits.page.pagetitle
order by total_events desc
