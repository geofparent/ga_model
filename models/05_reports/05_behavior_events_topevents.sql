with hits as(
    select * from {{ ref('03_hits') }}
),
sessions as(
    select * from {{ ref('03_sessions') }}
)

select
  hits.eventInfo_eventCategory as eventInfo_eventCategory,
  hits.eventInfo_eventAction as eventInfo_eventAction,
  hits.eventInfo_eventLabel as eventInfo_eventLabel,
  count(*) as total_events,
  count(distinct concat(cast(hits.fullVisitorId as string), cast(hits.visitStartTime as string))) as unique_events,
  ifnull(sum(hits.eventInfo_eventValue),0) as event_value,
  ifnull(sum(hits.eventInfo_eventValue) / count(*),0) as avg_value
from
  hits
  LEFT JOIN
  sessions on hits.session_id = sessions.session_id
where
### is total visits needed if this is unnested? do we need a left join to sessions table for this?
  sessions.totals_visits = 1
  and hits.type = 'EVENT'
  and hits.eventInfo_eventCategory is not null
group by
  eventInfo_eventCategory
  , eventInfo_eventAction
  , eventInfo_eventLabel
order by total_events desc
