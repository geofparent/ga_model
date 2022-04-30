with hits as(
    select * from {{ ref('03_hits') }}
)

select
  session.trafficSource_campaign,
  session.trafficSource_source,
  session.trafficSource_medium,
  concat(session.trafficSource_source," / ",sessions.trafficSource_medium) as source_medium,
  --trafficsource.adcontent,
  -- trafficsource.keyword,
  count(distinct sessions.fullVisitorId) as users,
  count(distinct(case when totals.newvisits = 1 then sessions.fullVisitorId else null end)) as new_users,
  count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))) as sessions,
  count(distinct case when totals.bounces = 1 then concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string)) else null end ) / count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))) as bounce_rate,
  sum(totals.pageviews) / count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))) as pages_per_session,
  ifnull(sum(session.totals_timeonsite) / count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))),0) as average_session_duration,
  ifnull(sum(session.totals_transactions),0) as transactions,
  ifnull(sum(session.totals_totaltransactionrevenue),0)/1000000 as revenue,
  ifnull(sum(session.total_transactions) / count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))),0) as ecommerce_conversion_rate
from
  sessions
where
  sessions.totals_visits = 1
group by
  sessions.trafficSource_campaign
  ,sessions.trafficSource_source
  ,sessions.trafficSource_medium
  ,source_medium
  -- ,trafficSource.adcontent
  -- ,trafficsource.keyword
order by
  users desc
