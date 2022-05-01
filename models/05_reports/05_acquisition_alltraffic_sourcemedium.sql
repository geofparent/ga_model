with sessions as(
    select * from {{ ref('03_sessions') }}
)


select
  sessions.trafficSource_campaign as trafficSource_campaign,
  sessions.trafficSource_source as trafficSource_source,
  sessions.trafficSource_medium as trafficSource_medium,
  concat(sessions.trafficSource_source," / ",sessions.trafficSource_medium) as source_medium,
  sessions.trafficSource_adcontent as trafficSource_adcontent,
  sessions.trafficSource_keyword as trafficSource_keyword,
  count(distinct sessions.fullVisitorId) as users,
  count(distinct(case when sessions.totals_newvisits = 1 then sessions.fullVisitorId else null end)) as new_users,
  count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))) as sessions,
  count(distinct case when sessions.totals_bounces = 1 then concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string)) else null end ) / count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))) as bounce_rate,
  sum(sessions.totals_pageviews) / count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))) as pages_per_session,
  ifnull(sum(sessions.totals_timeOnSite) / count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))),0) as average_session_duration,
  ifnull(sum(sessions.totals_transactions),0) as transactions,
  ifnull(sum(sessions.totals_transactionRevenue),0)/1000000 as revenue,
  ifnull(sum(sessions.totals_transactions) / count(distinct concat(sessions.fullVisitorId, cast(sessions.visitStartTime as string))),0) as ecommerce_conversion_rate
from
  sessions
where
  sessions.totals_visits = 1
group by
trafficSource_campaign,
trafficSource_source,
trafficSource_medium,
source_medium,
trafficSource_adcontent,
trafficSource_keyword
order by
  users desc
