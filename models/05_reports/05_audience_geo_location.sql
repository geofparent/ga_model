with sessions as(
    select * from {{ ref('03_sessions') }}
)


select
  sessions.geoNetwork_country as geoNetwork_country,
  sessions.geoNetwork_region as geoNetwork_region,
  sessions.geoNetwork_city as geoNetwork_city,
  sessions.geoNetwork_continent as geoNetwork_continent,
  sessions.geoNetwork_subcontinent as geoNetwork_subContinent,
  count(distinct sessions.fullVisitorId) as users,
  count(distinct(case when sessions.totals_newVisits = 1 then sessions.fullVisitorId else null end)) as new_users,
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
  geoNetwork_country,
  geoNetwork_region,
  geoNetwork_city,
  geoNetwork_continent,
  geoNetwork_subcontinent
order by
  users desc
