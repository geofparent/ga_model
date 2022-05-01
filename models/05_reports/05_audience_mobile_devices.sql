with sessions as(
    select * from {{ ref('03_sessions') }}
)


select
  sessions.device_mobileDeviceBranding as device_mobileDeviceBranding,
  sessions.device_mobileDeviceMarketingName as device_mobileDeviceMarketingName,
  sessions.device_mobileDeviceModel as device_mobiledDviceModel,
  sessions.device_mobileDeviceInfo as device_mobileDeviceInfo,
  sessions.device_browser as device_browser,
  sessions.device_browserVersion as device_browserVersion,
  sessions.device_browserSize as device_browserSize,
  sessions.device_operatingSystem as device_operatingSystem,
  sessions.device_operatingSystemVersion as device_operatingSystemVersion,
  sessions.device_screenResolution as device_screenResolution,
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
device_mobileDeviceBranding,
device_mobileDeviceMarketingName,
device_mobiledDviceModel,
device_mobileDeviceInfo,
device_browser,
device_browserVersion,
device_browserSize,
device_operatingSystem,
device_operatingSystemVersion,
device_screenResolution,
order by
  users desc
