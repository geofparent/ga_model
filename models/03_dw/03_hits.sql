{% set custom_dim_list = dbt_utils.get_query_results_as_dict(
  "SELECT
  distinct(c.index) as index_num
  FROM {{source('ga360_export','sessions_export')}} g,
  UNNEST(customDimensions) as c
  where {{tableRange()}}
  order by index") %}



SELECT
  date AS date,
  clientId AS clientId,
  fullVisitorId AS fullVisitorId,
  visitNumber AS visitNumber,
  visitId AS visitId,
  visitStartTime AS visitStartTime,
  hitNumber AS hitNumber,
  time AS time,
  hour AS hour,
  minute AS minute,
  isSecure AS isSecure,
  isInteraction AS isInteraction,
  isEntrance AS isEntrance,
  isExit AS isExit,
  referer AS referer,
  page.pagePath AS page_pagePath,
  page.hostname AS page_hostname,
  page.pageTitle AS page_pageTitle,
  page.searchKeyword AS page_searchKeyword,
  page.searchCategory AS page_searchCategory,
  page.pagePathLevel1 AS page_pagePathLevel1,
  page.pagePathLevel2 AS page_pagePathLevel2,
  page.pagePathLevel3 AS page_pagePathLevel3,
  page.pagePathLevel4 AS page_pagePathLevel4,
  TRANSACTION.transactionId AS transaction_transactionId,
  TRANSACTION.transactionRevenue AS transaction_transactionRevenue,
  TRANSACTION.transactionTax AS transaction_transactionTax,
  TRANSACTION.transactionShipping AS transaction_transactionShipping,
  TRANSACTION.affiliation AS transaction_affiliation,
  TRANSACTION.currencyCode AS transaction_currencyCode,
  TRANSACTION.localTransactionRevenue AS transaction_localTransactionRevenue,
  TRANSACTION.localTransactionTax AS transaction_localTransactionTax,
  TRANSACTION.localTransactionShipping AS transaction_localTransactionShipping,
  TRANSACTION.transactionCoupon AS transaction_transactionCoupon,
  item.transactionId AS item_transactionId,
  item.productName AS item_productName,
  item.productCategory AS item_productCategory,
  item.productSku AS item_productSku,
  item.itemQuantity AS item_itemQuantity,
  item.itemRevenue AS item_itemRevenue,
  item.currencyCode AS item_currencyCode,
  item.localItemRevenue AS item_localItemRevenue,
  exceptionInfo.description AS exceptionInfo_description,
  exceptionInfo.isFatal AS exceptionInfo_isFatal,
  exceptionInfo.exceptions AS exceptionInfo_exceptions,
  exceptionInfo.fatalExceptions AS exceptionInfo_fatalExceptions,
  eventInfo.eventCategory AS eventInfo_eventCategory,
  eventInfo.eventAction AS eventInfo_eventAction,
  eventInfo.eventLabel AS eventInfo_eventLabel,
  eventInfo.eventValue AS eventInfo_eventValue,
  refund.refundAmount AS refund_refundAmount,
  refund.localRefundAmount AS refund_localRefundAmount,
  eCommerceAction.action_type AS eCommerceAction_action_type,
  eCommerceAction.step AS eCommerceAction_step,
  eCommerceAction.option AS eCommerceAction_option,
  type AS type,
  social.socialInteractionNetwork AS social_socialInteractionNetwork,
  social.socialInteractionAction AS social_socialInteractionAction,
  social.socialInteractions AS social_socialInteractions,
  social.socialInteractionTarget AS social_socialInteractionTarget,
  social.socialNetwork AS social_socialNetwork,
  social.uniqueSocialInteractions AS social_uniqueSocialInteractions,
  social.hasSocialSourceReferral AS social_hasSocialSourceReferral,
  social.socialInteractionNetworkAction AS social_socialInteractionNetworkAction,
  sourcePropertyInfo.sourcePropertyDisplayName AS sourcePropertyInfo_sourcePropertyDisplayName,
  sourcePropertyInfo.sourcePropertyTrackingId AS sourcePropertyInfo_sourcePropertyTrackingId,
  contentGroup.contentGroup1 AS contentGroup_contentGroup1,
  contentGroup.contentGroup2 AS contentGroup_contentGroup2,
  contentGroup.contentGroup3 AS contentGroup_contentGroup3,
  contentGroup.contentGroup4 AS contentGroup_contentGroup4,
  contentGroup.contentGroup5 AS contentGroup_contentGroup5,
  contentGroup.previousContentGroup1 AS contentGroup_previousContentGroup1,
  contentGroup.previousContentGroup2 AS contentGroup_previousContentGroup2,
  contentGroup.previousContentGroup3 AS contentGroup_previousContentGroup3,
  contentGroup.previousContentGroup4 AS contentGroup_previousContentGroup4,
  contentGroup.previousContentGroup5 AS contentGroup_previousContentGroup5,
  contentGroup.contentGroupUniqueViews1 AS contentGroup_contentGroupUniqueViews1,
  contentGroup.contentGroupUniqueViews2 AS contentGroup_contentGroupUniqueViews2,
  contentGroup.contentGroupUniqueViews3 AS contentGroup_contentGroupUniqueViews3,
  contentGroup.contentGroupUniqueViews4 AS contentGroup_contentGroupUniqueViews4,
  contentGroup.contentGroupUniqueViews5 AS contentGroup_contentGroupUniqueViews5,
  {% for i in custom_dim_list.index_num.values %}
  {{ i }} AS {{i}}
    {%- if not loop.last -%}
    ,
    {%- endif -%}
    {%- endfor %}

FROM (
  SELECT
    parse_DATE('%Y%m%d',
      date) AS date,
    clientId,
    fullVisitorId,
    visitNumber,
    visitId,
    visitStartTime,
    h.* EXCEPT( product,
      customdimensions,
      customMetrics,
      customVariables,
      promotion,
      publisher,
      latencyTracking,
      publisher_infos,
      contentInfo,
      appInfo,
      experiment,
      promotionActionInfo,
      dataSource
    ),
    {% for i in custom_dim_list.index_num.values %}
    (SELECT
      value
    FROM
    h.customDimensions
    WHERE
      INDEX = {{ i }}) AS {{ i }}
      {%- if not loop.last -%}
      ,
      {%- endif -%}
      {%- endfor %}

  FROM
    {{source('ga360_export','sessions_export')}} t,
    t.hits AS h,
    h.product AS p
  WHERE
    {{tableRange()}} )
