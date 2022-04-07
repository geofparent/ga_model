SELECT
  date AS date,
  fullvisitorid AS fullvisitorid,
  visitid AS visitid,
  visitstarttime AS visitstarttime,
  hitNumber AS hitNumber,
  sourcePropertyInfo.sourcePropertyDisplayName AS sourcePropertyInfo_sourcePropertyDisplayName,
  sourcePropertyInfo.sourcePropertyTrackingId AS sourcePropertyInfo_sourcePropertyTrackingId,
  productSKU AS productSKU,
  v2ProductName AS v2ProductName,
  v2ProductCategory AS v2ProductCategory,
  productVariant AS productVariant,
  productBrand AS productBrand,
  productRevenue AS productRevenue,
  localProductRevenue AS localProductRevenue,
  productPrice AS productPrice,
  localProductPrice AS localProductPrice,
  productQuantity AS productQuantity,
  productRefundAmount AS productRefundAmount,
  localProductRefundAmount AS localProductRefundAmount,
  isImpression AS isImpression,
  isClick AS isClick,
  productListName AS productListName,
  productListPosition AS productListPosition,
  productCouponCode AS productCouponCode
  #,
  #leverage_revenue AS leverage_revenue,

FROM (
  SELECT
    parse_DATE('%Y%m%d',
      date) AS date,
    fullvisitorid,
    visitid,
    visitstarttime,
    h.hitNumber,
    h.sourcePropertyInfo,
    p.* EXCEPT(customdimensions,
      customMetrics)
      #,
    #(
    #SELECT
    #  value
  #  FROM
    #  p.customMetrics
    #WHERE
      #INDEX = 6) AS leverage_revenue,

  FROM
    {{source('ga360_export','sessions_export')}} t,
    t.hits AS h,
    h.product AS p
  WHERE
    {{tableRange()}} )
