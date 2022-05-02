with hits as(
    select * from {{ ref('03_hits') }}
),
products as(
    select * from {{ ref('03_products') }}
),
sessions as(
    select * from {{ ref('03_sessions') }}
)


select
  products.v2ProductName as v2ProductName,
  products.productSKU as productSKU,
  products.v2ProductCategory as v2ProductCategory,
  products.productBrand as productBrand,
  ifnull(sum(case when hits.eCommerceAction_action_type = '6' then products.productRevenue else null end)/1000000,0) as product_revenue,
  count(case when hits.eCommerceAction_action_type = '6' then hits.transaction_transactionId else null end) as unique_purchases,
  ifnull(sum(case when hits.eCommerceAction_action_type = '6' then products.productQuantity else null end),0) as quantity,
  ifnull(safe_divide(sum(case when hits.eCommerceAction_action_type = '6' then products.productRevenue else null end)/1000000,sum(case when hits.eCommerceAction_action_type = '6' then products.productQuantity else null end)),0) as avg_price,
  ifnull(safe_divide(sum(case when hits.eCommerceAction_action_type = '6' then products.productQuantity else null end),count(case when hits.eCommerceAction_action_type = '6' then hits.transaction_transactionId else null end)),0) as avg_quantity,
  ifnull(safe_divide(count(case when hits.eCommerceAction_action_type = '3' then hits.fullVisitorId else null end),count(case when hits.eCommerceAction_action_type = '2' and products.isImpression is null then hits.fullVisitorId else null end)),0) as cart_to_detail_rate,
  ifnull(safe_divide(count(case when hits.eCommerceAction_action_type = '6' then hits.transaction_transactionId else null end),count(case when hits.eCommerceAction_action_type = '2' and products.isImpression is null then hits.fullVisitorId else null end)),0) as buy_to_detail_rate
from
  sessions
  LEFT JOIN
  hits on sessions.session_id = hits.session_id
  LEFT JOIN
  products on hits.hit_id = products.hit_id

where
  sessions.totals_visits = 1
group by
  v2ProductName
  ,productSKU
  ,v2ProductCategory
  ,productBrand
order by
  product_revenue desc
