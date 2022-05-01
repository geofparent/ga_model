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
  hits.item_transactionId as item_transactionId,
  hits.date as date,
  ifnull(sum(hits.transaction_transactionRevenue)/1000000,0) as revenue,
  ifnull(sum(hits.transaction_transactionTax)/1000000,0) as tax,
  #ifnull(sum(hits.transaction.transactionshipping)/1000000,0) as shipping,
  #ifnull(sum(hits.refund.refundAmount),0) as refund_amount,
  ifnull(sum(case when hits.eCommerceAction_action_type = '6' then products.productquantity else null end),0) as quantity
from
  sessions
  LEFT JOIN
  hits on sessions.session_id = hits.session_id
  LEFT JOIN
  products on hits.item_productSku = products.productSKU
where
  sessions.totals_visits = 1
  and hits.transaction_transactionId is not null
group by
  item_transactionId
  ,date
order by
  revenue desc
