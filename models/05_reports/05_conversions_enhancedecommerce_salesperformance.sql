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
  date,
  ifnull(sum(hits.transaction_transactionRevenue)/1000000,0) as revenue,
  ifnull(sum(hits.transaction_transactionTax)/1000000,0) as tax,
  #ifnull(sum(hits.transaction.transactionshipping)/1000000,0) as shipping,
  #ifnull(sum(hits.refund.refundAmount),0) as refund_amount,
  ifnull(sum(case when hits.eCommerceAction_action_type = '6' then products.productquantity else null end),0) as quantity
from
  hits
  LEFT JOIN
  ### PROBABLY NEED TO JOIN ON DATE RO TRANSACTION_ID?
  products on hits.item_productSku = products.productSKU
  LEFT JOIN
  sessions on hits.session_id = sessions.session_id
where
  sessions.totals_visits = 1
  and hits.transaction_transactionId is not null
group by
  transaction_transactionId
  ,date
order by
  revenue desc
