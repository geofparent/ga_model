
with hits as(
    select * from {{ ref('03_hits') }}
),

with products as(
    select * from {{ ref('03_products') }}
),


select
  hits.item.transactionid,
  -- date,
  ifnull(sum(hits.transaction_transactionRevenue)/1000000,0) as revenue,
  ifnull(sum(hits.transaction_transactionTax)/1000000,0) as tax,
  #ifnull(sum(hits.transaction.transactionshipping)/1000000,0) as shipping,
  #ifnull(sum(hits.refund.refundAmount),0) as refund_amount,
  ifnull(sum(case when eCommerceAction_action_type = '6' then productquantity else null end),0) as quantity
from
  hits
  JOIN
  products as products on hits.item_productSku == products.productSKU
where
  totals.visits = 1
  and hits.transaction_transactionId is not null
group by
  hits.transaction_transactionId
  -- ,date
order by
  revenue desc
