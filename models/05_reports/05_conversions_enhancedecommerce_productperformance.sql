with hits as(
    select * from {{ ref('03_hits') }}
),

with products as(
    select * from {{ ref('03_products') }}
),

with sessions as(
    select * from {{ ref('03_sessions') }}
),

select
  v2productname as product,
  -- productsku,
  -- v2productcategory as product_category,
  -- productbrand,
  ifnull(sum(case when hits.ecommerceaction.action_type = '6' then productrevenue else null end)/1000000,0) as product_revenue,
  count(case when hits.ecommerceaction.action_type = '6' then hits.transaction.transactionid else null end) as unique_purchases,
  ifnull(sum(case when hits.ecommerceaction.action_type = '6' then productquantity else null end),0) as quantity,
  ifnull(safe_divide(sum(case when hits.ecommerceaction.action_type = '6' then productrevenue else null end)/1000000,sum(case when hits.ecommerceaction.action_type = '6' then productquantity else null end)),0) as avg_price,
  ifnull(safe_divide(sum(case when hits.ecommerceaction.action_type = '6' then productquantity else null end),count(case when hits.ecommerceaction.action_type = '6' then hits.transaction.transactionid else null end)),0) as avg_quantity,
  ifnull(safe_divide(count(case when hits.ecommerceaction.action_type = '3' then fullvisitorid else null end),count(case when hits.ecommerceaction.action_type = '2' and product.isimpression is null then fullvisitorid else null end)),0) as cart_to_detail_rate,
  ifnull(safe_divide(count(case when hits.ecommerceaction.action_type = '6' then hits.transaction.transactionid else null end),count(case when hits.ecommerceaction.action_type = '2' and product.isimpression is null then fullvisitorid else null end)),0) as buy_to_detail_rate
from
  hits
  LEFT JOIN
  product on hits.item_productSku = products.productSKU
  LEFT JOIN
  sessions on hits.session_id = sessions.session_id

where
###
  sessions.totals_visits = 1
  and {{tableRange()}}
group by
  product
  -- ,productsku
  -- ,product_category
  -- ,productbrand
order by
  product_revenue desc
