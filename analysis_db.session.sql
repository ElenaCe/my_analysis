
-- Looking into the data

-- -- How many records per event_type? 
-- select 
--     count(*) as record_count, 
--     event_type 
-- FROM ecommerce_schema.oct_dec_19
-- group by event_type;

-- -- Checking for records where user_id is null
-- results show none have value null
-- select 
--     sum(case when user_id is null then 1 else 0 end) as record_user_id_null_count, 
--     sum(case when user_id is not null then 1 else 0 end) as record_user_id_not_null_count, 
--     event_type 
-- FROM ecommerce_schema.oct_dec_19
-- group by event_type;

-- WITH user_count_per_action AS (
--     SELECT 
--         COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS user_view_count,
--         COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS user_add_cart_count,
--         COUNT(DISTINCT CASE WHEN event_type = 'remove_from_cart' THEN user_id END) AS user_remove_cart_count,
--         COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS user_purchase_count
--     FROM ecommerce_schema.oct_dec_19
-- )
-- SELECT 
--     (user_add_cart_count * 100) / (user_view_count) AS conv_add_cart_from_view,
--     (user_purchase_count * 100) / (user_add_cart_count) AS conv_purchase_from_add_cart,
--     (user_remove_cart_count * 100) / (user_add_cart_count) AS percentage_user_remove_from_cart,
--     (user_purchase_count * 100) / (user_view_count) AS conv_purchase_from_view_cart
-- from user_count_per_action;

-- select *     FROM ecommerce_schema.oct_dec_19 limit 1;

with 
first_calculation  AS (
SELECT 
   date_trunc('month', event_time) as event_month,
   event_type as action_name,
   count(DISTINCT product_id) as product_count,
   count(DISTINCT user_id) as user_count,
   count(DISTINCT user_session) as session_count,
   round(avg(price),2) as price_avg,
   sum(price) as price_tot

FROM ecommerce_schema.oct_dec_19
GROUP BY 
    event_month,
    action_name
)
select
    *,
    round(product_count * 1.0 / user_count,0) as avg_product_per_user,
    round(session_count * 1.0 / user_count,0) as avg_session_per_user
from first_calculation
