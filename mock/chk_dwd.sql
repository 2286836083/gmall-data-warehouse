USE gmall;
SELECT 'dwd_user_behavior_di' AS t, COUNT(*) AS c FROM dwd_user_behavior_di;
SELECT 'dwd_order_fact_di' AS t, COUNT(*) AS c FROM dwd_order_fact_di;
SELECT 'dim_user' AS t, COUNT(*) AS c FROM dim_user;
SELECT 'dim_sku' AS t, COUNT(*) AS c FROM dim_sku;