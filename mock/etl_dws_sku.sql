-- ds-tuned (small cluster)
SET hive.exec.mode.local.auto=false;
SET mapreduce.map.memory.mb=1024;
SET mapreduce.reduce.memory.mb=1024;
SET yarn.app.mapreduce.am.resource.mb=1024;
SET mapreduce.map.java.opts=-Xmx768m;
SET mapreduce.reduce.java.opts=-Xmx768m;
SET yarn.app.mapreduce.am.command-opts=-Xmx768m;
SET hive.exec.reducers.bytes.per.reducer=128000000;
USE gmall;
SET hive.exec.mode.local.auto=false;

-- ============ dws_sku_day : 商品主题宽表（5 天） ============
INSERT OVERWRITE TABLE dws_sku_day PARTITION (dt='2026-08-20')
SELECT
    b.sku_id,
    MAX(s.category_id) category_id,
    COUNT(CASE WHEN b.event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN b.event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN b.event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN b.event_type='pay'   THEN 1 END) pay_cnt,
    COALESCE(SUM(CASE WHEN o.is_paid=1 THEN o.total_amount ELSE 0 END),0) gmv
FROM dwd_user_behavior_di b
LEFT JOIN dim_sku s ON b.sku_id = s.sku_id
LEFT JOIN dwd_order_fact_di o ON b.user_id = o.user_id AND b.dt = o.dt AND o.is_paid = 1
WHERE b.dt = '2026-08-20' AND b.sku_id IS NOT NULL AND b.sku_id != ''
GROUP BY b.sku_id;

INSERT OVERWRITE TABLE dws_sku_day PARTITION (dt='2026-08-21')
SELECT
    b.sku_id,
    MAX(s.category_id) category_id,
    COUNT(CASE WHEN b.event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN b.event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN b.event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN b.event_type='pay'   THEN 1 END) pay_cnt,
    COALESCE(SUM(CASE WHEN o.is_paid=1 THEN o.total_amount ELSE 0 END),0) gmv
FROM dwd_user_behavior_di b
LEFT JOIN dim_sku s ON b.sku_id = s.sku_id
LEFT JOIN dwd_order_fact_di o ON b.user_id = o.user_id AND b.dt = o.dt AND o.is_paid = 1
WHERE b.dt = '2026-08-21' AND b.sku_id IS NOT NULL AND b.sku_id != ''
GROUP BY b.sku_id;

INSERT OVERWRITE TABLE dws_sku_day PARTITION (dt='2026-08-22')
SELECT
    b.sku_id,
    MAX(s.category_id) category_id,
    COUNT(CASE WHEN b.event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN b.event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN b.event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN b.event_type='pay'   THEN 1 END) pay_cnt,
    COALESCE(SUM(CASE WHEN o.is_paid=1 THEN o.total_amount ELSE 0 END),0) gmv
FROM dwd_user_behavior_di b
LEFT JOIN dim_sku s ON b.sku_id = s.sku_id
LEFT JOIN dwd_order_fact_di o ON b.user_id = o.user_id AND b.dt = o.dt AND o.is_paid = 1
WHERE b.dt = '2026-08-22' AND b.sku_id IS NOT NULL AND b.sku_id != ''
GROUP BY b.sku_id;

INSERT OVERWRITE TABLE dws_sku_day PARTITION (dt='2026-08-23')
SELECT
    b.sku_id,
    MAX(s.category_id) category_id,
    COUNT(CASE WHEN b.event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN b.event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN b.event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN b.event_type='pay'   THEN 1 END) pay_cnt,
    COALESCE(SUM(CASE WHEN o.is_paid=1 THEN o.total_amount ELSE 0 END),0) gmv
FROM dwd_user_behavior_di b
LEFT JOIN dim_sku s ON b.sku_id = s.sku_id
LEFT JOIN dwd_order_fact_di o ON b.user_id = o.user_id AND b.dt = o.dt AND o.is_paid = 1
WHERE b.dt = '2026-08-23' AND b.sku_id IS NOT NULL AND b.sku_id != ''
GROUP BY b.sku_id;

INSERT OVERWRITE TABLE dws_sku_day PARTITION (dt='2026-08-24')
SELECT
    b.sku_id,
    MAX(s.category_id) category_id,
    COUNT(CASE WHEN b.event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN b.event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN b.event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN b.event_type='pay'   THEN 1 END) pay_cnt,
    COALESCE(SUM(CASE WHEN o.is_paid=1 THEN o.total_amount ELSE 0 END),0) gmv
FROM dwd_user_behavior_di b
LEFT JOIN dim_sku s ON b.sku_id = s.sku_id
LEFT JOIN dwd_order_fact_di o ON b.user_id = o.user_id AND b.dt = o.dt AND o.is_paid = 1
WHERE b.dt = '2026-08-24' AND b.sku_id IS NOT NULL AND b.sku_id != ''
GROUP BY b.sku_id;