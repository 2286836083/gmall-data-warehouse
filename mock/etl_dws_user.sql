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

-- ============ dws_user_day : 用户主题宽表（5 天） ============
INSERT OVERWRITE TABLE dws_user_day PARTITION (dt='2026-08-20')
SELECT
    user_id,
    COUNT(CASE WHEN event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN event_type='pay'   THEN 1 END) pay_cnt,
    SUM(CASE WHEN event_type='pay' THEN o.total_amount ELSE 0 END) pay_amount,
    COUNT(DISTINCT session_id) session_cnt,
    MIN(event_time) first_event_time,
    MAX(event_time) last_event_time
FROM dwd_user_behavior_di ub
LEFT JOIN (SELECT user_id, total_amount FROM dwd_order_fact_di WHERE dt='2026-08-20' AND is_paid=1) o
    ON ub.user_id = o.user_id
WHERE ub.dt = '2026-08-20'
GROUP BY ub.user_id;

INSERT OVERWRITE TABLE dws_user_day PARTITION (dt='2026-08-21')
SELECT
    user_id,
    COUNT(CASE WHEN event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN event_type='pay'   THEN 1 END) pay_cnt,
    SUM(CASE WHEN event_type='pay' THEN o.total_amount ELSE 0 END) pay_amount,
    COUNT(DISTINCT session_id) session_cnt,
    MIN(event_time) first_event_time,
    MAX(event_time) last_event_time
FROM dwd_user_behavior_di ub
LEFT JOIN (SELECT user_id, total_amount FROM dwd_order_fact_di WHERE dt='2026-08-21' AND is_paid=1) o
    ON ub.user_id = o.user_id
WHERE ub.dt = '2026-08-21'
GROUP BY ub.user_id;

INSERT OVERWRITE TABLE dws_user_day PARTITION (dt='2026-08-22')
SELECT
    user_id,
    COUNT(CASE WHEN event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN event_type='pay'   THEN 1 END) pay_cnt,
    SUM(CASE WHEN event_type='pay' THEN o.total_amount ELSE 0 END) pay_amount,
    COUNT(DISTINCT session_id) session_cnt,
    MIN(event_time) first_event_time,
    MAX(event_time) last_event_time
FROM dwd_user_behavior_di ub
LEFT JOIN (SELECT user_id, total_amount FROM dwd_order_fact_di WHERE dt='2026-08-22' AND is_paid=1) o
    ON ub.user_id = o.user_id
WHERE ub.dt = '2026-08-22'
GROUP BY ub.user_id;

INSERT OVERWRITE TABLE dws_user_day PARTITION (dt='2026-08-23')
SELECT
    user_id,
    COUNT(CASE WHEN event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN event_type='pay'   THEN 1 END) pay_cnt,
    SUM(CASE WHEN event_type='pay' THEN o.total_amount ELSE 0 END) pay_amount,
    COUNT(DISTINCT session_id) session_cnt,
    MIN(event_time) first_event_time,
    MAX(event_time) last_event_time
FROM dwd_user_behavior_di ub
LEFT JOIN (SELECT user_id, total_amount FROM dwd_order_fact_di WHERE dt='2026-08-23' AND is_paid=1) o
    ON ub.user_id = o.user_id
WHERE ub.dt = '2026-08-23'
GROUP BY ub.user_id;

INSERT OVERWRITE TABLE dws_user_day PARTITION (dt='2026-08-24')
SELECT
    user_id,
    COUNT(CASE WHEN event_type='view'  THEN 1 END) pv_cnt,
    COUNT(CASE WHEN event_type='cart'  THEN 1 END) cart_cnt,
    COUNT(CASE WHEN event_type='order' THEN 1 END) order_cnt,
    COUNT(CASE WHEN event_type='pay'   THEN 1 END) pay_cnt,
    SUM(CASE WHEN event_type='pay' THEN o.total_amount ELSE 0 END) pay_amount,
    COUNT(DISTINCT session_id) session_cnt,
    MIN(event_time) first_event_time,
    MAX(event_time) last_event_time
FROM dwd_user_behavior_di ub
LEFT JOIN (SELECT user_id, total_amount FROM dwd_order_fact_di WHERE dt='2026-08-24' AND is_paid=1) o
    ON ub.user_id = o.user_id
WHERE ub.dt = '2026-08-24'
GROUP BY ub.user_id;