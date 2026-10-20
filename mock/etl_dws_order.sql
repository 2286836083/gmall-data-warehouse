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

-- ============ dws_order_day : 订单主题宽表（5 天） ============
INSERT OVERWRITE TABLE dws_order_day PARTITION (dt='2026-08-20')
SELECT
    COUNT(*) total_orders,
    SUM(CASE WHEN order_status='已支付' THEN 1 ELSE 0 END) paid_orders,
    SUM(CASE WHEN order_status='已取消' THEN 1 ELSE 0 END) cancelled_orders,
    SUM(CASE WHEN order_status='已退款' THEN 1 ELSE 0 END) refunded_orders,
    SUM(total_amount) total_amount,
    SUM(CASE WHEN is_paid=1 THEN total_amount ELSE 0 END) paid_amount,
    COUNT(DISTINCT user_id) unique_buyers,
    COUNT(DISTINCT CASE WHEN is_paid=1 THEN user_id END) unique_payers
FROM dwd_order_fact_di
WHERE dt='2026-08-20';

INSERT OVERWRITE TABLE dws_order_day PARTITION (dt='2026-08-21')
SELECT
    COUNT(*) total_orders,
    SUM(CASE WHEN order_status='已支付' THEN 1 ELSE 0 END) paid_orders,
    SUM(CASE WHEN order_status='已取消' THEN 1 ELSE 0 END) cancelled_orders,
    SUM(CASE WHEN order_status='已退款' THEN 1 ELSE 0 END) refunded_orders,
    SUM(total_amount) total_amount,
    SUM(CASE WHEN is_paid=1 THEN total_amount ELSE 0 END) paid_amount,
    COUNT(DISTINCT user_id) unique_buyers,
    COUNT(DISTINCT CASE WHEN is_paid=1 THEN user_id END) unique_payers
FROM dwd_order_fact_di
WHERE dt='2026-08-21';

INSERT OVERWRITE TABLE dws_order_day PARTITION (dt='2026-08-22')
SELECT
    COUNT(*) total_orders,
    SUM(CASE WHEN order_status='已支付' THEN 1 ELSE 0 END) paid_orders,
    SUM(CASE WHEN order_status='已取消' THEN 1 ELSE 0 END) cancelled_orders,
    SUM(CASE WHEN order_status='已退款' THEN 1 ELSE 0 END) refunded_orders,
    SUM(total_amount) total_amount,
    SUM(CASE WHEN is_paid=1 THEN total_amount ELSE 0 END) paid_amount,
    COUNT(DISTINCT user_id) unique_buyers,
    COUNT(DISTINCT CASE WHEN is_paid=1 THEN user_id END) unique_payers
FROM dwd_order_fact_di
WHERE dt='2026-08-22';

INSERT OVERWRITE TABLE dws_order_day PARTITION (dt='2026-08-23')
SELECT
    COUNT(*) total_orders,
    SUM(CASE WHEN order_status='已支付' THEN 1 ELSE 0 END) paid_orders,
    SUM(CASE WHEN order_status='已取消' THEN 1 ELSE 0 END) cancelled_orders,
    SUM(CASE WHEN order_status='已退款' THEN 1 ELSE 0 END) refunded_orders,
    SUM(total_amount) total_amount,
    SUM(CASE WHEN is_paid=1 THEN total_amount ELSE 0 END) paid_amount,
    COUNT(DISTINCT user_id) unique_buyers,
    COUNT(DISTINCT CASE WHEN is_paid=1 THEN user_id END) unique_payers
FROM dwd_order_fact_di
WHERE dt='2026-08-23';

INSERT OVERWRITE TABLE dws_order_day PARTITION (dt='2026-08-24')
SELECT
    COUNT(*) total_orders,
    SUM(CASE WHEN order_status='已支付' THEN 1 ELSE 0 END) paid_orders,
    SUM(CASE WHEN order_status='已取消' THEN 1 ELSE 0 END) cancelled_orders,
    SUM(CASE WHEN order_status='已退款' THEN 1 ELSE 0 END) refunded_orders,
    SUM(total_amount) total_amount,
    SUM(CASE WHEN is_paid=1 THEN total_amount ELSE 0 END) paid_amount,
    COUNT(DISTINCT user_id) unique_buyers,
    COUNT(DISTINCT CASE WHEN is_paid=1 THEN user_id END) unique_payers
FROM dwd_order_fact_di
WHERE dt='2026-08-24';