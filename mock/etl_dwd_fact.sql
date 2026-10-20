USE gmall;
SET hive.exec.mode.local.auto=false;

-- ============ 清洗行为明细（5 天） ============
INSERT OVERWRITE TABLE dwd_user_behavior_di PARTITION (dt='2026-08-20')
SELECT 
    CONCAT(o.user_id, '_', CAST(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss') AS STRING)) AS behavior_id,
    o.user_id,
    o.event_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss')) AS event_time,
    o.sku_id,
    COALESCE(s.category_id, 'unknown') AS category_id,
    o.session_id
FROM ods_user_log_di o
LEFT JOIN dim_sku s ON o.sku_id = s.sku_id
WHERE o.dt = '2026-08-20'
  AND o.user_id IS NOT NULL AND o.user_id != ''
  AND o.event_type IN ('view', 'cart', 'order', 'pay');

INSERT OVERWRITE TABLE dwd_user_behavior_di PARTITION (dt='2026-08-21')
SELECT 
    CONCAT(o.user_id, '_', CAST(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss') AS STRING)) AS behavior_id,
    o.user_id,
    o.event_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss')) AS event_time,
    o.sku_id,
    COALESCE(s.category_id, 'unknown') AS category_id,
    o.session_id
FROM ods_user_log_di o
LEFT JOIN dim_sku s ON o.sku_id = s.sku_id
WHERE o.dt = '2026-08-21'
  AND o.user_id IS NOT NULL AND o.user_id != ''
  AND o.event_type IN ('view', 'cart', 'order', 'pay');

INSERT OVERWRITE TABLE dwd_user_behavior_di PARTITION (dt='2026-08-22')
SELECT 
    CONCAT(o.user_id, '_', CAST(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss') AS STRING)) AS behavior_id,
    o.user_id,
    o.event_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss')) AS event_time,
    o.sku_id,
    COALESCE(s.category_id, 'unknown') AS category_id,
    o.session_id
FROM ods_user_log_di o
LEFT JOIN dim_sku s ON o.sku_id = s.sku_id
WHERE o.dt = '2026-08-22'
  AND o.user_id IS NOT NULL AND o.user_id != ''
  AND o.event_type IN ('view', 'cart', 'order', 'pay');

INSERT OVERWRITE TABLE dwd_user_behavior_di PARTITION (dt='2026-08-23')
SELECT 
    CONCAT(o.user_id, '_', CAST(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss') AS STRING)) AS behavior_id,
    o.user_id,
    o.event_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss')) AS event_time,
    o.sku_id,
    COALESCE(s.category_id, 'unknown') AS category_id,
    o.session_id
FROM ods_user_log_di o
LEFT JOIN dim_sku s ON o.sku_id = s.sku_id
WHERE o.dt = '2026-08-23'
  AND o.user_id IS NOT NULL AND o.user_id != ''
  AND o.event_type IN ('view', 'cart', 'order', 'pay');

INSERT OVERWRITE TABLE dwd_user_behavior_di PARTITION (dt='2026-08-24')
SELECT 
    CONCAT(o.user_id, '_', CAST(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss') AS STRING)) AS behavior_id,
    o.user_id,
    o.event_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP(o.log_time, 'yyyy-MM-dd HH:mm:ss')) AS event_time,
    o.sku_id,
    COALESCE(s.category_id, 'unknown') AS category_id,
    o.session_id
FROM ods_user_log_di o
LEFT JOIN dim_sku s ON o.sku_id = s.sku_id
WHERE o.dt = '2026-08-24'
  AND o.user_id IS NOT NULL AND o.user_id != ''
  AND o.event_type IN ('view', 'cart', 'order', 'pay');

-- ============ 清洗订单事实（5 天） ============
INSERT OVERWRITE TABLE dwd_order_fact_di PARTITION (dt='2026-08-20')
SELECT 
    order_id, user_id,
    CASE order_status
        WHEN '已付款' THEN '已支付'
        WHEN '付款成功' THEN '已支付'
        WHEN '取消' THEN '已取消'
        ELSE order_status
    END AS order_status,
    total_amount,
    FROM_UNIXTIME(UNIX_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss')) AS create_time,
    CASE WHEN pay_time IS NOT NULL AND pay_time != '' THEN FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss')) ELSE NULL END AS pay_time,
    CASE WHEN order_status IN ('已支付','已付款','付款成功') THEN 1 ELSE 0 END AS is_paid
FROM ods_order_info_di
WHERE dt='2026-08-20' AND user_id IS NOT NULL AND user_id != '' AND total_amount >= 0;

INSERT OVERWRITE TABLE dwd_order_fact_di PARTITION (dt='2026-08-21')
SELECT 
    order_id, user_id,
    CASE order_status
        WHEN '已付款' THEN '已支付'
        WHEN '付款成功' THEN '已支付'
        WHEN '取消' THEN '已取消'
        ELSE order_status
    END AS order_status,
    total_amount,
    FROM_UNIXTIME(UNIX_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss')) AS create_time,
    CASE WHEN pay_time IS NOT NULL AND pay_time != '' THEN FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss')) ELSE NULL END AS pay_time,
    CASE WHEN order_status IN ('已支付','已付款','付款成功') THEN 1 ELSE 0 END AS is_paid
FROM ods_order_info_di
WHERE dt='2026-08-21' AND user_id IS NOT NULL AND user_id != '' AND total_amount >= 0;

INSERT OVERWRITE TABLE dwd_order_fact_di PARTITION (dt='2026-08-22')
SELECT 
    order_id, user_id,
    CASE order_status
        WHEN '已付款' THEN '已支付'
        WHEN '付款成功' THEN '已支付'
        WHEN '取消' THEN '已取消'
        ELSE order_status
    END AS order_status,
    total_amount,
    FROM_UNIXTIME(UNIX_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss')) AS create_time,
    CASE WHEN pay_time IS NOT NULL AND pay_time != '' THEN FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss')) ELSE NULL END AS pay_time,
    CASE WHEN order_status IN ('已支付','已付款','付款成功') THEN 1 ELSE 0 END AS is_paid
FROM ods_order_info_di
WHERE dt='2026-08-22' AND user_id IS NOT NULL AND user_id != '' AND total_amount >= 0;

INSERT OVERWRITE TABLE dwd_order_fact_di PARTITION (dt='2026-08-23')
SELECT 
    order_id, user_id,
    CASE order_status
        WHEN '已付款' THEN '已支付'
        WHEN '付款成功' THEN '已支付'
        WHEN '取消' THEN '已取消'
        ELSE order_status
    END AS order_status,
    total_amount,
    FROM_UNIXTIME(UNIX_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss')) AS create_time,
    CASE WHEN pay_time IS NOT NULL AND pay_time != '' THEN FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss')) ELSE NULL END AS pay_time,
    CASE WHEN order_status IN ('已支付','已付款','付款成功') THEN 1 ELSE 0 END AS is_paid
FROM ods_order_info_di
WHERE dt='2026-08-23' AND user_id IS NOT NULL AND user_id != '' AND total_amount >= 0;

INSERT OVERWRITE TABLE dwd_order_fact_di PARTITION (dt='2026-08-24')
SELECT 
    order_id, user_id,
    CASE order_status
        WHEN '已付款' THEN '已支付'
        WHEN '付款成功' THEN '已支付'
        WHEN '取消' THEN '已取消'
        ELSE order_status
    END AS order_status,
    total_amount,
    FROM_UNIXTIME(UNIX_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss')) AS create_time,
    CASE WHEN pay_time IS NOT NULL AND pay_time != '' THEN FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss')) ELSE NULL END AS pay_time,
    CASE WHEN order_status IN ('已支付','已付款','付款成功') THEN 1 ELSE 0 END AS is_paid
FROM ods_order_info_di
WHERE dt='2026-08-24' AND user_id IS NOT NULL AND user_id != '' AND total_amount >= 0;