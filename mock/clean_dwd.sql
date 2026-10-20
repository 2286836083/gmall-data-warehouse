USE gmall;
-- ============================================================
-- DWD 数据清洗（节点3 clean_dwd 一键脚本）
-- 顺序很重要：先建维度表 dim_sku / dim_user，
-- 因为行为明细表要 LEFT JOIN dim_sku 补品类维度
-- ============================================================
SET hive.exec.mode.local.auto=false;

-- ============ 第一部分：维度表 ============
-- 1. dim_sku
INSERT OVERWRITE TABLE dim_sku
SELECT sku_id, MAX(sku_name) sku_name, MAX(category_id) category_id, MAX(category_name) category_name, MAX(brand_name) brand_name, MAX(price) price
FROM ods_sku_info_di GROUP BY sku_id;

-- 2. dim_user（第一次：生成用户画像字段）
INSERT OVERWRITE TABLE dim_user
SELECT user_id, '2026-01-01' register_date,
  CASE WHEN rand()<0.5 THEN '男' ELSE '女' END gender,
  CASE WHEN rand()<0.25 THEN '18-24' WHEN rand()<0.5 THEN '25-34' WHEN rand()<0.75 THEN '35-44' ELSE '45+' END age_range,
  CASE WHEN rand()<0.3 THEN '一线' WHEN rand()<0.6 THEN '二线' WHEN rand()<0.8 THEN '三线' ELSE '其他' END city_level,
  CASE WHEN rand()<0.7 THEN '普通' WHEN rand()<0.9 THEN '银卡' WHEN rand()<0.97 THEN '金卡' ELSE '黑卡' END vip_level,
  NULL first_order_date
FROM (SELECT DISTINCT user_id FROM ods_user_log_di UNION SELECT DISTINCT user_id FROM ods_order_info_di) t;

-- 3. 回填 dim_user 首单日期
INSERT OVERWRITE TABLE dim_user
SELECT d.user_id, d.register_date, d.gender, d.age_range, d.city_level, d.vip_level, f.first_order_date
FROM dim_user d
LEFT JOIN (SELECT user_id, MIN(substr(create_time,1,10)) first_order_date FROM ods_order_info_di WHERE create_time IS NOT NULL AND create_time!='' GROUP BY user_id) f
ON d.user_id = f.user_id;

-- ============ 第二部分：行为明细（5 天） ============
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

-- ============ 第三部分：订单事实（5 天） ============
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
