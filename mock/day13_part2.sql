USE gmall;
SET hive.exec.mode.local.auto=false;

-- ======================================================
-- 练习1：用户留存率（次日留存）
-- 说明：原文档引用了 ads_dau.new_dau_users 列，但本项目 ads_dau 无此列。
--       改为基于 dws_user_day 用 LEFT JOIN 计算"当天活跃且次日仍活跃"的比例。
--       08-24 时次日(08-25)无数据，故次日留存=今天活跃但在08-25也有行为的比例=0。
--       这里改用 08-23 -> 08-24 展示真实留存。
-- ======================================================
SELECT 
    a.dt,
    COUNT(DISTINCT a.user_id) AS today_active,
    COUNT(DISTINCT CASE WHEN b.user_id IS NOT NULL THEN a.user_id END) AS next_day_active,
    ROUND(COUNT(DISTINCT CASE WHEN b.user_id IS NOT NULL THEN a.user_id END) * 1.0 
          / NULLIF(COUNT(DISTINCT a.user_id), 0), 4) AS retention_rate
FROM dws_user_day a
LEFT JOIN dws_user_day b 
    ON a.user_id = b.user_id AND b.dt = DATE_ADD(a.dt, 1)
WHERE a.dt = '2026-08-23'
GROUP BY a.dt;

-- 多天留存：08-20 到 08-24 的次日留存
SELECT 
    a.dt,
    COUNT(DISTINCT a.user_id) AS today_active,
    COUNT(DISTINCT CASE WHEN b.user_id IS NOT NULL THEN a.user_id END) AS next_day_active,
    ROUND(COUNT(DISTINCT CASE WHEN b.user_id IS NOT NULL THEN a.user_id END) * 1.0 
          / NULLIF(COUNT(DISTINCT a.user_id), 0), 4) AS retention_rate
FROM dws_user_day a
LEFT JOIN dws_user_day b 
    ON a.user_id = b.user_id AND b.dt = DATE_ADD(a.dt, 1)
WHERE a.dt BETWEEN '2026-08-20' AND '2026-08-24'
GROUP BY a.dt ORDER BY a.dt;

-- ======================================================
-- 练习2：用户价值分层（RFM 简化版）
-- 说明：dws_user_day 按 user 聚合 pay_amount（一天可能有多次，累加）
-- ======================================================
WITH user_value AS (
    SELECT user_id, SUM(pay_amount) AS total_pay, MAX(dt) AS last_pay_dt
    FROM dws_user_day
    GROUP BY user_id
)
SELECT 
    CASE 
        WHEN total_pay >= 1000 THEN '高价值'
        WHEN total_pay >= 500 THEN '中价值'
        WHEN total_pay > 0 THEN '低价值'
        ELSE '无消费'
    END AS user_group,
    COUNT(*) AS user_cnt,
    ROUND(AVG(total_pay), 2) AS avg_pay
FROM user_value
GROUP BY 
    CASE 
        WHEN total_pay >= 1000 THEN '高价值'
        WHEN total_pay >= 500 THEN '中价值'
        WHEN total_pay > 0 THEN '低价值'
        ELSE '无消费'
    END;

-- ======================================================
-- 练习3：商品关联分析（买了 A 又买 B）
-- 说明：本项目行为表 event_type 需确认含 'pay'；若不含则改为 'order'。
--       先确认真实 event_type 取值。
-- ======================================================
SELECT DISTINCT event_type FROM dwd_user_behavior_di WHERE dt='2026-08-24';

-- 关联分析（基于支付行为）
WITH user_skus AS (
    SELECT DISTINCT user_id, sku_id FROM dwd_user_behavior_di
    WHERE event_type='pay'
)
SELECT a.sku_id AS sku_a, b.sku_id AS sku_b, COUNT(*) AS co_buy_cnt
FROM user_skus a
JOIN user_skus b ON a.user_id = b.user_id AND a.sku_id < b.sku_id
GROUP BY a.sku_id, b.sku_id
HAVING COUNT(*) >= 2
ORDER BY co_buy_cnt DESC
LIMIT 10;