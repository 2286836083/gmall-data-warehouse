USE gmall;
SET hive.exec.mode.local.auto=false;

-- ======================================================
-- 2.1 窗口函数
-- ======================================================
-- 每个品类下 GMV Top 3 的商品（TOP5 展示, 取自08-24）
SELECT * FROM (
    SELECT category_id, sku_id, gmv,
        ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY gmv DESC) AS rn
    FROM dws_sku_day
    WHERE dt = '2026-08-24'
) t WHERE rn <= 3 ORDER BY category_id, rn;

-- DAU 7 日滑动平均（数据仅5天,窗口从1天起累积）
SELECT dt, dau,
    AVG(dau) OVER (ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS dau_7day_avg
FROM ads_dau ORDER BY dt;

-- DAU 环比（今天 vs 昨天）
SELECT dt, dau, LAG(dau) OVER (ORDER BY dt) AS dau_yesterday,
    ROUND((dau - LAG(dau) OVER (ORDER BY dt)) * 1.0 / NULLIF(LAG(dau) OVER (ORDER BY dt), 0), 4) AS dau_mom
FROM ads_dau ORDER BY dt;

-- ======================================================
-- 2.2 CTE
-- ======================================================
-- 不用 CTE vs 用 CTE：某用户当天行为数 > 10
WITH user_behavior AS (
    SELECT user_id, COUNT(*) AS cnt FROM dwd_user_behavior_di
    WHERE dt = '2026-08-24' GROUP BY user_id
)
SELECT user_id, cnt FROM user_behavior WHERE cnt > 10 ORDER BY cnt DESC LIMIT 10;

-- 多 CTE 串联：漏斗各步骤人数（改为 JOIN 方式，规避 UDAF 内子查询）
WITH view_users AS (SELECT DISTINCT user_id FROM dwd_user_behavior_di WHERE dt='2026-08-24' AND event_type='view'),
cart_users AS (SELECT DISTINCT user_id FROM dwd_user_behavior_di WHERE dt='2026-08-24' AND event_type='cart'),
pay_users AS (SELECT DISTINCT user_id FROM dwd_user_behavior_di WHERE dt='2026-08-24' AND event_type='pay')
SELECT COUNT(DISTINCT v.user_id) AS view_uv,
       COUNT(DISTINCT c.user_id) AS cart_uv,
       COUNT(DISTINCT p.user_id) AS pay_uv
FROM view_users v
LEFT JOIN cart_users c ON v.user_id = c.user_id
LEFT JOIN pay_users p ON v.user_id = p.user_id;

-- ======================================================
-- 2.3 复杂 JOIN
-- ======================================================
-- 正确：过滤条件放 ON 里（保留所有行为），只看C01品类
SELECT b.user_id, b.sku_id, s.category_id, s.sku_name
FROM dwd_user_behavior_di b
LEFT JOIN dim_sku s ON b.sku_id = s.sku_id AND s.category_id = 'C01'
WHERE b.dt = '2026-08-24'
LIMIT 10;

-- 多表 JOIN：行为 + 品类 + 用户VIP（取08-24前10条）
SELECT /*+ STREAMTABLE(dwd_user_behavior_di) */
    b.user_id, b.event_type, s.category_id, u.vip_level
FROM dwd_user_behavior_di b
JOIN dim_sku s ON b.sku_id = s.sku_id
JOIN dim_user u ON b.user_id = u.user_id
WHERE b.dt = '2026-08-24'
LIMIT 10;