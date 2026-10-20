-- ============================================================
-- 新链路（真实数据 REES46）DWS / ADS 计算
--
-- 前置：先执行 evt_ddl.sql 建表；再确保 DWD 三张表已就绪
--       （gmall.dwd_event_log_di / dwd_purchase_fact_di / dim_product）
--
-- 【相对旧链路的四个实质性改进】——这些就是本次的技术含金量
--
--   改进 1｜动态分区替代硬编码
--     旧脚本：5 天写成 5 条 INSERT（PARTITION (dt='2026-08-20') 重复 5 遍）
--     新脚本：PARTITION (dt) 动态写，**31 天只需 1 条语句**
--     意义：旧写法扩展到 31 天要复制 31 遍，完全不可维护
--
--   改进 2｜窗口函数一次算完，替代逐日 JOIN 历史
--     旧 ads_dau：每一天都要 LEFT JOIN「历史所有天」+「昨天」，复杂度 O(n²)
--     新做法：MIN(dt) OVER (PARTITION BY user_id) 求首次活跃日、
--             LAG(dt) OVER (...) 求上一次活跃日 → **一次扫描产出全部口径**
--     意义：这同时让 new_dau / wakeup_dau **从恒为 0 变成真实值**
--           （旧链路是模拟数据，每天都是同一批用户，算出来必然是 0）
--
--   改进 3｜先聚合再关联维表，而不是先关联再聚合
--     错误写法：10.6M 行 LEFT JOIN dim_product 之后再 GROUP BY（多一次大表 shuffle）
--     正确写法：先 GROUP BY 到「商品 × 天」小表，再关 14 万行维表
--
--   改进 4｜★ 拒绝不可逆的聚合（这条最容易错）
--     类目层的「去重买家数」**不能**从商品层 SUM 加总 ——
--     同一个用户买了 A、B 两个商品，在商品层各算 1 次，加总就变成 2。
--     所以类目层必须**回 DWD 明细重新 COUNT(DISTINCT user_id)**。
--     这正是"聚合不可逆"的典型：SUM 可加，COUNT(DISTINCT) 不可加。
--
-- 执行：beeline -u jdbc:hive2://lrh-1:10000 -f evt_etl.sql
-- ============================================================


-- ===== STEP 1 / DWS·用户主题（dws_user_event_day）=====
INSERT OVERWRITE TABLE gmall.dws_user_event_day PARTITION (dt)
SELECT
  user_id,
  COUNT(CASE WHEN event_type = 'view'     THEN 1 END) AS pv_cnt,
  COUNT(CASE WHEN event_type = 'cart'     THEN 1 END) AS cart_cnt,
  COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_cnt,
  SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS pay_amount,
  COUNT(DISTINCT user_session)  AS session_cnt,
  MIN(event_time)               AS first_event_time,
  MAX(event_time)               AS last_event_time,
  dt
FROM gmall.dwd_event_log_di
GROUP BY user_id, dt;


-- ===== STEP 2 / DWS·商品主题（dws_product_event_day）=====
-- 改进 3：先在明细层聚合到「商品 × 天」，再关联 14 万行的 dim_product
INSERT OVERWRITE TABLE gmall.dws_product_event_day PARTITION (dt)
SELECT
  a.product_id,
  p.category_l1,
  p.category_l2,
  p.category_l3,
  p.brand,
  a.pv_cnt,
  a.cart_cnt,
  a.purchase_cnt,
  a.gmv,
  a.buyer_cnt,
  a.dt
FROM (
  SELECT
    product_id,
    dt,
    COUNT(CASE WHEN event_type = 'view'     THEN 1 END) AS pv_cnt,
    COUNT(CASE WHEN event_type = 'cart'     THEN 1 END) AS cart_cnt,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_cnt,
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS gmv,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS buyer_cnt
  FROM gmall.dwd_event_log_di
  GROUP BY product_id, dt
) a
LEFT JOIN gmall.dim_product p ON a.product_id = p.product_id;


-- ===== STEP 3 / DWS·类目主题（dws_category_event_day）=====
-- 改进 4：买家数必须回明细重算，不能从商品层 SUM（COUNT(DISTINCT) 不可加）
-- 粒度选 category_l1 + category_l2，不取三级 —— 三级在 166,794 个商品上过于碎片化
INSERT OVERWRITE TABLE gmall.dws_category_event_day PARTITION (dt)
SELECT
  category_l1,
  category_l2,
  COUNT(CASE WHEN event_type = 'view'     THEN 1 END) AS pv_cnt,
  COUNT(CASE WHEN event_type = 'cart'     THEN 1 END) AS cart_cnt,
  COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_cnt,
  SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS gmv,
  COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS buyer_cnt,
  dt
FROM gmall.dwd_event_log_di
GROUP BY category_l1, category_l2, dt;


-- ===== STEP 4 / DWS·交易主题（dws_trade_day）=====
-- 交易类指标从「购买事实表」出，而不是每次去事件流里筛 purchase —— 语义更清晰
INSERT OVERWRITE TABLE gmall.dws_trade_day PARTITION (dt)
SELECT
  COUNT(1)                                  AS order_cnt,
  COUNT(DISTINCT user_id)                   AS buyer_cnt,
  SUM(amount)                               AS gmv,
  ROUND(SUM(amount) / COUNT(1), 2)          AS avg_order_amount,
  COUNT(DISTINCT product_id)                AS product_cnt,
  dt
FROM gmall.dwd_purchase_fact_di
GROUP BY dt;


-- ===== STEP 5 / ADS·活跃用户（ads_active_user_day）=====
-- 改进 2：一次扫描算出 DAU / 新增 / 连续活跃 / 回流
--   first_dt = 该用户首次活跃日     → new_dau：首次活跃就是当天
--   prev_dt  = 该用户上一次活跃日   → active_dau：上一次活跃正好是昨天（连续）
--                                  → wakeup_dau：非首次，且上一次活跃不是昨天（回流）
INSERT OVERWRITE TABLE gmall.ads_active_user_day
SELECT
  t.dt,
  COUNT(1)                                                             AS dau,
  COUNT(CASE WHEN t.first_dt = t.dt THEN 1 END)                        AS new_dau,
  COUNT(CASE WHEN t.prev_dt  = CAST(DATE_SUB(t.dt, 1) AS STRING) THEN 1 END) AS active_dau,
  COUNT(CASE WHEN t.first_dt < t.dt
              AND (t.prev_dt IS NULL OR t.prev_dt <> CAST(DATE_SUB(t.dt, 1) AS STRING))
             THEN 1 END)                                               AS wakeup_dau
FROM (
  SELECT
    user_id,
    dt,
    MIN(dt) OVER (PARTITION BY user_id)                       AS first_dt,
    LAG(dt) OVER (PARTITION BY user_id ORDER BY dt)           AS prev_dt
  FROM gmall.dws_user_event_day
) t
GROUP BY t.dt;


-- ===== STEP 6 / ADS·转化漏斗（ads_funnel_day）=====
-- dws_user_event_day 已是「一行 = 一个用户一天」，所以直接 COUNT 即可，不需要再 DISTINCT
INSERT OVERWRITE TABLE gmall.ads_funnel_day
SELECT
  dt,
  COUNT(CASE WHEN pv_cnt > 0         THEN 1 END) AS view_uv,
  COUNT(CASE WHEN cart_cnt > 0       THEN 1 END) AS cart_uv,
  COUNT(CASE WHEN purchase_cnt > 0   THEN 1 END) AS purchase_uv,
  ROUND(COUNT(CASE WHEN cart_cnt > 0 THEN 1 END) * 1.0
        / NULLIF(COUNT(CASE WHEN pv_cnt > 0 THEN 1 END), 0), 4)     AS cart_rate,
  ROUND(COUNT(CASE WHEN purchase_cnt > 0 THEN 1 END) * 1.0
        / NULLIF(COUNT(CASE WHEN cart_cnt > 0 THEN 1 END), 0), 4)   AS cart_to_purchase_rate,
  ROUND(COUNT(CASE WHEN purchase_cnt > 0 THEN 1 END) * 1.0
        / NULLIF(COUNT(CASE WHEN pv_cnt > 0 THEN 1 END), 0), 4)     AS overall_rate
FROM gmall.dws_user_event_day
GROUP BY dt;


-- ===== STEP 7 / ADS·累计复购率（ads_repurchase_day）=====
-- 用窗口函数累计每个用户的购买次数，再按天统计「累计买过 ≥2 次」的用户占比
-- 旧做法：先建中间表按 (user,dt) 聚合，再逐日 SUM 一遍 —— 本次改成一次扫描
INSERT OVERWRITE TABLE gmall.ads_repurchase_day
SELECT
  dt,
  COUNT(1)                                              AS total_buyers,
  COUNT(CASE WHEN cum_orders >= 2 THEN 1 END)           AS repurchase_buyers,
  ROUND(COUNT(CASE WHEN cum_orders >= 2 THEN 1 END) * 1.0
        / NULLIF(COUNT(1), 0), 4)                       AS repurchase_rate
FROM (
  SELECT
    user_id,
    dt,
    SUM(purchase_cnt) OVER (
      PARTITION BY user_id ORDER BY dt
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_orders
  FROM gmall.dws_user_event_day
  WHERE purchase_cnt > 0
) t
GROUP BY dt;


-- ===== STEP 8 / ADS·TopN 商品（ads_top_product_day）=====
-- 每天只留 GMV 前 50（旧表每天 500 行，导出到 CSV 时还要再截 TopN，属于白导）
INSERT OVERWRITE TABLE gmall.ads_top_product_day
SELECT
  dt, rank_num, product_id, brand, category_l1, category_l2, gmv, purchase_cnt, buyer_cnt
FROM (
  SELECT
    dt,
    product_id,
    brand,
    category_l1,
    category_l2,
    gmv,
    purchase_cnt,
    buyer_cnt,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY gmv DESC, purchase_cnt DESC) AS rank_num
  FROM gmall.dws_product_event_day
  WHERE gmv > 0
) t
WHERE rank_num <= 50;
