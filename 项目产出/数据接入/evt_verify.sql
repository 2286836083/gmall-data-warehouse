-- ============================================================
-- 新链路 DWS / ADS 校验脚本
--
-- 三层校验，从"数对不对"到"方向对不对"：
--   第一层：行数与分区数（结构完整）
--   第二层：交叉对账（同一批 purchase 事件，按三种维度分组的 GMV 必须相等）
--   第三层：方向性校验（漏斗必须逐级收窄、复购用户不可能多于总买家）
--
-- 第二层是本次最有价值的校验：它证明「商品/类目/交易」三条独立计算路径
-- 最终收敛到同一个金额上，任何一条路径写错都会在这里暴露。
-- ============================================================

-- ---------- 第一层：行数与分区数 ----------
SELECT 'dws_user_event_day'    AS tbl, COUNT(DISTINCT dt) AS part_cnt, COUNT(1) AS row_cnt FROM gmall.dws_user_event_day
UNION ALL SELECT 'dws_product_event_day',  COUNT(DISTINCT dt), COUNT(1) FROM gmall.dws_product_event_day
UNION ALL SELECT 'dws_category_event_day', COUNT(DISTINCT dt), COUNT(1) FROM gmall.dws_category_event_day
UNION ALL SELECT 'dws_trade_day',          COUNT(DISTINCT dt), COUNT(1) FROM gmall.dws_trade_day
UNION ALL SELECT 'ads_active_user_day',    COUNT(DISTINCT dt), COUNT(1) FROM gmall.ads_active_user_day
UNION ALL SELECT 'ads_funnel_day',         COUNT(DISTINCT dt), COUNT(1) FROM gmall.ads_funnel_day
UNION ALL SELECT 'ads_repurchase_day',     COUNT(DISTINCT dt), COUNT(1) FROM gmall.ads_repurchase_day
UNION ALL SELECT 'ads_top_product_day',    COUNT(DISTINCT dt), COUNT(1) FROM gmall.ads_top_product_day;

-- ---------- 第二层：交叉对账（GMV 三路必须相等） ----------
-- 维度不同、算法不同，但算的是同一批 purchase 事件，合计必须一致
SELECT '商品维度 GMV'     AS item, ROUND(SUM(gmv), 2)    AS val FROM gmall.dws_product_event_day
UNION ALL SELECT '类目维度 GMV',    ROUND(SUM(gmv), 2)    FROM gmall.dws_category_event_day
UNION ALL SELECT '交易维度 GMV',    ROUND(SUM(gmv), 2)    FROM gmall.dws_trade_day
UNION ALL SELECT 'DWD事实表 GMV',   ROUND(SUM(amount), 2) FROM gmall.dwd_purchase_fact_di;

-- ---------- 第三层：方向性校验（前两项结果必须为 0；第三项不是违例，是业务现象） ----------
-- ⚠️ 2026-09-28 修正：原来写成「浏览≥加购 AND 加购≥购买」一条判断，结果 31 天里 15 天"违例"。
--    实测拆开后才看清：
--      · 浏览 < 加购 的天数 = 0    → 浏览 ≥ 加购 恒成立
--      · 浏览 < 购买 的天数 = 0    → 浏览 ≥ 购买 恒成立
--      · 购买 > 加购 的天数 = 15   → 加购与购买**不构成嵌套关系**
--    原因是用户可以「不进购物车直接购买」，所以加购不是购买的必经环节。
--    全月加购用户日 119,832 vs 购买用户日 133,258，购买反而多 11%。
--    → 真正的漏斗是「浏览 → 购买」两环节；加购是**并行的另一条路径**，不是必经中间态。
SELECT '浏览<加购 的违例天数' AS item, COUNT(1) AS val FROM gmall.ads_funnel_day
WHERE view_uv < cart_uv
UNION ALL
SELECT '浏览<购买 的违例天数', COUNT(1) FROM gmall.ads_funnel_day
WHERE view_uv < purchase_uv
UNION ALL
-- 复购用户数不可能多于总买家数
SELECT '复购数>总买家数的违例天数', COUNT(1) FROM gmall.ads_repurchase_day
WHERE repurchase_buyers > total_buyers
UNION ALL
-- 活跃用户的口径关系：new + active + wakeup 不应超过 dau
SELECT '分项之和>DAU 的违例天数', COUNT(1) FROM gmall.ads_active_user_day
WHERE (new_dau + active_dau + wakeup_dau) > dau
UNION ALL
-- ↓ 下面这条**不是违例**，是真实业务现象，列出来供业务解释用
SELECT '【业务现象·非违例】购买>加购 的天数', COUNT(1) FROM gmall.ads_funnel_day
WHERE purchase_uv > cart_uv;
