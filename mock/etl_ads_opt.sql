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
-- ============================================================
-- ADS 指标计算（优化版）
-- 优化思路（边开发边学）：
--   [经验1] 不用本地模式：本环境本地模式（job_local）对 JOIN+聚合查询
--           会偶发 return code 2（MapRedTask），故保持 YARN 分布式执行更稳。
--   [优化1] 转化漏斗 5 天是同一表 dws_user_day 的纯聚合 -> 用 GROUP BY dt
--           合并为【一次作业】产出 5 天，替代原来的 5 次 INSERT（5 次全表扫描）。
--   [优化2] 复购率用"中间结果表"：先把 dwd_order_fact_di 按 (user_id,dt) 聚合
--           到 tmp 小表，避免 5 次全表扫描事实表，再基于小表逐日累计。
--   [优化3] DAU 用 LEFT JOIN 派生表方式，规避"Hive 聚合函数内不支持子查询"报错。
-- ============================================================

SET hive.exec.mode.local.auto=false;

-- ---------- 指标一：DAU（ads_dau，非分区表） ----------
-- 思路：当天活跃用户 t，LEFT JOIN 历史/昨日，分别打 new/active/wakeup 标记
TRUNCATE TABLE ads_dau;
-- 08-20 首日：无历史、无昨日
INSERT INTO ads_dau
SELECT '2026-08-20',
  COUNT(DISTINCT t.user_id) dau, 0 new_dau, 0 active_dau, 0 wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-20' AND pv_cnt>0) t;

-- 08-21
INSERT INTO ads_dau
SELECT '2026-08-21',
  COUNT(DISTINCT t.user_id) dau,
  COUNT(DISTINCT CASE WHEN h.user_id IS NULL THEN t.user_id END) new_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NOT NULL THEN t.user_id END) active_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NULL AND h.user_id IS NOT NULL THEN t.user_id END) wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-21' AND pv_cnt>0) t
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt < '2026-08-21' AND pv_cnt>0) h ON t.user_id=h.user_id
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-20' AND pv_cnt>0) y ON t.user_id=y.user_id;

-- 08-22
INSERT INTO ads_dau
SELECT '2026-08-22',
  COUNT(DISTINCT t.user_id) dau,
  COUNT(DISTINCT CASE WHEN h.user_id IS NULL THEN t.user_id END) new_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NOT NULL THEN t.user_id END) active_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NULL AND h.user_id IS NOT NULL THEN t.user_id END) wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-22' AND pv_cnt>0) t
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt < '2026-08-22' AND pv_cnt>0) h ON t.user_id=h.user_id
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-21' AND pv_cnt>0) y ON t.user_id=y.user_id;

-- 08-23
INSERT INTO ads_dau
SELECT '2026-08-23',
  COUNT(DISTINCT t.user_id) dau,
  COUNT(DISTINCT CASE WHEN h.user_id IS NULL THEN t.user_id END) new_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NOT NULL THEN t.user_id END) active_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NULL AND h.user_id IS NOT NULL THEN t.user_id END) wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-23' AND pv_cnt>0) t
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt < '2026-08-23' AND pv_cnt>0) h ON t.user_id=h.user_id
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-22' AND pv_cnt>0) y ON t.user_id=y.user_id;

-- 08-24
INSERT INTO ads_dau
SELECT '2026-08-24',
  COUNT(DISTINCT t.user_id) dau,
  COUNT(DISTINCT CASE WHEN h.user_id IS NULL THEN t.user_id END) new_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NOT NULL THEN t.user_id END) active_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NULL AND h.user_id IS NOT NULL THEN t.user_id END) wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-24' AND pv_cnt>0) t
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt < '2026-08-24' AND pv_cnt>0) h ON t.user_id=h.user_id
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-23' AND pv_cnt>0) y ON t.user_id=y.user_id;

-- ---------- 指标二：转化漏斗（ads_conversion_funnel，非分区表） ----------
-- 优化：5 天都在 dws_user_day 上做纯聚合，用 GROUP BY dt 一次扫描产出 5 天，
--       替代原来 5 条独立 INSERT（原来是 5 次全表扫描）
TRUNCATE TABLE ads_conversion_funnel;
INSERT INTO ads_conversion_funnel
SELECT dt,
  COUNT(DISTINCT CASE WHEN pv_cnt>0   THEN user_id END) view_uv,
  COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END) cart_uv,
  COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END) order_uv,
  COUNT(DISTINCT CASE WHEN pay_cnt>0  THEN user_id END) pay_uv,
  ROUND(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) cart_rate,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END),0),4) order_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END),0),4) pay_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) overall_rate
FROM dws_user_day
WHERE dt IN ('2026-08-20','2026-08-21','2026-08-22','2026-08-23','2026-08-24')
GROUP BY dt;

-- ---------- 指标四：复购率（ads_repurchase_rate，非分区表） ----------
-- 优化：先用中间结果表 tmp_order_user_cnt 把 dwd 订单表按 (user,dt) 聚合一次，
--       之后 5 天累计都只扫这个小表，而不再每次全表扫 dwd_order_fact_di
DROP TABLE IF EXISTS tmp_order_user_cnt;
CREATE TABLE tmp_order_user_cnt AS
SELECT user_id, dt, COUNT(*) AS cnt
FROM dwd_order_fact_di
WHERE user_id IS NOT NULL
GROUP BY user_id, dt;

TRUNCATE TABLE ads_repurchase_rate;
-- 08-20：截止到 08-20 的累计下单用户
INSERT INTO ads_repurchase_rate
SELECT '2026-08-20' dt,
  COUNT(*) total_buyers,
  COUNT(CASE WHEN ord>=2 THEN 1 END) repurchase_buyers,
  ROUND(COUNT(CASE WHEN ord>=2 THEN 1 END)*1.0/NULLIF(COUNT(*),0),4) repurchase_rate
FROM (SELECT user_id, SUM(cnt) ord FROM tmp_order_user_cnt WHERE dt<='2026-08-20' GROUP BY user_id) t;

INSERT INTO ads_repurchase_rate
SELECT '2026-08-21' dt,
  COUNT(*) total_buyers,
  COUNT(CASE WHEN ord>=2 THEN 1 END) repurchase_buyers,
  ROUND(COUNT(CASE WHEN ord>=2 THEN 1 END)*1.0/NULLIF(COUNT(*),0),4) repurchase_rate
FROM (SELECT user_id, SUM(cnt) ord FROM tmp_order_user_cnt WHERE dt<='2026-08-21' GROUP BY user_id) t;

INSERT INTO ads_repurchase_rate
SELECT '2026-08-22' dt,
  COUNT(*) total_buyers,
  COUNT(CASE WHEN ord>=2 THEN 1 END) repurchase_buyers,
  ROUND(COUNT(CASE WHEN ord>=2 THEN 1 END)*1.0/NULLIF(COUNT(*),0),4) repurchase_rate
FROM (SELECT user_id, SUM(cnt) ord FROM tmp_order_user_cnt WHERE dt<='2026-08-22' GROUP BY user_id) t;

INSERT INTO ads_repurchase_rate
SELECT '2026-08-23' dt,
  COUNT(*) total_buyers,
  COUNT(CASE WHEN ord>=2 THEN 1 END) repurchase_buyers,
  ROUND(COUNT(CASE WHEN ord>=2 THEN 1 END)*1.0/NULLIF(COUNT(*),0),4) repurchase_rate
FROM (SELECT user_id, SUM(cnt) ord FROM tmp_order_user_cnt WHERE dt<='2026-08-23' GROUP BY user_id) t;

INSERT INTO ads_repurchase_rate
SELECT '2026-08-24' dt,
  COUNT(*) total_buyers,
  COUNT(CASE WHEN ord>=2 THEN 1 END) repurchase_buyers,
  ROUND(COUNT(CASE WHEN ord>=2 THEN 1 END)*1.0/NULLIF(COUNT(*),0),4) repurchase_rate
FROM (SELECT user_id, SUM(cnt) ord FROM tmp_order_user_cnt WHERE dt<='2026-08-24' GROUP BY user_id) t;

-- ---------- 指标三：TopN 热销（ads_top_sku，分区表） ----------
-- 分区表按 dt 写分区；ROW_NUMBER 按 GMV+支付件数排名
TRUNCATE TABLE ads_top_sku;
INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-20')
SELECT s.sku_id, MAX(sku.sku_name), MAX(s.category_id), s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-20' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;

INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-21')
SELECT s.sku_id, MAX(sku.sku_name), MAX(s.category_id), s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-21' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;

INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-22')
SELECT s.sku_id, MAX(sku.sku_name), MAX(s.category_id), s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-22' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;

INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-23')
SELECT s.sku_id, MAX(sku.sku_name), MAX(s.category_id), s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-23' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;

INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-24')
SELECT s.sku_id, MAX(sku.sku_name), MAX(s.category_id), s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-24' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;