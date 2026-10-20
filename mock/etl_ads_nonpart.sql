USE gmall;
SET hive.exec.mode.local.auto=false;

-- ads_dau / ads_conversion_funnel / ads_repurchase_rate 为非分区表（dt 是普通列）
-- 先清空，再逐日 INSERT INTO 追加 5 天
-- DAU 采用 LEFT JOIN 派生表方式，避免聚合函数内子查询报错

TRUNCATE TABLE ads_dau;
-- 08-20 (首日: 无历史, 无昨日)
INSERT INTO ads_dau
SELECT '2026-08-20',
  COUNT(DISTINCT t.user_id) dau,
  0 new_dau,
  0 active_dau,
  0 wakeup_dau
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

-- ===== 转化漏斗（非分区表） =====
TRUNCATE TABLE ads_conversion_funnel;
INSERT INTO ads_conversion_funnel
SELECT '2026-08-20' dt,
  COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END) view_uv,
  COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END) cart_uv,
  COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END) order_uv,
  COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END) pay_uv,
  ROUND(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) cart_rate,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END),0),4) order_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END),0),4) pay_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) overall_rate
FROM dws_user_day WHERE dt='2026-08-20';

INSERT INTO ads_conversion_funnel
SELECT '2026-08-21' dt,
  COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END) view_uv,
  COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END) cart_uv,
  COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END) order_uv,
  COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END) pay_uv,
  ROUND(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) cart_rate,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END),0),4) order_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END),0),4) pay_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) overall_rate
FROM dws_user_day WHERE dt='2026-08-21';

INSERT INTO ads_conversion_funnel
SELECT '2026-08-22' dt,
  COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END) view_uv,
  COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END) cart_uv,
  COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END) order_uv,
  COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END) pay_uv,
  ROUND(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) cart_rate,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END),0),4) order_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END),0),4) pay_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) overall_rate
FROM dws_user_day WHERE dt='2026-08-22';

INSERT INTO ads_conversion_funnel
SELECT '2026-08-23' dt,
  COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END) view_uv,
  COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END) cart_uv,
  COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END) order_uv,
  COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END) pay_uv,
  ROUND(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) cart_rate,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END),0),4) order_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END),0),4) pay_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) overall_rate
FROM dws_user_day WHERE dt='2026-08-23';

INSERT INTO ads_conversion_funnel
SELECT '2026-08-24' dt,
  COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END) view_uv,
  COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END) cart_uv,
  COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END) order_uv,
  COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END) pay_uv,
  ROUND(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) cart_rate,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN cart_cnt>0 THEN user_id END),0),4) order_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN order_cnt>0 THEN user_id END),0),4) pay_rate,
  ROUND(COUNT(DISTINCT CASE WHEN pay_cnt>0 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN pv_cnt>0 THEN user_id END),0),4) overall_rate
FROM dws_user_day WHERE dt='2026-08-24';

-- ===== 复购率（非分区表，累计到统计日） =====
TRUNCATE TABLE ads_repurchase_rate;
INSERT INTO ads_repurchase_rate
SELECT '2026-08-20' dt,
  COUNT(DISTINCT user_id) total_buyers,
  COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END) repurchase_buyers,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT user_id),0),4) repurchase_rate
FROM (SELECT user_id, COUNT(*) order_cnt FROM dwd_order_fact_di WHERE dt<='2026-08-20' AND user_id IS NOT NULL GROUP BY user_id) t;

INSERT INTO ads_repurchase_rate
SELECT '2026-08-21' dt,
  COUNT(DISTINCT user_id) total_buyers,
  COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END) repurchase_buyers,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT user_id),0),4) repurchase_rate
FROM (SELECT user_id, COUNT(*) order_cnt FROM dwd_order_fact_di WHERE dt<='2026-08-21' AND user_id IS NOT NULL GROUP BY user_id) t;

INSERT INTO ads_repurchase_rate
SELECT '2026-08-22' dt,
  COUNT(DISTINCT user_id) total_buyers,
  COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END) repurchase_buyers,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT user_id),0),4) repurchase_rate
FROM (SELECT user_id, COUNT(*) order_cnt FROM dwd_order_fact_di WHERE dt<='2026-08-22' AND user_id IS NOT NULL GROUP BY user_id) t;

INSERT INTO ads_repurchase_rate
SELECT '2026-08-23' dt,
  COUNT(DISTINCT user_id) total_buyers,
  COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END) repurchase_buyers,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT user_id),0),4) repurchase_rate
FROM (SELECT user_id, COUNT(*) order_cnt FROM dwd_order_fact_di WHERE dt<='2026-08-23' AND user_id IS NOT NULL GROUP BY user_id) t;

INSERT INTO ads_repurchase_rate
SELECT '2026-08-24' dt,
  COUNT(DISTINCT user_id) total_buyers,
  COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END) repurchase_buyers,
  ROUND(COUNT(DISTINCT CASE WHEN order_cnt>=2 THEN user_id END)*1.0/NULLIF(COUNT(DISTINCT user_id),0),4) repurchase_rate
FROM (SELECT user_id, COUNT(*) order_cnt FROM dwd_order_fact_di WHERE dt<='2026-08-24' AND user_id IS NOT NULL GROUP BY user_id) t;

-- ===== TopN 热销商品（分区表，按 dt 动态分区写入） =====
TRUNCATE TABLE ads_top_sku;
-- 08-20
INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-20')
SELECT s.sku_id, MAX(sku.sku_name) AS sku_name, MAX(s.category_id) AS category_id,
  s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-20' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;

INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-21')
SELECT s.sku_id, MAX(sku.sku_name) AS sku_name, MAX(s.category_id) AS category_id,
  s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-21' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;

INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-22')
SELECT s.sku_id, MAX(sku.sku_name) AS sku_name, MAX(s.category_id) AS category_id,
  s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-22' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;

INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-23')
SELECT s.sku_id, MAX(sku.sku_name) AS sku_name, MAX(s.category_id) AS category_id,
  s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-23' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;

INSERT OVERWRITE TABLE ads_top_sku PARTITION (dt='2026-08-24')
SELECT s.sku_id, MAX(sku.sku_name) AS sku_name, MAX(s.category_id) AS category_id,
  s.pay_cnt, s.gmv,
  ROW_NUMBER() OVER (ORDER BY s.gmv DESC, s.pay_cnt DESC) AS rank_num
FROM dws_sku_day s LEFT JOIN dim_sku sku ON s.sku_id = sku.sku_id
WHERE s.dt='2026-08-24' AND s.gmv > 0
GROUP BY s.sku_id, s.pay_cnt, s.gmv;