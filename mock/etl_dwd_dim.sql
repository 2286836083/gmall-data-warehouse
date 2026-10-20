USE gmall;
-- 强制走 YARN，避免本地模式内存/临时目录问题
SET hive.exec.mode.local.auto=false;
-- 1. dim_sku
INSERT OVERWRITE TABLE dim_sku
SELECT sku_id, MAX(sku_name) sku_name, MAX(category_id) category_id, MAX(category_name) category_name, MAX(brand_name) brand_name, MAX(price) price
FROM ods_sku_info_di GROUP BY sku_id;

-- 2. dim_user
INSERT OVERWRITE TABLE dim_user
SELECT user_id, '2026-01-01' register_date,
  CASE WHEN rand()<0.5 THEN '男' ELSE '女' END gender,
  CASE WHEN rand()<0.25 THEN '18-24' WHEN rand()<0.5 THEN '25-34' WHEN rand()<0.75 THEN '35-44' ELSE '45+' END age_range,
  CASE WHEN rand()<0.3 THEN '一线' WHEN rand()<0.6 THEN '二线' WHEN rand()<0.8 THEN '三线' ELSE '其他' END city_level,
  CASE WHEN rand()<0.7 THEN '普通' WHEN rand()<0.9 THEN '银卡' WHEN rand()<0.97 THEN '金卡' ELSE '黑卡' END vip_level,
  NULL first_order_date
FROM (SELECT DISTINCT user_id FROM ods_user_log_di UNION SELECT DISTINCT user_id FROM ods_order_info_di) t;

-- 3. 更新 dim_user 首单日期（用 substr 避免 LEFT( 与 LEFT JOIN 关键字冲突）
INSERT OVERWRITE TABLE dim_user
SELECT d.user_id, d.register_date, d.gender, d.age_range, d.city_level, d.vip_level, f.first_order_date
FROM dim_user d
LEFT JOIN (SELECT user_id, MIN(substr(create_time,1,10)) first_order_date FROM ods_order_info_di WHERE create_time IS NOT NULL AND create_time!='' GROUP BY user_id) f
ON d.user_id = f.user_id;