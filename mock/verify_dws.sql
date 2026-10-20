USE gmall;
SET hive.exec.mode.local.auto=false;
SELECT 'dws_user_day' t, COUNT(*) c, COUNT(DISTINCT dt) p FROM dws_user_day;
SELECT 'dws_sku_day' t, COUNT(*) c, COUNT(DISTINCT dt) p FROM dws_sku_day;
SELECT 'dws_order_day' t, COUNT(*) c, COUNT(DISTINCT dt) p FROM dws_order_day;