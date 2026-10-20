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
SET hive.exec.mode.local.auto=false;

SELECT '--- ads_dau ---' AS sec;
SELECT * FROM ads_dau ORDER BY dt;

SELECT '--- ads_conversion_funnel ---' AS sec;
SELECT * FROM ads_conversion_funnel ORDER BY dt;

SELECT '--- ads_repurchase_rate ---' AS sec;
SELECT * FROM ads_repurchase_rate ORDER BY dt;

SELECT '--- ads_top_sku (top3 of 08-24) ---' AS sec;
SELECT sku_id, sku_name, category_id, pay_cnt, gmv, rank_num FROM ads_top_sku
WHERE dt='2026-08-24' ORDER BY rank_num LIMIT 3;
SELECT '--- ads_top_sku 分区数 ---' AS sec;
SELECT dt, COUNT(*) FROM ads_top_sku GROUP BY dt ORDER BY dt;