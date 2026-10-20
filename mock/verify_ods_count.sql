USE gmall;
SELECT 'ods_user_log_di' AS t, COUNT(*) AS c FROM ods_user_log_di;
SELECT 'ods_order_info_di' AS t, COUNT(*) AS c FROM ods_order_info_di;
SELECT 'ods_sku_info_di' AS t, COUNT(*) AS c FROM ods_sku_info_di;
SELECT 'ods_user_log_di_partitions' AS t, COUNT(*) AS c FROM (SELECT DISTINCT dt FROM ods_user_log_di) sub;
SELECT 'ods_order_info_di_partitions' AS t, COUNT(*) AS c FROM (SELECT DISTINCT dt FROM ods_order_info_di) sub;