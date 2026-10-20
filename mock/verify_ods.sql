USE gmall;
SELECT 'ods_user_log_di' AS table_name, COUNT(*) AS cnt FROM ods_user_log_di
UNION ALL
SELECT 'ods_order_info_di', COUNT(*) FROM ods_order_info_di
UNION ALL
SELECT 'ods_sku_info_di', COUNT(*) FROM ods_sku_info_di;
SHOW PARTITIONS ods_user_log_di;
SELECT dt, COUNT(*) AS cnt FROM ods_user_log_di GROUP BY dt ORDER BY dt;
SELECT dt, COUNT(*) AS cnt FROM ods_order_info_di GROUP BY dt ORDER BY dt;
SELECT order_status, COUNT(*) AS cnt FROM ods_order_info_di GROUP BY order_status;
SELECT category_name, COUNT(*) AS cnt FROM ods_sku_info_di GROUP BY category_name;