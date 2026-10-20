USE gmall;
SELECT 'tables' AS info, COUNT(*) AS cnt FROM (
  SHOW TABLES
) t;
SELECT 'ods_user_log_di' AS tbl, COUNT(*) AS c FROM ods_user_log_di
UNION ALL SELECT 'ods_order_info_di', COUNT(*) FROM ods_order_info_di
UNION ALL SELECT 'ods_sku_info_di', COUNT(*) FROM ods_sku_info_di;