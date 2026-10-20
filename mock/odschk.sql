USE gmall;
SELECT 'ods_user_log' AS t, COUNT(*) AS c FROM ods_user_log_di;
SELECT 'ods_order' AS t, COUNT(*) AS c FROM ods_order_info_di;
SELECT 'ods_sku' AS t, COUNT(*) AS c FROM ods_sku_info_di;
SELECT 'ods_log_part' AS t, COUNT(DISTINCT dt) AS c FROM ods_user_log_di;
SELECT 'ods_order_part' AS t, COUNT(DISTINCT dt) AS c FROM ods_order_info_di;