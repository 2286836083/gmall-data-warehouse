USE gmall;
SET hive.exec.mode.local.auto=false;
SELECT sku_id, sku_name, category_id FROM dim_sku WHERE sku_id IN ('P1076','P1334');