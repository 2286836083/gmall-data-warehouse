-- Day 9 批量导入 ODS 层（gmall 库）
USE gmall;

-- 1. 商品信息（全量，单分区）
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/sku_info.csv'
OVERWRITE INTO TABLE ods_sku_info_di PARTITION (dt='2026-08-24');

-- 2. 用户行为日志（按天分区）
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/user_log_by_date/user_log_2026-08-20.csv'
OVERWRITE INTO TABLE ods_user_log_di PARTITION (dt='2026-08-20');
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/user_log_by_date/user_log_2026-08-21.csv'
OVERWRITE INTO TABLE ods_user_log_di PARTITION (dt='2026-08-21');
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/user_log_by_date/user_log_2026-08-22.csv'
OVERWRITE INTO TABLE ods_user_log_di PARTITION (dt='2026-08-22');
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/user_log_by_date/user_log_2026-08-23.csv'
OVERWRITE INTO TABLE ods_user_log_di PARTITION (dt='2026-08-23');
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/user_log_by_date/user_log_2026-08-24.csv'
OVERWRITE INTO TABLE ods_user_log_di PARTITION (dt='2026-08-24');

-- 3. 订单信息（按天分区，按 create_time 拆分）
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/order_info_by_date/order_2026-08-20.csv'
OVERWRITE INTO TABLE ods_order_info_di PARTITION (dt='2026-08-20');
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/order_info_by_date/order_2026-08-21.csv'
OVERWRITE INTO TABLE ods_order_info_di PARTITION (dt='2026-08-21');
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/order_info_by_date/order_2026-08-22.csv'
OVERWRITE INTO TABLE ods_order_info_di PARTITION (dt='2026-08-22');
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/order_info_by_date/order_2026-08-23.csv'
OVERWRITE INTO TABLE ods_order_info_di PARTITION (dt='2026-08-23');
LOAD DATA LOCAL INPATH '/home/lrh/workspace/mock_data/order_info_by_date/order_2026-08-24.csv'
OVERWRITE INTO TABLE ods_order_info_di PARTITION (dt='2026-08-24');