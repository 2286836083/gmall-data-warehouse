USE gmall;
SET hive.exec.mode.local.auto=false;

-- ========== 3.3 EXPLAIN 执行计划 ==========
-- JOIN + GROUP BY 的执行计划
EXPLAIN
SELECT s.category_id, COUNT(*) AS cnt
FROM dwd_user_behavior_di b
JOIN dim_sku s ON b.sku_id = s.sku_id
WHERE b.dt = '2026-08-24'
GROUP BY s.category_id;

-- ========== 3.2 分区裁剪验证 ==========
-- 确认 PartitionPruner 只保留目标分区
EXPLAIN EXTENDED
SELECT * FROM dws_user_day WHERE dt = '2026-08-24';

-- ========== 3.4 MapJoin 验证 ==========
-- 开启后 JOIN 计划应出现 Map Join Operator
SET hive.auto.convert.join = true;
SET hive.mapjoin.smalltable.filesize = 25000000;
EXPLAIN
SELECT /*+ MAPJOIN(s) */ b.user_id, s.category_id
FROM dwd_user_behavior_di b
JOIN dim_sku s ON b.sku_id = s.sku_id
WHERE b.dt = '2026-08-24';

-- ========== 3.1 ORC 转换（建 ORC 表并拷贝数据验证格式差异） ==========
-- 按 dt 动态分区写入需开启 nonstrict（见 3.5 调优参数）
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE IF NOT EXISTS dws_user_day_orc (
    user_id STRING, pv_cnt BIGINT, cart_cnt BIGINT,
    order_cnt BIGINT, pay_cnt BIGINT, pay_amount DECIMAL(18,2),
    session_cnt BIGINT, first_event_time TIMESTAMP, last_event_time TIMESTAMP
) PARTITIONED BY (dt STRING) STORED AS ORC;

INSERT OVERWRITE TABLE dws_user_day_orc PARTITION (dt)
SELECT user_id, pv_cnt, cart_cnt, order_cnt, pay_cnt, pay_amount,
       session_cnt, first_event_time, last_event_time, dt
FROM dws_user_day;

-- 验证 ORC 表行数与分区
SELECT dt, COUNT(*) AS cnt FROM dws_user_day_orc GROUP BY dt ORDER BY dt;