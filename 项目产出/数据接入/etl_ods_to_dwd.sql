-- ============================================================
-- ETL：ODS → DWD / DIM（真实数据集 REES46 版）
--
-- 前置：先执行 dwd_ddl.sql 建表
-- 执行：beeline -u jdbc:hive2://lrh-1:10000 -f etl_ods_to_dwd.sql
--
-- ⚠️ 这个脚本要跑真实 MapReduce 作业（1063 万行），耗时较长（预计 5~15 分钟）。
--    通过 SSH 远程调用时**必须放后台**，否则会因单次调用超时被中断、
--    且中断后拿不到任何输出（表现是"静默无输出"，很容易误判成脚本有问题）。
--
-- 【清洗规则总览】（全部有实测依据）
--   1. 时间解析    "2019-10-01 00:00:11 UTC" → 剥离 " UTC" 后 CAST 成 timestamp
--   2. 去重        按原始 9 个字段整行去重；实测存在 7,091 行完全重复
--   3. 类目拆级    category_code 按 "." 拆成 1/2/3 级，缺层补 unknown
--   4. 空值归一    brand / category_code 空值 → unknown（避免 NULL 干扰 GROUP BY）
--   5. 零价保留    不删。price=0 只出现在 view/cart，是「未定价展示」，不是错误；
--                  purchase 无 0 值，所以不影响 GMV 口径
-- ============================================================

-- 允许动态分区（一次写入 31 个日期分区）
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
-- ORC 写入用中间压缩，减少落盘与后续读取开销
SET hive.exec.compress.intermediate = true;
SET hive.exec.compress.output = true;


-- ============================================================
-- 第 1 步：ODS → dwd_event_log_di（事件明细）
-- ============================================================
INSERT OVERWRITE TABLE gmall.dwd_event_log_di PARTITION (dt)
SELECT
  -- event_id 用「原始 9 字段」参与 md5：与去重用的键完全一致，
  -- 保证去重之后 event_id 一定唯一（用归一化后的字段会引入碰撞风险）
  MD5(CONCAT_WS('|',
        o.event_time,
        o.event_type,
        CAST(o.product_id    AS STRING),
        CAST(o.category_id   AS STRING),
        o.category_code,
        o.brand,
        CAST(o.price         AS STRING),
        CAST(o.user_id       AS STRING),
        o.user_session))                                   AS event_id,
  o.user_id,
  o.event_type,
  CAST(REGEXP_REPLACE(o.event_time, ' UTC$', '') AS TIMESTAMP) AS event_time,
  o.product_id,
  o.category_id,
  o.category_code,
  -- 类目拆级：SPLIT 用字符组 [.] 匹配点号，避免反斜杠转义歧义；
  -- 数组越界在 Hive 里返回 NULL，COALESCE 兜底成 unknown
  COALESCE(NULLIF(SPLIT(o.category_code, '[.]')[0], ''), 'unknown') AS category_l1,
  COALESCE(NULLIF(SPLIT(o.category_code, '[.]')[1], ''), 'unknown') AS category_l2,
  COALESCE(NULLIF(SPLIT(o.category_code, '[.]')[2], ''), 'unknown') AS category_l3,
  COALESCE(NULLIF(o.brand, ''), 'unknown')                 AS brand,
  o.price,
  o.user_session,
  SUBSTR(o.event_time, 1, 10)                              AS dt
FROM (
  -- 去重：实测存在 7,091 行「9 个字段完全相同」的重复记录
  SELECT DISTINCT
         event_time, event_type, product_id, category_id, category_code,
         brand, price, user_id, user_session
  FROM gmall.ods_event_log_di
) o;


-- ============================================================
-- 第 2 步：dwd_event_log_di → dwd_purchase_fact_di（购买事实）
-- ============================================================
INSERT OVERWRITE TABLE gmall.dwd_purchase_fact_di PARTITION (dt)
SELECT
  event_id    AS order_id,
  user_id,
  product_id,
  category_id,
  category_l1,
  category_l2,
  category_l3,
  brand,
  price       AS amount,
  event_time  AS pay_time,
  1           AS is_paid,
  dt
FROM gmall.dwd_event_log_di
WHERE event_type = 'purchase';


-- ============================================================
-- 第 3 步：dwd_event_log_di → dim_product（商品维度）
-- ============================================================
-- 一个商品在同一天可能出现多条不同属性的事件（实测源数据存在商品属性不一致），
-- 所以不能直接 SELECT DISTINCT，而要「先按属性组合计数，再取出现次数最多的那组」。
INSERT OVERWRITE TABLE gmall.dim_product
SELECT
  product_id, category_id, category_code,
  category_l1, category_l2, category_l3, brand,
  first_seen_dt, last_seen_dt, event_cnt
FROM (
  SELECT
    product_id, category_id, category_code,
    category_l1, category_l2, category_l3, brand,
    MIN(dt)  OVER (PARTITION BY product_id) AS first_seen_dt,
    MAX(dt)  OVER (PARTITION BY product_id) AS last_seen_dt,
    COUNT(1) OVER (PARTITION BY product_id) AS event_cnt,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY attr_cnt DESC, dt DESC      -- 取出现最多的属性组合；并列时取更近的
    ) AS rn
  FROM (
    SELECT
      product_id, category_id, category_code,
      category_l1, category_l2, category_l3, brand, dt,
      COUNT(1) OVER (
        PARTITION BY product_id, category_id, category_code,
                     category_l1, category_l2, category_l3, brand
      ) AS attr_cnt
    FROM gmall.dwd_event_log_di
  ) a
) b
WHERE rn = 1;


-- ============================================================
-- 校验（跑完自动输出，用于确认 ETL 结果）
-- ============================================================
SELECT 'dwd_event_log_di' AS tbl, COUNT(DISTINCT dt) AS part_cnt, COUNT(1) AS row_cnt
FROM gmall.dwd_event_log_di
UNION ALL
SELECT 'dwd_purchase_fact_di', COUNT(DISTINCT dt), COUNT(1) FROM gmall.dwd_purchase_fact_di
UNION ALL
SELECT 'dim_product', COUNT(DISTINCT 1), COUNT(1) FROM gmall.dim_product;
