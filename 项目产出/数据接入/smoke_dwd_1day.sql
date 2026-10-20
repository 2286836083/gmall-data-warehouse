-- ============================================================
-- 小规模冒烟验证：只处理 1 天数据，跑通「同型 SQL」的关键算子
--
-- 【为什么先做这个】
--   全量第 2 步跑了 21 分 39 秒后失败（3 个 DataNode 全被排除、写入 0 字节）。
--   直接全量重跑风险高——要再等 20 多分钟才知道结果。
--   这里用 1 天数据（约 34 万行）把同一套算子跑一遍：
--     SELECT DISTINCT 去重 → MD5 生成 event_id → SPLIT 拆类目 → ORC 落盘
--   唯一没覆盖的是「动态分区写入」，所以这一条单独验证。
--
-- 判定：本脚本跑通 → 算子与 SQL 本身没问题，问题在写入压力/资源；
--       本脚本也失败 → SQL 写法或环境有硬伤，要先修 SQL。
-- ============================================================

DROP TABLE IF EXISTS gmall.tmp_smoke_event;

CREATE TABLE gmall.tmp_smoke_event
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
AS
SELECT
  MD5(CONCAT_WS('|',
        o.event_time, o.event_type, CAST(o.product_id AS STRING),
        CAST(o.category_id AS STRING), o.category_code, o.brand,
        CAST(o.price AS STRING), CAST(o.user_id AS STRING), o.user_session)) AS event_id,
  o.user_id,
  o.event_type,
  CAST(REGEXP_REPLACE(o.event_time, ' UTC$', '') AS TIMESTAMP) AS event_time,
  o.product_id,
  o.category_id,
  o.category_code,
  COALESCE(NULLIF(SPLIT(o.category_code, '[.]')[0], ''), 'unknown') AS category_l1,
  COALESCE(NULLIF(SPLIT(o.category_code, '[.]')[1], ''), 'unknown') AS category_l2,
  COALESCE(NULLIF(SPLIT(o.category_code, '[.]')[2], ''), 'unknown') AS category_l3,
  COALESCE(NULLIF(o.brand, ''), 'unknown') AS brand,
  o.price,
  o.user_session
FROM (
  SELECT DISTINCT
         event_time, event_type, product_id, category_id, category_code,
         brand, price, user_id, user_session
  FROM gmall.ods_event_log_di
  WHERE dt = '2019-10-01'
) o;

SELECT COUNT(1) AS smoke_row_cnt FROM gmall.tmp_smoke_event;
