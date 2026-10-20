-- ============================================================
-- ODS 层数据画像（DWD 清洗规则的设计依据）
--
-- 用途：数据源变更或新增接入后跑一遍，把「真实脏数据长什么样」用数字固定下来，
--       避免凭印象设计清洗规则。
--
-- 性能设计：**刻意合并成 4 条单次扫描语句**。
--   初版拆成 10 条独立查询，每条都是一次 MapReduce，其中多条要对 1063 万行做
--   多列 COUNT(DISTINCT)，总耗时超过 15 分钟、被中断且输出丢失。
--   合并后：把多个指标塞进同一个 SUM(IF(...)) 里，一次扫描出全部指标。
--
-- 执行：beeline -u jdbc:hive2://lrh-1:10000 --outputformat=csv2 -f profile_ods.sql
-- ============================================================

SET hive.cli.print.header=true;

-- ---------- A. 基础画像：总行数 + 各字段空值/异常（单次全表扫描）----------
SELECT
  COUNT(1)                                                   AS total_rows,
  SUM(IF(price = 0, 1, 0))                                   AS zero_price,
  SUM(IF(price < 0, 1, 0))                                   AS neg_price,
  SUM(IF(brand = '' OR brand IS NULL, 1, 0))                 AS brand_null,
  SUM(IF(category_code = '' OR category_code IS NULL, 1, 0)) AS code_null,
  SUM(IF(category_id = 0, 1, 0))                             AS catid_zero,
  SUM(IF(user_id = 0, 1, 0))                                 AS uid_zero,
  SUM(IF(user_session = '' OR user_session IS NULL, 1, 0))   AS sess_null
FROM gmall.ods_event_log_di;

-- ---------- B. 商品维的一对多脏数据（一次 GROUP BY product_id）----------
-- 决定 dim_product 该「聚合」还是该「报错」：一个 product_id 若对应多个类目/品牌，
-- 说明源数据质量有问题，维表必须选一个代表值而不是直接 SELECT DISTINCT。
SELECT
  COUNT(1)                  AS products,
  SUM(IF(dc > 1, 1, 0))     AS multi_category,
  SUM(IF(db > 1, 1, 0))     AS multi_brand,
  SUM(IF(dp > 1, 1, 0))     AS multi_price
FROM (
  SELECT product_id,
         COUNT(DISTINCT category_code) AS dc,
         COUNT(DISTINCT brand)         AS db,
         COUNT(DISTINCT price)         AS dp
  FROM gmall.ods_event_log_di
  GROUP BY product_id
) t;

-- ---------- C. 类目路径层级深度（决定拆几级类目）----------
-- 用字符组 [.] 匹配点号，避免反斜杠转义在不同引擎下的歧义
SELECT SIZE(SPLIT(category_code, '[.]')) AS depth, COUNT(1) AS cnt
FROM gmall.ods_event_log_di
WHERE category_code <> ''
GROUP BY SIZE(SPLIT(category_code, '[.]'))
ORDER BY depth;

-- ---------- D. 会话是否跨天（决定会话粒度分析的写法）----------
SELECT
  COUNT(1)                  AS sessions,
  SUM(IF(d > 1, 1, 0))      AS multi_day_sessions
FROM (
  SELECT user_session, COUNT(DISTINCT dt) AS d
  FROM gmall.ods_event_log_di
  GROUP BY user_session
) t;
