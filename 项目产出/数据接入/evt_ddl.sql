-- ============================================================
-- 新链路（真实数据 REES46）DWS / ADS 建表
--
-- 【命名策略：为什么不沿用旧表名】
--   旧链路（模拟数据）的 dws_user_day / ads_dau / ads_top_sku 等表
--   **现在仍是 ECharts 大屏的数据源**，一旦同名覆盖，大屏当场断掉。
--   所以新表用**全新的语义化名字并存** —— 这与 DWD 层的做法完全一致
--   （新的 dwd_event_log_di 与旧的 dwd_user_behavior_di 也是并存）。
--   等新链路全绿、大屏切过去并验证通过后，再统一决定旧表去留。
--
-- 【与旧链路的三个结构差异，都是真实数据带来的】
--   1. 事件类型不同：REES46 只有 view / cart / purchase
--      → 旧链路的「下单(order_cnt) + 支付(pay_cnt)」两个字段合并为 purchase_cnt
--      → 旧链路的「已支付/已取消/已退款」三种订单状态在新链路**不存在**
--   2. 多了类目维度：真实数据有 category_l1/l2/l3 与 brand
--      → 新增 dws_category_event_day（类目主题），这是旧链路没有的
--   3. 用户有"首次出现日"：真实数据能算出**真正的新增用户**
--      → 旧 ads_dau 的 new_dau / wakeup_dau 恒为 0，新表能算出真实值
--
-- 执行：beeline -u jdbc:hive2://lrh-1:10000 -f evt_ddl.sql
-- ============================================================

-- ---------- 1. DWS：用户主题（用户 × 天） ----------
DROP TABLE IF EXISTS gmall.dws_user_event_day;

CREATE TABLE gmall.dws_user_event_day (
  user_id          bigint    COMMENT '用户ID',
  pv_cnt           bigint    COMMENT '当日浏览事件数（view）',
  cart_cnt         bigint    COMMENT '当日加购事件数（cart）',
  purchase_cnt     bigint    COMMENT '当日购买事件数（purchase，一笔即一单）',
  pay_amount       double    COMMENT '当日购买金额合计，即该用户贡献的 GMV',
  session_cnt      bigint    COMMENT '当日会话数（去重 user_session，一次连续访问算一个）',
  first_event_time timestamp COMMENT '当日首次事件时间',
  last_event_time  timestamp COMMENT '当日末次事件时间'
)
COMMENT 'DWS 用户主题宽表：用户 × 天 的行为与消费汇总（真实数据链路）'
PARTITIONED BY (dt string COMMENT '日期分区，格式 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- 2. DWS：商品主题（商品 × 天） ----------
DROP TABLE IF EXISTS gmall.dws_product_event_day;

CREATE TABLE gmall.dws_product_event_day (
  product_id   bigint COMMENT '商品ID',
  category_l1  string COMMENT '一级类目（取 dim_product 的规范值，缺失为 unknown）',
  category_l2  string COMMENT '二级类目',
  category_l3  string COMMENT '三级类目',
  brand        string COMMENT '品牌（缺失为 unknown）',
  pv_cnt       bigint COMMENT '当日浏览数',
  cart_cnt     bigint COMMENT '当日加购数',
  purchase_cnt bigint COMMENT '当日购买数',
  gmv          double COMMENT '当日该商品成交金额（purchase 事件的 price 合计）',
  buyer_cnt    bigint COMMENT '当日购买该商品的去重用户数'
)
COMMENT 'DWS 商品主题宽表：商品 × 天 的流量与成交汇总（真实数据链路）'
PARTITIONED BY (dt string COMMENT '日期分区')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- 3. DWS：类目主题（类目 × 天）—— 真实数据新增 ----------
DROP TABLE IF EXISTS gmall.dws_category_event_day;

CREATE TABLE gmall.dws_category_event_day (
  category_l1  string COMMENT '一级类目',
  category_l2  string COMMENT '二级类目',
  pv_cnt       bigint COMMENT '当日浏览数',
  cart_cnt     bigint COMMENT '当日加购数',
  purchase_cnt bigint COMMENT '当日购买数',
  gmv          double COMMENT '当日该二级类目成交金额',
  buyer_cnt    bigint COMMENT '当日该二级类目去重买家数（★必须回明细重算，不能从商品层加总）'
)
COMMENT 'DWS 类目主题宽表：类目 × 天 的流量与成交汇总（真实数据链路新增，旧链路没有）'
PARTITIONED BY (dt string COMMENT '日期分区')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- 4. DWS：交易主题（交易 × 天，每分区一行） ----------
DROP TABLE IF EXISTS gmall.dws_trade_day;

CREATE TABLE gmall.dws_trade_day (
  order_cnt        bigint COMMENT '成交笔数（一行 purchase 事件 = 一笔）',
  buyer_cnt        bigint COMMENT '去重买家数',
  gmv              double COMMENT '成交金额合计',
  avg_order_amount double COMMENT '客单价 = gmv / order_cnt',
  product_cnt      bigint COMMENT '涉及商品数（去重）'
)
COMMENT 'DWS 交易主题宽表：交易 × 天 汇总（每分区一行；不含订单状态，因为源数据没有该概念；不含会话数，因为购买事实表未保留该字段）'
PARTITIONED BY (dt string COMMENT '日期分区')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- 5. ADS：活跃用户（每天一行） ----------
DROP TABLE IF EXISTS gmall.ads_active_user_day;

CREATE TABLE gmall.ads_active_user_day (
  dt          string COMMENT '日期',
  dau         bigint COMMENT '日活跃用户数（当天有任意事件的去重用户）',
  new_dau     bigint COMMENT '新增用户数（首次出现日 = 当天）',
  active_dau  bigint COMMENT '连续活跃用户数（上一次活跃日 = 昨天）',
  wakeup_dau  bigint COMMENT '回流用户数（非首次，且上一次活跃日不是昨天）'
)
COMMENT 'ADS 活跃用户日报：一次扫描同时产出 DAU / 新增 / 连续活跃 / 回流四个口径'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- 6. ADS：转化漏斗（三环节） ----------
DROP TABLE IF EXISTS gmall.ads_funnel_day;

CREATE TABLE gmall.ads_funnel_day (
  dt                    string COMMENT '日期',
  view_uv               bigint COMMENT '浏览用户数',
  cart_uv               bigint COMMENT '加购用户数',
  purchase_uv           bigint COMMENT '购买用户数',
  cart_rate             double COMMENT '浏览→加购 转化率',
  cart_to_purchase_rate double COMMENT '加购→购买 转化率',
  overall_rate          double COMMENT '浏览→购买 整体转化率'
)
COMMENT 'ADS 转化漏斗日报：view → cart → purchase 三环节（源数据无 remove_from_cart，故为三环节而非四环节）'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- 7. ADS：累计复购率（每天一行） ----------
DROP TABLE IF EXISTS gmall.ads_repurchase_day;

CREATE TABLE gmall.ads_repurchase_day (
  dt                 string COMMENT '日期',
  total_buyers       bigint COMMENT '截止当日累计购买过的用户数',
  repurchase_buyers  bigint COMMENT '截止当日累计购买 ≥2 次的用户数',
  repurchase_rate    double COMMENT '累计复购率 = repurchase_buyers / total_buyers'
)
COMMENT 'ADS 累计复购率日报：用窗口函数累计，替代旧做法（中间表 + 逐日重算）'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- 8. ADS：TopN 商品（每天一行一商品） ----------
DROP TABLE IF EXISTS gmall.ads_top_product_day;

CREATE TABLE gmall.ads_top_product_day (
  dt           string COMMENT '日期',
  rank_num     int    COMMENT '当日 GMV 排名（1 = 最高）',
  product_id   bigint COMMENT '商品ID',
  brand        string COMMENT '品牌',
  category_l1  string COMMENT '一级类目',
  category_l2  string COMMENT '二级类目',
  gmv          double COMMENT '当日成交金额',
  purchase_cnt bigint COMMENT '当日购买数',
  buyer_cnt    bigint COMMENT '当日去重买家数'
)
COMMENT 'ADS TopN 商品日报：每天只保留 GMV 前 50 名（旧表每天 500 行，导出时还要再截 TopN）'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
