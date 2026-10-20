-- ============================================================
-- DWD / DIM 层建表（真实数据集 REES46 版）
--
-- 【设计依据】全部来自 ODS 画像的实测数字，不是拍脑袋：
--   · event_time 原始形态 "2019-10-01 00:00:11 UTC" → 需剥离后缀再解析
--   · 存在 7,091 行「9 个字段完全相同」的重复行 → 必须去重
--   · category_code 是点分层级路径（electronics.smartphone）→ 可拆 3 级
--   · brand 空 14.4%、category_code 空 31.9% → 归一为 unknown，避免 NULL 干扰分组
--   · price=0 只出现在 view/cart，purchase 无 0 值 → 不影响 GMV 口径
--
-- 【与旧表的区别（不破坏性）】
--   旧表（dwd_user_behavior_di / dwd_order_fact_di / dim_sku / dim_user）
--   是为模拟数据设计的，本次**不改不删**，新表用新名字并存。
--   等 DWS/ADS 全部切到新链路、大屏验证通过后，再统一决定旧表去留。
--
-- 【存储格式】用 ORC + SNAPPY，而不是旧表的 TEXTFILE：
--   TEXTFILE 是纯文本，每次查询都要全量解析；ORC 是列式存储，
--   按列读取 + 自带轻量索引，体积约为文本的 1/5~1/10，扫描快得多。
--   后面做 Hive vs Spark 性能对比时，列式格式才是真实生产形态。
--
-- 执行：beeline -u jdbc:hive2://lrh-1:10000 -f dwd_ddl.sql
-- ============================================================

-- ---------- 1. DWD 明细层：清洗后的事件明细 ----------
DROP TABLE IF EXISTS gmall.dwd_event_log_di;

CREATE TABLE gmall.dwd_event_log_di (
  event_id      string     COMMENT '事件唯一ID：md5(原始9字段拼接)，去重后唯一',
  user_id       bigint     COMMENT '用户ID',
  event_type    string     COMMENT '事件类型：view 浏览 / cart 加购 / purchase 购买',
  event_time    timestamp  COMMENT '事件时间（已剥离 " UTC" 后缀并解析为时间戳）',
  product_id    bigint     COMMENT '商品ID',
  category_id   bigint     COMMENT '类目ID',
  category_code string     COMMENT '类目路径原文（点分层级，如 electronics.smartphone）',
  category_l1   string     COMMENT '一级类目（由 category_code 拆解；无类目信息时为 unknown）',
  category_l2   string     COMMENT '二级类目（不存在该层级时为 unknown）',
  category_l3   string     COMMENT '三级类目（不存在该层级时为 unknown）',
  brand         string     COMMENT '品牌（源数据缺失时归一为 unknown）',
  price         double     COMMENT '商品价格；view/cart 存在 0 值（未定价），purchase 无 0 值',
  user_session  string     COMMENT '会话ID（一次连续访问，同会话内事件序列完整）'
)
COMMENT 'DWD 明细层：电商用户行为事件明细（已去重、解析时间、拆解类目、归一空值）'
PARTITIONED BY (dt string COMMENT '日期分区，取自事件时间的前 10 位')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- 2. DWD 事实层：购买事实 ----------
-- REES46 没有独立的订单表，一行 purchase 事件即一笔成交，
-- 因此订单事实只能从事件流派生。保留这张表的价值：
-- 让「订单类指标」（订单数、GMV、客单价）走独立的、语义清晰的表，
-- 而不是让 DWS/ADS 每次都去事件流里筛 purchase 再聚合。
DROP TABLE IF EXISTS gmall.dwd_purchase_fact_di;

CREATE TABLE gmall.dwd_purchase_fact_di (
  order_id    string     COMMENT '订单ID（用购买事件的 event_id；一行 purchase = 一笔成交）',
  user_id     bigint     COMMENT '用户ID',
  product_id  bigint     COMMENT '商品ID',
  category_id bigint     COMMENT '类目ID',
  category_l1 string     COMMENT '一级类目',
  category_l2 string     COMMENT '二级类目',
  category_l3 string     COMMENT '三级类目',
  brand       string     COMMENT '品牌',
  amount      double     COMMENT '成交金额（= 购买事件价格；实测 purchase 无 0 值）',
  pay_time    timestamp  COMMENT '支付时间（= 购买事件时间）',
  is_paid     int        COMMENT '是否已支付：由 purchase 事件派生，恒为 1'
)
COMMENT 'DWD 事实层：购买事实（由 purchase 事件派生，一行一笔成交）'
PARTITIONED BY (dt string COMMENT '日期分区，取自事件时间的前 10 位')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- 3. DIM 维度层：商品维度 ----------
-- 一行一个商品，保留「稳定属性」：类目、品牌、首末出现日期、累计事件数。
--
-- ⚠️ 刻意不放 price：价格会随时间变化，属于「事实属性」而不是「维度属性」。
--    维表放一个会变的字段，是维度建模里典型的反模式——下游每次 join 都得
--    先想清楚"我拿到的是哪天的价格"。价格事实统一去 dwd_event_log_di 取。
DROP TABLE IF EXISTS gmall.dim_product;

CREATE TABLE gmall.dim_product (
  product_id    bigint  COMMENT '商品ID',
  category_id   bigint  COMMENT '类目ID',
  category_code string  COMMENT '类目路径原文',
  category_l1   string  COMMENT '一级类目',
  category_l2   string  COMMENT '二级类目',
  category_l3   string  COMMENT '三级类目',
  brand         string  COMMENT '品牌（同商品出现多个品牌时取出现次数最多的）',
  first_seen_dt string  COMMENT '首次出现日期',
  last_seen_dt  string  COMMENT '最后出现日期',
  event_cnt     bigint  COMMENT '累计事件数（含浏览/加购/购买）'
)
COMMENT 'DIM 维度层：商品维度（由全量事件流派生，一行一个商品；不含价格）'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
