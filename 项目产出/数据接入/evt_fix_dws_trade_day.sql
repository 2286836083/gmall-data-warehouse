-- ============================================================
-- 一次性修复脚本：重建 dws_trade_day（去掉源表里不存在的 session_cnt）
--
-- 【为什么要单独一个修复脚本，而不是重跑 evt_ddl.sql】
--   evt_ddl.sql 里每张表都是 DROP + CREATE。重跑它会连带把已经跑出数据的
--   dws_user_event_day（241s）、dws_product_event_day（63s）、dws_category_event_day（47s）
--   一起 DROP 掉 —— 白扔 6 分钟。
--   dws_trade_day 因为第 5 步失败，本身就是空的，所以只重建它一张最省。
--
-- 【错误回顾】
--   原写法：COUNT(DISTINCT user_session) FROM gmall.dwd_purchase_fact_di
--   报错：SemanticException [Error 10004] Invalid table alias or column reference 'user_session'
--   根因：dwd_purchase_fact_di 的字段是
--         order_id / user_id / product_id / category_id / category_l1~l3 /
--         brand / amount / pay_time / is_paid / dt —— **没有 user_session**。
--         购买事实表是从事件流派生的「成交视角」，刻意不保留会话信息。
--   教训：写跨表 SQL 前先对着表结构核字段，别凭印象写。
-- ============================================================

DROP TABLE IF EXISTS gmall.dws_trade_day;

CREATE TABLE gmall.dws_trade_day (
  order_cnt        bigint COMMENT '成交笔数（一行 purchase 事件 = 一笔）',
  buyer_cnt        bigint COMMENT '去重买家数',
  gmv              double COMMENT '成交金额合计',
  avg_order_amount double COMMENT '客单价 = gmv / order_cnt',
  product_cnt      bigint COMMENT '涉及商品数（去重）'
)
COMMENT 'DWS 交易主题宽表：交易 × 天 汇总（每分区一行；不含订单状态与会话数，源表没有这两个信息）'
PARTITIONED BY (dt string COMMENT '日期分区')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

DESC gmall.dws_trade_day;
