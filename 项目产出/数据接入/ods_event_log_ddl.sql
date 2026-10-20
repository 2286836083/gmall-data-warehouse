-- ============================================================
-- ODS 原始层建表：电商用户行为事件日志
--
-- 数据来源：REES46 eCommerce behavior data（Kaggle / CC BY 4.0）
--           取 2019-10 单月，按用户 1/4 采样后共 31 天、1063 万条
--
-- 设计说明：
--   1. 只有一张事件表。真实埋点数据本来就是「一行一个事件」的宽表，
--      商品维表和订单事实应该在 DWD 层从事件流派生，不必在 ODS 重复存储。
--   2. 【不加工】是 ODS 的核心原则：
--      - event_time 保留原始文本（含 " UTC" 后缀），解析放到 DWD
--      - brand / category_code 的空值保留为空串，不在这里转 NULL
--      - price 的 0 值是真实数据特征（疑似赠品或采集异常），原样保留
--      这些都留到 DWD 清洗层统一处理，ODS 只负责「原样落地」。
--   3. 分区列 dt 取自 event_time 的前 10 位，文件本身不含 dt 列。
--
-- 执行：beeline -u jdbc:hive2://lrh-1:10000 -f ods_event_log_ddl.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS gmall
  COMMENT '电商用户行为分析离线数仓';

DROP TABLE IF EXISTS gmall.ods_event_log_di;

CREATE TABLE gmall.ods_event_log_di (
  event_time    string  COMMENT '事件时间（原始文本，形如 2019-10-01 00:00:00 UTC，解析在 DWD 层做）',
  event_type    string  COMMENT '事件类型：view 浏览 / cart 加购 / purchase 购买',
  product_id    bigint  COMMENT '商品ID',
  category_id   bigint  COMMENT '商品类目ID',
  category_code string  COMMENT '类目路径（点分层级，如 electronics.smartphone，可拆 3 级；原数据约 31.9% 为空）',
  brand         string  COMMENT '品牌（原数据约 14.4% 为空，清洗在 DWD 层做）',
  price         double  COMMENT '商品价格（USDT；0 值仅出现在 view/cart，purchase 无 0 值）',
  user_id       bigint  COMMENT '用户ID',
  user_session  string  COMMENT '会话ID（一次连续访问，同会话内事件序列完整）'
)
COMMENT 'ODS 原始层：电商用户行为事件日志（REES46 2019-10，按 dt 分区，原样接入不做加工）'
PARTITIONED BY (dt string COMMENT '日期分区，取自 event_time 前 10 位')
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
