-- ============================================
-- gmall 电商数仓 建库 + 全分层 DDL
-- ODS / DWD / DWS / ADS / dim 共 11 张表
-- 依据: Day7_数仓理论与分层建模教程.sql 6.3/5.3 节
-- ============================================

CREATE DATABASE IF NOT EXISTS gmall
COMMENT '电商用户行为分析离线数仓';

USE gmall;

-- ========== ODS 原始数据层 ==========

CREATE TABLE IF NOT EXISTS ods_user_log_di (
    log_time    STRING COMMENT '原始日志时间',
    user_id     STRING COMMENT '用户ID',
    event_type  STRING COMMENT '事件类型',
    page_url    STRING COMMENT '页面URL',
    sku_id      STRING COMMENT '商品SKU',
    session_id  STRING COMMENT '会话ID'
) COMMENT '用户行为日志-ODS'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE IF NOT EXISTS ods_order_info_di (
    order_id     STRING COMMENT '订单ID',
    user_id      STRING COMMENT '用户ID',
    order_status STRING COMMENT '订单状态',
    total_amount DECIMAL(18,2) COMMENT '订单金额',
    create_time  STRING COMMENT '创建时间',
    pay_time     STRING COMMENT '支付时间'
) COMMENT '订单信息-ODS'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- ========== DWD 明细数据层 ==========

CREATE TABLE IF NOT EXISTS dwd_user_behavior_di (
    behavior_id STRING COMMENT '行为ID',
    user_id     STRING COMMENT '用户ID',
    event_type  STRING COMMENT '事件类型：view/cart/order/pay',
    event_time  TIMESTAMP COMMENT '事件时间',
    sku_id      STRING COMMENT '商品SKU',
    category_id STRING COMMENT '品类ID',
    session_id  STRING COMMENT '会话ID'
) COMMENT '用户行为明细-DWD'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE IF NOT EXISTS dwd_order_fact_di (
    order_id     STRING COMMENT '订单ID',
    user_id      STRING COMMENT '用户ID',
    order_status STRING COMMENT '标准化状态',
    total_amount DECIMAL(18,2) COMMENT '订单金额',
    create_time  TIMESTAMP COMMENT '创建时间',
    pay_time     TIMESTAMP COMMENT '支付时间',
    is_paid      INT COMMENT '是否已支付：1/0'
) COMMENT '订单事实-DWD'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- ========== DWS 汇总数据层 ==========

CREATE TABLE IF NOT EXISTS dws_user_day (
    user_id      STRING COMMENT '用户ID',
    pv_cnt       BIGINT COMMENT '浏览次数',
    cart_cnt     BIGINT COMMENT '加购次数',
    order_cnt    BIGINT COMMENT '下单次数',
    pay_cnt      BIGINT COMMENT '支付次数',
    pay_amount   DECIMAL(18,2) COMMENT '支付金额',
    session_cnt  BIGINT COMMENT '会话数'
) COMMENT '用户日汇总-DWS'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE IF NOT EXISTS dws_sku_day (
    sku_id       STRING COMMENT '商品SKU',
    category_id  STRING COMMENT '品类ID',
    pv_cnt       BIGINT COMMENT '浏览次数',
    cart_cnt     BIGINT COMMENT '加购次数',
    order_cnt    BIGINT COMMENT '下单次数',
    pay_cnt      BIGINT COMMENT '支付次数',
    gmv          DECIMAL(18,2) COMMENT '成交金额'
) COMMENT '商品日汇总-DWS'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- ========== ADS 应用数据层 ==========

CREATE TABLE IF NOT EXISTS ads_dau (
    dt         STRING COMMENT '日期',
    dau        BIGINT COMMENT '日活去重用户数',
    new_dau    BIGINT COMMENT '新增日活',
    wakeup_dau BIGINT COMMENT '回流日活'
) COMMENT '日活跃统计-ADS'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE IF NOT EXISTS ads_conversion_funnel (
    dt          STRING COMMENT '日期',
    view_uv     BIGINT COMMENT '浏览用户数',
    cart_uv     BIGINT COMMENT '加购用户数',
    order_uv    BIGINT COMMENT '下单用户数',
    pay_uv      BIGINT COMMENT '支付用户数',
    cart_rate   DECIMAL(6,4) COMMENT '浏览→加购转化率',
    order_rate  DECIMAL(6,4) COMMENT '加购→下单转化率',
    pay_rate    DECIMAL(6,4) COMMENT '下单→支付转化率',
    overall_rate DECIMAL(6,4) COMMENT '整体转化率'
) COMMENT '转化漏斗-ADS'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE IF NOT EXISTS ads_top_sku (
    sku_id      STRING COMMENT '商品SKU',
    sku_name    STRING COMMENT '商品名称',
    category_id STRING COMMENT '品类ID',
    pay_cnt     BIGINT COMMENT '支付件数',
    gmv         DECIMAL(18,2) COMMENT '成交金额',
    rank_num    INT COMMENT '排名'
) COMMENT 'TopN热销商品-ADS'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- ========== 维度表（星型模型，5.3 节） ==========

CREATE TABLE IF NOT EXISTS dim_user (
    user_id         STRING COMMENT '用户ID（代理键/自然键）',
    register_date   STRING COMMENT '注册日期',
    gender          STRING COMMENT '性别',
    age_range       STRING COMMENT '年龄段：18-24/25-34/...',
    city_level      STRING COMMENT '城市等级：一线/二线/三线/其他',
    vip_level       STRING COMMENT '会员等级',
    first_order_dt  STRING COMMENT '首单日期'
) COMMENT '用户维度表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE IF NOT EXISTS dim_sku (
    sku_id          STRING COMMENT '商品SKU',
    spu_id          STRING COMMENT '商品SPU',
    sku_name        STRING COMMENT '商品名称',
    category_id     STRING COMMENT '品类ID',
    category_name   STRING COMMENT '品类名称',
    brand_id        STRING COMMENT '品牌ID',
    brand_name      STRING COMMENT '品牌名称',
    price_range     STRING COMMENT '价格区间',
    is_new          STRING COMMENT '是否新品：Y/N'
) COMMENT '商品维度表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

SHOW TABLES;