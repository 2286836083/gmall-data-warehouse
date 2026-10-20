-- ============================================
-- Day8 电商数仓完整落地方案
-- 以 Day8_项目业务设计与数据模型落地.md 为权威
-- ODS/DWD/DWS/ADS/DIM 共 14 张表
-- 说明：先 DROP 重建，确保结构与 Day8 完全一致
-- ============================================

USE gmall;

-- ========== ODS 原始数据层 ==========

DROP TABLE IF EXISTS ods_user_log_di;
CREATE TABLE IF NOT EXISTS ods_user_log_di (
    log_time    STRING COMMENT '日志时间',
    user_id     STRING COMMENT '用户ID',
    event_type  STRING COMMENT '事件类型：view/cart/order/pay',
    page_url    STRING COMMENT '页面URL',
    sku_id      STRING COMMENT '商品SKU',
    session_id  STRING COMMENT '会话ID'
) COMMENT '用户行为日志-ODS'
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

DROP TABLE IF EXISTS ods_order_info_di;
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

DROP TABLE IF EXISTS ods_sku_info_di;
CREATE TABLE IF NOT EXISTS ods_sku_info_di (
    sku_id        STRING COMMENT '商品SKU',
    sku_name      STRING COMMENT '商品名称',
    category_id   STRING COMMENT '品类ID',
    category_name STRING COMMENT '品类名称',
    brand_id      STRING COMMENT '品牌ID',
    brand_name    STRING COMMENT '品牌名称',
    price         DECIMAL(10,2) COMMENT '单价'
) COMMENT '商品信息-ODS'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- ========== DWD 明细数据层 ==========

DROP TABLE IF EXISTS dwd_user_behavior_di;
CREATE TABLE IF NOT EXISTS dwd_user_behavior_di (
    behavior_id STRING COMMENT '行为唯一ID',
    user_id     STRING COMMENT '用户ID',
    event_type  STRING COMMENT '事件类型',
    event_time  TIMESTAMP COMMENT '事件时间',
    sku_id      STRING COMMENT '商品SKU',
    category_id STRING COMMENT '品类ID',
    session_id  STRING COMMENT '会话ID'
) COMMENT '用户行为明细-DWD'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

DROP TABLE IF EXISTS dwd_order_fact_di;
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

-- ========== 维度表 ==========

DROP TABLE IF EXISTS dim_user;
CREATE TABLE IF NOT EXISTS dim_user (
    user_id          STRING COMMENT '用户ID',
    register_date    STRING COMMENT '注册日期',
    gender           STRING COMMENT '性别',
    age_range        STRING COMMENT '年龄段',
    city_level       STRING COMMENT '城市等级',
    vip_level        STRING COMMENT '会员等级',
    first_order_date STRING COMMENT '首单日期'
) COMMENT '用户维度表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

DROP TABLE IF EXISTS dim_sku;
CREATE TABLE IF NOT EXISTS dim_sku (
    sku_id        STRING COMMENT '商品SKU',
    sku_name      STRING COMMENT '商品名称',
    category_id   STRING COMMENT '品类ID',
    category_name STRING COMMENT '品类名称',
    brand_name    STRING COMMENT '品牌名称',
    price         DECIMAL(10,2) COMMENT '单价'
) COMMENT '商品维度表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- ========== DWS 汇总数据层 ==========

DROP TABLE IF EXISTS dws_user_day;
CREATE TABLE IF NOT EXISTS dws_user_day (
    user_id          STRING COMMENT '用户ID',
    pv_cnt           BIGINT COMMENT '浏览次数',
    cart_cnt         BIGINT COMMENT '加购次数',
    order_cnt        BIGINT COMMENT '下单次数',
    pay_cnt          BIGINT COMMENT '支付次数',
    pay_amount       DECIMAL(18,2) COMMENT '支付金额',
    session_cnt      BIGINT COMMENT '会话数',
    first_event_time TIMESTAMP COMMENT '首次活跃时间',
    last_event_time  TIMESTAMP COMMENT '最后活跃时间'
) COMMENT '用户日汇总-DWS'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

DROP TABLE IF EXISTS dws_sku_day;
CREATE TABLE IF NOT EXISTS dws_sku_day (
    sku_id      STRING COMMENT '商品SKU',
    category_id STRING COMMENT '品类ID',
    pv_cnt      BIGINT COMMENT '浏览次数',
    cart_cnt    BIGINT COMMENT '加购次数',
    order_cnt   BIGINT COMMENT '下单次数',
    pay_cnt     BIGINT COMMENT '支付次数',
    gmv         DECIMAL(18,2) COMMENT '成交金额'
) COMMENT '商品日汇总-DWS'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 注：dws_order_day 原 DDL 将 dt 既作普通列又作分区列(报 Column repeated)，已去掉普通列 dt
DROP TABLE IF EXISTS dws_order_day;
CREATE TABLE IF NOT EXISTS dws_order_day (
    total_orders     BIGINT COMMENT '总订单数',
    paid_orders      BIGINT COMMENT '已支付订单数',
    cancelled_orders BIGINT COMMENT '已取消订单数',
    refunded_orders  BIGINT COMMENT '已退款订单数',
    total_amount     DECIMAL(18,2) COMMENT '订单总金额',
    paid_amount      DECIMAL(18,2) COMMENT '实际支付金额',
    unique_buyers    BIGINT COMMENT '下单用户数',
    unique_payers    BIGINT COMMENT '支付用户数'
) COMMENT '订单日汇总-DWS'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- ========== ADS 应用数据层 ==========

DROP TABLE IF EXISTS ads_dau;
CREATE TABLE IF NOT EXISTS ads_dau (
    dt         STRING COMMENT '日期',
    dau        BIGINT COMMENT '日活跃用户（去重）',
    new_dau    BIGINT COMMENT '新增日活',
    active_dau BIGINT COMMENT '连续活跃用户数',
    wakeup_dau BIGINT COMMENT '回流用户数'
) COMMENT '日活跃统计-ADS'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

DROP TABLE IF EXISTS ads_conversion_funnel;
CREATE TABLE IF NOT EXISTS ads_conversion_funnel (
    dt           STRING COMMENT '日期',
    view_uv      BIGINT COMMENT '浏览用户数',
    cart_uv      BIGINT COMMENT '加购用户数',
    order_uv     BIGINT COMMENT '下单用户数',
    pay_uv       BIGINT COMMENT '支付用户数',
    cart_rate    DECIMAL(6,4) COMMENT '浏览→加购转化率',
    order_rate   DECIMAL(6,4) COMMENT '加购→下单转化率',
    pay_rate     DECIMAL(6,4) COMMENT '下单→支付转化率',
    overall_rate DECIMAL(6,4) COMMENT '整体转化率'
) COMMENT '转化漏斗-ADS'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 注：ads_top_sku 原 DDL 将 dt 既作普通列又作分区列(报 Column repeated)，已去掉普通列 dt
DROP TABLE IF EXISTS ads_top_sku;
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

DROP TABLE IF EXISTS ads_repurchase_rate;
CREATE TABLE IF NOT EXISTS ads_repurchase_rate (
    dt                STRING COMMENT '统计日期',
    total_buyers      BIGINT COMMENT '累计下单用户数',
    repurchase_buyers BIGINT COMMENT '复购用户数（下单≥2次）',
    repurchase_rate   DECIMAL(6,4) COMMENT '复购率'
) COMMENT '用户复购率-ADS'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

SHOW TABLES;