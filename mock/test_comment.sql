-- 重建 gmall 库（metastore 已重建，需重新建库建表）
CREATE DATABASE IF NOT EXISTS gmall COMMENT '电商用户行为分析离线数仓';
USE gmall;

-- IDENTITY：建一张带中文字段注释的临时表，验证中文注释存取
DROP TABLE IF EXISTS zz_test_comment;
CREATE TABLE zz_test_comment (
    user_id STRING COMMENT '用户编号',
    user_name STRING COMMENT '用户姓名'
) COMMENT '测试中文注释表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 用 SHOW CREATE TABLE 确认中文注释是否被正确存储（而非 ?）
SHOW CREATE TABLE zz_test_comment;