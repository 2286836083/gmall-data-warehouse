USE gmall;
DROP TABLE IF EXISTS zz_chk;
CREATE TABLE zz_chk (c1 STRING COMMENT '中文注释字段测试', c2 INT COMMENT 'second column中文')
COMMENT 'table中文注释测试'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 写一行中文数据再读回，验证数据编码链路
INSERT INTO zz_chk VALUES ('中文行', 1);
SELECT c1, c2 FROM zz_chk;
-- 显示建表语句看注释
SHOW CREATE TABLE zz_chk;