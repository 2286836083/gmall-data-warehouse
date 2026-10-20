#!/bin/bash
# 导入 DolphinScheduler 初始 SQL 到数据库
SQL=/home/hadoop/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/tools/sql/sql/dolphinscheduler_mysql.sql
echo "SQL 文件: $SQL"
echo "=== 导入开始（可能需要 1-2 分钟）==="
mysql -uds_user -pds_password123 dolphinscheduler < $SQL 2>&1 | grep -viE 'using a password|warning'
echo "=== 导入完成 ==="
echo "=== 验证表数量 ==="
mysql -uds_user -pds_password123 -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='dolphinscheduler';" 2>/dev/null