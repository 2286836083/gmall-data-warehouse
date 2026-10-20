#!/bin/bash
# 直接执行，不用变量，避免 sudo 管道拆分问题
echo "=== 0. root 能否进 mysql ==="
echo hadoop123 | sudo -S mysql -e "SELECT 'root_ok' AS r;" 2>&1 | grep -viE 'password'

echo "=== 1. 建库 ==="
echo hadoop123 | sudo -S mysql -e "CREATE DATABASE IF NOT EXISTS dolphinscheduler DEFAULT CHARACTER SET utf8mb4;" 2>&1 | grep -viE 'password'
echo "=== 2. 建用户 ==="
echo hadoop123 | sudo -S mysql -e "CREATE USER IF NOT EXISTS 'ds_user'@'%' IDENTIFIED BY 'ds_password123'; GRANT ALL PRIVILEGES ON dolphinscheduler.* TO 'ds_user'@'%'; FLUSH PRIVILEGES;" 2>&1 | grep -viE 'password'
echo "=== 3. 确认库 ==="
echo hadoop123 | sudo -S mysql -N -e "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='dolphinscheduler';" 2>&1 | grep -viE 'password'
echo "=== 4. 确认用户 ==="
echo hadoop123 | sudo -S mysql -N -e "SELECT user,host FROM mysql.user WHERE user='ds_user';" 2>&1 | grep -viE 'password'
echo "=== 5. ds_user 连接测试 ==="
mysql -uds_user -pds_password123 -e "SELECT 1 AS ok;" 2>&1 | grep -viE 'password|warning'