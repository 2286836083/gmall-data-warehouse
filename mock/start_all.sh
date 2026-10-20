#!/bin/bash
# 带 sudo 密码启动大数据全栈并等待就绪
PW=hadoop123

echo "== 1. HDFS =="
echo $PW | sudo -S systemctl start hadoop-hdfs 2>&1 | grep -viE '^\[sudo\] ' 
echo $PW | sudo -S systemctl is-active hadoop-hdfs 2>/dev/null

echo "== 2. YARN =="
echo $PW | sudo -S systemctl start hadoop-yarn 2>&1 | grep -viE '^\[sudo\] '
echo $PW | sudo -S systemctl is-active hadoop-yarn 2>/dev/null

echo "== 3. 等 HDFS 出安全模式 =="
echo $PW | sudo -S /opt/hadoop-3.3.6/bin/hdfs dfsadmin -safemode wait 2>/dev/null | tail -1

echo "== 4. Hive Metastore =="
echo $PW | sudo -S systemctl start hive-metastore 2>&1 | grep -viE '^\[sudo\] '
echo $PW | sudo -S systemctl is-active hive-metastore 2>/dev/null

echo "== 5. HiveServer2 =="
echo $PW | sudo -S systemctl start hive-hiveserver2 2>&1 | grep -viE '^\[sudo\] '
echo $PW | sudo -S systemctl is-active hive-hiveserver2 2>/dev/null

echo "== 6. 等待 HiveServer2(10000) 就绪 =="
for i in $(seq 1 18); do
  if ss -tlnp 2>/dev/null | grep -q ':10000'; then
    echo "HiveServer2(10000) 就绪，等待 $i 秒后已可连接"
    break
  fi
  sleep 5
done
ss -tlnp 2>/dev/null | grep -E ':9083|:10000|:9870|:8088'