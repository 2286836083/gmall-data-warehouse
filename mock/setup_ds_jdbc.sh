#!/bin/bash
# 下载 MySQL Connector/J 并复制到各服务 libs 目录
BASE=/home/hadoop/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin
VER=8.0.33
JAR=mysql-connector-j-$VER.jar
DRIVER_DIR=$BASE/tools/libs

echo "=== 下载 MySQL Connector/J $VER ==="
if [ ! -f "$DRIVER_DIR/$JAR" ]; then
  wget -q "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/$VER/$JAR" -O "$DRIVER_DIR/$JAR" && echo "下载完成" || { echo "Maven 链接失败，尝试镜像"; wget -q "https://mirrors.aliyun.com/maven2/com/mysql/mysql-connector-j/$VER/$JAR" -O "$DRIVER_DIR/$JAR" && echo "阿里镜像下载完成"; }
fi
ls -lh "$DRIVER_DIR/$JAR"

echo "=== 复制到各服务 libs ==="
for svc in standalone-server/libs/standalone-server api-server/libs master-server/libs worker-server/libs alert-server/libs; do
  cp "$DRIVER_DIR/$JAR" "$BASE/$svc/" && echo "已复制到 $svc"
done

echo "=== 确认 mysql jar 位置 ==="
find $BASE -iname 'mysql-connector*.jar' 2>/dev/null