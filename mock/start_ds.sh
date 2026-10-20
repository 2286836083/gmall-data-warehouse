#!/bin/bash
# 启动 DolphinScheduler standalone 并等待就绪
source /etc/profile 2>/dev/null
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
BASE=~/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin

# 先停掉可能残留的（忽略报错）
$BASE/bin/dolphinscheduler-daemon.sh stop standalone-server >/dev/null 2>&1
sleep 2

echo "== 启动 standalone-server =="
$BASE/bin/dolphinscheduler-daemon.sh start standalone-server 2>&1 | tail -3

echo "== 等待 12345 端口 =="
for i in $(seq 1 24); do
  if ss -tlnp 2>/dev/null | grep -q ':12345'; then
    echo "12345 已监听"
    break
  fi
  sleep 5
done
ss -tlnp 2>/dev/null | grep ':12345' || echo "仍未监听"
sleep 15
echo "== 健康检查 =="
curl -s -m5 http://127.0.0.1:12345/dolphinscheduler/actuator/health 2>/dev/null || echo "无响应"