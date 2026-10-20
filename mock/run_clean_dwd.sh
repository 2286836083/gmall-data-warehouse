#!/bin/bash
# 脱离控制终端跑 beeline，避免 jline SIGTTOU 冻结（默认用户即 hadoop，无需 sudo）
pkill -f 'BeeLine.*clean_dwd' 2>/dev/null
sleep 2
rm -f /tmp/clean_dwd_run.log
setsid bash -c 'source /etc/profile; beeline -u jdbc:hive2://localhost:10000 --silent=true -f /home/hadoop/workspace/clean_dwd.sql </dev/null >/tmp/clean_dwd_run.log 2>&1; echo EXIT_CODE=$? >> /tmp/clean_dwd_run.log' </dev/null >/dev/null 2>&1 &
sleep 5
echo "== beeline process =="
pgrep -af 'BeeLine' | head -3
echo "== log head =="
head -8 /tmp/clean_dwd_run.log 2>/dev/null
