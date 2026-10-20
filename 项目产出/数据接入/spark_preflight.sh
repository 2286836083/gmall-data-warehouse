#!/bin/bash
# ============================================================
# Spark 旁路实验的「开工前检查」：有残留 Spark 应用就拒绝开工
#
# 【为什么必须检查】
#   DS 停任务**杀不掉** ssh 出去的远端进程（见《排障汇总》第 20 坑）：
#   断开 ssh 时 ssd 不发 SIGHUP，spark-sql 会变成孤儿进程继续跑。
#   如果上一轮被中断，YARN 上可能还挂着 SparkSQL 应用 ——
#   新的一轮起来会和它**抢资源**，而且两边都往 gmall_bench 写 → 数据互相污染。
#
# 【判定与处置】
#   发现残留 → exit 1（拒绝开工）；人工确认没人在跑之后，用 yarn application -kill 清掉。
#
# 用法：bash /home/lrh/workspace/spark_preflight.sh
# ============================================================
set -u
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH=$JAVA_HOME/bin:$PATH
Y=/opt/hadoop/hadoop-3.3.6/bin/yarn
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline

echo "== [1/3] 残留 Spark 应用检查 =="
$Y application -list 2>/dev/null | grep -E "Application-Id|SparkSQL" | head -10
n=$($Y application -list 2>/dev/null | grep -c "SparkSQL" || true)
if [ "${n:-0}" -gt 0 ]; then
  echo "!! 发现 $n 个残留 SparkSQL 应用 —— 拒绝开工（避免抢资源 / 污染 gmall_bench）"
  echo "   处置：确认没人在跑之后  $Y application -kill <appId>"
  exit 1
fi
echo "OK：无残留 Spark 应用"

echo "== [2/3] 目标库可访问性 =="
$BEELINE -u jdbc:hive2://lrh-1:10000 --silent=true -e "SHOW DATABASES;" 2>/dev/null \
  | grep -q "gmall" && echo "OK：gmall 库可访问" || { echo "!! gmall 库不可访问"; exit 1; }

echo "== [3/3] 分段 SQL 是否就绪（8 段）=="
c=$(ls -1 /home/lrh/workspace/spark_bench/step_*.sql 2>/dev/null | wc -l)
if [ "$c" -ne 8 ]; then
  echo "!! 分段 SQL 不是 8 个（实际 $c）—— 先跑 spark_bench_run.sh 的生成段"
  exit 1
fi
echo "OK：分段 SQL 共 $c 段"

echo "PREFLIGHT_OK"
exit 0
