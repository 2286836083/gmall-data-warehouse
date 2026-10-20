#!/usr/bin/env bash
# ============================================================
# 把 REES46 真实数据装载进 ODS 层
#
#   在 lrh-1 上执行：  bash load_ods.sh
#
# 前置：数据已传到 /home/lrh/workspace/real_data/（31 个按天 CSV，文件名即日期）
#
# 做的事：
#   1. 建表（执行 ods_event_log_ddl.sql，会重建，幂等）
#   2. 逐天 ADD PARTITION + LOAD DATA LOCAL INPATH
#   3. 校验：分区数、总行数、各天行数、各事件类型分布
# ============================================================
set -eu

DATADIR="${1:-/home/lrh/workspace/real_data}"
HERE="$(cd "$(dirname "$0")" && pwd)"
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
URL="jdbc:hive2://lrh-1:10000"
TABLE=gmall.ods_event_log_di

export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH="$JAVA_HOME/bin:$PATH"

if [ ! -d "$DATADIR" ]; then
  echo "数据目录不存在：$DATADIR"; exit 1
fi
n_files=$(ls -1 "$DATADIR"/*.csv 2>/dev/null | wc -l)
if [ "$n_files" -eq 0 ]; then
  echo "目录里没有 CSV：$DATADIR"; exit 1
fi
echo "==== 找到 $n_files 个按天 CSV ===="

bl() { "$BEELINE" -u "$URL" --silent=true "$@"; }

echo
echo "==== [1/3] 建表 ===="
bl -f "$HERE/ods_event_log_ddl.sql" 2>/dev/null && echo "  建表完成"
bl -e "DESC $TABLE" 2>/dev/null | head -12 | sed 's/^/  /'

echo
echo "==== [2/3] 逐天装载 ===="
# 把 62 条语句（31 组 ALTER + LOAD）写进一个文件一次性执行。
# 为什么不是每天单独调一次 beeline：每次 beeline 都要冷启一个 JVM（约 5 秒），
# 31 天 × 3 次调用 ≈ 93 次启动，白白等七八分钟。
SQL="/tmp/load_ods_$$.sql"
: > "$SQL"
n=0
for f in "$DATADIR"/*.csv; do
  d=$(basename "$f" .csv)
  n=$((n + 1))
  echo "ALTER TABLE $TABLE ADD IF NOT EXISTS PARTITION (dt='$d');" >> "$SQL"
  echo "LOAD DATA LOCAL INPATH '$f' OVERWRITE INTO TABLE $TABLE PARTITION (dt='$d');" >> "$SQL"
done
echo "  已生成 $n 天的装载语句，开始执行（拷贝 1.3GB 数据进 HDFS，需几分钟）..."
bl -f "$SQL" 2>/dev/null
rm -f "$SQL"
echo "  装载语句执行完毕"

echo
echo "==== [3/3] 校验 ===="
echo "-- 分区数（应为 31）--"
bl -e "SELECT COUNT(DISTINCT dt) AS part_cnt FROM $TABLE;" 2>/dev/null | grep -E '^[0-9]+$' | sed 's/^/  分区数: /'

echo "-- 总行数（应约 10,630,245）--"
bl -e "SELECT COUNT(1) AS total_rows FROM $TABLE;" 2>/dev/null | grep -E '^[0-9]+$' | sed 's/^/  总行数: /'

echo "-- 事件类型分布 --"
bl --outputformat=csv2 -e "SELECT event_type, COUNT(1) AS cnt FROM $TABLE GROUP BY event_type ORDER BY cnt DESC;" 2>/dev/null | sed 's/^/  /'

echo "-- 每日行数（前 5 天与后 5 天）--"
bl --outputformat=csv2 -e "SELECT dt, COUNT(1) AS cnt FROM $TABLE GROUP BY dt ORDER BY dt LIMIT 5;" 2>/dev/null | sed 's/^/  /'
echo "  ..."
bl --outputformat=csv2 -e "SELECT dt, COUNT(1) AS cnt FROM $TABLE GROUP BY dt ORDER BY dt DESC LIMIT 5;" 2>/dev/null | sed 's/^/  /'

echo
echo "==== HDFS 落盘情况 ===="
hdfs dfs -du -s -h /user/hive/warehouse/gmall.db/ods_event_log_di 2>/dev/null | sed 's/^/  总占用: /'
hdfs dfs -ls /user/hive/warehouse/gmall.db/ods_event_log_di 2>/dev/null | head -4 | sed 's/^/  /'

echo
echo "==== 完成 ===="
