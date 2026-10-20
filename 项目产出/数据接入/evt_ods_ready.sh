#!/bin/bash
# ============================================================
# DS 工作流 gmall_evt_pipeline · 第 2 个节点：ODS 就绪（必要时装载）
#
# 【为什么写成"就绪检查"而不是直接 load_ods.sh】
#   ODS 是**一次性接入口**，不是每天都要重来：
#   把 1.3 GB 原始 CSV 重新 LOAD 一遍要十几分钟，且会 OVERWRITE 现有分区。
#   而原始 CSV（/home/lrh/workspace/real_data/）目前**已不在集群上**
#   （按约定大数据文件放在项目外的 D:\数据集\，避免 OneDrive 同步数 GB）。
#   所以：
#     · 源文件在 → 老老实实重装载（保证"从源头可复现"）
#     · 源文件不在 → 不假装成功，而是用**外部判据**核验集群上已有的 ODS
#                    是否与基线一致（31 分区 / 10,630,245 行），一致才放行
#   两条路都以"ODS 确实完整"为唯一放行条件，不会静默放过坏数据。
# ============================================================
set -uo pipefail

BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
URL="jdbc:hive2://lrh-1:10000"
TABLE=gmall.ods_event_log_di
DATADIR=/home/lrh/workspace/real_data
EXPECT_PART=31
EXPECT_ROWS=10630245

export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH=$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

n=$(ls -1 "$DATADIR"/*.csv 2>/dev/null | wc -l)
echo "源数据目录 $DATADIR ：$n 个按天 CSV"
if [ "${n:-0}" -ge "$EXPECT_PART" ]; then
  echo "→ 源文件齐，执行 load_ods.sh 重新装载（会 OVERWRITE 31 个分区）"
  bash /home/lrh/workspace/load_ods.sh || { echo "!! load_ods.sh 失败"; exit 1; }
else
  echo "→ 源文件不在（只有 ${n:-0} 个），跳过重载；改为核验集群上已装载的 ODS"
fi

bl () { "$BEELINE" -u "$URL" --silent=true --outputformat=csv2 -e "$1" 2>/dev/null | tr -d '\r' | grep -E '^[0-9]+$' | head -1; }

part=$(bl "select count(distinct dt) from $TABLE;")
rows=$(bl "select count(1) from $TABLE;")
echo "  分区数 = ${part:-?}（期望 $EXPECT_PART）"
echo "  总行数 = ${rows:-?}（期望 $EXPECT_ROWS）"

[ "${part:-0}" -eq "$EXPECT_PART" ] || { echo "!! ODS 分区数不符，终止"; exit 1; }
[ "${rows:-0}" -eq "$EXPECT_ROWS" ] || { echo "!! ODS 总行数与基线不一致，终止"; exit 1; }

echo "==== ODS READY ===="
