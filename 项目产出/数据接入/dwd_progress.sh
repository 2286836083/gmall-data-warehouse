#!/bin/bash
# ============================================================
# DWD 执行进度探针 —— 一次调用输出全部可观测状态
#
# 【为什么单独写一个】
#   非交互 SSH 里 PATH 不含 Hadoop，裸写 yarn/mapred 会 command not found，
#   若再用 2>/dev/null 吞掉报错，就会误判成「作业没提交」。
#   这里统一用全路径，避免这个坑。
#
# 用法：ssh lrh@lrh-1 bash /home/lrh/workspace/dwd_progress.sh [运行目录名]
#       默认 dwd_run（DWD 清洗落表）；也可传 evt_run（DWS/ADS 汇总）
# ============================================================
cd /home/lrh/workspace || exit 1
RUN="${1:-dwd_run}"
HADOOP=/opt/hadoop/hadoop-3.3.6/bin

echo "### 进程"
pgrep -f "bash ${RUN}.sh" >/dev/null && echo "runner=RUNNING" || echo "runner=STOPPED"
if [ -f "$RUN/done" ]; then echo "done=$(cat "$RUN/done")"; fi
echo "start_epoch=$(head -1 "$RUN/status.tsv" 2>/dev/null | awk -F'\t' '{print $4}' | xargs -I{} date -d '{}' +%s 2>/dev/null)"

echo "### 步骤状态"
cat "$RUN/status.tsv" 2>/dev/null

echo "### 当前步骤"
cat "$RUN/current" 2>/dev/null || echo "(无)"

echo "### MR 作业"
JID=$("$HADOOP/yarn" application -list 2>/dev/null | awk '/application_/{print $1; exit}')
if [ -n "$JID" ]; then
  echo "app=$JID"
  "$HADOOP/mapred" job -status "$JID" 2>/dev/null \
    | grep -E "State:|^\s*map [0-9]+%|^\s*reduce [0-9]+%|Total maps|Total reduces" \
    | sed 's/^\s*/  /'
else
  echo "app=(无运行中的作业)"
fi

echo "### Hive 当前阶段百分比（最后一行）"
journalctl -u hive-server2 -n 80 --no-pager 2>/dev/null \
  | grep -oE "Stage-[0-9]+ map = [0-9]+%,  reduce = [0-9]+%" | tail -1 | sed 's/^/  /'

echo "### Hive 子作业编排"
journalctl -u hive-server2 -n 80 --no-pager 2>/dev/null \
  | grep -E "Total jobs|Launching Job [0-9]+ out of" | tail -3 \
  | sed 's/.*hive\[[0-9]*\]: //' | sed 's/^/  /'

echo "### Hive 最近 3 行原始日志"
journalctl -u hive-server2 -n 3 --no-pager 2>/dev/null \
  | sed 's/.*hive\[[0-9]*\]: //' | cut -c1-150 | sed 's/^/  /'

echo "### 内存磁盘"
free -m | sed -n '2p' | sed 's/^/  /'
df -h / | tail -1 | sed 's/^/  /'
