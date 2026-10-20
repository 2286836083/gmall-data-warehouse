#!/bin/bash
# ============================================================
# ODS -> DWD / DIM 分步执行器（带进度状态文件）
#
# 【为什么要有这个脚本】
#   etl_ods_to_dwd.sql 里是 3 条 INSERT，一次性 beeline -f 提交的话，
#   中途无法知道跑到哪了——只能干等。这里把它拆成 5 个可观测步骤，
#   每步的起止时间、结果、退出码都写进 status.tsv，外部随时可读。
#
# 【产出】
#   $RUN/status.tsv   每行：编号 | 名称 | 状态 | 时刻 | 用时
#   $RUN/current      当前正在跑的步骤（跑完即删）
#   $RUN/done         全部完成的标志
#   $RUN/run.log      完整日志（含 beeline 输出）
#
# 【用法】
#   nohup bash run_dwd.sh > run.log 2>&1 &
#   watch -n5 cat status.tsv
# ============================================================
set -u

WS=/home/lrh/workspace
RUN=$WS/dwd_run
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH=$JAVA_HOME/bin:$PATH
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
URL="jdbc:hive2://lrh-1:10000"

mkdir -p "$RUN"
: > "$RUN/status.tsv"
rm -f "$RUN/current" "$RUN/done"

# ---- 公共 Hive 会话参数：每个分段都要重新声明（beeline 每次调用是新会话）----
# 2026-09-28 首次运行失败后的调优（根因：写入并发过高把 DataNode 全踢出 pipeline）
#   失败现象：could only be written to 0 of the 1 minReplication nodes，
#             3 node(s) are excluded，HDFS Write: 0；一条 INSERT 被拆成 11 个 MR 作业
#   对策就是下面这几行：降并发、少开文件、容忍一个节点掉线
cat > "$RUN/setup.sql" <<'SQL'
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
SET hive.exec.compress.intermediate = true;
SET hive.exec.compress.output = true;
-- ↓ 2026-09-28 新增：reducer 按分区排序后再写，避免同时打开上百个分区文件
SET hive.optimize.sort.dynamic.partition = true;
-- ↓ 新增：默认按数据量估出 6 个 reducer，并发写 31 个分区 × 3 副本压力过大，压到 3
SET mapreduce.job.reduces = 3;
-- ↓ 新增：3 个副本落在 3 个节点上，掉一个就整体失败；写入期降到 2，跑完恢复 3
SET dfs.replication = 2;
-- ↓ 新增：关掉小文件合并作业，减少额外 MR 作业（本次 11 个作业里有合并作业）
SET hive.merge.mapfiles = false;
SET hive.merge.mapredfiles = false;
SQL

# ---- 按注释标记把主 SQL 切成 4 段（主 SQL 始终是唯一事实来源）----
awk -v d="$RUN" '
  /^-- 第 2 步/ {p=2}
  /^-- 第 3 步/ {p=3}
  /^-- 校验/   {p=4}
  { if (p==0) print > (d"/p1.sql");
    else if (p==2) print > (d"/p2.sql");
    else if (p==3) print > (d"/p3.sql");
    else print > (d"/p4.sql") }
' "$WS/etl_ods_to_dwd.sql"

for i in 1 2 3 4; do
  cat "$RUN/setup.sql" "$RUN/p$i.sql" > "$RUN/p${i}_run.sql"
  printf '  分段 p%d: %s 行\n' "$i" "$(wc -l < "$RUN/p${i}_run.sql")"
done

run_step () {   # $1 编号  $2 名称  $3 SQL文件  [$4=loud]
  local no="$1" name="$2" f="$3" loud="${4:-}"
  local t0 t1 rc st el silent
  silent="--silent=true"
  [ "$loud" = "loud" ] && silent=""
  t0=$(date +%s)
  echo "$no|$name|RUNNING|$t0" > "$RUN/current"
  printf '%s\t%s\tSTART\t%s\n' "$no" "$name" "$(date '+%F %T')" >> "$RUN/status.tsv"
  echo ""
  echo ">>>>>> [$(date '+%T')] 步骤 $no · $name 开始"
  # shellcheck disable=SC2086
  $BEELINE -u "$URL" $silent -f "$f"
  rc=$?
  t1=$(date +%s); el=$((t1 - t0))
  if [ $rc -eq 0 ]; then st=OK; else st=FAIL; fi
  printf '%s\t%s\t%s\t%s\t%ss\n' "$no" "$name" "$st" "$(date '+%F %T')" "$el" >> "$RUN/status.tsv"
  echo ">>>>>> [$(date '+%T')] 步骤 $no · $name → $st（用时 ${el}s，退出码 $rc）"
  rm -f "$RUN/current"
  return $rc
}

echo "=========== 开始时间 $(date '+%F %T') ==========="

run_step 1 "建表 DWD/DIM 三张"   "$WS/dwd_ddl.sql"  || exit 1
run_step 2 "清洗事件明细 1063万"  "$RUN/p1_run.sql" || exit 2
run_step 3 "派生购买事实"        "$RUN/p2_run.sql" || exit 3
run_step 4 "派生商品维度"        "$RUN/p3_run.sql" || exit 4
run_step 5 "校验行数"            "$RUN/p4_run.sql" loud || exit 5

echo "ALL_DONE $(date '+%F %T')" | tee "$RUN/done"
