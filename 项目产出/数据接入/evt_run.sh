#!/bin/bash
# ============================================================
# 新链路（真实数据）DWS / ADS 分步执行器
#
# 【为什么要把 evt_etl.sql 切成 8 段跑】
#   一次性 beeline -f 提交的话，中途无法知道跑到哪一步了。切成 8 段后
#   每段单独一个 MR 作业，状态写进 status.tsv，进度看板能实时显示。
#
# 【步骤清单】共 10 步
#   1    建表（DDL）
#   2-5  DWS 四张主题宽表（重，每张都要扫 1063 万行明细）
#   6-9  ADS 四个指标表（轻，基于 DWS 小表）
#   10   校验
#
# 【产出】
#   $RUN/status.tsv   编号 | 名称 | 状态 | 时刻 | 用时
#   $RUN/current      当前步骤（跑完即删）
#   $RUN/done         全部完成标志
#
# 【用法】
#   setsid nohup bash evt_run.sh > run_evt.log 2>&1 < /dev/null &
# ============================================================
set -u

# 断点续跑：evt_run.sh [起始步骤号]。默认 1（全新跑）；传 3 就从第 3 步接着跑。
START_FROM="${1:-1}"

WS=/home/lrh/workspace
RUN=$WS/evt_run
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH=$JAVA_HOME/bin:$PATH
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
URL="jdbc:hive2://lrh-1:10000"

mkdir -p "$RUN"
if [ "$START_FROM" -le 1 ]; then
  : > "$RUN/status.tsv"
  rm -f "$RUN/current" "$RUN/done"
else
  echo "(续跑模式：从第 $START_FROM 步开始，保留已有 status.tsv)"
fi

# ---- 公共 Hive 会话参数 ----------------------------------------------------
# 直接复用实验 03 复盘出来的调优：动态分区写入必须降并发，否则 DataNode 会被全踢出
cat > "$RUN/setup.sql" <<'SQL'
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
SET hive.exec.compress.intermediate = true;
SET hive.exec.compress.output = true;
SET hive.optimize.sort.dynamic.partition = true;
SET mapreduce.job.reduces = 3;
SET dfs.replication = 2;
SET hive.merge.mapfiles = false;
SET hive.merge.mapredfiles = false;
SET hive.exec.mode.local.auto = false;
-- ↓ 2026-09-28 第 3 步 GC overhead limit exceeded 后的修复
--   根因：按 product_id × dt 分组，分组组合可达 430 万个，
--   map 端聚合哈希表扛不住 9/27 调优压到的 -Xmx410m 堆。
--   → 把聚合路径的容器恢复到 1024MB（旧 DWS 脚本原本就是 768m 堆）
--   → 并把 map 端哈希上限压到堆的 30%，超了就退化到落盘而不是硬撑
SET mapreduce.map.memory.mb = 1024;
SET mapreduce.reduce.memory.mb = 1024;
SET mapreduce.map.java.opts = -Xmx820m;
SET mapreduce.reduce.java.opts = -Xmx820m;
SET hive.map.aggr.hash.percentmemory = 0.3;
-- ↓ 让每个 map 少读一点：128MB 一个分片，摊薄单个 map 的哈希规模
SET mapreduce.input.fileinputformat.split.maxsize = 134217728;
SQL

# ---- 按「-- ===== STEP n / 名称 =====」标记把 evt_etl.sql 切成多段 ----
awk -v d="$RUN" '
  /^-- ===== STEP / { n++; print > (d"/step_" n ".sql"); next }
  n > 0 { print >> (d"/step_" n ".sql") }
' "$WS/evt_etl.sql"

# ⚠️ 关键：必须把 setup.sql 拼在每个分段**前面**，否则参数等于没写！
#    2026-09-28 踩过：只生成了 setup.sql 却没拼接，
#    结果 hive.exec.dynamic.partition.mode 仍是 strict，
#    第 2 步动态分区写入直接报 SemanticException [Error 10096]。
#    教训：**"参数文件生成了" ≠ "参数生效了"**，必须验证参数真的到达执行上下文。
for f in "$RUN"/step_*.sql; do
  cat "$RUN/setup.sql" "$f" > "${f%.sql}_run.sql"
  rm -f "$f"
done

STEP_NAMES=(
  "DWS·用户主题（用户×天）"
  "DWS·商品主题（商品×天）"
  "DWS·类目主题（类目×天）"
  "DWS·交易主题（交易×天）"
  "ADS·活跃用户 DAU/新增/回流"
  "ADS·转化漏斗（三环节）"
  "ADS·累计复购率"
  "ADS·TopN 商品"
)

echo "STEP_NAMES 共 ${#STEP_NAMES[@]} 个；实际切出分段数：$(ls -1 "$RUN"/step_*_run.sql 2>/dev/null | wc -l)"

# 启动前先自检参数是否真的拼进去了（避免重演 10096）
if ! grep -q "hive.exec.dynamic.partition.mode" "$RUN/step_1_run.sql" 2>/dev/null; then
  echo "!! 参数未拼接进分段文件，终止执行"; exit 1
fi
echo "自检通过：分段文件已包含公共 Hive 会话参数"

run_step () {   # $1 编号  $2 名称  $3 SQL文件  [$4=loud]
  local no="$1" name="$2" f="$3" loud="${4:-}"
  local t0 t1 rc st silent
  if [ "$no" -lt "$START_FROM" ]; then
    echo "  (跳过步骤 $no · 已完成)"
    return 0
  fi
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
  t1=$(date +%s); local el=$((t1 - t0))
  if [ $rc -eq 0 ]; then st=OK; else st=FAIL; fi
  printf '%s\t%s\t%s\t%s\t%ss\n' "$no" "$name" "$st" "$(date '+%F %T')" "$el" >> "$RUN/status.tsv"
  echo ">>>>>> [$(date '+%T')] 步骤 $no · $name → $st（用时 ${el}s，退出码 $rc）"
  rm -f "$RUN/current"
  return $rc
}

echo "=========== 开始时间 $(date '+%F %T') ==========="

run_step 1 "建表 DWS/ADS 八张" "$WS/evt_ddl.sql" || exit 1

i=0
for name in "${STEP_NAMES[@]}"; do
  i=$((i + 1))
  run_step $((i + 1)) "$name" "$RUN/step_${i}_run.sql" || exit $((i + 1))
done

run_step 10 "校验（行数+交叉对账+方向性）" "$WS/evt_verify.sql" loud || exit 10

echo "ALL_DONE $(date '+%F %T')" | tee "$RUN/done"
