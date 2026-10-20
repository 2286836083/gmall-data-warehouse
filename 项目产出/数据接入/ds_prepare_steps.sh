#!/bin/bash
# ============================================================
# 为 DolphinScheduler 工作流 gmall_evt_pipeline 生成"每节点一份"的分段 SQL
#
# 【为什么需要这个脚本】
#   DS 的每个 SHELL 节点只能执行一个 SQL 文件，而 etl_ods_to_dwd.sql /
#   evt_etl.sql 都是"一个文件里多条 INSERT"。所以要按节点切成多段。
#
# 【单一时真源，绝不复制粘贴】
#   · SQL 正文：直接切 etl_ods_to_dwd.sql / evt_etl.sql（唯一事实来源）
#   · 公共 SET 参数：从 dwd_run.sh / evt_run.sh 的 setup heredoc 里**抽取**，
#     而不是在这里重抄一遍。否则以后调优只改了 runner、忘了改这里，
#     就会重演"参数看起来有、其实没生效"（本项目的 Error 10096 老坑）。
#
# 【产物】/home/lrh/workspace/ds_steps/
#   dwd_setup.sql / evt_setup.sql    从 runner 抽出来的公共 SET 参数
#   dwd_step2..dwd_step5.sql         DWD 各节点（setup 已拼好）
#   evt_step1..evt_step8.sql         DWS/ADS 各节点（setup 已拼好）
#   .ready                           全部生成 + 自检通过后才写，作为完成标志
#
# 【退出码】任何一步失败都非 0 —— DS 就是靠退出码判定节点成功/失败的
# ============================================================
set -uo pipefail

WS=/home/lrh/workspace
OUT=$WS/ds_steps
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline

fail () { echo "!! $*"; exit 1; }

rm -rf "$OUT"; mkdir -p "$OUT" || fail "无法创建 $OUT"

# ---- 1. 从两个 runner 里抽取公共 SET 参数块 -------------------------------
# dwd_run.sh / evt_run.sh 里各有一个  cat > "$RUN/setup.sql" <<'SQL' ... SQL
# 的 heredoc，这里把它整段抠出来，避免二次维护。
for pair in "dwd_run.sh:dwd_setup.sql" "evt_run.sh:evt_setup.sql"; do
  src="$WS/${pair%%:*}"; dst="$OUT/${pair##*:}"
  sed -n "/<<'SQL'/,/^SQL\$/p" "$src" | sed '1d;$d' > "$dst"
  [ -s "$dst" ] || fail "未能从 $src 抽取到 SET 参数块"
done

# ---- 2. 按注释标记切分 SQL 正文 -------------------------------------------
# DWD：与 dwd_run.sh 完全一致的切法（第 2 步 / 第 3 步 / 校验 三个标记）
awk -v d="$OUT" '
  /^-- 第 2 步/ {p=2}
  /^-- 第 3 步/ {p=3}
  /^-- 校验/   {p=4}
  { if (p==0)      print > (d"/dwd_raw1.sql");
    else if (p==2) print > (d"/dwd_raw2.sql");
    else if (p==3) print > (d"/dwd_raw3.sql");
    else           print > (d"/dwd_raw4.sql") }
' "$WS/etl_ods_to_dwd.sql" || fail "切分 etl_ods_to_dwd.sql 失败"

# DWS/ADS：按 "-- ===== STEP n / ..." 标记切 8 段
awk -v d="$OUT" '
  /^-- ===== STEP / { n++; print > (d"/evt_raw_" n ".sql"); next }
  n > 0 { print >> (d"/evt_raw_" n ".sql") }
' "$WS/evt_etl.sql" || fail "切分 evt_etl.sql 失败"

# ---- 3. 每个分段前面拼上 setup（beeline 每次调用都是全新会话！）----------
for i in 1 2 3 4; do
  [ -s "$OUT/dwd_raw$i.sql" ] || fail "etl_ods_to_dwd.sql 没切出第 $i 段"
  cat "$OUT/dwd_setup.sql" "$OUT/dwd_raw$i.sql" > "$OUT/dwd_step$((i+1)).sql"
done
for i in 1 2 3 4 5 6 7 8; do
  [ -s "$OUT/evt_raw_$i.sql" ] || fail "evt_etl.sql 没切出第 $i 段"
  cat "$OUT/evt_setup.sql" "$OUT/evt_raw_$i.sql" > "$OUT/evt_step$i.sql"
done
rm -f "$OUT"/dwd_raw*.sql "$OUT"/evt_raw_*.sql

# ---- 4. 启动前自检：参数必须真的拼进每个分段文件 -------------------------
for f in "$OUT"/dwd_step*.sql "$OUT"/evt_step*.sql; do
  grep -qF 'hive.exec.dynamic.partition.mode' "$f" || fail "$(basename "$f") 缺 hive.exec.dynamic.partition.mode（参数没拼上）"
  grep -qF 'mapreduce.job.reduces'            "$f" || fail "$(basename "$f") 缺 mapreduce.job.reduces（参数没拼上）"
done

# ---- 5. 汇总 --------------------------------------------------------------
echo "已生成分段 SQL → $OUT"
for f in "$OUT"/dwd_step*.sql "$OUT"/evt_step*.sql; do
  printf "  %-22s %5s 行\n" "$(basename "$f")" "$(wc -l < "$f")"
done
echo OK > "$OUT/.ready"
echo "PREPARE_STEPS_OK"
