#!/bin/bash
# ============================================================
# DS 工作流 gmall_evt_pipeline · 第 1 个节点：开工前体检
#
# 【为什么要一个"体检"节点】
#   1. systemctl is-active = active ≠ 服务真能用（本项目已踩过：HiveServer2
#      标记 active 后还要几十秒才监听 10000）→ 所以必须用外部判据。
#   2. DS 停任务时**杀不掉**远端 ssh→beeline 进程（本项目排障文档第 4 坑）。
#      上一次被停掉的 dws_order，其 beeline 在 lrh-1 上又跑了 5 天数据。
#      所以开工前必须确认没有残留，否则两个 beeline 抢 HiveServer2。
#   3. DS 每个节点只跑一个文件，需要先把 SQL 切成每节点一份。
#
# 体检顺序：服务 → 规模 → HS2 真可用 → 无残留 → 生成分段 SQL
# 任何一项不过就非 0 退出，DS 会让工作流停在第一个节点，不会往下污染数据。
# ============================================================
set -uo pipefail

H=/opt/hadoop/hadoop-3.3.6/bin
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH=$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

echo "==== [1/5] 基础服务（lrh-1）===="
rc=0
for s in hadoop zookeeper mysql hive-metastore hive-server2; do
  a=$(systemctl is-active "$s" 2>/dev/null)
  printf "  %-16s %s\n" "$s" "$a"
  [ "$a" = "active" ] || rc=1
done
[ $rc -eq 0 ] || { echo "!! 有服务不是 active，终止"; exit 1; }

echo "==== [2/5] 集群规模 ===="
dn=$($H/hdfs dfsadmin -report 2>/dev/null | grep -oP 'Live datanodes \(\K[0-9]+' | head -1)
nm=$($H/yarn node -list 2>/dev/null | grep -oP 'Total Nodes:\K[0-9]+' | head -1)
echo "  Live DataNode = ${dn:-?}    NodeManager = ${nm:-?}"
[ "${dn:-0}" = "3" ] || { echo "!! Live DataNode 不是 3 个，终止"; exit 1; }
[ "${nm:-0}" = "3" ] || { echo "!! NodeManager 不是 3 个，终止"; exit 1; }

echo "==== [3/5] HiveServer2 真可用（外部判据）===="
up=0
for i in $(seq 1 60); do
  if ss -lntp 2>/dev/null | grep -q ':10000'; then up=1; break; fi
  sleep 5
done
[ $up -eq 1 ] || { echo "!! 等待 300 秒后 10000 端口仍未监听（is-active 会说谎）"; exit 1; }
if $BEELINE -u jdbc:hive2://lrh-1:10000 --silent=true -e "select 1 as probe;" >/dev/null 2>&1; then
  echo "  端口监听 + 真实查询 都通过"
else
  echo "!! 10000 端口在监听，但真实查询失败（连接能建立、查询跑不通）"
  exit 1
fi

echo "==== [4/5] 残留进程 / 集群是否空闲 ===="
# 用中括号拆开关键字：-f 匹配整条命令行，若直接写 'jdbc:hive2'，
# 这条检查命令**自己**的命令行里就含有该字符串，会被算成一个"残留进程"（假阳性）。
# 写成 jdbc[:]hive2 后，正则仍匹配 jdbc:hive2，但本命令的字面内容不再匹配自己。
left=$(pgrep -af 'jdbc[:]hive2' 2>/dev/null | grep -v pgrep || true)
if [ -n "$left" ]; then
  echo "$left" | sed 's/^/  /'
  echo "!! 检测到 beeline 正在跑。很可能是上一轮被 DS 停掉后残留下来的孤儿进程"
  echo "   （DS 只杀 worker 上的进程树，杀不掉 lrh-1 上 sshd 拉起的 beeline）。"
  echo "   两个 beeline 同跑会互抢 HiveServer2，故本节点拒绝开工。"
  echo "   确认没有别人在跑之后，手工清理：pkill -f 'jdbc:hive2'"
  exit 1
fi
apps=$($H/yarn application -list 2>/dev/null | grep -cE '^[[:space:]]*application_[0-9]+_[0-9]+')
echo "  运行中的 YARN 应用数 = $apps"
[ "${apps:-1}" -eq 0 ] || { echo "!! 集群上有 YARN 应用在跑，不是空闲状态，终止"; exit 1; }

echo "==== [5/5] 生成每节点分段 SQL ===="
bash /home/lrh/workspace/ds_prepare_steps.sh || exit 1

echo "==== PREFLIGHT OK ===="
