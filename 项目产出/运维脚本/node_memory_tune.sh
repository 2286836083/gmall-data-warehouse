#!/usr/bin/env bash
# ============================================================
# 单节点内存调优脚本（需 root 执行，幂等，可重复运行不会重复追加）
#
#   sudo bash node_memory_tune.sh
#
# 调优内容：
#   1. Hadoop 各 daemon 堆上限 512m（原来未设置 -> JVM 按物理内存自适应约 1GB）
#   2. YARN RM / NM 堆 512m
#   3. YARN 容器调度上下限 + vcore 数
#   4. MapReduce 容器内存 512m、JVM 重用 10、关闭推测执行
#   5. ZooKeeper 堆 1000m -> 512m
#   6. vm.swappiness 60 -> 10
#   7. MySQL 关闭 performance_schema（仅装有 MySQL 的节点）
#
# 改前自动备份到 /opt/backup/YYYY-MM-DD/
# 只改配置文件，不重启服务；重启由调用方决定时机。
# ============================================================
set -u

HD=/opt/hadoop/hadoop-3.3.6/etc/hadoop
ZK_ENV=/opt/zookeeper/apache-zookeeper-3.8.7-bin/bin/zkEnv.sh
MY_CNF=/etc/mysql/mysql.conf.d/mysqld.cnf
BK="/opt/backup/$(date +%F)"
MARK="LRH-TUNING"

say() { echo "    $*"; }

# 在 </configuration> 前插入一个 <property>，已存在则跳过
add_xml_prop() {
  local file="$1" name="$2" val="$3"
  [ -f "$file" ] || { say "跳过（文件不存在）: $file"; return 0; }
  if grep -q "<name>${name}</name>" "$file"; then
    say "已存在，跳过: $name"
    return 0
  fi
  awk -v n="$name" -v v="$val" '
    /<\/configuration>/ && !done {
      print "  <property>"
      print "    <name>" n "</name>"
      print "    <value>" v "</value>"
      print "  </property>"
      done=1
    }
    { print }
  ' "$file" > "$file.tmp" && mv "$file.tmp" "$file"
  say "新增: $name = $val"
}

echo "==== 内存调优 @ $(hostname) ===="

echo "[1/7] 备份配置 -> $BK"
mkdir -p "$BK"
for f in "$HD/hadoop-env.sh" "$HD/yarn-env.sh" "$HD/mapred-env.sh" \
         "$HD/yarn-site.xml" "$HD/mapred-site.xml" "$ZK_ENV" \
         /etc/sysctl.conf "$MY_CNF"; do
  if [ -f "$f" ]; then cp -f "$f" "$BK/$(basename "$f")" && say "备份 $f"; fi
done

echo "[2/7] Hadoop daemon 堆上限（hadoop-env.sh）"
if grep -q "$MARK" "$HD/hadoop-env.sh" 2>/dev/null; then
  say "已调优过，跳过"
else
  {
    echo ""
    echo "# ==== $MARK (2026-09-27) 内存调优 ===="
    echo "# 未设置时 JVM 会按物理内存自动推算最大堆，4GB 机器上约 1GB，"
    echo "# 对这套轻量集群过大，统一压到 512m。"
    echo "export HADOOP_HEAPSIZE_MAX=512"
  } >> "$HD/hadoop-env.sh"
  say "追加 HADOOP_HEAPSIZE_MAX=512"
fi

echo "[3/7] YARN RM/NM 堆（yarn-env.sh）"
if grep -q "$MARK" "$HD/yarn-env.sh" 2>/dev/null; then
  say "已调优过，跳过"
else
  {
    echo ""
    echo "# ==== $MARK (2026-09-27) ===="
    echo "export YARN_RESOURCEMANAGER_HEAPSIZE=512"
    echo "export YARN_NODEMANAGER_HEAPSIZE=512"
  } >> "$HD/yarn-env.sh"
  say "追加 RM/NM 堆 512m"
fi

echo "[4/7] YARN 容器调度（yarn-site.xml）"
add_xml_prop "$HD/yarn-site.xml" yarn.scheduler.minimum-allocation-mb 512
add_xml_prop "$HD/yarn-site.xml" yarn.scheduler.maximum-allocation-mb 2048
add_xml_prop "$HD/yarn-site.xml" yarn.nodemanager.resource.cpu-vcores 2

echo "[5/7] MapReduce 容器与 JVM 重用（mapred-site.xml）"
add_xml_prop "$HD/mapred-site.xml" mapreduce.map.memory.mb 512
add_xml_prop "$HD/mapred-site.xml" mapreduce.reduce.memory.mb 512
add_xml_prop "$HD/mapred-site.xml" mapreduce.map.java.opts -Xmx410m
add_xml_prop "$HD/mapred-site.xml" mapreduce.reduce.java.opts -Xmx410m
add_xml_prop "$HD/mapred-site.xml" mapreduce.job.jvm.numtasks 10
add_xml_prop "$HD/mapred-site.xml" yarn.app.mapreduce.am.resource.mb 768
add_xml_prop "$HD/mapred-site.xml" yarn.app.mapreduce.am.command-opts -Xmx614m
add_xml_prop "$HD/mapred-site.xml" mapreduce.map.speculative false
add_xml_prop "$HD/mapred-site.xml" mapreduce.reduce.speculative false

echo "[6/7] ZooKeeper 堆 与 swappiness"
if [ -f "$ZK_ENV" ] && grep -q 'ZK_SERVER_HEAP:-1000' "$ZK_ENV"; then
  sed -i 's/ZK_SERVER_HEAP:-1000/ZK_SERVER_HEAP:-512/' "$ZK_ENV"
  say "zkEnv.sh: ZK_SERVER_HEAP 默认值 1000 -> 512"
else
  say "zkEnv.sh 无需改动（或已改过）"
fi
if grep -q "$MARK" /etc/sysctl.conf 2>/dev/null; then
  say "swappiness 已调优过，跳过"
else
  {
    echo ""
    echo "# ==== $MARK (2026-09-27) 降低无谓换出 ===="
    echo "vm.swappiness=10"
  } >> /etc/sysctl.conf
  say "已写入 vm.swappiness=10"
fi
sysctl -q -p 2>/dev/null || true
say "当前 swappiness = $(cat /proc/sys/vm/swappiness)"

echo "[7/7] MySQL 调优（仅装有 MySQL 的节点）"
if [ -f "$MY_CNF" ]; then
  if grep -q "$MARK" "$MY_CNF"; then
    say "已调优过，跳过"
  elif grep -q '^\[mysqld\]' "$MY_CNF"; then
    sed -i '/^\[mysqld\]/a performance_schema = OFF\ninnodb_buffer_pool_size = 128M' "$MY_CNF"
    say "已在 [mysqld] 下追加 performance_schema=OFF, innodb_buffer_pool_size=128M"
  else
    printf '\n[mysqld]\nperformance_schema = OFF\ninnodb_buffer_pool_size = 128M\n' >> "$MY_CNF"
    say "已追加 [mysqld] 段"
  fi
else
  say "本机无 MySQL，跳过"
fi

echo "==== 完成 @ $(hostname)（配置已改，尚未重启服务）===="
