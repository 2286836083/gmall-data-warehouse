#!/usr/bin/env bash
# ============================================================
# 修复 worker 节点开机自启不可靠的问题（方案 A：各节点自力启动）
#
# 背景：
#   lrh-2 / lrh-3 上原本【没有】hadoop.service。它们的 DataNode / NodeManager
#   是靠 lrh-1 的 hadoop.service 执行 start-all.sh 时经 SSH 远程拉起的。
#   三台同时冷启动时，lrh-1 执行 start-all.sh 那一刻 worker 还没准备好接受 SSH，
#   于是 worker 的进程【静默启动失败、不报错】。
#   表现：hdfs dfsadmin -report 只有 Live datanodes (1)，yarn node -list 只有 1 个节点。
#
# 方案 A 的做法：
#   - lrh-1  : hadoop.service 改为【只启动本机 5 个守护进程】，不再远程管理 worker
#   - lrh-2/3: 新增 hadoop-worker.service，只启动本机的 DataNode + NodeManager
#   这样每个节点自己拥有并启动自己的进程，彻底消除启动时序依赖与孤儿进程。
#
# 用法（在 lrh-1 上执行）：
#   sudo bash fix_worker_autostart.sh
#
# 幂等：重复执行安全；改前自动备份到 /opt/backup/<日期>/
# 回滚：见脚本末尾输出的提示
# ============================================================
set -u

HD=/opt/hadoop/hadoop-3.3.6
BK="/opt/backup/$(date +%F)"
MARK="# LRH-AUTOSTART-FIX"

say() { echo "  $*"; }

# ---------- 公共的环境变量块 ----------
ENV_BLOCK="Environment=JAVA_HOME=/opt/jdk/jdk1.8.0_202
Environment=HADOOP_HOME=$HD
Environment=HADOOP_CONF_DIR=$HD/etc/hadoop
Environment=HADOOP_MAPRED_HOME=$HD
Environment=PATH=/opt/jdk/jdk1.8.0_202/bin:$HD/bin:$HD/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

mkdir -p "$BK"

# ============================================================
# 1. lrh-1：hadoop.service 改为只启动本机守护进程
# ============================================================
if [ "$(hostname)" = "lrh-1" ]; then
  echo "==== [lrh-1] 改写 hadoop.service 为「只管本机」===="
  if [ -f /etc/systemd/system/hadoop.service ]; then
    cp -f /etc/systemd/system/hadoop.service "$BK/hadoop.service.bak"
    say "已备份原文件 -> $BK/hadoop.service.bak"
  fi

  cat > /etc/systemd/system/hadoop.service <<EOF
[Unit]
Description=Hadoop Master (NameNode + SecondaryNameNode + DataNode + ResourceManager + NodeManager)
$MARK
After=network-online.target zookeeper.service
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=lrh
Group=lrh
$ENV_BLOCK
ExecStart=$HD/bin/hdfs --daemon start namenode
ExecStart=$HD/bin/hdfs --daemon start secondarynamenode
ExecStart=$HD/bin/hdfs --daemon start datanode
ExecStart=$HD/bin/yarn --daemon start resourcemanager
ExecStart=$HD/bin/yarn --daemon start nodemanager
ExecStop=$HD/bin/yarn --daemon stop nodemanager
ExecStop=$HD/bin/hdfs --daemon stop datanode
ExecStop=$HD/bin/yarn --daemon stop resourcemanager
ExecStop=$HD/bin/hdfs --daemon stop secondarynamenode
ExecStop=$HD/bin/hdfs --daemon stop namenode
TimeoutStartSec=240
TimeoutStopSec=240

[Install]
WantedBy=multi-user.target
EOF
  say "已写入新的 hadoop.service（本机 5 个守护进程，不再远程管理 worker）"

  systemctl daemon-reload
  systemctl enable hadoop >/dev/null 2>&1
  say "已 daemon-reload 并 ensure enable"
fi

# ============================================================
# 2. lrh-2 / lrh-3：新增 hadoop-worker.service
# ============================================================
case "$(hostname)" in
  lrh-2|lrh-3)
    echo "==== [$(hostname)] 新增 hadoop-worker.service ===="
    if [ -f /etc/systemd/system/hadoop-worker.service ]; then
      cp -f /etc/systemd/system/hadoop-worker.service "$BK/hadoop-worker.service.bak"
      say "已备份原文件"
    fi

    cat > /etc/systemd/system/hadoop-worker.service <<EOF
[Unit]
Description=Hadoop Worker (DataNode + NodeManager)
$MARK
After=network-online.target zookeeper.service
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=lrh
Group=lrh
$ENV_BLOCK
ExecStart=$HD/bin/hdfs --daemon start datanode
ExecStart=$HD/bin/yarn --daemon start nodemanager
ExecStop=$HD/bin/yarn --daemon stop nodemanager
ExecStop=$HD/bin/hdfs --daemon stop datanode
TimeoutStartSec=240
TimeoutStopSec=240

[Install]
WantedBy=multi-user.target
EOF
    say "已写入 hadoop-worker.service"

    systemctl daemon-reload
    systemctl enable hadoop-worker >/dev/null 2>&1
    say "已 daemon-reload 并设为开机自启"
    ;;
esac

echo
echo "==== 本机 systemd 状态 ===="
for u in hadoop hadoop-worker; do
  if systemctl list-unit-files 2>/dev/null | grep -q "^$u.service"; then
    printf "  %-16s enabled=%s  active=%s\n" "$u" \
      "$(systemctl is-enabled $u 2>/dev/null)" "$(systemctl is-active $u 2>/dev/null)"
  fi
done

echo
echo "执行完毕。回滚方式："
echo "  sudo cp -f $BK/hadoop.service.bak /etc/systemd/system/hadoop.service 2>/dev/null"
echo "  sudo rm -f /etc/systemd/system/hadoop-worker.service"
echo "  sudo systemctl daemon-reload"
