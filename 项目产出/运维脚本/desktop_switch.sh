#!/usr/bin/env bash
# ============================================================
# 三节点桌面环境开关（GNOME 桌面 on / off）
#
# 用法（在 lrh-1 上执行）：
#   bash desktop_switch.sh off     关闭三台节点的桌面，释放内存
#   bash desktop_switch.sh on      恢复三台节点的桌面
#   bash desktop_switch.sh status  只看当前状态，不做任何改动
#
# 说明：
#   - 关闭桌面 = 把 systemd 默认启动目标切到 multi-user.target 并立即隔离，
#     同时停掉 gdm3。**不卸载任何软件包**，GNOME 全套原封不动留在磁盘上。
#   - 该操作不影响 DataNode / NodeManager / ZooKeeper / Hive / MySQL：
#     这些服务都挂在 multi-user.target 下，且 worker 上的 HDFS 守护进程
#     由 lrh-1 通过 SSH 拉起，不受 systemd 隔离影响。
#   - 实测单台可释放约 540-560MB 内存。
#   - 随时可逆：bash desktop_switch.sh on 即恢复。
# ============================================================
set -u

NODES="lrh-1 lrh-2 lrh-3"
ACTION="${1:-status}"

case "$ACTION" in
  off)    TARGET="multi-user.target" ;;
  on)     TARGET="graphical.target" ;;
  status) TARGET="" ;;
  *)      echo "用法: bash $0 {off|on|status}" ; exit 1 ;;
esac

SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=8"

report() {
  ssh $SSH_OPTS "$1" bash -s <<'REMOTE'
echo "  默认启动目标 : $(systemctl get-default)"
echo "  gdm3         : $(systemctl is-active gdm3)"
NS=$(pgrep -c gnome-shell 2>/dev/null || true)
echo "  gnome-shell  : ${NS:-0} 个进程"
echo "  关键守护进程 : $(pgrep -f 'proc_datanode|proc_nodemanager|QuorumPeerMain' 2>/dev/null | wc -l) 个（DataNode/NodeManager/ZooKeeper）"
free -m | awk 'NR==2{printf "  内存         : 已用 %s MB / 可用 %s MB\n", $3, $7}'
REMOTE
}

switch_to() {
  ssh $SSH_OPTS "$1" bash -s -- "$2" <<'REMOTE'
sudo -n systemctl set-default "$1" >/dev/null 2>&1
sudo -n systemctl isolate "$1" >/dev/null 2>&1
REMOTE
}

echo "操作: $ACTION"
echo

for n in $NODES; do
  echo "===== $n ====="
  if [ -n "$TARGET" ]; then
    switch_to "$n" "$TARGET"
    sleep 3
  fi
  report "$n"
  echo
done

if [ "$ACTION" = "off" ]; then
  echo "桌面已关闭。需要恢复时执行: bash $0 on"
  echo "临时用一次图形界面（不改默认值，重启后自动回到文本模式）:"
  echo "  ssh lrh-1 'sudo systemctl isolate graphical.target'"
elif [ "$ACTION" = "on" ]; then
  echo "桌面已恢复。如需再关闭: bash $0 off"
fi
