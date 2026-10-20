#!/bin/bash
# ============================================================
# DS 工作流「实时进度监视器」（集群侧）
#
# 每分钟把"工作流实例状态 + 每个节点状态 + 集群当前在跑什么"
# 追加到 /home/lrh/workspace/ds_steps/monitor.log，供本机侧轮询读取。
#
# 符合项目约定：长任务一律远端写状态文件、本机轮询，
# 不要在阻塞等待里空 sleep（见 CONTEXT.md §一.①）。
#
# 【用法】
#   setsid nohup bash ds_monitor.sh <processInstanceId> >/dev/null 2>&1 </dev/null &
#   tail -20 /home/lrh/workspace/ds_steps/monitor.log
#
# 实例终态（5=STOP / 6=FAILURE / 7=SUCCESS / 9=KILL / 13=FORCED_SUCCESS）
# 时自动退出，不留后台进程。
# ============================================================
set -u
PI="$1"
OUT=/home/lrh/workspace/ds_steps/monitor.log
H=/opt/hadoop/hadoop-3.3.6/bin
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH=$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
M() { sudo -n mysql dolphinscheduler --default-character-set=utf8mb4 -N -B -e "$1" 2>/dev/null; }

mkdir -p /home/lrh/workspace/ds_steps
: > "$OUT"
terminal="5 6 7 9 13"

while true; do
  st=$(M "select state from t_ds_process_instance where id=$PI;")
  {
    echo "=============== $(date '+%F %T') ==============="
    echo "INSTANCE id=$PI state=$st"
    M "select concat('  ', rpad(t.name,20,' '), ' state=', t.state,
                     ' start=', ifnull(t.start_time,'-'), ' end=', ifnull(t.end_time,'-'))
       from t_ds_task_instance t where t.process_instance_id=$PI order by t.id;"
    apps=$($H/yarn application -list 2>/dev/null | grep -cE '^[[:space:]]*application_')
    echo "  YARN running apps = ${apps:-0}"
    # 用中括号拆开关键字：-f 匹配整条命令行，直接写 'jdbc:hive2' 时
    # 这条命令自身会被算进去，得到假阳性 1（详见排障汇总第 7 坑）。
    echo "  beeline on lrh-1   = $(pgrep -cf 'jdbc[:]hive2' 2>/dev/null || echo 0)"
  } >> "$OUT"
  for t in $terminal; do
    [ "$st" = "$t" ] && { echo "TERMINAL state=$st at $(date '+%F %T')" >> "$OUT"; exit 0; }
  done
  sleep 60
done
