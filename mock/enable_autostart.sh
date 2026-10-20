#!/bin/bash
# 一次性配置：让 Hadoop/Hive/DolphinScheduler 随 WSL(systemd) 开机自启
# 用法: bash enable_autostart.sh
set -e

echo hadoop123 | sudo -S bash -c '
set -e

# 1. 给四个 static 单元追加 [Install]，使其可以 enable
for u in hadoop-hdfs hadoop-yarn hive-metastore hive-hiveserver2; do
  f=/etc/systemd/system/$u.service
  if ! grep -q "^\[Install\]" "$f"; then
    printf "\n[Install]\nWantedBy=multi-user.target\n" >> "$f"
    echo ">> $u 追加 [Install]"
  fi
done

# 2. DolphinScheduler standalone 注册为 systemd 服务
cat > /etc/systemd/system/dolphinscheduler-standalone.service <<EOF
[Unit]
Description=DolphinScheduler Standalone Server
After=network.target mysql.service
Wants=mysql.service

[Service]
Type=oneshot
RemainAfterExit=yes
User=hadoop
ExecStart=/home/hadoop/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh start standalone-server
ExecStop=/home/hadoop/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh stop standalone-server
TimeoutStartSec=180

[Install]
WantedBy=multi-user.target
EOF
echo ">> 创建 dolphinscheduler-standalone.service"

# 3. 重载并使能自启
systemctl daemon-reload
systemctl enable hadoop-hdfs hadoop-yarn hive-metastore hive-hiveserver2 dolphinscheduler-standalone
'

echo "== 完成，当前 enable 状态："
systemctl is-enabled hadoop-hdfs hadoop-yarn hive-metastore hive-hiveserver2 dolphinscheduler-standalone
