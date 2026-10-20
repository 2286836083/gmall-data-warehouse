#!/bin/bash
# 等待 wget 下载完成后校验
for i in $(seq 1 40); do
  # 检查 wget 是否还在
  if ! ps aux | grep -q '[w]get -q https://archive'; then
    break
  fi
  sleep 5
done
echo "wget 结束，开始校验"
gzip -t /home/hadoop/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin.tar.gz 2>/dev/null && echo "=== 校验通过，下载完成 ===" || echo "=== 仍损坏/未完成 ==="
ls -lh /home/hadoop/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin.tar.gz