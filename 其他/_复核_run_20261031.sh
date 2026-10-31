#!/bin/bash
# 双引擎对账复核执行器（只读）
set -u
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export HADOOP_HOME=/opt/hadoop/hadoop-3.3.6
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
SPARK=/opt/spark-3.5.9-bin-hadoop3
LOG=/tmp/verify_1031.log
$SPARK/bin/spark-sql \
  --master yarn --deploy-mode client \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=6 \
  --conf spark.executor.memory=1408m \
  --conf spark.executor.cores=2 \
  --conf spark.executor.instances=3 \
  --conf spark.executor.memoryOverhead=384 \
  --conf spark.driver.memory=1g \
  -f /tmp/verify_1031.sql > $LOG 2>&1
echo "EXIT=$?"
