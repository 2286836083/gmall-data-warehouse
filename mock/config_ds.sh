#!/bin/bash
# 配置 DolphinScheduler application.yaml 使用 MySQL
CFG=/home/hadoop/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/standalone-server/conf/application.yaml
cp $CFG $CFG.bak.$(date +%s)
echo "备份完成"

# 用 sed 替换 datasource 段的 driver / url / username / password
# 匹配 datasource: 下的 driver-class-name, url, username, password（H2 那组，即第一个出现的 spring.datasource）
SPRING_DS=$(grep -n '^[[:space:]]*datasource:' $CFG | head -1 | cut -d: -f1)
echo "datasource 起始行: $SPRING_DS"

# 直接针对 H2 driver 行做替换
sed -i 's|driver-class-name: org.h2.Driver|driver-class-name: com.mysql.cj.jdbc.Driver|' $CFG
sed -i 's|jdbc:h2:mem:dolphinscheduler;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=true|jdbc:mysql://127.0.0.1:3306/dolphinscheduler?useUnicode=true\&characterEncoding=UTF-8\&serverTimezone=Asia/Shanghai|' $CFG
sed -i 's|^    username: sa$|    username: ds_user|' $CFG
sed -i 's|^    password: ""$|    password: ds_password123|' $CFG

echo "=== 修改后的 datasource 段 ==="
grep -n -A4 '^[[:space:]]*datasource:' $CFG | head -12