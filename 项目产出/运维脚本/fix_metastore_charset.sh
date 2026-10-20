#!/usr/bin/env bash
# =============================================================
# 根治 Hive 元数据库中文注释乱码
#
# 【根因】（已用十六进制实测坐实，不是猜测）
#   Hive 3.1.3 官方 metastore schema 把「承载注释」的列声明成了 latin1：
#       COLUMNS_V2.COMMENT      varchar(256) CHARACTER SET latin1
#       TABLE_PARAMS.PARAM_VALUE mediumtext  CHARACTER SET latin1
#   而 Hive 连 MySQL 的 JDBC 串是 utf8mb4（hive-site.xml 配了 characterEncoding=UTF-8）
#   → 写中文时 MySQL 必须做 utf8mb4 → latin1 的字符集转换
#   → 中文在 latin1 里没有对应编码，被替换成 '?'（0x3F）
#   证据：'??SKU' 的十六进制是 3F3F534B55 —— 3F 就是问号字符本身，
#         说明是「存储时已被替换」，不是终端显示问题；ASCII 的 SKU 则原样保留。
#
# 【修法】
#   把承载注释的列改成 utf8mb4。只改注释列，不动索引列。
#   ⚠️ 为什么不改 COLUMN_NAME / TBL_NAME 等：它们参与索引，
#      varchar(767) 转 utf8mb4 后单列就是 3068 字节，加上 bigint 主键超过
#      InnoDB 3072 字节索引上限，ALTER 会失败。实际这些列只存 ASCII，无需改。
#
# 【副作用】
#   已损坏的 '?' 无法从数据库恢复（0x3F 丢了原始信息），改完 schema 后
#   需要重新执行一次带中文注释的 DDL/ALTER 才能补齐——见 reapply_comments.sql
#
# 【幂等】可重复执行。
# 【回滚】还原元数据库：sudo mysql hive < /opt/backup/<日期>/hive_metastore_*.sql
#
# 执行：sudo bash fix_metastore_charset.sh
# =============================================================
set -uo pipefail

MY="sudo -n mysql --default-character-set=utf8mb4 hive"

echo "==================== [1/5] 改前记录 ===================="
printf "  含问号的列注释(行): %s\n" "$($MY -N -e "SELECT COUNT(*) FROM COLUMNS_V2 WHERE COMMENT LIKE '%?%';" 2>/dev/null)"
printf "  含问号的表注释(行): %s\n" "$($MY -N -e "SELECT COUNT(*) FROM TABLE_PARAMS WHERE PARAM_KEY='comment' AND PARAM_VALUE LIKE '%?%';" 2>/dev/null)"

echo
echo "==================== [2/5] 停 Hive（避免改 schema 时有并发写）===================="
sudo -n systemctl stop hive-server2  2>/dev/null
echo "  hive-server2  : $(systemctl is-active hive-server2)"
sudo -n systemctl stop hive-metastore 2>/dev/null
echo "  hive-metastore: $(systemctl is-active hive-metastore)"
sleep 2

echo
echo "==================== [3/5] 把注释列迁到 utf8mb4 ===================="
# 格式：表名|列名|类型
COLS="
COLUMNS_V2|COMMENT|varchar(256)
TYPE_FIELDS|COMMENT|varchar(256)
TABLE_PARAMS|PARAM_VALUE|mediumtext
PARTITION_PARAMS|PARAM_VALUE|varchar(4000)
PARTITION_KEYS|PKEY_COMMENT|varchar(4000)
DBS|DESC|varchar(4000)
DATABASE_PARAMS|PARAM_VALUE|varchar(4000)
CTLGS|DESC|varchar(4000)
SD_PARAMS|PARAM_VALUE|mediumtext
SERDE_PARAMS|PARAM_VALUE|mediumtext
INDEX_PARAMS|PARAM_VALUE|varchar(4000)
SERDES|DESCRIPTION|varchar(4000)
"
FAIL=0
while IFS='|' read -r t c ty; do
  [ -z "${t:-}" ] && continue
  if $MY -e "ALTER TABLE \`$t\` MODIFY \`$c\` $ty CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;" 2>/tmp/alter_err.txt; then
    printf "  OK   %-18s %-14s %s\n" "$t" "$c" "$ty"
  else
    printf "  FAIL %-18s %-14s %s\n" "$t" "$c" "$ty"
    head -2 /tmp/alter_err.txt | sed 's/^/         /'
    FAIL=$((FAIL + 1))
  fi
done <<< "$COLS"
echo "  失败项数：$FAIL"

echo
echo "==================== [4/5] 启 Hive ===================="
sudo -n systemctl start hive-metastore
sleep 8
echo "  hive-metastore: $(systemctl is-active hive-metastore)"
sudo -n systemctl start hive-server2
sleep 10
echo "  hive-server2  : $(systemctl is-active hive-server2)"

echo
echo "==================== [5/5] 校验 ===================="
echo "  -- 注释列现在的字符集（应全部 utf8mb4）--"
$MY -N -e "
SELECT TABLE_NAME, COLUMN_NAME, CHARACTER_SET_NAME
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='hive'
  AND ((TABLE_NAME='COLUMNS_V2' AND COLUMN_NAME='COMMENT')
    OR (TABLE_NAME='TABLE_PARAMS' AND COLUMN_NAME='PARAM_VALUE')
    OR (TABLE_NAME='PARTITION_KEYS' AND COLUMN_NAME='PKEY_COMMENT')
    OR (TABLE_NAME='DBS' AND COLUMN_NAME='DESC'))
ORDER BY TABLE_NAME;" 2>/dev/null | sed 's/^/    /'
echo
echo "  -- 索引列确认未被改动（应仍是 latin1，这是刻意保留的）--"
$MY -N -e "
SELECT TABLE_NAME, COLUMN_NAME, CHARACTER_SET_NAME
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='hive'
  AND ((TABLE_NAME='COLUMNS_V2' AND COLUMN_NAME='COLUMN_NAME')
    OR (TABLE_NAME='TBLS' AND COLUMN_NAME='TBL_NAME'))
ORDER BY TABLE_NAME;" 2>/dev/null | sed 's/^/    /'
