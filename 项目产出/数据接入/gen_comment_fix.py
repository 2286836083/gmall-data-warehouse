#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成「Hive 元数据库中文注释修复」SQL 脚本。

背景
----
Hive 3.1.3 官方 metastore schema 把承载注释的列声明成了 latin1，
而 Hive 连 MySQL 用的是 utf8mb4 连接，写中文时 MySQL 做
utf8mb4 -> latin1 转换，中文字符无法映射就被替换成 '?'（0x3F）。
原始信息已丢失，只能按 DDL 里的原文重新写一遍。

设计原则
--------
1. **只改注释字段，不碰任何结构**。所以用 UPDATE COLUMNS_V2.COMMENT 这种方式，
   而不是 ALTER TABLE ... CHANGE COLUMN（后者必须重述类型，一旦与当前元数据
   不一致就会静默改掉表结构，风险不可接受）。
2. **交叉校验**。注释文本来自 DDL，但「这张表是不是真有这一列」以元数据库为准；
   任何一边对不上都只报告、不硬写。
3. **幂等**。可重复执行。

输入
----
  --dump   metastore 元数据快照（由 mysqldump 同类查询导出，制表符分隔）
  --ddl    一个或多个建表 DDL 文件（中文注释的权威来源）
  --db     目标库名（默认 gmall）
  -o       输出 SQL 文件

用法
----
  python gen_comment_fix.py --dump metastore_dump.txt \
      --ddl gmall_ddl_day8.sql ods_event_log_ddl.sql \
      --db gmall -o fix_comments.sql
"""
import argparse
import re
import sys

SECTIONS = ["DB", "TABLES", "COLUMNS", "TBLCOMMENT", "PKEYS"]


# ----------------------------------------------------------------------
# 解析 metastore 快照
# ----------------------------------------------------------------------
def load_dump(path):
    data = {k: [] for k in SECTIONS}
    cur = None
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n").rstrip("\r")
            m = re.match(r"#+ SECTION:(\w+) #+", line)
            if m:
                name = m.group(1)
                cur = name if name in data else None
                continue
            if cur and line:
                data[cur].append(line.split("\t"))
    return data


# ----------------------------------------------------------------------
# 解析 DDL
# ----------------------------------------------------------------------
COL_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s+(.+?)\s+COMMENT\s+'((?:[^']|'')*)'\s*$",
                    re.I | re.S)


def split_top_level(body):
    """按顶层逗号切分列定义，忽略括号内的逗号和引号内的逗号。"""
    parts, cur, depth, in_str = [], [], 0, False
    i = 0
    while i < len(body):
        ch = body[i]
        if in_str:
            cur.append(ch)
            if ch == "'":
                if i + 1 < len(body) and body[i + 1] == "'":
                    cur.append("'")
                    i += 1
                else:
                    in_str = False
        else:
            if ch == "'":
                in_str = True
                cur.append(ch)
            elif ch == "(":
                depth += 1
                cur.append(ch)
            elif ch == ")":
                depth -= 1
                cur.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(cur))
                cur = []
            else:
                cur.append(ch)
        i += 1
    if "".join(cur).strip():
        parts.append("".join(cur))
    return parts


def parse_ddl(path):
    """返回 (库注释, {表名: {'cols': {列: 注释}, 'tbl': 表注释, 'pkeys': {列: 注释}}})"""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        text = fh.read()

    mdb = re.search(r"CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\w`]+\s*"
                    r"COMMENT\s+'((?:[^']|'')*)'", text, re.I)
    db_comment = mdb.group(1).replace("''", "'") if mdb else None

    out = {}
    pat = re.compile(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
                     r"([A-Za-z_][\w.]*)\s*\(", re.I)
    pos = 0
    while True:
        m = pat.search(text, pos)
        if not m:
            break
        name = m.group(1).split(".")[-1]
        start = m.end()
        depth, in_str, j = 1, False, start
        while j < len(text) and depth > 0:
            ch = text[j]
            if in_str:
                if ch == "'":
                    if j + 1 < len(text) and text[j + 1] == "'":
                        j += 2
                        continue
                    in_str = False
            else:
                if ch == "'":
                    in_str = True
                elif ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
            j += 1
        body = text[start:j - 1]
        rest = text[j:]
        pos = j

        entry = {"cols": {}, "tbl": None, "pkeys": {}}

        mc = re.match(r"\s*COMMENT\s+'((?:[^']|'')*)'", rest, re.I)
        if mc:
            entry["tbl"] = mc.group(1).replace("''", "'")

        mp = re.search(r"PARTITIONED\s+BY\s*\(([^)]*)\)", rest, re.I)
        if mp:
            for item in mp.group(1).split(","):
                mm = re.match(r"\s*([A-Za-z_]\w*)\s+(\S+)"
                              r"(?:\s+COMMENT\s+'((?:[^']|'')*)')?", item, re.I)
                if mm:
                    entry["pkeys"][mm.group(1).lower()] = (mm.group(3) or "").replace("''", "'")

        for part in split_top_level(body):
            cm = COL_RE.match(part)
            if cm:
                entry["cols"][cm.group(1).lower()] = cm.group(3).replace("''", "'")

        out[name.lower()] = entry
    return db_comment, out


# ----------------------------------------------------------------------
def esc(s):
    return s.replace("\\", "\\\\").replace("'", "''")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", required=True)
    ap.add_argument("--ddl", nargs="+", required=True)
    ap.add_argument("--db", default="gmall")
    ap.add_argument("-o", "--out", required=True)
    args = ap.parse_args()

    dump = load_dump(args.dump)

    # 表 -> TBL_ID / CD_ID
    tbl_id, cd_id = {}, {}
    for row in dump["TABLES"]:
        if len(row) >= 3:
            tbl_id[row[0]] = row[1]
            cd_id[row[0]] = row[2]

    # 表 -> 现有列（用于交叉校验）
    live_cols = {}
    for row in dump["COLUMNS"]:
        if len(row) >= 3:
            live_cols.setdefault(row[0], {})[row[1].lower()] = row[2]

    # 表 -> 是否已有 comment 行
    has_tbl_comment = {row[0] for row in dump["TBLCOMMENT"] if len(row) >= 2}

    # 分区键
    live_pkeys = {}
    for row in dump["PKEYS"]:
        if len(row) >= 3:
            live_pkeys.setdefault(row[0], {})[row[1].lower()] = True

    # CD_ID 是否被多张表共用（共用的话 UPDATE 会误伤别的表）
    cd_owner = {}
    shared = []
    for t, c in cd_id.items():
        if c in cd_owner:
            shared.append((c, cd_owner[c], t))
        else:
            cd_owner[c] = t

    ddl = {}
    db_comment = None
    for f in args.ddl:
        dc, tables = parse_ddl(f)
        ddl.update(tables)
        if dc and not db_comment:
            db_comment = dc

    sql = [
        "-- " + "=" * 74,
        "-- Hive 元数据库中文注释修复（由 gen_comment_fix.py 生成，请勿手工编辑）",
        "--",
        "-- 根因：Hive 3.1.3 官方 metastore schema 把注释列声明为 latin1，",
        "--       而 Hive 用 utf8mb4 连接写库，中文被 MySQL 替换成 '?'（0x3F）。",
        "-- 前提：注释列的字符集已迁移为 utf8mb4（见 fix_metastore_charset.sh）",
        "-- 说明：只 UPDATE 注释字段，不改任何表结构；幂等，可重复执行。",
        "-- " + "=" * 74,
        "",
        "SET NAMES utf8mb4;",
        "",
    ]

    stats = {"col_ok": 0, "col_miss": 0, "tbl_ok": 0, "tbl_miss": 0,
             "pk_ok": 0, "pk_miss": 0}
    problems = []

    if shared:
        problems.append("⚠️ 有 CD_ID 被多张表共用，列注释可能误伤：%s" % shared)
        print("⚠️ CD_ID 共用：%s" % shared, file=sys.stderr)

    # ---------- 库注释 ----------
    db_desc = dump["DB"][0][1] if dump["DB"] and len(dump["DB"][0]) >= 2 else "NULL"
    sql.append("-- ---------- 0. 库注释 ----------")
    sql.append("-- 原值为 %s" % db_desc)
    if db_comment:
        sql.append("UPDATE DBS SET `DESC`='%s' WHERE NAME='%s';"
                   % (esc(db_comment), esc(args.db)))
    else:
        sql.append("-- DDL 里未找到 CREATE DATABASE ... COMMENT，跳过")
    sql.append("")

    for tbl in sorted(tbl_id):
        d = ddl.get(tbl)
        if not d:
            problems.append("DDL 里没有表 %s，跳过" % tbl)
            continue

        sql.append("-- ---------- %s ----------" % tbl)

        # 表注释
        if d["tbl"]:
            if tbl in has_tbl_comment:
                sql.append("UPDATE TABLE_PARAMS SET PARAM_VALUE='%s' "
                           "WHERE TBL_ID=%s AND PARAM_KEY='comment';"
                           % (esc(d["tbl"]), tbl_id[tbl]))
            else:
                sql.append("INSERT INTO TABLE_PARAMS (TBL_ID, PARAM_KEY, PARAM_VALUE) "
                           "VALUES (%s,'comment','%s');" % (tbl_id[tbl], esc(d["tbl"])))
            stats["tbl_ok"] += 1
        else:
            problems.append("%s：DDL 里没写表注释" % tbl)
            stats["tbl_miss"] += 1

        # 列注释
        live = live_cols.get(tbl, {})
        for col, comment in d["cols"].items():
            if col in live_pkeys.get(tbl, {}):
                continue          # 分区列的注释走 PKEY_COMMENT
            if col not in live:
                problems.append("%s.%s：DDL 有但元数据库无此列，跳过" % (tbl, col))
                stats["col_miss"] += 1
                continue
            sql.append("UPDATE COLUMNS_V2 SET COMMENT='%s' "
                       "WHERE CD_ID=%s AND COLUMN_NAME='%s';"
                       % (esc(comment), cd_id[tbl], esc(col)))
            stats["col_ok"] += 1

        # 分区列注释
        for col, comment in d["pkeys"].items():
            if not comment:
                continue
            if col not in live_pkeys.get(tbl, {}):
                problems.append("%s.%s：DDL 说是分区列但元数据库里没有" % (tbl, col))
                stats["pk_miss"] += 1
                continue
            sql.append("UPDATE PARTITION_KEYS SET PKEY_COMMENT='%s' "
                       "WHERE TBL_ID=%s AND PKEY_NAME='%s';"
                       % (esc(comment), tbl_id[tbl], esc(col)))
            stats["pk_ok"] += 1

        # 反向检查：元数据库有、DDL 没给注释的列
        ddl_flat = set(d["cols"]) | set(d["pkeys"])
        for col in sorted(live):
            if col not in ddl_flat:
                problems.append("%s.%s：元数据库有但 DDL 里没找到（注释将保持原样）" % (tbl, col))

        sql.append("")

    sql += [
        "-- ---------- 校验 ----------",
        "SELECT t.TBL_NAME, c.COLUMN_NAME, c.COMMENT",
        "FROM COLUMNS_V2 c JOIN SDS s ON c.CD_ID=s.CD_ID JOIN TBLS t ON t.SD_ID=s.SD_ID",
        "WHERE c.COMMENT LIKE '%?%';",
    ]

    with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(sql) + "\n")

    print("已生成：%s" % args.out)
    print("  表注释  : 写入 %d 条 / 缺失 %d 条" % (stats["tbl_ok"], stats["tbl_miss"]))
    print("  列注释  : 写入 %d 条 / 缺失 %d 条" % (stats["col_ok"], stats["col_miss"]))
    print("  分区注释: 写入 %d 条 / 缺失 %d 条" % (stats["pk_ok"], stats["pk_miss"]))
    print("  涉及表  : %d 张" % len([t for t in tbl_id if t in ddl]))
    if problems:
        print("\n需要注意的 %d 处：" % len(problems))
        for p in problems:
            print("  - %s" % p)


if __name__ == "__main__":
    main()
