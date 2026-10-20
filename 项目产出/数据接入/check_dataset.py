#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据集体检脚本 —— 拿到原始文件后先跑这个，判断数据是否可用、是否被二次加工过。

用法：
    python3 check_dataset.py <文件路径> [--rows 2000000]

支持 .csv 与 .csv.gz，流式分块读取，5GB+ 文件也不会撑爆内存。
只用标准库，不依赖 pandas。

输出内容：
    1. 是否带表头、列数、列名
    2. 总行数（按采样块估算或全量统计）
    3. 日期跨度（最早 / 最晚事件时间）
    4. event_type 分布
    5. 各列空值率（brand / category_code 在原始数据里缺失率很高，属正常）
    6. price 的 min / max / 均值
    7. 唯一 user_id / product_id / user_session 数量
    8. 每日事件量走势（判断数据是否"平"，这是能否用于演示的关键）
"""
import csv
import gzip
import io
import os
import sys
import zipfile
from collections import Counter, defaultdict

# REES46 原始数据集的预期列（无表头时的顺序）
EXPECTED = ["event_time", "event_type", "product_id", "category_id",
            "category_code", "brand", "price", "user_id", "user_session"]


def open_text(path):
    """支持 .csv / .csv.gz / .zip（zip 内取第一个 csv），不必先解压大文件"""
    if path.endswith(".zip"):
        zf = zipfile.ZipFile(path)
        name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if name is None:
            raise SystemExit("ZIP 里没有找到 CSV 文件")
        print("ZIP 内文件：%s" % name)
        return io.TextIOWrapper(zf.open(name), encoding="utf-8", errors="replace")
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def looks_like_header(row):
    joined = " ".join(row).lower()
    return ("event_time" in joined) or ("event_type" in joined)


def parse_day(value):
    """event_time 形如 '2019-10-01 00:00:00 UTC'，取前 10 位即日期"""
    value = (value or "").strip().strip('"')
    return value[:10] if len(value) >= 10 else ""


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    path = sys.argv[1]
    max_rows = None
    if "--rows" in sys.argv:
        max_rows = int(sys.argv[sys.argv.index("--rows") + 1])

    if not os.path.exists(path):
        print("文件不存在：%s" % path)
        sys.exit(1)

    size_gb = os.path.getsize(path) / 1024 ** 3
    print("=" * 62)
    print("文件：%s" % path)
    print("大小：%.2f GB" % size_gb)
    print("=" * 62)

    total = 0
    bad_rows = 0
    header = None
    cols = None
    event_types = Counter()
    per_day = defaultdict(int)
    nulls = Counter()
    price_min, price_max, price_sum, price_cnt = None, None, 0.0, 0
    users, products, sessions = set(), set(), set()
    first_rows = []
    min_ts, max_ts = None, None

    with open_text(path) as fh:
        reader = csv.reader(fh)
        for row in reader:
            if not row:
                continue
            if cols is None:
                if looks_like_header(row):
                    header = row
                    cols = [c.strip().lower() for c in row]
                    print("表头：有 ->", header)
                    continue
                cols = EXPECTED
                print("表头：无（按 REES46 标准列顺序解析）")
                print("列数：%d" % len(row))
            if len(row) != len(cols):
                bad_rows += 1
                if len(first_rows) < 3:
                    first_rows.append(row)
                continue
            rec = dict(zip(cols, row))
            if len(first_rows) < 3:
                first_rows.append(row)

            total += 1
            et = (rec.get("event_type") or "").strip()
            event_types[et] += 1

            ts = (rec.get("event_time") or "").strip()
            d = parse_day(ts)
            if d:
                per_day[d] += 1
                if min_ts is None or ts < min_ts:
                    min_ts = ts
                if max_ts is None or ts > max_ts:
                    max_ts = ts

            for c in cols:
                v = (rec.get(c) or "").strip()
                if v == "" or v.upper() == "NULL":
                    nulls[c] += 1

            try:
                p = float(rec.get("price") or "")
                price_sum += p
                price_cnt += 1
                price_min = p if price_min is None else min(price_min, p)
                price_max = p if price_max is None else max(price_max, p)
            except ValueError:
                pass

            if len(users) < 12_000_000:
                users.add(rec.get("user_id"))
            if len(products) < 12_000_000:
                products.add(rec.get("product_id"))
            if len(sessions) < 12_000_000:
                sessions.add(rec.get("user_session"))

            if max_rows and total >= max_rows:
                print("（已达 --rows 上限 %d，提前停止）" % max_rows)
                break

    print("-" * 62)
    print("总行数        ：%s" % format(total, ","))
    if bad_rows:
        print("列数不匹配行数：%s（示例：%s）" % (format(bad_rows, ","), first_rows[:1]))
    print("前 3 行示例   ：")
    for r in first_rows[:3]:
        print("   ", r)

    print("-" * 62)
    print("日期跨度      ：%s ~ %s" % (min_ts, max_ts))
    print("覆盖天数      ：%d 天" % len(per_day))

    print("-" * 62)
    print("事件类型分布  ：")
    for k, v in event_types.most_common():
        print("    %-18s %12s  (%.2f%%)" % (k, format(v, ","), v * 100.0 / max(total, 1)))

    print("-" * 62)
    print("空值率        ：")
    for c in cols:
        n = nulls.get(c, 0)
        print("    %-16s %12s  (%.2f%%)" % (c, format(n, ","), n * 100.0 / max(total, 1)))

    print("-" * 62)
    if price_cnt:
        print("price         ：min=%.2f  max=%.2f  avg=%.2f" %
              (price_min, price_max, price_sum / price_cnt))
    CAP = 12_000_000
    print("唯一 user_id  ：%s%s" % (format(len(users), ","), "  (已截断统计)" if len(users) >= CAP else ""))
    print("唯一 product_id：%s%s" % (format(len(products), ","), "  (已截断统计)" if len(products) >= CAP else ""))
    print("唯一 session  ：%s%s" % (format(len(sessions), ","), "  (已截断统计)" if len(sessions) >= CAP else ""))

    print("-" * 62)
    print("每日事件量（判断数据是否'平'，这是能否用于演示的关键）：")
    days = sorted(per_day.items())
    mx = max((v for _, v in days), default=1)
    for d, v in days:
        bar = "#" * int(v * 40 / mx)
        print("    %s  %10s  %s" % (d, format(v, ","), bar))
    if len(days) > 1:
        vals = [v for _, v in days]
        spread = (max(vals) - min(vals)) * 100.0 / max(max(vals), 1)
        print("    日波动幅度：%.1f%%  %s" %
              (spread, "→ 有波动，适合演示 ✅" if spread > 15 else "→ 太平，演示效果差 ⚠️"))
    print("=" * 62)


if __name__ == "__main__":
    main()
