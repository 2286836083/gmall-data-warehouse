#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按用户采样 + 按天切分 —— 从 ZIP 直接流式处理，不落 5GB 中间文件。

用法：
    python3 sample_and_split.py <输入.zip|.csv|.csv.gz> <输出目录> [--ratio 4]

--ratio N  表示按用户 1/N 采样（默认 4，即保留约 1/4 的用户）

为什么按「用户」采样而不是按「行」随机抽样：
    按行随机抽会把同一个 session 的事件拆散——用户浏览了、加购了，
    但"加购"那条恰好没被抽中，漏斗分析和复购率就全失真了。
    按 user_id 哈希采样能保留每个被选中用户的完整行为序列，
    指标口径才站得住脚。

产出：
    <输出目录>/2019-10-01.csv ... 每天一个文件（不带表头，供 Hive TEXTFILE 装载）
    <输出目录>/manifest.txt        本次采样的统计摘要
"""
import csv
import gzip
import io
import os
import sys
import zipfile
import zlib
from collections import Counter, defaultdict

COLS = ["event_time", "event_type", "product_id", "category_id",
        "category_code", "brand", "price", "user_id", "user_session"]


def open_text(path):
    if path.endswith(".zip"):
        zf = zipfile.ZipFile(path)
        name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if name is None:
            raise SystemExit("ZIP 里没有找到 CSV")
        return io.TextIOWrapper(zf.open(name), encoding="utf-8", errors="replace")
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def keep_user(uid, ratio):
    """对 user_id 做稳定的 CRC32 哈希取模，保证同一用户每次采样结果一致"""
    return zlib.crc32(uid.encode("utf-8")) % ratio == 0


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    src, out_dir = sys.argv[1], sys.argv[2]
    ratio = 4
    if "--ratio" in sys.argv:
        ratio = int(sys.argv[sys.argv.index("--ratio") + 1])

    os.makedirs(out_dir, exist_ok=True)

    def open_part(day):
        return open(os.path.join(out_dir, "%s.csv" % day), "w", encoding="utf-8", newline="")

    files = {}      # day -> 文件句柄（必须持有，否则关不掉、数据可能没落盘）
    writers = {}    # day -> csv.writer

    in_rows = out_rows = 0
    skipped = 0
    per_day = Counter()
    ev_types = Counter()
    header_skipped = False

    with open_text(src) as fh:
        reader = csv.reader(fh)
        for row in reader:
            if not row:
                continue
            if not header_skipped:
                header_skipped = True
                if "event_time" in " ".join(row).lower():
                    continue                     # 跳过表头
            if len(row) != len(COLS):
                skipped += 1
                continue
            in_rows += 1
            if in_rows % 5_000_000 == 0:
                print("    ...已读取 %s 行，命中 %s 行" %
                      (format(in_rows, ","), format(out_rows, ",")), flush=True)

            uid = row[7].strip()
            if not uid or not keep_user(uid, ratio):
                continue

            day = row[0].strip()[:10]
            if day not in writers:
                fh_out = open_part(day)
                files[day] = fh_out
                # 必须显式指定 \n 作行分隔符：csv.writer 默认是 \r\n，
                # 而 Hive TEXTFILE 用 \n 断行，残留的 \r 会粘在最后一列上
                writers[day] = csv.writer(fh_out, lineterminator="\n")
            writers[day].writerow(row)
            out_rows += 1
            per_day[day] += 1
            ev_types[row[1].strip()] += 1

    for fh_out in files.values():
        fh_out.close()

    lines = []
    lines.append("=" * 62)
    lines.append("采样摘要")
    lines.append("=" * 62)
    lines.append("源文件     ：%s" % src)
    lines.append("采样比例   ：按用户 1/%d（CRC32 哈希取模，可复现）" % ratio)
    lines.append("读入行数   ：%s" % format(in_rows, ","))
    lines.append("保留行数   ：%s  (%.2f%%)" % (format(out_rows, ","), out_rows * 100.0 / max(in_rows, 1)))
    lines.append("列数不匹配 ：%s" % format(skipped, ","))
    lines.append("覆盖天数   ：%d 天" % len(per_day))
    lines.append("")
    lines.append("事件类型分布：")
    for k, v in ev_types.most_common():
        lines.append("    %-18s %12s  (%.2f%%)" % (k, format(v, ","), v * 100.0 / max(out_rows, 1)))
    lines.append("")
    lines.append("每日事件量：")
    days = sorted(per_day.items())
    if days:
        mx = max(v for _, v in days)
        for d, v in days:
            lines.append("    %s  %10s  %s" % (d, format(v, ","), "#" * int(v * 40 / mx)))
        vals = [v for _, v in days]
        spread = (max(vals) - min(vals)) * 100.0 / max(max(vals), 1)
        lines.append("")
        lines.append("日波动幅度：%.1f%%  %s" %
                     (spread, "→ 有波动，适合演示" if spread > 15 else "→ 太平，演示效果差"))
    lines.append("=" * 62)

    report = "\n".join(lines)
    print(report)
    with open(os.path.join(out_dir, "manifest.txt"), "w", encoding="utf-8") as f:
        f.write(report + "\n")


if __name__ == "__main__":
    main()
