#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按日期拆分行为日志与订单文件（Day 9）"""
import csv
import os
from collections import defaultdict


def split_by_date(infile, outdir, date_idx, prefix):
    os.makedirs(outdir, exist_ok=True)
    d = defaultdict(list)
    with open(infile, encoding="utf-8") as f:
        for row in csv.reader(f, delimiter="\t"):
            d[row[date_idx][:10]].append(row)
    for dt, rows in sorted(d.items()):
        path = f"{outdir}/{prefix}_{dt}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter="\t").writerows(rows)
        print(f"{dt}: {len(rows)}")


BASE = os.path.expanduser("~/workspace/mock_data")
split_by_date(f"{BASE}/user_log.csv", f"{BASE}/user_log_by_date", 0, "user_log")
split_by_date(f"{BASE}/order_info.csv", f"{BASE}/order_info_by_date", 4, "order")
print("SPLIT_DONE")