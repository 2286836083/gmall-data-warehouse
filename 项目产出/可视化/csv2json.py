#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把 beeline 导出的 CSV 合并成大屏用的 dashboard_data.js

用法：  python3 csv2json.py <导出目录>
读取：  <导出目录>/raw/*.csv
产出：  <导出目录>/dashboard_data.js   （内容为 window.DASHBOARD_DATA = {...};）
"""
import csv
import datetime
import json
import os
import sys

# 每张表导出时选定的列，顺序必须与 export_dashboard_data.sh 中的 SQL 保持一致
SPEC = {
    "dau":        ["dt", "dau", "new_dau", "active_dau", "wakeup_dau"],
    "funnel":     ["dt", "view_uv", "cart_uv", "order_uv", "pay_uv", "overall_rate"],
    "repurchase": ["dt", "total_buyers", "repurchase_buyers", "repurchase_rate"],
    "top_sku":    ["rank_num", "sku_name", "category_id", "pay_cnt", "gmv"],
    "order_day":  ["dt", "total_orders", "paid_orders", "cancelled_orders",
                   "refunded_orders", "total_amount", "paid_amount",
                   "unique_buyers", "unique_payers"],
}

# 数值型列，转成真正的数字（否则 ECharts 会按字符串处理，图表坐标轴会错）
NUMERIC = {
    "dau": "dau new_dau active_dau wakeup_dau",
    "funnel": "view_uv cart_uv order_uv pay_uv overall_rate",
    "repurchase": "total_buyers repurchase_buyers repurchase_rate",
    "top_sku": "rank_num pay_cnt gmv",
    "order_day": ("total_orders paid_orders cancelled_orders refunded_orders "
                  "total_amount paid_amount unique_buyers unique_payers"),
}


def load_csv(path, columns, numeric_cols):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader, None)                      # 跳过表头
        for line in reader:
            if not line or not line[0].strip():  # 跳过空行
                continue
            row = {}
            for idx, key in enumerate(columns):
                raw = line[idx].strip() if idx < len(line) else ""
                if raw == "" or raw.upper() == "NULL":
                    row[key] = None
                elif key in numeric_cols:
                    try:
                        row[key] = float(raw) if "." in raw else int(raw)
                    except ValueError:
                        row[key] = raw
                else:
                    row[key] = raw
            rows.append(row)
    return rows


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    out_dir = sys.argv[1]
    raw_dir = os.path.join(out_dir, "raw")

    data = {}
    for name, columns in SPEC.items():
        numeric_cols = set(NUMERIC.get(name, "").split())
        data[name] = load_csv(os.path.join(raw_dir, name + ".csv"),
                              columns, numeric_cols)
    data["generated_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    js = ("// 由 csv2json.py 自动生成，请勿手工编辑\n"
          "window.DASHBOARD_DATA = "
          + json.dumps(data, ensure_ascii=False, indent=2) + ";\n")

    dst = os.path.join(out_dir, "dashboard_data.js")
    with open(dst, "w", encoding="utf-8") as fh:
        fh.write(js)

    print("  生成 %s" % dst)
    for name in SPEC:
        print("    %-12s %d 行" % (name, len(data[name])))


if __name__ == "__main__":
    main()
