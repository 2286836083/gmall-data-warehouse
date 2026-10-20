#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把 beeline 导出的 CSV 合并成大屏用的 dashboard_data_real.js（真实数据链路）

用法：  python3 csv2json_real.py <导出目录>
读取：  <导出目录>/raw/*.csv
产出：  <导出目录>/dashboard_data_real.js

与旧版 csv2json.py 的区别：数据源换成了真实数据链路的表，字段名也换成了新表的真实字段名
（不再用旧表的 sku_name / order_day 之类名字，避免"名字没变、含义变了"这种坑）。
"""
import csv
import datetime
import json
import os
import sys

# 每张表导出时选定的列，顺序必须与 export_dashboard_real.sh 中的 SQL 保持一致
SPEC = {
    # 活跃用户（列与旧 ads_dau 完全一致，能算出真实的新增/回流）
    "dau":         ["dt", "dau", "new_dau", "active_dau", "wakeup_dau"],
    # 转化漏斗：只有 3 环节（view -> cart -> purchase），多两个环节转化率
    "funnel":      ["dt", "view_uv", "cart_uv", "purchase_uv",
                    "cart_rate", "cart_to_purchase_rate", "overall_rate"],
    # 累计复购率（注意是**累计**口径：截止当日的累计值）
    "repurchase":  ["dt", "total_buyers", "repurchase_buyers", "repurchase_rate"],
    # TopN 商品（取最新一天的 Top15）。真实数据没有"商品名"，用 品牌 + 类目 描述
    "top_product": ["rank_num", "product_id", "brand", "category_l1", "category_l2",
                    "gmv", "purchase_cnt", "buyer_cnt"],
    # 交易日报（每分区一行）：只有成交口径，没有订单状态
    "trade_day":   ["dt", "order_cnt", "buyer_cnt", "gmv", "avg_order_amount", "product_cnt"],
    # 类目主题（新链路独有）：全月按一级类目汇总
    "category":    ["category_l1", "gmv", "purchase_cnt"],
}

# 数值型列，转成真正的数字（否则 ECharts 会按字符串处理，坐标轴会错）
NUMERIC = {
    "dau":         "dau new_dau active_dau wakeup_dau",
    "funnel":      "view_uv cart_uv purchase_uv cart_rate cart_to_purchase_rate overall_rate",
    "repurchase":  "total_buyers repurchase_buyers repurchase_rate",
    "top_product": "rank_num product_id gmv purchase_cnt buyer_cnt",
    "trade_day":   "order_cnt buyer_cnt gmv avg_order_amount product_cnt",
    "category":    "gmv purchase_cnt",
}

# 需要四舍五入的列 → 保留几位小数
# 为什么必须做：双精度累加会产生浮点噪声，Hive 导出来的是
#   80952.47000000006   这种值，直接进 tooltip 会很难看。
ROUND = {
    "gmv": 2, "avg_order_amount": 2,
    "cart_rate": 4, "cart_to_purchase_rate": 4, "overall_rate": 4, "repurchase_rate": 4,
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
                        v = float(raw) if "." in raw else int(raw)
                        if key in ROUND:            # 去掉浮点噪声（80952.47000000006 -> 80952.47）
                            v = round(float(v), ROUND[key])
                            if ROUND[key] == 2:
                                v = round(float(v), 2)
                        row[key] = v
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

    # 让大屏能自己说清"这份数据是谁、什么区间"（旧版没有这个，切换数据源后容易被误读）
    dts = [r["dt"] for r in data.get("dau", []) if r.get("dt")]
    data["source"] = "real"
    data["source_desc"] = "REES46 真实数据（1063 万行）"
    data["range"] = ("%s ~ %s" % (dts[0], dts[-1])) if dts else ""
    data["generated_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    js = ("// 由 csv2json_real.py 自动生成（真实数据链路），请勿手工编辑\n"
          "window.DASHBOARD_DATA = "
          + json.dumps(data, ensure_ascii=False, indent=2) + ";\n")

    dst = os.path.join(out_dir, "dashboard_data_real.js")
    with open(dst, "w", encoding="utf-8") as fh:
        fh.write(js)

    print("  生成 %s" % dst)
    for name in SPEC:
        print("    %-12s %d 行" % (name, len(data[name])))


if __name__ == "__main__":
    main()
