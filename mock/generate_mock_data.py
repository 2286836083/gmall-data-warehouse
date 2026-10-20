#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Day 9 电商数据模拟生成器
生成三类数据：用户行为日志、订单信息、商品信息
适配环境：hadoop@LiRuiheng (WSL)，工作目录 /home/hadoop/workspace
"""
import random
import csv
import os
from datetime import datetime, timedelta

# ========== 配置 ==========
OUTPUT_DIR = os.path.expanduser("~/workspace/mock_data")
USER_COUNT = 1000          # 模拟用户数
SKU_COUNT = 500            # 模拟商品数
LOG_COUNT = 100000         # 行为日志条数
ORDER_COUNT = 50000        # 订单条数
DATE_START = "2026-08-20"
DATE_END = "2026-08-24"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========== 用户ID池 ==========
USER_IDS = [f"U{10000 + i}" for i in range(USER_COUNT)]

# ========== 商品池 ==========
BRANDS = ["Apple", "小米", "华为", "OPPO", "vivo", "三星", "荣耀", "一加"]
CATEGORIES = [
    ("C01", "手机"), ("C02", "电脑"), ("C03", "耳机"),
    ("C04", "手表"), ("C05", "平板"), ("C06", "配件")
]
SKU_POOL = []
for i in range(SKU_COUNT):
    cat_id, cat_name = random.choice(CATEGORIES)
    brand = random.choice(BRANDS)
    sku_id = f"P{1000 + i}"
    sku_name = f"{brand} {cat_name} {random.randint(1, 20)}代"
    price = round(random.uniform(99, 12999), 2)
    SKU_POOL.append({
        "sku_id": sku_id, "sku_name": sku_name,
        "category_id": cat_id, "category_name": cat_name,
        "brand_id": f"B{random.randint(1, 50)}", "brand_name": brand,
        "price": price
    })

# ========== 1. 商品信息表 ==========
print("正在生成商品信息...")
with open(f"{OUTPUT_DIR}/sku_info.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter="\t")
    for sku in SKU_POOL:
        writer.writerow([sku["sku_id"], sku["sku_name"], sku["category_id"],
                         sku["category_name"], sku["brand_id"], sku["brand_name"], sku["price"]])
print(f"SKU info: {OUTPUT_DIR}/sku_info.csv, {SKU_COUNT} rows")

# ========== 2. 用户行为日志 ==========
print("正在生成用户行为日志...")
EVENT_TYPES = ["view", "cart", "order", "pay"]
WEIGHTS = [70, 15, 10, 5]

def random_datetime(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

with open(f"{OUTPUT_DIR}/user_log.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter="\t")
    for i in range(LOG_COUNT):
        user_id = random.choice(USER_IDS)
        event_type = random.choices(EVENT_TYPES, weights=WEIGHTS)[0]
        sku = random.choice(SKU_POOL)
        log_time = random_datetime(DATE_START, DATE_END)
        session_id = f"S_{user_id}_{log_time.strftime('%Y%m%d')}"
        page_url = f"/product/detail?sku_id={sku['sku_id']}"
        writer.writerow([log_time.strftime("%Y-%m-%d %H:%M:%S"), user_id,
                         event_type, page_url, sku["sku_id"], session_id])
print(f"User log: {OUTPUT_DIR}/user_log.csv, {LOG_COUNT} rows")

# ========== 3. 订单信息 ==========
print("正在生成订单信息...")
ORDER_STATUSES = ["已支付", "已支付", "已支付", "已支付", "待支付", "已取消", "已退款"]

with open(f"{OUTPUT_DIR}/order_info.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter="\t")
    for i in range(ORDER_COUNT):
        order_id = f"O{datetime.now().strftime('%Y%m%d')}{100000 + i}"
        user_id = random.choice(USER_IDS)
        sku = random.choice(SKU_POOL)
        status = random.choice(ORDER_STATUSES)
        total_amount = round(sku["price"] * random.randint(1, 3), 2)
        create_time = random_datetime(DATE_START, DATE_END)
        if status == "已支付":
            pay_time_str = (create_time + timedelta(minutes=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S")
        else:
            pay_time_str = ""
        writer.writerow([order_id, user_id, status, total_amount,
                         create_time.strftime("%Y-%m-%d %H:%M:%S"), pay_time_str])
print(f"Order info: {OUTPUT_DIR}/order_info.csv, {ORDER_COUNT} rows")

print(f"\nAll done! Files in: {os.path.abspath(OUTPUT_DIR)}")