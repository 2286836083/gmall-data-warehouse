#!/usr/bin/env bash
# ============================================================
# 从 Hive 导出【真实数据链路】的大屏数据 -> dashboard_data_real.js
#
#   在 lrh-1 上执行：  bash export_dashboard_real.sh [输出目录]
#   默认输出目录：/home/lrh/workspace/dashboard_real
#
# 【和旧版 export_dashboard_data.sh 的区别】
#   旧版读的是**模拟数据链路**的 5 张表（ads_dau / ads_conversion_funnel /
#   ads_repurchase_rate / ads_top_sku / dws_order_day）。
#   本版读的是**真实数据链路（REES46 1063 万行）**的表：
#
#     旧表                        新表                        结构差异
#     ads_dau                     ads_active_user_day         ✅ 列完全一样
#     ads_conversion_funnel       ads_funnel_day              ⚠️ 4 环节 -> 3 环节
#                                                              （真实数据没有"下单/支付"之分，
#                                                                只有 view/cart/purchase）
#     ads_repurchase_rate         ads_repurchase_day          ✅ 列完全一样
#     ads_top_sku                 ads_top_product_day         ⚠️ 没有"商品名"（真实数据只有
#                                                                 product_id/品牌/类目路径）
#     dws_order_day               ads_trade_day               ⚠️ 没有订单状态（已支付/已取消/
#                                                                 已退款），只有成交口径
#     （无）                      ads_category_day            ✅ 新链路独有：类目主题
#
# 【2026-10-05 优化：全部改为只读 ADS 层】
#   改前 trade_day 读 dws_trade_day、category 读 dws_category_event_day —— 这两支查询直接读
#   DWS 层、**跳过了 ADS**，违反"应用层只消费 ADS"的分层原则（对标 Olist 项目时发现）。
#   现已补建 ads_trade_day / ads_category_day 两张 ADS 表，本脚本 6 支查询全部只读 ADS 层。
#   验证方式：改后导出的 dashboard_data_real.js 与改前**逐字节一致**（除 generated_at）。
#
# 为什么导出成 .js 而不是 .json：
#   HTML 用 file:// 双击打开时，浏览器会以 CORS 策略拦截 fetch 读本地 JSON，
#   页面会白屏；而 <script src="..."> 不受该限制。所以直接生成
#   `window.DASHBOARD_DATA = {...}`。
#
# 关于 --outputformat：beeline 只支持 table/vertical/csv2/tsv2/dsv/csv/tsv，
#   **没有 json**，所以用 csv2 导出再由 Python 转 JSON。
# ============================================================
set -eu

OUT="${1:-/home/lrh/workspace/dashboard_real}"
HERE="$(cd "$(dirname "$0")" && pwd)"
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
URL="jdbc:hive2://localhost:10000"

export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH="$JAVA_HOME/bin:$PATH"

mkdir -p "$OUT/raw"

run() {
  name="$1"; sql="$2"
  printf '  导出 %-12s' "$name"
  "$BEELINE" -u "$URL" --silent=true --outputformat=csv2 -e "$sql" 2>/dev/null > "$OUT/raw/$name.csv"
  printf '  %s 行（含表头）\n' "$(wc -l < "$OUT/raw/$name.csv")"
}

echo "== 从 Hive 导出【真实数据链路】大屏数据 =="
run dau         "SELECT dt, dau, new_dau, active_dau, wakeup_dau FROM gmall.ads_active_user_day ORDER BY dt"
run funnel      "SELECT dt, view_uv, cart_uv, purchase_uv, cart_rate, cart_to_purchase_rate, overall_rate FROM gmall.ads_funnel_day ORDER BY dt"
run repurchase  "SELECT dt, total_buyers, repurchase_buyers, repurchase_rate FROM gmall.ads_repurchase_day ORDER BY dt"
run top_product "SELECT rank_num, product_id, brand, category_l1, category_l2, gmv, purchase_cnt, buyer_cnt FROM gmall.ads_top_product_day ORDER BY dt DESC, rank_num ASC LIMIT 15"
run trade_day   "SELECT dt, order_cnt, buyer_cnt, gmv, avg_order_amount, product_cnt FROM gmall.ads_trade_day ORDER BY dt"
run category    "SELECT category_l1, ROUND(SUM(gmv), 2) AS gmv, SUM(purchase_cnt) AS purchase_cnt FROM gmall.ads_category_day GROUP BY category_l1 ORDER BY gmv DESC LIMIT 12"

echo "== 转换为 dashboard_data_real.js =="
python3 "$HERE/csv2json_real.py" "$OUT"

echo
echo "完成。产物："
echo "  $OUT/dashboard_data_real.js"
echo "  $OUT/raw/*.csv"
echo
echo "把 dashboard_data_real.js 拷回本机 D:\\数据开发\\项目产出\\可视化\\ 即可（和 dashboard_real.html 同目录）"
