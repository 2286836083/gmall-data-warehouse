#!/usr/bin/env bash
# ============================================================
# 从 Hive 导出大屏数据 -> dashboard_data.js
#
#   在 lrh-1 上执行：  bash export_dashboard_data.sh [输出目录]
#   默认输出目录：/home/lrh/workspace/dashboard
#
# 为什么导出成 .js 而不是 .json：
#   大屏 HTML 用 file:// 双击打开时，浏览器会以 CORS 策略拦截 fetch 读取本地
#   JSON 文件，页面会白屏。而 <script src="..."> 不受该限制。
#   所以这里直接生成 `window.DASHBOARD_DATA = {...}`，页面用 script 标签加载。
#
# 关于 --outputformat：beeline 只支持 table/vertical/csv2/tsv2/dsv/csv/tsv，
#   **没有 json**，所以用 csv2 导出再由 Python 转 JSON。
# ============================================================
set -eu

OUT="${1:-/home/lrh/workspace/dashboard}"
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

echo "== 从 Hive 导出大屏数据 =="
run dau        "SELECT dt, dau, new_dau, active_dau, wakeup_dau FROM gmall.ads_dau ORDER BY dt"
run funnel     "SELECT dt, view_uv, cart_uv, order_uv, pay_uv, overall_rate FROM gmall.ads_conversion_funnel ORDER BY dt"
run repurchase "SELECT dt, total_buyers, repurchase_buyers, repurchase_rate FROM gmall.ads_repurchase_rate ORDER BY dt"
run top_sku    "SELECT rank_num, sku_name, category_id, pay_cnt, gmv FROM gmall.ads_top_sku ORDER BY dt DESC, rank_num ASC LIMIT 15"
run order_day  "SELECT dt, total_orders, paid_orders, cancelled_orders, refunded_orders, total_amount, paid_amount, unique_buyers, unique_payers FROM gmall.dws_order_day ORDER BY dt"

echo "== 转换为 dashboard_data.js =="
python3 "$HERE/csv2json.py" "$OUT"

echo
echo "完成。产物："
echo "  $OUT/dashboard_data.js"
echo "  $OUT/raw/*.csv"
