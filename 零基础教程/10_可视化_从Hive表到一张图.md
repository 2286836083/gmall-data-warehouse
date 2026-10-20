# 10 · 可视化：把 Hive 表变成一张图

> **这章解决什么**：ADS 表算出来了，怎么变成老板看得懂的图？
> 以及一个更本质的问题：**怎么保证"图上的数"和"Hive 里的数"是同一个数？**
>
> 这一章的目标不是成为前端工程师，而是**能把数据正确、快速地呈现出来**。

---

## 一、完整链路：从表到图的三段

```
① 导出     Hive 表  ──beeline csv2──▶  CSV  ──Python──▶  dashboard_data.js
② 承载     dashboard.html 用 <script src="dashboard_data.js"> 加载数据
③ 渲染     ECharts 读 JS 里的变量，画出图
```

**为什么是这三段，而不是"网页直接连 Hive"**：

| 方案 | 问题 |
|---|---|
| 网页直连 Hive | 需要 JDBC 网关、要鉴权、页面要联网、演示环境一断网就白屏 |
| 网页 `fetch` 读本地 JSON | ⚠️ **`file://` 协议下会被浏览器的 CORS 策略拦截，页面直接白屏** |
| ✅ **导出成 `.js` 用 `<script>` 加载** | 双击就能打开、离线可用、无依赖 |

> 📌 **本项目对照**：这是案例项目**真实踩过的坑**——第一版用 `fetch` 读本地 JSON，
> 双击打开后页面全白（因为 `file://` 下 CORS 拦截）。
> 改成导出成 `.js` 文件用 `<script src>` 加载才正常。
> **交付物要"双击即开"，这是演示场景的硬要求。**

---

## 二、第一步：定义数据契约（★ 最重要，也比画图本身重要）

**数据契约 = 前端和后端的约定**：我给你什么字段、什么类型、什么粒度。

**在写第一行前端代码之前，先把契约定下来。** 案例项目的 6 张图：

| 图 | 数据源（ADS 表） | 契约（字段） | 粒度 |
|---|---|---|---|
| 活跃用户趋势 | `ads_active_user_day` | `dt, dau, new_dau, active_dau, wakeup_dau` | 天（31 行） |
| 转化漏斗 | `ads_funnel_day` | `dt, view_uv, cart_uv, purchase_uv` | 天 |
| 复购率趋势 | `ads_repurchase_day` | `dt, repurchase_rate` | 天 |
| 交易大盘 | `ads_trade_day` | `dt, gmv, order_cnt, avg_order_amount` | 天 |
| 类目结构 | `ads_category_day` | `category_l1, gmv, gmv_ratio` | 类目 |
| Top 商品 | `ads_top_product_day` | `product_id, gmv, rank_num` | 商品 |

**定契约的三条原则**：

1. **粒度要匹配图**：趋势图要"一行一天"，饼图要"一行一类"，不能把 1550 行的 TopN 表直接塞给趋势图
2. **字段名用业务语言**：`gmv` 而不是 `sum_price_1`
3. **类型要干净**：数字就是数字，别导出成字符串（否则 ECharts 画图时会乱）

---

## 三、第二步：导出（beeline + Python）

### 3.1 用 beeline 导出 CSV

```bash
beeline -u jdbc:hive2://lrh-1:10000 \
  --outputformat=csv2 --showHeader=true --silent=true \
  -e "SELECT dt, dau, new_dau, active_dau, wakeup_dau
      FROM gmall.ads_active_user_day ORDER BY dt;" > dau.csv
```

> ⚠️ `--outputformat` **没有 json**。合法值是 `table/vertical/csv2/tsv2/dsv/csv/tsv`。
> 要 JSON 就导出 csv2 再转（下一步）。

### 3.2 CSV → JS

```python
import csv, json, datetime

def read_csv(path):
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

data = {
    "generated_at": datetime.datetime.now().isoformat(timespec='seconds'),
    "dau": [
        {"dt": r["dt"], "dau": int(r["dau"]), "new_dau": int(r["new_dau"]),
         "active_dau": int(r["active_dau"]), "wakeup_dau": int(r["wakeup_dau"])}
        for r in read_csv("dau.csv")
    ],
    "trade": [
        {"dt": r["dt"], "gmv": float(r["gmv"]), "order_cnt": int(r["order_cnt"])}
        for r in read_csv("trade.csv")
    ],
}

with open("dashboard_data.js", "w", encoding="utf-8") as f:
    f.write("window.DASHBOARD_DATA = ")
    json.dump(data, f, ensure_ascii=False, indent=2)
    f.write(";\n")
```

**关键的两点**：

1. **写进 `.js` 的是 `window.DASHBOARD_DATA = {...};`**，不是裸 JSON —— 这样 `<script src>` 加载后就是一个全局变量
2. **类型转换在 Python 里做**（`int()` / `float()`），别留给前端

> 💡 案例项目导出时保留了 `generated_at` 时间戳。
> 这个字段在一次重要验证中立了大功：改完 ADS 之后，
> **新旧导出文件逐字节比对、只差 `generated_at`**，证明改动没有影响数据。

---

## 四、第三步：画图（ECharts 最小可用）

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>电商数据大屏</title>
  <script src="echarts.min.js"></script>        <!-- 离线库，同目录 -->
  <script src="dashboard_data.js"></script>      <!-- 导出的数据 -->
</head>
<body>
  <div id="chart_dau" style="width:100%;height:320px;"></div>
  <script>
    const D = window.DASHBOARD_DATA;

    const chart = echarts.init(document.getElementById('chart_dau'));
    chart.setOption({
      title:  { text: '每日活跃用户' },
      tooltip:{ trigger: 'axis' },
      legend: { data: ['DAU', '新增', '回流'] },
      xAxis:  { type: 'category', data: D.dau.map(x => x.dt) },
      yAxis:  { type: 'value' },
      series: [
        { name: 'DAU',  type: 'line', data: D.dau.map(x => x.dau) },
        { name: '新增', type: 'line', data: D.dau.map(x => x.new_dau) },
        { name: '回流', type: 'line', data: D.dau.map(x => x.wakeup_dau) }
      ]
    });
  </script>
</body>
</html>
```

**三个文件必须同目录**（页面用相对路径引用）：

```
项目产出/可视化/
├── dashboard_real.html
├── dashboard_data_real.js     ← 数据
└── echarts.min.js             ← 离线图表库
```

> ⚠️ 缺任何一个，双击打开就是白屏。移动文件时三个一起移动。

---

## 五、★ 口径一致性：怎么证明"图上的数"是对的

这是可视化环节**最容易被跳过、也最要命**的一步。

### 5.1 三层对账

| 层 | 怎么做 |
|---|---|
| **Hive 内部** | GMV 四路收敛（第 08 章 §8.3） |
| **导出环节** | 导出 CSV 的总行数 = Hive 表的行数；CSV 里的 `SUM(gmv)` = Hive 里的 `SUM(gmv)` |
| **页面环节** | 打开页面，看图上显示的 GMV 与 CSV 里的值一致 |

### 5.2 一个可执行的对账脚本

```bash
# Hive 里算的
beeline -u jdbc:hive2://lrh-1:10000 --silent=true --outputformat=csv2 \
  -e "SELECT ROUND(SUM(gmv),2) FROM gmall.ads_trade_day;"

# 导出文件里算的（Python）
python -c "
import json,re
s=open('dashboard_data.js',encoding='utf-8').read()
s=s[s.index('=')+1:].rstrip(';\n')
d=json.loads(s)
print(round(sum(x['gmv'] for x in d['trade']),2))
"
```

**两个数必须一模一样。** 不一样就说明导出环节有过滤/转换错误。

> 📌 **本项目对照**：案例项目的真实对账结果——
> Hive 里 GMV `57,575,996.50`，大屏上 `57,575,996.50`，成交笔数 `186,059`，**一分不差**。

---

## 六、常见坑（每一个都会让图"看起来对但其实错"）

| 坑 | 现象 | 解法 |
|---|---|---|
| **`file://` 下 `fetch` 白屏** | 双击打开页面全白 | 导出成 `.js` 用 `<script src>` 加载 |
| **浮点噪声** | 比率算出来是 `0.30000000000000004` | 导出前 `ROUND(x, 4)` |
| **字符串数字** | 图上 Y 轴从 0 开始乱跳，或排序按字典序 | Python 导出时转 `int()` / `float()` |
| **漏斗排序错** | `view < cart` 导致漏斗画反 | 画图前**在 SQL 或 JS 里显式排序**，别假设数据本身有序 |
| **双 Y 轴误导** | 两条线共用 Y 轴，量级差 1000 倍时小的那条贴底 | 要么用双 Y 轴并**标注清楚**，要么拆成两张图 |
| **日期没对齐** | X 轴 31 个标签挤成一团 | 减少标签密度（`axisLabel.interval`）或倾斜显示 |
| **缺数据没显示成"缺"** | 某天没数据 → 折线直接连过去，看起来像 0 | 前端要把缺失处理成断点或 NULL |
| **图表和表口径不同** | 图上是"下单数"，表里是"支付数" | 契约里写清每个字段的口径定义 |

> ⚠️ **最后一条最危险**。它不报错、不难看，只是**数字 quietly 不对**。
> 这就是为什么第 07 章强调"口径要写成可执行的句子"。

---

## 七、动手实验室

### 实验 10-1：导出一张表并画出来

```bash
# 1) 导出
beeline -u jdbc:hive2://lrh-1:10000 --outputformat=csv2 --silent=true \
  -e "SELECT dt, gmv, order_cnt FROM gmall.ads_trade_day ORDER BY dt;" > trade.csv
head -3 trade.csv

# 2) 转成 js（用 §3.2 的脚本）
python csv2js.py

# 3) 建 html（用 §4 的模板）
# 4) 双击打开 —— 看到折线
```

### 实验 10-2：做一次严格对账

按 §5.2 的方法，比较 Hive 里的 GMV 和页面数据里的 GMV。
**不一样就先别往下做**，回去查导出脚本。

### 实验 10-3：人眼检查（**自动化测不出来**）

打开页面，用眼睛看：

- [ ] X 轴 31 个日期标签有没有挤成一团？
- [ ] 图例和线的颜色对得上吗？
- [ ] 有没有哪个数明显不合理（比如负数、超过总数的比例）？
- [ ] 空值/缺失日期是怎么显示的？

> 📌 **本项目对照**：案例项目专门留了一条待办"让用户打开新版看一眼"。
> 因为刚经历过一次"测试全绿、只有人眼能看出问题"的事故——
> **自动化能验证数字，验证不了"图好不好看、看不看得懂"。**

---

## 八、自测

**每题标了对应的正文小节——答不出来就回去看那节，不要硬想。**

1. 为什么不用 `fetch` 读本地 JSON？—— 对应 **§一**
2. 数据契约应该包含哪三样东西？—— 对应 **§二**
3. 从 Hive 到页面，数据经过了哪三段？—— 对应 **§一**
4. 你怎么证明页面上的数字和 Hive 里的一致？—— 对应 **§五**
5. 举一个"图看起来对、其实口径错了"的例子。—— 对应 **§六**

<details>
<summary><b>参考答案要点</b>（先自己答一遍，再点开核对）</summary>

**1. 为什么不用 fetch**
- `file://` 协议下，浏览器会因为 **CORS 策略拦掉**本地 JSON 请求 → **页面直接白屏**
- 解法：导出成 **`dashboard_data.js`**，用 `<script src>` 加载（本质是"用脚本标签绕开 CORS"）
- 收益：双击即开、离线可用、无依赖 —— 这是演示场景的硬要求

**2. 数据契约三样**
- **字段 / 类型 / 粒度**（最好再加每个字段的**口径定义**）
- 例：`dt(string) / gmv(double) / order_cnt(bigint)`，粒度"一行一天"
- 三条原则：粒度要匹配图（趋势图要一行一天）；字段用业务语言；**类型要干净**（数字别导成字符串）

**3. 三段**
```
① 导出    Hive ──beeline csv2──▶ CSV ──Python──▶ dashboard_data.js
② 承载    html 用 <script src="dashboard_data.js"> 加载
③ 渲染    ECharts 读 window.DASHBOARD_DATA 画图
```
- 关键：导出的 `.js` 里写的是 `window.DASHBOARD_DATA = {...};`，不是裸 JSON

**4. 怎么证明数字一致（三层对账）**
- **Hive 内部**：核心指标交叉对账（案例 GMV 四路收敛）
- **导出环节**：CSV 的行数与 `SUM(gmv)` 对 Hive
- **页面环节**：页面上显示的数对 CSV
- 案例实测：Hive `57,575,996.50` = 大屏 `57,575,996.50`，成交 `186,059` —— **一分不差**

**5. "图对、口径错"的例子**
- 图上是"**下单数**"，表里是"**支付数**"（两个词看起来一样，口径不同，且**不报错**）
- 或：类目占比的分母**不含 `unknown`**，但另一张图的 GMV **含 `unknown`** → 两图加不到 100%
- 为什么最危险：它不报错、也不难看，只是**数字 quietly 不对** ——
  所以第 07 章才要求"口径写成可执行的句子"
</details>

---

## 下一章

[11 · 调优与排错](./11_调优与排错.md) —— 遇到报错不靠猜。这一章决定你是"会跑"还是"能扛事"。
