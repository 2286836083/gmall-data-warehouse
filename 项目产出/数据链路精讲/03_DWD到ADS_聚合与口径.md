# 第三站 · DWD → DWS → ADS：聚合与口径

> 建立：2026-10-15　|　**全部数字为当天在 lrh-1 实测，可复现**
> 上一站：`02_ODS到DWD_清洗与派生.md`

---

## 一、目标（一句话）

把 1063 万行明细，**塌缩成几十张日报里的几行数字**。这一站决定的不是"数据长什么样"，而是**"口径"**——每个指标到底怎么定义。

| | |
|---|---|
| **输入** | `dwd_event_log_di` + `dwd_purchase_fact_di` + `dim_product` |
| **输出** | DWS 4 张（主题宽表）+ ADS 4 张（业务日报） |
| **谁在干** | Hive，脚本 `evt_etl.sql`（198 行，8 段） |
| **怎么知道干对了** | 各层行数可对账、GMV 与 DWD 一致、口径自洽 |

---

## 二、★ 贯穿案例的最后一跳：5 行 → 1 行 → 1 个计数

回顾 `513781527` 在 2019-10-01 的 5 行明细（DWD）：

```
view      26300090  294.47  session=85d85ce7
purchase  26300090  294.47  session=85d85ce7
view      26300090  294.47  session=85d85ce7
view      26300076  215.71  session=16bf7503
purchase  26300076  215.71  session=16bf7503
```

### 塌缩成 DWS 的 1 行（实测）

```
user_id    pv_cnt  cart_cnt  purchase_cnt  pay_amount           session_cnt  dt
513781527  3       0         2             510.18000000000006   2            2019-10-01
```

**逐字段验算**（请你自己算一遍，这是最好的理解方式）：

| 字段 | 算法 | 手算 |
|---|---|---|
| `pv_cnt` = 3 | 数 `event_type='view'` 的行 | 第 1、3、4 行 → **3** ✅ |
| `cart_cnt` = 0 | 数 `event_type='cart'` 的行 | 没有 → **0** ✅ |
| `purchase_cnt` = 2 | 数 `event_type='purchase'` 的行 | 第 2、5 行 → **2** ✅ |
| `pay_amount` = 510.18 | purchase 行 price 求和 | 294.47 + 215.71 = **510.18** ✅ |
| `session_cnt` = 2 | `COUNT(DISTINCT user_session)` | `85d85ce7` + `16bf7503` → **2** ✅ |

> 5 行明细 → 1 行汇总，**压缩比 5:1**。
> 全表维度：`dwd_event_log_di` 1063 万行 → `dws_user_event_day` 约 41 万行（用户×天）。

### 再塌缩成 ADS 里的 1 个计数

这个用户在 `ads_active_user_day` 里**没有自己的一行**——他只是当天 `dau` 这个数字里的 `+1`。

实测 2019-10-01 那行：

```
dt          dau     new_dau  active_dau  wakeup_dau
2019-10-01  47854   47854    0           0
```

**他贡献的是 `new_dau` 的 +1**（因为 10-01 是他的首次活跃日，见下方窗口函数演算）。

---

## 三、DWS 层：四个主题，四种粒度

| 表 | 粒度（一行代表什么） | 行数规模 |
|---|---|---|
| `dws_user_event_day` | **一个用户 × 一天** | 约 41 万 |
| `dws_product_event_day` | **一个商品 × 一天** | 数百万 |
| `dws_category_event_day` | **一个二级类目 × 一天** | 数千 |
| `dws_trade_day` | **一天**（每分区只有 1 行） | 31 |

> **"粒度"是数仓最重要的概念。** 拿到任何一张表，第一件事是问：这一行代表什么？
> 答不上来就用 `DESC` 看表注释——本项目 30 张表都带 `【粒度】` 声明。

实测 2019-10-01 的 `dws_trade_day`（只有一行）：

```
order_cnt  buyer_cnt  gmv                  avg_order_amount  product_cnt  dt
4970       3612       1605669.790000004    323.07            1874         2019-10-01
```

---

## 四、ADS 层：四张业务日报

| 表 | 回答什么业务问题 | 实测 10-01 的值 |
|---|---|---|
| `ads_active_user_day` | 今天多少活跃用户？有多少是新增/连续/回流？ | dau 47,854 |
| `ads_funnel_day` | 浏览→加购→购买，转化怎么样？ | 见下方陷阱 2 |
| `ads_repurchase_day` | 累计复购率是多少？ | — |
| `ads_top_product_day` | 今天卖得最好的 50 个商品是哪些？ | Top1 是 apple |

实测 Top3（`ads_top_product_day`）：

```
rank_num  product_id  brand  category_l1   gmv               purchase_cnt  buyer_cnt
1         1005115     apple  electronics   110238.52999999998  113           78
2         1005105     apple  electronics   59450.16000000004   42            40
3         1004249     apple  electronics   47170.38999999999   64            48
```

> ADS 层**没有分区**（`dt` 是普通列）。因为每天只有几十到几千行，分区反而会制造大量小文件。
> **分区是为了减少扫描量，行数本来就少时，分区是负担不是优化。**

---

## 五、四个实质性改进（这是本项目的含金量）

代码头部注释（`evt_etl.sql` 第 7~29 行）自己总结了四条。逐条配上真实数据：

### 改进 1｜动态分区：31 天 1 条语句，不是 31 条

```sql
INSERT OVERWRITE TABLE gmall.dws_user_event_day PARTITION (dt)   -- 注意：不写死 dt 的值
SELECT ..., dt FROM ... GROUP BY user_id, dt;
```

旧写法要把 `PARTITION (dt='2019-10-01')` 复制 31 遍。**配合 `SET hive.exec.dynamic.partition.mode=nonstrict`**（忘了会报错，这是本项目踩过的坑）。

### 改进 2｜★ 窗口函数：一次扫描算出新增/连续/回流

**这是全套文档最值得学的一段。** 先手算，再看代码。

`513781527` 在 10 月活跃了 **9 天**（实测）：

```
2019-10-01, 10-02, 10-06, 10-10, 10-11, 10-14, 10-16, 10-17, 10-24
```

代码做了两件事：

```sql
MIN(dt) OVER (PARTITION BY user_id)                       AS first_dt,   -- 首次活跃日
LAG(dt) OVER (PARTITION BY user_id ORDER BY dt)           AS prev_dt     -- 上一次活跃日
```

**手工推演这张表**：

| dt | first_dt | prev_dt | 昨天(DATE_SUB) | 判定 |
|---|---|---|---|---|
| 10-01 | 10-01 | (NULL) | 09-30 | **new_dau**（首次活跃就是今天） |
| 10-02 | 10-01 | 10-01 | 10-01 | **active_dau**（prev = 昨天 → 连续） |
| 10-06 | 10-01 | 10-02 | 10-05 | **wakeup_dau**（断了 3 天 → 回流） |
| 10-10 | 10-01 | 10-06 | 10-09 | **wakeup_dau** |
| 10-11 | 10-01 | 10-10 | 10-10 | **active_dau** |
| 10-14 | 10-01 | 10-11 | 10-13 | **wakeup_dau** |
| 10-16 | 10-01 | 10-14 | 10-15 | **wakeup_dau** |
| 10-17 | 10-01 | 10-16 | 10-16 | **active_dau** |
| 10-24 | 10-01 | 10-17 | 10-23 | **wakeup_dau** |

**统计：new=1、active=3、wakeup=5。** 三个口径一次扫描全出来了。

**对比旧做法**：每一天都要 `LEFT JOIN` "历史所有天" + "昨天"，复杂度 O(n²)。窗口函数是 O(n log n) 排序后一次扫描。

> 更关键的是注释里那句：*"这同时让 new_dau / wakeup_dau 从恒为 0 变成真实值"*——
> **旧链路用模拟数据，每天都是同一批用户，算出来必然是 0。口径再对，数据不对也是白搭。**

### 改进 3｜先聚合再关联维表

```sql
FROM (
  SELECT product_id, dt, COUNT(...) ... 
  FROM gmall.dwd_event_log_di GROUP BY product_id, dt     -- ① 先聚合到小表
) a
LEFT JOIN gmall.dim_product p ON a.product_id = p.product_id;  -- ② 再关维表
```

错误写法是 1063 万行先 JOIN 14 万行维表再 GROUP BY —— 多一次大表 shuffle。
**正确顺序：先把大表压小，再关联。**

### 改进 4｜★ 拒绝不可逆的聚合（最容易错的一条）

> **类目层的「去重买家数」不能从商品层 SUM 加总。**
> 同一个用户买了 A、B 两个商品，在商品层各算 1 次，加总就变成 2。

所以 `dws_category_event_day` **回 DWD 明细重新算**：

```sql
COUNT(DISTINCT CASE WHEN event_type='purchase' THEN user_id END) AS buyer_cnt,
...
FROM gmall.dwd_event_log_di        -- 注意：不是 FROM dws_product_event_day
GROUP BY category_l1, category_l2, dt;
```

**一句话记住**：**`SUM` 可加，`COUNT(DISTINCT)` 不可加。**
跨层级传递指标前，先问一句"这个指标是可加的吗"。

---

## 六、★ 三个由真实数据揭示的陷阱

这三条**不在代码注释里，是我查数据时撞出来的**。它们比代码本身更值得记。

### 陷阱 1｜首日效应：10-01 的 active_dau 和 wakeup_dau 必然是 0

```
2019-10-01: dau=47854, new_dau=47854, active_dau=0, wakeup_dau=0
```

**不是 bug，是定义使然**：数据集第一天，每个用户都是"首次出现"，`active_dau` 要求"昨天活跃过"——昨天没有数据。

> **启示**：做留存/回流分析时，**第一个时间窗口的数据不可用**。
> 这不是数据质量问题，是口径的边界条件。看到 0 先想"是不是边界"，别急着查代码。

### 陷阱 2｜★ 转化率大于 100%：加购不是购买的必经路径

实测 `ads_funnel_day` 2019-10-01：

```
view_uv=47844   cart_uv=2276   purchase_uv=3612
cart_rate=0.0476   cart_to_purchase_rate=1.587   overall_rate=0.0755
```

**`cart_to_purchase_rate = 1.587`，即 158.7% —— 转化率超过 100%？**

验算：`purchase_uv / cart_uv = 3612 / 2276 = 1.587` ✅ 没算错。

**根因**：**加购用户数（2,276）比购买用户数（3,612）还少。**
也就是说，**大量用户完全不进购物车，直接购买**。

> **结论（重要，做漏斗时必看）**：
> 用户级漏斗只有「浏览 → 购买」两个环节，**不是三个**。
> 加购是与购买**并行**的一条路径，不是必经的中间态。
> 所以 `cart_rate` 和 `cart_to_purchase_rate` 是**两个独立指标，不能相乘**。

这条是本项目实测 31 天才发现的（购买 > 加购的日子有 15 天）。**看到"漏斗"两个字就默认三环节，是典型的想当然。**

### 陷阱 3｜浮点尾巴：510.18 变成了 510.18000000000006

```
dws_user_event_day.pay_amount = 510.18000000000006   （本应是 510.18）
dws_trade_day.gmv            = 1605669.790000004
ads_top_product_day.gmv      = 110238.52999999998
```

`price` 字段类型是 `double`，累加 113 笔之后误差就显出来了。

> **为什么金融场景用 DECIMAL 不用 DOUBLE**：
> DOUBLE 是二进制浮点，无法精确表示 0.1（就像十进制无法精确表示 1/3）。
> 单笔看不出来，**累加几千笔后误差会累积到可见**。
>
> 本项目源数据是 CSV 文本、`price` 建表时就用了 `double`，所以留下了这个尾巴。
> 这是**已知的取舍**，不是 bug——但如果这是真实交易系统，金额必须用 `DECIMAL(18,2)`。

### 陷阱 4｜unknown 是最大的"类目"

实测 `dws_category_event_day` 2019-10-01 中 `category_l1='unknown'` 那一行：

```
category_l1  category_l2  pv_cnt  purchase_cnt  gmv                buyer_cnt
unknown      unknown      98847   1042          174551.12999999986  836
```

**98,847 次浏览、17.4 万 GMV 落在 `unknown`**。源数据 `category_code` 有 31.9% 为空（见 ODS 表注释）。

而且——**我们贯穿案例的那个用户，两笔交易全在这里面**（他的商品 `category_code` 都是空的）。

> **做类目分析前必须显式处理**：要么排除，要么单列成"未分类"。
> 默默让它参与排名，会让 `unknown` 成为"最热类目"，结论完全失真。

---

## 七、判据：怎么证明这一站算对了

```bash
# 1. DWD 与 DWS 的 GMV 对账（应一致，误差只在浮点）
$B --silent=true --outputformat=csv2 -e \
  "SELECT ROUND(SUM(price),2) FROM gmall.dwd_event_log_di WHERE dt='2019-10-01' AND event_type='purchase';"
$B --silent=true --outputformat=csv2 -e \
  "SELECT ROUND(gmv,2) FROM gmall.dws_trade_day WHERE dt='2019-10-01';"

# 2. 买家数对账（两条路径都应为 3612）
$B --silent=true --outputformat=csv2 -e \
  "SELECT COUNT(DISTINCT user_id) FROM gmall.dwd_purchase_fact_di WHERE dt='2019-10-01';"
$B --silent=true --outputformat=csv2 -e \
  "SELECT buyer_cnt FROM gmall.dws_trade_day WHERE dt='2019-10-01';"

# 3. ★ 验证陷阱 2：加购用户数 < 购买用户数（反直觉但真实）
$B --silent=true --outputformat=csv2 -e \
  "SELECT dt, cart_uv, purchase_uv, cart_to_purchase_rate FROM gmall.ads_funnel_day
   WHERE purchase_uv > cart_uv ORDER BY dt LIMIT 10;"

# 4. 复现贯穿案例的 DWS 行
$B --silent=true --outputformat=csv2 -e \
  "SELECT * FROM gmall.dws_user_event_day WHERE dt='2019-10-01' AND user_id=513781527;"
```

第 3 条尤其建议跑一次 —— 你会看到**有 15 天的"加购→购买转化率"大于 100%**，亲眼确认陷阱 2 不是个别日期的偶然。

---

## 八、自测题

1. 什么叫「粒度」？说出 `dws_user_event_day` 和 `dws_trade_day` 各自的粒度。
2. `COUNT(DISTINCT)` 为什么不能跨层加总？举一个你项目里的例子。
3. 为什么 2019-10-01 的 `active_dau` 一定是 0？
4. `cart_to_purchase_rate = 1.587` 说明什么？这个数字能不能和 `cart_rate` 相乘得到整体转化率？
5. 为什么 `price` 用 double 会算出 `510.18000000000006`？生产环境该怎么改？
6. 做类目分析时，`category_l1 = 'unknown'` 该怎么处理？不处理会怎样？

---

## 九、整套链路回顾

```
原始 CSV（31 文件 / 1.3GB / 1063 万行）
   │  load_ods.sh：原样落地，不加工
   ▼
ODS  ods_event_log_di          TEXTFILE，保留 " UTC" 后缀、空串、price=0
   │  etl_ods_to_dwd.sql：清洗 5 规则
   ▼
DWD  dwd_event_log_di          ORC，去重后 md5 主键、时间解析、类目归一
     dwd_purchase_fact_di      ← 从 purchase 事件派生
     dim_product               ← 从事件流派生（刻意不含价格）
   │  evt_etl.sql：8 段聚合
   ▼
DWS  dws_user_event_day        （用户×天）约 41 万行
     dws_product_event_day     （商品×天）
     dws_category_event_day    （类目×天）★ 买家数回明细重算
     dws_trade_day             （天）
   │
   ▼
ADS  ads_active_user_day / ads_funnel_day / ads_repurchase_day / ads_top_product_day
   │  export_dashboard_real.sh
   ▼
ECharts 大屏（项目产出/可视化/dashboard_real.html）
```

**一个用户的数据在这条路上**：5 行原始文本 → 5 行干净明细 → 1 行汇总 → 日报里的 1 个计数。
