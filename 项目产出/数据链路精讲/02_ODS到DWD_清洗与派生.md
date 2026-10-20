# 第二站 · ODS → DWD：清洗与派生

> 建立：2026-10-15　|　**全部数据为当天在 lrh-1 实测**
> 上一站：`01_原始数据到ODS_原样落地.md`

---

## 一、目标（一句话）

把"能查但不好用"的原始表，变成"干净、可信、可直接聚合"的明细层，并**从事件流里派生出事实表和维表**。

| | |
|---|---|
| **输入** | `ods_event_log_di`（1 张表，1063 万行） |
| **输出** | `dwd_event_log_di` + `dwd_purchase_fact_di` + `dim_product` |
| **谁在干** | Hive（跑真实 MapReduce，1063 万行，耗时 5~15 分钟） |
| **怎么知道干对了** | 行数比 ODS 少（去重）、`event_id` 唯一、类目无 NULL |

---

## 二、★ 核心：看一条真实数据怎么变形

这是整套文档最重要的部分。**真实用户 `513781527` 在 2019-10-01 的 5 个事件**：

### ODS 里的样子（清洗前 —— 实测）

```
event_time                  event_type  product_id  category_code  brand    price   user_session
2019-10-01 05:39:49 UTC     view        26300090    (空)           lucente  294.47  85d85ce7-...
2019-10-01 05:40:04 UTC     purchase    26300090    (空)           lucente  294.47  85d85ce7-...
2019-10-01 05:40:43 UTC     view        26300090    (空)           lucente  294.47  85d85ce7-...
2019-10-01 11:22:06 UTC     view        26300076    (空)           lucente  215.71  16bf7503-...
2019-10-01 11:22:21 UTC     purchase    26300076    (空)           lucente  215.71  16bf7503-...
```

### DWD 里的样子（清洗后 —— 实测）

```
event_id                          event_type  event_time            product_id  l1      l2      l3      brand    price
a7a84190f0b7b00a10405b9d901d5d07  view        2019-10-01 11:22:06.0 26300076    unknown unknown unknown lucente  215.71
8b6984908f9a94ed8e0585e0f5bcdc43  purchase    2019-10-01 11:22:21.0 26300076    unknown unknown unknown lucente  215.71
7a6401c216cd38e9fd78b2a3ba8877ad  view        2019-10-01 05:39:49.0 26300090    unknown unknown unknown lucente  294.47
745242f62308424973a94b94bed3ab66  purchase    2019-10-01 05:40:04.0 26300090    unknown unknown unknown lucente  294.47
3f79e450f99c6be22ef0a68d258b360e  view        2019-10-01 05:40:43.0 26300090    unknown unknown unknown lucente  294.47
```

### 逐项对照——变了什么

| 变化 | 前 | 后 | 为什么 |
|---|---|---|---|
| **时间** | `2019-10-01 05:39:49 UTC`（文本） | `2019-10-01 05:39:49.0`（timestamp） | 剥掉 ` UTC` 再 CAST，才能做时间运算 |
| **主键** | 无 | `7a6401c2...`（md5 32 位） | 去重后需要唯一标识 |
| **类目** | `category_code` 空 | `l1/l2/l3` 全部 `unknown` | 空值归一，见规则 4 |
| **行数** | 5 | 5 | 这 5 行无重复；全表共去掉 7,091 行 |

> **⚠️ 注意一个真实的坑**：这个用户买的 2 个商品 `category_code` 都是空的，所以在类目层它们**全部落入 `unknown`**。
> 后面你会看到 `unknown` 是 10-01 当天 pv 数最高的"类目"（98,847 次浏览）。
> 这不是 bug，是源数据 31.9% 缺失的直接后果 —— **做类目分析必须显式处理它**。

---

## 三、五条清洗规则（逐条配真实代码）

代码位置：`项目产出/数据接入/etl_ods_to_dwd.sql`

### 规则 1｜时间解析

```sql
CAST(REGEXP_REPLACE(o.event_time, ' UTC$', '') AS TIMESTAMP) AS event_time,
```

`REGEXP_REPLACE(..., ' UTC$', '')` 把结尾的 ` UTC` 替换成空，`$` 锚定行尾避免误伤中间。
然后 `CAST(... AS TIMESTAMP)`。**只有变成时间类型，后面才能 DATE_SUB、才能比大小。**

### 规则 2｜去重（实测 7,091 行完全重复）

```sql
FROM (
  SELECT DISTINCT
         event_time, event_type, product_id, category_id, category_code,
         brand, price, user_id, user_session
  FROM gmall.ods_event_log_di
) o;
```

按**原始 9 个字段整行去重**。实测存在 7,091 行"9 个字段完全相同"的记录 —— 典型的上报重试或采集重复。

### 规则 3｜生成主键 event_id

```sql
MD5(CONCAT_WS('|', o.event_time, o.event_type, CAST(o.product_id AS STRING),
      CAST(o.category_id AS STRING), o.category_code, o.brand,
      CAST(o.price AS STRING), CAST(o.user_id AS STRING), o.user_session)) AS event_id,
```

**这里有个容易忽略的设计**：md5 用的是**原始 9 字段**，而不是归一化后的字段。

> 代码注释原话：*"与去重用的键完全一致，保证去重之后 event_id 一定唯一
> （用归一化后的字段会引入碰撞风险）"*

为什么？因为归一化会**丢信息**——`category_code` 空和 `category_code=''` 归一化后都是 `unknown`，两个不同的原始行会算出同一个 md5。**主键必须建立在信息没被压缩的那一层。**

### 规则 4｜类目拆级 + 空值归一

```sql
COALESCE(NULLIF(SPLIT(o.category_code, '[.]')[0], ''), 'unknown') AS category_l1,
COALESCE(NULLIF(SPLIT(o.category_code, '[.]')[1], ''), 'unknown') AS category_l2,
COALESCE(NULLIF(SPLIT(o.category_code, '[.]')[2], ''), 'unknown') AS category_l3,
COALESCE(NULLIF(o.brand, ''), 'unknown')                          AS brand,
```

三层嵌套，从里往外读：

1. `SPLIT('electronics.smartphone', '[.]')[0]` → `electronics`（`[.]` 是字符组，比 `'\\.'` 少一层转义歧义）
2. `NULLIF(..., '')` → 如果是空串就变成 NULL
3. `COALESCE(..., 'unknown')` → NULL 兜底成 `unknown`

**★ 为什么归一成 `unknown` 而不留 NULL？**（这是面试高频题）

> 因为 **NULL 会干扰 GROUP BY 和 JOIN**。
> NULL 在 SQL 里代表"未知"，`GROUP BY` 会把所有 NULL 归为一组（看着还行），
> 但一旦出现 `COUNT(col)`（不数 NULL）、`col = 'x'`（NULL 永远不匹配）就会静默出错。
> 换成字符串 `'unknown'`，它就变成一个**普通的、可见的、能被过滤的值**。

代价是：分析时必须记得排除它。**没有完美方案，只有显式方案。**

### 规则 5｜★ price = 0 不删（一个反直觉的决策）

```sql
o.price,    -- 原样保留，不做任何处理
```

很多人第一反应是"0 元是脏数据，删掉"。**这里刻意不删**，理由是实测依据：

> `price = 0` 只出现在 `view`/`cart`，`purchase` 里**没有 0 值**。
> 所以 0 是"商品未定价时的展示价格"，是真实业务状态，不是错误。
> 更关键：**它不影响 GMV 口径**（GMV 只统计 purchase）。

> **这个方法论可以迁移**：清洗前先问"这个异常值会不会污染我要算的指标"，
> 而不是无脑删所有看起来奇怪的行。**误删真实数据比留下脏数据更严重。**

---

## 四、派生：从事件流里长出两张新表

### ① `dwd_purchase_fact_di`（购买事实表）

```sql
INSERT OVERWRITE TABLE gmall.dwd_purchase_fact_di PARTITION (dt)
SELECT
  event_id AS order_id, user_id, product_id, ..., 
  price AS amount, event_time AS pay_time,
  1 AS is_paid,          -- 由 purchase 事件派生，恒为 1
  dt
FROM gmall.dwd_event_log_di
WHERE event_type = 'purchase';
```

**为什么要有这张表？** 数据集里没有独立的订单表，一行 `purchase` 事件就是一笔成交。
保留它的价值写在 DDL 注释里：

> 让「订单类指标」（订单数、GMV、客单价）走独立的、语义清晰的表，
> 而不是让 DWS/ADS 每次都去事件流里筛 purchase 再聚合。

**这就是数仓"分层"的意义之一**：把"从 1063 万行里筛 purchase"这件事**只做一次**，后续所有交易指标都从这张小表出。

### ② `dim_product`（商品维表）—— 两个设计点都很值钱

**设计点 A：一个商品为什么不能直接 DISTINCT？**

代码注释给出了实测原因：**源数据存在商品属性不一致**（同一商品在不同事件里 brand/category 不同）。
所以做法是"先数每个属性组合出现多少次，取出现最多的那组"：

```sql
ROW_NUMBER() OVER (
  PARTITION BY product_id
  ORDER BY attr_cnt DESC, dt DESC   -- 取出现最多的属性组合；并列时取更近的
) AS rn
...
WHERE rn = 1;
```

**设计点 B：★ 维表里刻意不放 price**

```sql
-- ⚠️ 刻意不放 price：价格会随时间变化，属于「事实属性」而不是「维度属性」。
--    维表放一个会变的字段，是维度建模里典型的反模式——下游每次 join 都得
--    先想清楚"我拿到的是哪天的价格"。价格事实统一去 dwd_event_log_di 取。
```

> **这是维度建模最经典的一条分界线**：
> - **维度属性** = 描述"它是谁"，**稳定不变**（商品叫什么、属于哪个类目、什么品牌）
> - **事实属性** = 描述"发生了什么"，**随事件变化**（这次卖了多少钱）
>
> 价格属于后者。把它塞进维表，等于宣称"这个商品的价格是 X"——但这个宣称只在某一刻成立。

实测佐证（`dim_product` 里查 `product_id=26300090`）：

```
product_id  category_code  category_l1  brand    first_seen_dt  last_seen_dt  event_cnt
26300090    (空)           unknown      lucente  2019-10-01     2019-10-28    220
```

一个商品、220 次事件、横跨 10-01 到 10-28 —— **但表里没有价格**。价格在这 220 次事件里可能变过。

---

## 五、存储格式在这一站换成了 ORC

| 层 | 格式 | 理由 |
|---|---|---|
| ODS | `TEXTFILE` | 和原始 CSV 一致，装载即拷文件，无需转换 |
| DWD | `ORC` + `SNAPPY` | 列式存储，按列读 + 自带索引，体积约为文本 1/5~1/10 |

> DDL 注释原话：*"TEXTFILE 是纯文本，每次查询都要全量解析；ORC 是列式存储，按列读取 + 自带轻量索引……
> 后面做 Hive vs Spark 性能对比时，列式格式才是真实生产形态。"*

---

## 六、判据

脚本末尾自带校验（跑完自动输出）：

```sql
SELECT 'dwd_event_log_di' AS tbl, COUNT(DISTINCT dt), COUNT(1) FROM gmall.dwd_event_log_di
UNION ALL SELECT 'dwd_purchase_fact_di', COUNT(DISTINCT dt), COUNT(1) FROM gmall.dwd_purchase_fact_di
UNION ALL SELECT 'dim_product', COUNT(DISTINCT 1), COUNT(1) FROM gmall.dim_product;
```

**自己动手**：

```bash
# 1. 验证 event_id 真的唯一（去重是否生效）
$B --silent=true --outputformat=csv2 -e \
  "SELECT COUNT(1) AS rows, COUNT(DISTINCT event_id) AS uniq FROM gmall.dwd_event_log_di;"

# 2. 验证类目没有 NULL（归一是否生效）
$B --silent=true --outputformat=csv2 -e \
  "SELECT COUNT(1) FROM gmall.dwd_event_log_di WHERE category_l1 IS NULL;"

# 3. 复现本文的贯穿案例：看 513781527 的 5 行在 DWD 长什么样
$B --silent=true --outputformat=csv2 -e \
  "SELECT event_id, event_type, event_time, product_id, category_l1, brand, price
   FROM gmall.dwd_event_log_di WHERE dt='2019-10-01' AND user_id=513781527;"

# 4. 验证 purchase 无 0 值（规则 5 的依据）
$B --silent=true --outputformat=csv2 -e \
  "SELECT event_type, MIN(price), MAX(price) FROM gmall.dwd_event_log_di GROUP BY event_type;"
```

第 4 条跑完你会看到 `purchase` 的 MIN 不是 0，而 `view`/`cart` 的 MIN 是 0 —— **这就是"保留 price=0"这个决策的证据**。

---

## 七、自测题

1. 为什么 `event_id` 用**原始字段**算 md5，而不是用清洗后的字段？
2. 空值为什么归一成 `'unknown'` 而不是留 NULL？代价是什么？
3. `price = 0` 为什么保留？如果 GMV 口径改成"所有事件价格之和"会怎样？
4. 商品维表为什么**不放价格**？如果放了，下游分析会遇到什么问题？
5. 一个商品在源数据里属性前后不一致时，`dim_product` 是怎么决定用哪个值的？

---

## 下一站

→ `03_DWD到ADS_聚合与口径.md`

那里，`513781527` 的 5 行明细会**塌缩成 1 行**（`pv=3, purchase=2, pay=510.18`），
再变成 ADS 日报里的 1 个计数。同时你会看到三个真实数据揭示的陷阱 —— 包括一个**转化率大于 100%** 的诡异数字。
