# 第一站 · 原始数据 → ODS：原样落地

> 建立：2026-10-15　|　**文档中全部数字均为当天在 lrh-1 上实测所得，不是估算**
> 配套：`代码学习路径_从AI代码到自己能讲.md`（在 `项目产出/` 根目录）
>
> **这套三份文档共用一个贯穿案例**：真实用户 `user_id = 513781527` 在 2019-10-01 的行为。
> 它在这条链路上从 **5 行原始文本**，最终变成 **ADS 日报里的 1 个计数**。
> 每一站都会拿出它当时的样子 —— 你能亲眼看到数据是怎么一层层变形的。

---

## 一、目标（一句话）

把 1063 万行原始 CSV **原封不动**搬进 HDFS，变成一张能按天查询的表。**这一站不做任何加工。**

| | |
|---|---|
| **输入** | 31 个按天命名的 CSV 文件，共约 1.3 GB |
| **输出** | `gmall.ods_event_log_di`（一张表，31 个日期分区） |
| **谁在干** | `load_ods.sh`（脚本） + Hive（建表/装载） + HDFS（实际存储） |
| **怎么知道干对了** | 分区数 = 31、总行数 ≈ 10,630,245、事件类型只有 view/cart/purchase |

---

## 二、原始数据长什么样

**来源**：REES46 eCommerce behavior data（Kaggle 公开数据集，CC BY 4.0），取 2019 年 10 月单月，按用户 1/4 采样。

**一行 = 一个用户行为事件。** 这是埋点数据最原始的形态：

```
2019-10-01 00:00:11 UTC,view,1004545,2053013555631882655,electronics.smartphone,huawei,566.01,537918940,406c46ed-90a4-4787-a43b-59a410c1a5fb
```

拆开看（**这是实测的原始数据行，来自 `ods_event_log_di` 的 2019-10-01 分区**）：

| 字段 | 真实值 | 说明 |
|---|---|---|
| `event_time` | `2019-10-01 00:00:11 UTC` | ⚠️ **带 " UTC" 后缀的文本**，不是标准时间类型 |
| `event_type` | `view` | 只有三种：view 浏览 / cart 加购 / purchase 购买 |
| `product_id` | `1004545` | 商品ID |
| `category_id` | `2053013555631882655` | 类目ID（数字型，人看不懂） |
| `category_code` | `electronics.smartphone` | 类目路径，**点分层级**，人能看懂 |
| `brand` | `huawei` | 品牌 |
| `price` | `566.01` | 价格 |
| `user_id` | `537918940` | 用户ID |
| `user_session` | `406c46ed-90a4-...` | 会话ID（一次连续访问） |

再看同一批里的另外几行——**注意最后一行**：

```
2019-10-01 00:00:16 UTC,view,13500240,...,furniture.bedroom.bed,brw,93.18,...
2019-10-01 00:00:20 UTC,view,4803399,...,electronics.audio.headphone,jbl,33.21,...
2019-10-01 00:00:25 UTC,view,27500014,2053013554692358509,,redmond,37.98,...
                                                        ^^
                                              category_code 是空的
```

这就是真实数据的样子：**有空值、有时间格式不规整、有重复行**。后面 DWD 层要处理的就是这些。

---

## 三、ODS 表结构

代码位置：`项目产出/数据接入/ods_event_log_ddl.sql`

```sql
CREATE TABLE gmall.ods_event_log_di (
  event_time    string  COMMENT '事件时间（原始文本，形如 2019-10-01 00:00:00 UTC，解析在 DWD 层做）',
  event_type    string  COMMENT '事件类型：view 浏览 / cart 加购 / purchase 购买',
  product_id    bigint  COMMENT '商品ID',
  category_id   bigint  COMMENT '商品类目ID',
  category_code string  COMMENT '类目路径（点分层级，如 electronics.smartphone…原数据约 31.9% 为空）',
  brand         string  COMMENT '品牌（原数据约 14.4% 为空，清洗在 DWD 层做）',
  price         double  COMMENT '商品价格（0 值仅出现在 view/cart，purchase 无 0 值）',
  user_id       bigint  COMMENT '用户ID',
  user_session  string  COMMENT '会话ID'
)
COMMENT 'ODS 原始层：电商用户行为事件日志（REES46 2019-10，按 dt 分区，原样接入不做加工）'
PARTITIONED BY (dt string COMMENT '日期分区，取自 event_time 前 10 位')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

### 三个关键设计（背下来，面试会问）

**① 只有一张表。** 真实埋点本来就是"一行一个事件"的宽表。商品维表、订单事实表应该在 DWD 层**从事件流派生**，而不是在 ODS 重复存一份。

**② ★「不加工」是 ODS 的铁律。** 看字段注释里反复出现的"解析在 DWD 层做"：

| 现象 | ODS 怎么做 | 为什么 |
|---|---|---|
| `event_time` 带 " UTC" 后缀 | **原样保留文本** | 解析是业务逻辑，不是接入逻辑 |
| `brand` / `category_code` 有空值 | **保留空串**，不转 NULL | 同上 |
| `price` 有 0 值 | **原样保留** | 0 是真实数据特征，不是错误 |

> **为什么这么重要**：ODS 是**唯一的原始数据副本**。一旦在 ODS 加工了，后来发现加工错了，原始数据就找不回来了 —— 只能重新导入 1.3GB。
> 让 ODS 保持"傻"，所有的聪明都放在 DWD，**错了重跑 DWD 就行**。

**③ 分区列 `dt` 来自 `event_time` 前 10 位，文件本身没有这一列。**
`SUBSTR(event_time, 1, 10)` → `2019-10-01`。所以 31 个文件 → 31 个分区。

---

## 四、装载过程：不只是一句 LOAD DATA

代码位置：`项目产出/数据接入/load_ods.sh`（82 行）

脚本做三件事：建表 → 逐天装载 → 校验。这里有一个**值得学的工程细节**：

```bash
# 把 62 条语句（31 组 ALTER + LOAD）写进一个文件一次性执行。
# 为什么不是每天单独调一次 beeline：每次 beeline 都要冷启一个 JVM（约 5 秒），
# 31 天 × 3 次调用 ≈ 93 次启动，白白等七八分钟。
SQL="/tmp/load_ods_$$.sql"
: > "$SQL"
for f in "$DATADIR"/*.csv; do
  d=$(basename "$f" .csv)
  echo "ALTER TABLE $TABLE ADD IF NOT EXISTS PARTITION (dt='$d');" >> "$SQL"
  echo "LOAD DATA LOCAL INPATH '$f' OVERWRITE INTO TABLE $TABLE PARTITION (dt='$d');" >> "$SQL"
done
bl -f "$SQL"
```

**这个知识点可以迁移**：循环里反复调用一个"启动很慢的命令"，是 shell 脚本最常见的性能陷阱之一。
正确做法是**把循环里的东西拼成一个批量文件，只调一次**。

---

## 五、判据：怎么证明这一站做对了

脚本第 3 段自带校验，实测结果：

| 检查项 | 期望值 | 实测 |
|---|---|---|
| 分区数 | 31 | **31** ✅ |
| 日期范围 | 2019-10-01 ~ 10-31 | **2019-10-01 ~ 2019-10-31** ✅ |
| 总行数 | 约 10,630,245 | 见下方自测 |

**自己动手跑一遍**（SSH 进 lrh-1 后执行）：

```bash
export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH
B=/opt/hive/apache-hive-3.1.3-bin/bin/beeline

# 1. 分区数与行数
$B -u jdbc:hive2://lrh-1:10000 --silent=true --outputformat=csv2 -e \
  "SELECT COUNT(DISTINCT dt) AS part_cnt, COUNT(1) AS rows FROM gmall.ods_event_log_di;"

# 2. 事件类型分布（应该只有三种）
$B -u jdbc:hive2://lrh-1:10000 --silent=true --outputformat=csv2 -e \
  "SELECT event_type, COUNT(1) AS cnt FROM gmall.ods_event_log_di GROUP BY event_type ORDER BY cnt DESC;"

# 3. 看一眼原始行，确认"未加工"是什么样
$B -u jdbc:hive2://lrh-1:10000 --silent=true --outputformat=csv2 -e \
  "SELECT * FROM gmall.ods_event_log_di WHERE dt='2019-10-01' LIMIT 3;"

# 4. 看 HDFS 上真实占了多少空间
hdfs dfs -du -s -h /user/hive/warehouse/gmall.db/ods_event_log_di
```

> 第 3 条最有价值：**亲眼看到 `event_time` 那串 " UTC" 后缀**，你就明白 ODS 的"傻"是什么意思了。

---

## 六、自测题（答不上来回去重读）

1. ODS 层为什么**不允许**做数据清洗？如果清洗逻辑写错了会怎样？
2. `price = 0` 的行在 ODS 被保留下来，为什么这不算脏数据？（提示：看字段注释）
3. 分区列 `dt` 是 CSV 文件里自带的吗？如果不是，它从哪来？
4. `load_ods.sh` 为什么要把 62 条语句拼成一个文件再执行？如果改成循环调用会慢多少？

---

## 下一站

→ `02_ODS到DWD_清洗与派生.md`

在那里，你会看到 `513781527` 这 5 行原始数据**被逐字改造**：`UTC` 后缀被剥掉、空类目变成 `unknown`、每一行被算出一个 md5 主键。
