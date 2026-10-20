# 06 · Hive 与 HQL：用 SQL 指挥一群机器

> **这章解决什么**：让你用熟悉的 SQL 操作 HDFS 上的 TB 级数据。
> 同时回答：Hive 的表和 MySQL 的表到底有什么不同？为什么必须有分区？为什么 `INSERT` 前面要加 4 行 `SET`？
>
> **前提**：你已经学过 SQL 基础（`SELECT / WHERE / GROUP BY / JOIN`）。这章只讲 Hive 特有的部分。

---

## 一、Hive 是什么（以及它不是什么）

### 1.1 一句话

**Hive = SQL 翻译官。** 你写 SQL，它翻译成 MapReduce（或 Tez/Spark）作业去跑，再把结果写回 HDFS。

```
你写的：  SELECT dt, COUNT(DISTINCT user_id) FROM dwd_event_log_di GROUP BY dt;
              ↓ Hive 编译
实际跑的： 2~3 个 MapReduce 作业（读块 → 局部聚合 → shuffle → 全局聚合 → 写 HDFS）
```

### 1.2 ⚠️ Hive 不是数据库

这是初学者最大的认知错位：

| | MySQL（数据库） | Hive（数据仓库） |
|---|---|---|
| 存数据在哪 | 本地磁盘的专有格式 | **HDFS 上的普通文件**（CSV/ORC…） |
| 表结构（元数据）在哪 | 和数据在一起 | **MySQL 里**（叫 Metastore） |
| 写的语言 | SQL | HQL（像 SQL，但不完全一样） |
| 延迟 | 毫秒级 | **秒 ~ 小时级**（每次都要启作业） |
| 支持改单行吗 | 支持 | **传统上不支持**（离线场景不需要） |
| 典型数据量 | GB 级 | **TB ~ PB 级** |

**所以 Hive 里的"表"到底是什么**：

```
Hive 表 = MySQL 里的一条元数据记录（列名、类型、分区、文件位置）
        + HDFS 上的一个目录（里面是真正的数据文件）
```

**推论（非常重要）**：

- `DROP TABLE`（内部表）= 删 MySQL 记录 **+ 删 HDFS 目录**，数据真没了
- 你可以直接用 `hdfs dfs -put` 往表目录里塞文件，**Hive 就能查到**（不经过 Hive 也行）
- 这就是为什么"分区字段"在文件里通常**不存在**，而是体现在目录名上（第 04 章的 `dt=2019-10-01/`）

### 1.3 Hive 的两个服务进程

| 进程 | 端口 | 作用 |
|---|---|---|
| **MetaStore** | 9083 | 管理元数据（表结构存哪、长什么样） |
| **HiveServer2 (HS2)** | 10000 | 接收客户端 SQL 请求（你连的是它） |

你用的客户端叫 **beeline**（JDBC 客户端）。

> 📌 **本项目对照**：HS2 被 systemd 标记 `active` 之后，**至少还要等 50 秒**才真正监听 10000 端口。
> **判断服务可用，必须用外部判据**：`ss -lntp | grep 10000` + **发一次真实查询**。

---

## 二、beeline：每天要敲的那个命令

```bash
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline        # 建议设成变量
URL="jdbc:hive2://lrh-1:10000"

# 方式一：交互式（进去一条条敲，适合探索）
$BEELINE -u $URL

# 方式二：执行单条（适合脚本里取数）
$BEELINE -u $URL --silent=true -e "SELECT COUNT(1) FROM gmall.ods_event_log_di;"

# 方式三：★ 执行一个 SQL 文件（生产环境的主力方式）
$BEELINE -u $URL -f etl_ods_to_dwd.sql

# 指定输出格式（导出数据时用）
$BEELINE -u $URL --outputformat=csv2 -e "SELECT dt, COUNT(1) FROM t GROUP BY dt;" > out.csv
```

| 参数 | 说明 |
|---|---|
| `--silent=true` | 去掉多余的状态输出，只留结果 |
| `--outputformat=csv2` | 输出 CSV。**合法值只有 `table/vertical/csv2/tsv2/dsv/csv/tsv`** |
| `-f file.sql` | 执行文件（**多语句务必写文件，别在命令行里拼**） |
| `--showHeader=false` | 不要表头（导出给程序读时用） |

> ⚠️ **两个真实的坑**：
> 1. **`--outputformat=json` 不存在**。合法值只有上面那一串，传 json 会报错。
>    要 JSON 就导出 csv2 再用 Python 转。
> 2. **多语句别在命令行里拼长 SQL**——引号很容易被 shell 剥掉，而且失败时你看不出是哪一句错了。
>    一律写成文件 `beeline -f`。

---

## 三、建表：Hive 特有的四个决策

### 3.1 一张真实的建表语句（逐行讲）

```sql
CREATE TABLE gmall.ods_event_log_di (
  event_time    string  COMMENT '事件时间（原始文本，解析在 DWD 层做）',
  event_type    string  COMMENT 'view 浏览 / cart 加购 / purchase 购买',
  product_id    bigint  COMMENT '商品ID',
  category_code string  COMMENT '类目路径，如 electronics.smartphone',
  brand         string  COMMENT '品牌（约 14.4% 为空）',
  price         double  COMMENT '价格',
  user_id       bigint  COMMENT '用户ID',
  user_session  string  COMMENT '会话ID'
)
COMMENT 'ODS 原始层：电商用户行为事件日志'
PARTITIONED BY (dt string COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

> 📌 这是案例项目 `ods_event_log_ddl.sql` 的简化版（真实版 10,630,245 行，31 个分区）。

四个决策点：

#### 决策① 内部表 vs 外部表

| | 内部表（managed） | 外部表（external） |
|---|---|---|
| 写法 | `CREATE TABLE` | `CREATE EXTERNAL TABLE` |
| `DROP TABLE` 时 | **数据一起删** | **只删元数据，数据还在** |
| 适用 | 中间结果、可重算的 | **原始数据、怕误删的** |

```sql
CREATE EXTERNAL TABLE ... LOCATION '/data/raw/events/';
```

> **经验法则**：**ODS 层用外部表**（原始数据是资产，不能因为一句 DROP 就没了）；
> 中间层和结果层用内部表（反正能重算）。

#### 决策② 分区（★ 没有分区的表在生产上是不可接受的）

```sql
PARTITIONED BY (dt string)
```

**为什么**：不带分区的表，查一天数据要扫全表。带分区后：

```
/user/hive/warehouse/gmall.db/ods_event_log_di/
  ├── dt=2019-10-01/     ← 查 10-01 时只读这个目录
  ├── dt=2019-10-02/
  └── ...
```

```sql
WHERE dt = '2019-10-01'    -- 只会读一个目录，快几十倍
```

**分区字段是"伪列"**：它不在数据文件里，体现在目录名上。
但你在 SQL 里可以像普通列一样用它。

#### 决策③ 存储格式

| 格式 | 特点 | 用在哪 |
|---|---|---|
| `TEXTFILE` | 纯文本（CSV），**可读、可直接 put** | ODS 原始层 |
| `ORC` | **列式 + 压缩**，体积可降到 1/5，查询快很多 | DWD/DWS/ADS |
| `PARQUET` | 列式，Spark 生态常用 | 与 Spark 混用时 |

```sql
STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');
```

**为什么列式快**：`SELECT SUM(price) FROM t` 只需要读 price 那一列，
行式存储要把整行（包括你没查的字段）都读出来。

**具体少读多少，可以算出来**：假设表有 9 个字段、每个字段大小差不多——

| 存储方式 | 读 1 列要碰的数据 | 相对量 |
|---|---|---|
| 行式（TEXTFILE） | 整行 9 个字段 | **9 份** |
| 列式（ORC） | 只有 price 一列 | **1 份** |

→ 少读约 **8/9**。再叠加 ORC 自带的压缩（体积降到约 1/5），
**同一个查询要过磁盘的数据量可能只有原来的 1/45**。这就是"IO 少一个数量级"的算法。也是为什么
数仓的 DWD/DWS/ADS 层基本不存 TEXTFILE。

#### 决策④ 数据类型

`int / bigint / double / string / boolean / timestamp / date`。
**拿不准就用 `bigint` 和 `string`**（Hive 对类型不如 MySQL 严格，用错类型比类型不够精确更常出错）。

---

## 四、装载数据：三种方式

```sql
-- 方式一：从本地文件加载（文件会被复制到 HDFS）★ ODS 最常用
LOAD DATA LOCAL INPATH '/home/lrh/workspace/new_chain/real_data/2019-10-01.csv'
  OVERWRITE INTO TABLE gmall.ods_event_log_di PARTITION (dt='2019-10-01');

-- 方式二：从 HDFS 上的路径加载（移动，不复制）
LOAD DATA INPATH '/tmp/upload/2019-10-01.csv'
  OVERWRITE INTO TABLE gmall.ods_event_log_di PARTITION (dt='2019-10-01');

-- 方式三：★ INSERT OVERWRITE（层与层之间的主力方式）
INSERT OVERWRITE TABLE gmall.dwd_event_log_di PARTITION (dt)
SELECT ..., dt FROM gmall.ods_event_log_di;
```

### 4.1 静态分区 vs 动态分区（★ 必踩的坑）

**静态分区**：分区值写死在语句里，一次只能写一个分区。

```sql
INSERT OVERWRITE TABLE t PARTITION (dt='2019-10-01') SELECT ... WHERE dt='2019-10-01';
```

**动态分区**：分区值来自 SELECT 的最后一列，一次写 N 个分区。

```sql
-- ★ 这 4 行 SET 必须带，否则报 Error 10096
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;

INSERT OVERWRITE TABLE gmall.dwd_event_log_di PARTITION (dt)
SELECT col1, col2, ..., dt          -- ★ dt 必须在 SELECT 列表的最后
FROM gmall.ods_event_log_di;
```

**为什么必须是 `nonstrict`？** 这两个值在管"允不允许危险写法"：

| 值 | 规则 | 后果 |
|---|---|---|
| `strict`（默认） | **至少要有 1 个静态分区**（如 `PARTITION (dt='2019-10-01')`） | 防止手滑写成"一次覆盖全表所有分区" |
| `nonstrict` | 允许**全部**分区都是动态的（`PARTITION (dt)`） | 才能一次写 31 天；但**写错就是把所有分区重写一遍** |

所以 `nonstrict` 放宽的不是某个"功能"，是**一道防误操作的安全阀**。
这也解释了为什么报错是 `Error 10096`：strict 模式下 `PARTITION (dt)` 这种纯动态写法会被直接拦下。

> ⚠️ **这个坑案例项目踩过两次**，第二次是**明知故犯**：
> 因为凭记忆手写 SQL、绕开了会自动拼 `setup.sql` 的执行器，结果又报 `Error 10096`。
> **教训：靠记忆防坑不可靠，要把坑固化进脚本模板。** 这四行 SET 请直接复制进你的每个写分区的脚本。

---

## 五、HQL 与标准 SQL 的差异（写之前先看）

### 5.1 常用且好用的

```sql
-- ① CASE WHEN 做"行转列"式的条件统计（数仓里最高频的写法）
COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS pv_cnt

-- ② 窗口函数：一次扫描算出多个口径（★ 极大简化代码）
MIN(dt) OVER (PARTITION BY user_id) AS first_dt                    -- 首次活跃日
LAG(dt) OVER (PARTITION BY user_id ORDER BY dt) AS prev_dt         -- 上一次活跃日
ROW_NUMBER() OVER (PARTITION BY dt ORDER BY gmv DESC) AS rank_num  -- 排名
SUM(x) OVER (PARTITION BY user_id ORDER BY dt
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)     -- 累计求和

-- ③ 去重 + 拆字段
SELECT DISTINCT a, b, c FROM t                      -- 整行去重
SPLIT(category_code, '[.]')[0]                      -- 按点拆，取第 1 段
COALESCE(NULLIF(brand, ''), 'unknown')              -- 空串转 NULL 再兜底成 unknown
REGEXP_REPLACE(event_time, ' UTC$', '')             -- 去掉尾部的 " UTC"

-- ④ 防止除零
ROUND(a * 1.0 / NULLIF(b, 0), 4)
```

### 5.2 Hive 没有 / 不一样的（**踩过就记住**）

| 你想写 | Hive 的情况 |
|---|---|
| `SEQUENCE(0, 364)` | ❌ **没有这个函数**（`sequence()` 是 Spark SQL 的）<br>替代：`SELECT posexplode(split(repeat(',', 364), ','))` 造 365 行 |
| `INSERT INTO ... VALUES` | 支持但极慢（一条一个作业），离线数仓不这么用 |
| `UPDATE / DELETE` | 传统上不支持（要开事务表，生产离线数仓基本不用） |
| `rows` 当别名 | ❌ **`rows` 是保留字**，用它做别名会让语句解析失败 |
| `dayofweek()` | ⚠️ **1 = 星期日**（不是周一！），做星期维度必须换算 |
| 子查询别名 | **必须给每个派生表起别名**（`FROM (...) t`），否则报错 |
| `=` 比较 NULL | `NULL = NULL` 返回 NULL 不是 true，用 `IS NULL` |

> 📌 **"Hive 与 Spark SQL 高度相似"本身就是陷阱。**
> 跨引擎迁移时，语法假设**必须先验证再写进脚本**。

---

## 六、常用 SET 参数（写在脚本头部）

```sql
-- 动态分区（写分区表必带，见 §4.1）
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- 压缩：减少落盘与网络
SET hive.exec.compress.intermediate = true;
SET hive.exec.compress.output = true;

-- 内存：★ 按作业类型给（见第 11 章，不是一套走天下）
SET mapreduce.map.memory.mb = 1024;
SET mapreduce.map.java.opts = -Xmx820m;

-- 并行执行无依赖的阶段
SET hive.exec.parallel = true;

-- 小表自动走 map-join（避免大表 JOIN 的 shuffle）
SET hive.auto.convert.join = true;
```

> ⚠️ **"参数文件生成了" ≠ "参数生效了"**。
> 公共 SET 如果写在一个单独的 `setup.sql` 里，必须**拼进每个分段 SQL** 才生效。
> 案例项目就因为漏拼，报了 `Error 10096`。
> **配置类动作启动前要 `grep` 自检，查不到就终止。**

---

## 七、EXPLAIN：看你的 SQL 会怎么跑

```sql
EXPLAIN SELECT dt, COUNT(1) FROM gmall.dwd_event_log_di GROUP BY dt;
```

看三件事：

1. **`STAGE DEPENDENCIES`** → 有几个 Stage 就要跑几个作业，串行还是并行
2. **`Map Operator Tree` / `Reduce Operator Tree`** → map 端和 reduce 端各干什么
3. 有没有出现你没预期的 `Map Join` / `Sort Merge Join` / 多个 `Group By`

**什么时候用**：SQL 跑得比你预期慢很多的时候。先看 EXPLAIN，别急着加资源。

---

## 八、动手实验室

### 实验 6-1：完整走一遍 建库 → 建表 → 装载 → 查询

```bash
cat > /tmp/lab01.sql <<'EOF'
CREATE DATABASE IF NOT EXISTS lab COMMENT '练习库';

DROP TABLE IF EXISTS lab.ods_events;
CREATE TABLE lab.ods_events (
  event_time string, event_type string, product_id bigint,
  brand string, price double, user_id bigint
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

SHOW TABLES IN lab;
DESC lab.ods_events;
EOF

$BEELINE -u $URL -f /tmp/lab01.sql
```

```bash
# 造两天的 CSV
mkdir -p /tmp/labdata
printf '2019-10-01 00:00:01,view,101,apple,999.0,u1\n2019-10-01 00:00:02,purchase,101,apple,999.0,u1\n' > /tmp/labdata/2019-10-01.csv
printf '2019-10-02 00:00:01,view,102,samsung,499.0,u2\n2019-10-02 00:00:02,purchase,102,samsung,499.0,u2\n' > /tmp/labdata/2019-10-02.csv

cat > /tmp/lab02.sql <<'EOF'
LOAD DATA LOCAL INPATH '/tmp/labdata/2019-10-01.csv' OVERWRITE INTO TABLE lab.ods_events PARTITION (dt='2019-10-01');
LOAD DATA LOCAL INPATH '/tmp/labdata/2019-10-02.csv' OVERWRITE INTO TABLE lab.ods_events PARTITION (dt='2019-10-02');
SELECT dt, COUNT(1) AS cnt, COUNT(DISTINCT user_id) AS uv FROM lab.ods_events GROUP BY dt;
EOF

$BEELINE -u $URL -f /tmp/lab02.sql
```

```bash
# ★ 去 HDFS 上看它变成了什么
hdfs dfs -ls /user/hive/warehouse/lab.db/ods_events/
# 应该看到 dt=2019-10-01/ 和 dt=2019-10-02/ 两个目录
```

### 实验 6-2：体会分区的威力

```sql
-- 不带分区条件（全表扫）
SELECT COUNT(1) FROM lab.ods_events;
-- 带分区条件（只扫一个目录）
SELECT COUNT(1) FROM lab.ods_events WHERE dt = '2019-10-01';
```

数据量小的时候看不出差别。想象这张表有 1000 万个分区目录、1TB 数据——
**没带分区条件，就是把 1TB 全读一遍。**

### 实验 6-3：动态分区（体会那 4 行 SET）

```bash
cat > /tmp/lab03.sql <<'EOF'
DROP TABLE IF EXISTS lab.dwd_events;
CREATE TABLE lab.dwd_events (event_type string, product_id bigint, user_id bigint)
PARTITIONED BY (dt string) STORED AS ORC;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;

INSERT OVERWRITE TABLE lab.dwd_events PARTITION (dt)
SELECT event_type, product_id, user_id, dt FROM lab.ods_events;

SELECT dt, COUNT(1) FROM lab.dwd_events GROUP BY dt;
EOF

$BEELINE -u $URL -f /tmp/lab03.sql
```

**然后故意删掉那 4 行 SET 再跑一次**，看报什么错（`Error 10096`）。**亲手踩一次，比记十遍牢。**

---

## 九、常见坑

| 现象 | 根因 | 解法 |
|---|---|---|
| `Error 10096` | 动态分区没开 nonstrict | 补 4 行 SET |
| `Error 10004 Invalid table alias` | 引用了不存在的列/表 | **跨表写 SQL 前先 `DESC` 所有涉及的表** |
| `Error 10011 Invalid function sequence` | 用了 Spark 专有函数 | 换 Hive 写法（见 §5.2） |
| 中文注释变 `?` | 元数据库字符集 | 元库用 utf8mb4；客户端加 `--default-character-set=utf8mb4` |
| 语句静默返回空 | 用了保留字（如 `rows`）当别名，stderr 又被丢掉了 | 改名；**别把 stderr 丢掉** |
| `beeline --outputformat=json` 报错 | 没有这个格式 | 用 csv2 再转 |
| 查出来 0 行 | 分区没建 / 分区值不对 / 数据在别的目录 | `SHOW PARTITIONS t`；`hdfs dfs -ls` 看目录 |
| INSERT 把整表清空了 | `INSERT OVERWRITE` 到**非分区**表会覆盖全表 | 分区表必须动态分区模式，否则先清空整表 |

---

## 十、自测

**每题标了对应的正文小节——答不出来就回去看那节，不要硬想。**

1. Hive 表和 MySQL 表，本质区别是什么？DROP TABLE 时分别发生什么？—— 对应 **§1.2**
2. 为什么分区字段不在数据文件里，却能 `WHERE dt='...'`？—— 对应 **§3.1 决策②**
3. 写动态分区必须带哪 4 行 SET？少一行会报什么错？—— 对应 **§4.1**
4. ORC 比 TEXTFILE 快在哪？（说"列式 + 压缩"之后，能再说一句"少读了多少 IO"吗）—— 对应 **§3.1 决策③**
5. `hive.exec.dynamic.partition.mode=nonstrict` 到底在放宽什么限制？—— 对应 **§4.1**
6. SQL 跑得慢，你会先看什么？—— 对应 **§七**

<details>
<summary><b>参考答案要点</b>（先自己答一遍，再点开核对）</summary>

**1. Hive 表和 MySQL 表的本质区别**
- Hive 表 = **MySQL 里一条元数据记录** + **HDFS 上一个目录**（数据是 HDFS 上的普通文件）
- MySQL 表 = 表结构和数据都在同一个数据库里
- `DROP TABLE`：
  - **内部表** → 删元数据 **+ 删 HDFS 目录（数据真没了）**
  - **外部表** → 只删元数据，**数据还在**
- 结论：ODS 用外部表（原始数据是资产）；中间层用内部表（反正能重算）

**2. 分区字段为什么"不在文件里却能用"**
- 分区值体现在**目录名**上（`dt=2019-10-01/`），不在数据内容里
- Hive 把 `WHERE dt='2019-10-01'` 翻译成"只读那一个目录"，不用去读列值
- 这也是**分区的意义**：避免全表扫描

**3. 四行 SET / 少一行报什么**
```sql
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;      -- ★ 少这行报 Error 10096
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
```
- 另有硬性写法要求：**`dt` 必须是 SELECT 列表的最后一列**

**4. ORC 比 TEXTFILE 快在哪——少读多少可以算**
- 列式：只读需要的列。9 个字段查 1 列 → **少读约 8/9**
- 压缩：体积约降到 1/5
- 两者叠加 → 过磁盘的数据量可能只有原来的 **1/45**
- 所以 DWD/DWS/ADS 层基本不存 TEXTFILE（案例全用 `ORC + SNAPPY`）

**5. `nonstrict` 放宽了什么**
- 放宽的是**一道防误操作的安全阀**：
  - `strict`（默认）：**至少要有 1 个静态分区**，纯动态的 `PARTITION (dt)` 会被拦下
  - `nonstrict`：允许**全部**分区都是动态的 → 才能一次写 31 天
- 代价：写错就是把**所有分区重写一遍**，所以它放宽的是"安全"，不是"功能"

**6. SQL 慢先看什么**
- **先 `EXPLAIN`**，数 `STAGE DEPENDENCIES` 有几个 Stage（每个 Stage 通常一个作业）
- 先判断"是不是作业数太多/串行"，再决定改写法还是加资源
- 反面做法：不看 EXPLAIN 直接加内存
</details>

---

## 下一章

[07 · 维度建模与分层设计](./07_建模_维度建模与分层设计.md) —— **全课最重要的一章**。
会写 SQL 的人很多，能说清"这张表一行代表什么"的人很少。
