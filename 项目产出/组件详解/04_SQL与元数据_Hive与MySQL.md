# 组件详解 · 04 SQL 与元数据：Hive 与 MySQL

> **一句话定位**：**Hive 是个翻译官**——让你用 SQL 说话，它把 SQL 翻译成 MapReduce 作业交给 YARN 去跑；**MySQL 是账本**——记录"每张表长什么样、数据在 HDFS 的哪个目录"。
>
> 涵盖：Hive（编译器 + HiveServer2 + MetaStore + beeline）、MySQL，以及"分区"和"表格式"两个核心概念

---

## 〇、先建立直觉

| 名字 | 一句话类比 | 物理真相 |
|---|---|---|
| **Hive** | **翻译官**：你说中文（SQL），它翻译成车间能执行的工单（MapReduce） | 一个程序，负责把 SQL 编译成 MR 作业 |
| **MySQL** | **账本**：记着每张表有哪些字段、数据放在哪个目录 | 一个数据库进程，Hive **自己不存业务数据** |
| **MetaStore** | 图书管理员：谁问"这表长什么样"都找它 | 一个服务进程，包在 MySQL 外面，提供统一接口 |
| **HiveServer2** | **前台接待**：接你的 SQL 请求 | 一个 JVM 进程，监听 10000 端口 |
| **beeline** | **客户的电话**：连上前台提交需求 | 一个命令行客户端 |
| **分区（Partition）** | 把书按日期分成 31 格书架 | HDFS 上的 31 个子目录，如 `dt=2019-10-01/` |

**最重要的那句话**：

> ⚠️ **Hive 不是数据库。** 它自己没有存储引擎、不做事务（默认）、不算数据。它只是：
> **① 管元数据（表结构）② 把 SQL 翻译成 MapReduce ③ 把结果从 HDFS 读出来给你看。**
>
> 真正的存储是 HDFS，真正的计算是 MapReduce/YARN。**Hive 是套在这两者外面的一层"用法包装"。**

**为什么需要它？** 因为用 MapReduce 算"每天的活跃用户数"要写几十行 Java 代码（定义 Mapper、Reducer、Job 配置），而用 Hive 只要一句：

```sql
SELECT COUNT(DISTINCT user_id) FROM dwd_event_log_di WHERE dt = '2019-10-01';
```

---

## 一、为什么需要它们（没有会怎样）

**没有 Hive**：每个统计需求都要手写 Java 程序，改一个指标就要重新编译打包。数据分析师几乎无法独立工作。

**没有 MySQL**：Hive 必须自己想办法存"表结构、分区位置"这些元数据。用现成的数据库来存是最省事的选择——这也解释了为什么**跑 Hive 之前必须先起 MySQL**。

**四个部分的分工**：

```
     beeline ──JDBC:10000──► HiveServer2 ──► Hive 编译器（把 SQL 变成 MR 作业）
                                  │
                                  │ 问"这表有哪些字段、数据在哪"
                                  ▼
                              MetaStore ──JDBC:3306──► MySQL（账本：表结构、分区、注释）
                                  │
                                  │ 返回 HDFS 目录位置
                                  ▼
                        HDFS /user/hive/warehouse/gmall.db/...
```

---

## 二、按 7 问框架拆解

### 问题 1：谁和谁说话？走网络还是本地？

| 通信 | 端口 | 说明 |
|---|---|---|
| beeline → HiveServer2 | **10000** | JDBC 连接，你提交 SQL 就走这条 |
| HiveServer2 → MetaStore | **9083** | 问"表结构和分区在哪"（Thrift 协议） |
| MetaStore → MySQL | **3306** | 真正读元数据表的地方 |
| Hive 生成的 MR 作业 → YARN | 8088 | 把作业交给调度 |
| MR 作业 → HDFS | 8020 | 读数据、写结果 |

> 💡 **注意 9083 和 3306 的区别**：MetaStore 是**包在 MySQL 外面的一个服务**。为什么不直接连 MySQL？
> 因为 MetaStore 多做了一件事：**统一接口 + 缓存 + 权限控制**。Spark、Presto 等别的组件也通过 9083 读同一份元数据，这样大家看到的是同一套表定义。本项目规划 Spark 时说的"元数据打通"，打的其实就是这一层。

### 问题 2：数据此刻在哪？什么时候只在内存？

**这是 Hive 最需要建立清晰认知的一点**：

| 东西 | 存在哪 |
|---|---|
| **业务数据**（1063 万行事件） | **HDFS**（`/user/hive/warehouse/gmall.db/...`） |
| **表结构、分区列表、字段注释** | **MySQL**（`hive` 库里的 `TBLS`、`COLUMNS_V2`、`PARTITIONS` 等表） |
| SQL 的中间结果 | MR 的 map 输出在**子进程的本地磁盘**上 |
| SQL 的最终结果（INSERT 的情况） | 写回 **HDFS** 的新目录 |
| `SELECT ... ` 直接查的结果 | 只在**内存里**转一趟（这种情况不写 HDFS） |

> ⚠️ **一个关键区分**：
> - `INSERT OVERWRITE TABLE ... SELECT ...` → 会**真的跑 MR 作业**、真的写 HDFS
> - `SELECT COUNT(*) FROM ...` → 也会跑 MR 作业，但结果只返回给你，不落文件
>
> 所以"跑一条 SQL"的成本差异极大。本项目实测：**一条 INSERT 被拆成 11 个 MR 作业跑了 21 分钟，而一条校验 COUNT 只花了 176 秒。**

### 问题 3：谁负责"知道"？谁负责"存"？

| 角色 | 知道什么 |
|---|---|
| **MySQL** | 所有表的结构、字段类型、注释、分区列表（**账本本体**） |
| **MetaStore** | 把账本包装成一个服务，对外统一回答"表长什么样" |
| **Hive 编译器** | 知道"这条 SQL 该怎么拆成几个 MR 作业" |
| **NameNode** | 知道"这些 HDFS 目录里的数据块在哪几台机器上" |

**注意这里有三层"知道"，别搞混**：

```
Hive 知道："dwd_event_log_di 有 13 个字段，按 dt 分了 31 个区"
   ↓
NameNode 知道："dt=2019-10-01 这个目录下有个 620MB 的文件，切成 5 个块，分布在三台机器"
   ↓
DataNode 知道："我本地磁盘上存着第 2 号块"
```

### 问题 4：一个动作分几步？哪一步最贵？

**你敲一条 INSERT，背后发生的 8 步**：

```
① beeline 把 SQL 通过 JDBC 发给 HiveServer2（10000 端口）
② HiveServer2 向 MetaStore 问：这张表有哪些字段、数据在哪个目录
③ MetaStore 去 MySQL 查账本，返回结果
④ Hive 编译器把 SQL 翻译成一个「作业计划」——本项目这个计划是 11 个 MR 作业
⑤ 逐个把 MR 作业提交给 YARN
⑥ YARN 分配容器，各机器上的子进程读 HDFS 数据、执行 map
⑦ shuffle：把 map 输出跨网络搬到 reduce 处汇总
⑧ reduce 结果写回 HDFS（Hive 会先写临时目录，完成后改名成正式文件）
```

**最贵的是第 ⑦⑧ 步**（shuffle + 写回），不是第 ④ 步（翻译）。这跟人的直觉相反——**翻译一次很快，搬数据很慢。**

**一个特别值得理解的细节**：Hive 写数据时会先写到隐藏的临时目录：

```
/user/hive/warehouse/gmall.db/dwd_event_log_di/.hive-staging_hive_2026-09-28_.../_task_tmp.../dt=2019-10-22/_tmp.000000_3
                                          └─ 以 . 开头 = 隐藏目录 = 还没写好的半成品
```

写完之后再**整体改名**成正式文件。这个设计叫"**原子改名**"——保证别人查数据时**不会看到写了一半的残缺文件**。代价是：失败时这些临时目录会残留，需要清理。

> 本项目那次失败报错里的路径就带着 `.hive-staging_...`，一眼就能看出"死在写入阶段、还没改名成功"。

### 问题 5：出错了谁发现？怎么补救？

| 出错方 | 表现 | 补救 |
|---|---|---|
| MetaStore 连不上 MySQL | 所有 SQL 都报错，连表都读不到 | 起服务顺序错了。**必须先起 MySQL，再起 MetaStore，最后起 HiveServer2** |
| HiveServer2 挂了 | beeline 连不上 10000 端口 | 重启 `hive-server2`；正在跑的作业**会一起中断** |
| MR 作业失败 | 报 `FAILED: Execution Error, return code 2` | 真正原因藏在 `Caused by` 链里，要看 HiveServer2 日志 |
| 元数据里的中文注释变成 `???` | `DESC table` 显示问号 | **本项目已根治**（见第四章第 1 条） |

### 问题 6：换来了什么？代价是什么？

| 设计 | 换来了 | 代价 |
|---|---|---|
| **用 SQL 代替写 Java** | 分析师能独立干活；开发效率高 | 表达能力受限（复杂逻辑要写 UDF）；SQL 被翻译成 MR 后**不一定高效**（可能生成 11 个作业而人只想要 1 个） |
| **元数据放 MySQL** | 有现成的、事务安全、好备份 | **Hive 强依赖 MySQL**：MySQL 挂了 Hive 全废。这也是本项目"元数据库字符集问题"影响面这么大的原因 |
| **数据放 HDFS** | 能存海量、有副本保护 | 不支持行级更新（改一行也要重写整块）；**小文件多会把 NameNode 压垮** |
| **分区表** | 查询只扫需要的分区（"分区裁剪"），快几十倍 | 分区数不能太多（本项目 31 个正常，几千个就危险）；写入时要同时写多个分区文件（**这正是本次失败的原因**） |
| **表格式可选（TEXTFILE / ORC）** | ORC 体积小、扫描快，约是文本的 1/5~1/10 | ORC 不能直接用文本工具看；写入需要更多 CPU 做编码压缩 |

### 问题 7：规模放大 100 倍，先崩的是哪一环？

**按可能性排序：**

1. **MySQL** —— 元数据条目暴增（分区数 × 字段数），而且本项目 MySQL 只给了 128MB 的缓冲池
2. **NameNode 内存** —— 分区多 → 目录多 → 元数据条目多
3. **写入并发** —— 分区数 × reducer 数 × 副本数，本项目就崩在这一环

**怎么验证**：

```bash
# 看元数据库有多大（表结构、分区、注释都在这儿）
mysql -u root -p -e "SELECT COUNT(*) FROM hive.TBLS; SELECT COUNT(*) FROM hive.PARTITIONS; SELECT COUNT(*) FROM hive.COLUMNS_V2;"

# 看某张表的分区数
HIVE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
$HIVE -u jdbc:hive2://lrh-1:10000 --silent=true -e "SHOW PARTITIONS gmall.dwd_event_log_di" 2>/dev/null | wc -l
```

---

## 三、本项目实况

### 3.1 角色分布

| 角色 | 在哪台 | 端口 | 访问方式 |
|---|---|---|---|
| HiveServer2 | lrh-1 | **10000** | `jdbc:hive2://lrh-1:10000` |
| Hive MetaStore | lrh-1 | **9083** | `thrift://lrh-1:9083`（内部调用） |
| MySQL | lrh-1 | **3306** | 元数据库 `hive` + 调度库 `dolphinscheduler` |
| 数据仓库目录 | HDFS | — | `/user/hive/warehouse/gmall.db` |

### 3.2 数仓分层与表清单

| 层 | 作用 | 本项目有哪些表 |
|---|---|---|
| **ODS** | 原始数据原样进来，**不加工** | `ods_event_log_di`（真实数据）；另有模拟数据时代的 `ods_user_log_di` / `ods_order_info_di` / `ods_sku_info_di` |
| **DWD** | 清洗：去重、解析、拆字段、空值归一 | `dwd_event_log_di`、`dwd_purchase_fact_di`（新）；`dwd_user_behavior_di`、`dwd_order_fact_di`（旧） |
| **DIM** | 维度表：描述性属性 | `dim_product`（新）、`dim_sku` / `dim_user`（旧） |
| **DWS** | 主题宽表：按用户/商品/订单先汇总一层 | `dws_user_day`、`dws_sku_day`、`dws_order_day` |
| **ADS** | 指标层：直接给大屏用的结果 | `ads_dau`、`ads_conversion_funnel`、`ads_top_sku`、`ads_repurchase_rate` |

> 📌 **注意项目里现在有两套链路并存**（旧的模拟数据链路 + 新的真实数据链路），这是**刻意的**：新表用新名字，等新链路验证通过再决定旧表去留。`实验报告 03` 里说"不破坏已有数据"，指的就是这个。

### 3.3 常用命令

```bash
HIVE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
URL="jdbc:hive2://lrh-1:10000"

# 交互式进去
$HIVE -u $URL

# 执行单条 SQL（--silent=true 去掉横幅日志，但会吞掉所有输出）
$HIVE -u $URL --silent=true -e "SHOW TABLES IN gmall" 2>/dev/null

# 执行 SQL 文件（长任务必须后台跑）
setsid nohup $HIVE -u $URL --silent=true -f etl_ods_to_dwd.sql > run.log 2>&1 < /dev/null &

# 看表结构（注释是否正常，这里能验证中文注释问题）
$HIVE -u $URL --silent=true -e "DESC gmall.dwd_event_log_di" 2>/dev/null
```

### 3.4 分区表在生产中长什么样

本项目 `dwd_event_log_di` 在 HDFS 上的实际结构：

```
/user/hive/warehouse/gmall.db/dwd_event_log_di/
├── dt=2019-10-01/000002_0        ← 一个分区 = 一个目录
├── dt=2019-10-02/000002_0
├── ...
└── dt=2019-10-31/000002_0
```

**分区的好处**（本项目天天在用）：

```sql
-- 只扫 1 个分区（约 20MB），几秒出结果
SELECT COUNT(*) FROM dwd_event_log_di WHERE dt = '2019-10-01';

-- 不加分区条件 → 扫全部 31 个分区（620MB），慢得多
SELECT COUNT(*) FROM dwd_event_log_di;
```

这个机制叫**分区裁剪（partition pruning）**——Hive 看到 `dt = '2019-10-01'` 就只去读那一个目录。

> ⚠️ **但分区不是越多越好**：写入时每个分区是一个独立文件，分区多了写入并发的压力就成倍增长。**本项目 31 个分区就把写入压垮过一次。**

---

## 四、我们踩过的坑

### 坑 1：表注释和字段注释里的中文全变成 `???`（已根治）

**现象**：`DESC table` 显示 `??SKU`、`ODS ???????`。注意——**这不是显示问题，是存储时就被写坏了。**

**怎么确认是"存储型"而不是"显示型"乱码**（关键手法）：

```sql
SELECT HEX(COMMENT) FROM hive.COLUMNS_V2 WHERE ...;
-- 返回 3F3F534B55
-- 3F 是「问号」字符本身的编码，后面 534B55 是 ASCII 的 "SKU"
-- 中文全坏、ASCII 完好 → 这是字符集转换"无法映射就替换"的标准指纹
```

**根因**：Hive 3.1.3 官方的 metastore 建表脚本，把承载注释的列**声明成了 `latin1`**：

```
COLUMNS_V2.COMMENT          varchar(256)  CHARACTER SET latin1
TABLE_PARAMS.PARAM_VALUE    mediumtext    CHARACTER SET latin1
PARTITION_KEYS.PKEY_COMMENT varchar(4000) CHARACTER SET latin1
DBS.DESC                    varchar(4000) CHARACTER SET latin1
```

而 Hive 的 JDBC 连接是 utf8mb4 → 写中文时 MySQL 做 utf8mb4→latin1 转换，中文没有对应编码 → **被静默替换成 `?`，且不报任何错**。

**结论**：**这是 Hive 官方 schema 的缺陷，不是配置写错了。** 修法是把这 12 个列迁到 utf8mb4，再把存量注释补回去。

```sql
-- 修复脚本：项目产出/运维脚本/fix_metastore_charset.sh
-- 完整复盘：实验分析/实验02_Hive元数据库中文注释乱码根治.md（12 个假设 / 8 个证伪）
```

> ⭐ **一条可复用的排查方法论**：以后再遇到乱码，**先取 `HEX()` 判断是"显示型"（合法 UTF-8 字节）还是"存储型"（`3F3F` 开头的问号）**，不要靠猜编码。
> 本项目为此还排除了三个"看起来最可疑"的嫌疑，全部实测无问题：JVM 的 `file.encoding`（是 UTF-8）、JDBC 连接串（已配 `characterEncoding=UTF-8`）、MySQL 服务端（全 utf8mb4）。**排除掉这三样之后，才敢怀疑官方的建表脚本。**

### 坑 2：`beeline --outputformat=json` 不存在

很多教程（包括本项目早期文档）写 `beeline --outputformat=json` 导出数据。**实测：beeline 根本不支持 json。**

```bash
beeline --help | grep -A3 outputformat
# 合法取值只有：table / vertical / csv2 / tsv2 / dsv / csv / tsv
```

**正确做法**：导 `csv2`，再用 Python 转成 JSON。

```bash
beeline -u $URL --silent=true --outputformat=csv2 -e "SELECT ..." 2>/dev/null > out.csv
# 注意：csv2 里 NULL 会输出成字符串 "NULL"，转换时要判一下
```

### 坑 3：`--silent=true` 会吞掉所有输出（但报错还在）

**现象**：日志文件看起来是空的，但任务确实失败了，很容易误判成"脚本有问题"。

**机制**：`--silent` 屏蔽的是**正常输出**，**报错仍然会打到 stderr**——所以只要把 stderr 重定向到日志文件，错误还是能看到：

```bash
beeline ... -f xxx.sql > run.log 2>&1     # 2>&1 是关键，把 stderr 也收进日志
grep -v '^[[:space:]]*$' run.log | tail -20
```

**推论**：**需要看返回结果的步骤（比如校验行数）不要加 `--silent`。**

### 坑 4：`rows` 是 Hive 保留字

```sql
SELECT COUNT(*) AS rows FROM ...   -- ❌ 解析失败
SELECT COUNT(*) AS row_cnt FROM ... -- ✅ 用别的别名
```

**更阴险的是**：如果 stderr 被丢掉了，这个错误表现为"**静默返回空**"，会被误判成"数据丢了"。**所以别名别偷懒，用 `row_cnt` 这种。**

### 坑 5：用 `mysql` 客户端时中了中文乱码

```bash
mysql -u root -p ...                      # ❌ 它按系统 locale 猜字符集
mysql --default-character-set=utf8mb4 -u root -p ...   # ✅ 显式指定
```

不指定的话，SQL 文件里的中文会**在客户端这一侧就被破坏**，跟服务端无关。

---

## 五、面试高频追问

**Q1：Hive 和 MySQL 有什么区别？**
A：**定位完全不同**：
- **MySQL** 是**数据库**：自己存数据、支持行级更新和事务、适合在线的少量数据（毫秒级响应）
- **Hive** 是**数据仓库工具**：自己不存数据（存 HDFS）、不支持行级更新、适合离线的大批量数据（分钟级响应）

本项目里两者的关系是：**MySQL 存"表长什么样"，HDFS 存"数据本身"，Hive 负责把这两者串起来并翻译 SQL。**

**Q2：为什么 Hive 的元数据要放在 MySQL 里，不能放在 HDFS？**
A：因为元数据的访问模式跟大数据完全不同——**它是高频、小量、需要随机读写和事务**的（查一个字段类型、更新一条分区记录）。
HDFS 恰好**最不擅长**这个：它是为"大文件、一次写多次读、顺序扫描"设计的，随机更新极慢。
所以把元数据放在一个关系型数据库里是最合理的：**用对的工具做对的事**。代价是 Hive 强依赖 MySQL，MySQL 挂了 Hive 就全废。

**Q3：分区表为什么能加速查询？**
A：因为**分区在 HDFS 上就是目录**。Hive 看到 `WHERE dt = '2019-10-01'`，就能从元数据查出对应目录是 `.../dt=2019-10-01/`，**只读这一个目录**，这叫"分区裁剪"。
本项目实测：带分区条件的 COUNT 只扫约 20MB，不带条件的要扫 620MB 全部 31 个分区。
**但代价是**：写入时每个分区是一个独立文件，分区多了写入并发压力成倍增长——本项目 31 个分区把写入压垮过一次。

**Q4：为什么 DWD 层要改成 ORC 格式？**
A：两个原因：
1. **列式存储**——查询只需读用到的列。做 `SELECT COUNT(DISTINCT user_id)` 时，ORC 只读 `user_id` 那一列的数据，而 TEXTFILE 要把整行都读出来
2. **压缩**——体积约是纯文本的 1/5~1/10。本项目实测：原始 1.3GB → 清洗后 620MB（**注意清洗还增加了字段、去除了重复，体积反而小了**）

而且它是后续做 Hive vs Spark 性能对比的**真实生产形态**——生产环境没人用 TEXTFILE 做数仓。

**Q5：你怎么知道元数据里的中文是"存储坏了"而不是"显示不出来"？**
A：取十六进制看原始字节：

```sql
SELECT HEX(COMMENT) FROM hive.COLUMNS_V2 WHERE ...;
-- 坏的情况：3F3F534B55  ← 3F 就是问号字符本身
-- 好的情况：E4B8ADE69687  ← 以 E4 开头，是合法的 UTF-8 中文
```

**如果十六进制里已经是 `3F`，说明数据在写进去的时候就被替换了，任何客户端都显示不出中文**——这时候去改客户端编码、改显示设置全是白费功夫。
这一条判断把排查方向从"编码显示问题"直接转到"存储层字符集问题"，是本项目那 12 个假设里最关键的一步。

**Q6：一条 INSERT 语句，你能预估它跑多久吗？**
A：**不能只看 SQL 长短。** 本项目那次：一条 INSERT 被 Hive 编译成 **11 个 MR 作业**，跑了 21 分钟；而一条校验用的 COUNT 只要 176 秒。
需要看的量是：**输入数据量 × 分区数 × 副本数**。
**教训**：不能用编译期报告的 `Total jobs` 估算——那次编译期只报 2 个作业，执行中膨胀到 11 个。

---

## 附：本章名词速查

| 名词 | 一句话解释 |
|---|---|
| **Hive** | 把 SQL 翻译成 MapReduce 的工具，自己不存数据 |
| **MetaStore** | 提供"表长什么样"接口的服务，底层是 MySQL |
| **HiveServer2** | 接收 SQL 请求的服务进程，监听 10000 端口 |
| **beeline** | Hive 的命令行客户端 |
| **元数据（metadata）** | 描述数据的数据：表名、字段、类型、注释、分区位置 |
| **分区（Partition）** | 按字段值把数据切成子目录，如 `dt=2019-10-01/` |
| **分区裁剪** | 查询只扫匹配的分区目录，不扫全表 |
| **动态分区** | 写入时根据数据内容自动创建分区（本项目一次写 31 个） |
| **TEXTFILE / ORC** | 纯文本存储 / 列式压缩存储 |
| **`.hive-staging_` 目录** | 写入过程中的临时目录，写完整体改名，保证不出现半成品 |
| **UDF** | 自定义函数，SQL 内置函数不够用时自己写 |
| **`--silent=true`** | 屏蔽 beeline 的正常输出（**但报错仍会输出到 stderr**） |

---

*上一章：`03_调度与计算_YARN与MapReduce.md` · 下一章：`05_协作_调度与展示.md`*
