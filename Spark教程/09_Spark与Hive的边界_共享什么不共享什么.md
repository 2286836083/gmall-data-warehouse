# 09 · Spark 和 Hive 的边界：共享什么、不共享什么

> 本篇解决一件事：**"Spark 接在 Hive 上"这句话，到底接在哪一层？**
>
> 与 01 篇的分工：01 讲「它们各站什么位置」（概念），本篇讲「这个关系在机器上怎么落地」（机制）。
> **01 里 `metastore` / `9083` 只出现 1 次，本篇补的就是这一块。**

---

## 一、先纠正一个最容易说错的说法

> ❌ "Spark 是 Hive 的执行引擎"

这句话**只在一种特定配置下成立**。三种关系必须分清：

| 关系 | 谁主导 | 怎么配 | 特征 |
|---|---|---|---|
| **① Hive on Spark** | **Hive** | `hive.execution.engine=spark` | 你敲 `hive` / `beeline` 提交 SQL，Hive 负责解析优化，只是把执行从 MR 换成 Spark |
| **② Spark SQL + Hive Metastore** | **Spark** | `spark.sql.catalogImplementation=hive`<br>+ `metastore.version` | 你敲 `spark-sql` / `spark-submit`，**Spark 自己解析自己执行**，只去 Metastore 查表结构 |
| **③ Spark 完全独立** | **Spark** | 什么都不配 | 直接读 CSV/Parquet 路径，用内存 catalog，和 Hive 毫无关系 |

**判断方法（一眼就能分辨）**：

```
你用什么命令提交？
  ├─ hive / beeline      → 关系 ①（Hive 主导）
  ├─ spark-sql / spark-submit 且能查 Hive 表 → 关系 ②（本项目）
  └─ spark-submit 且只读写文件路径           → 关系 ③
```

**本项目是 ②。** 证据是这三行配置（`spark-defaults.conf`）：

```
spark.sql.hive.metastore.version        3.1.3
spark.sql.hive.metastore.jars           path
spark.sql.hive.metastore.jars.path      file:///opt/hive/apache-hive-3.1.3-bin/lib/*
```

这三行的意思是：**"别用你 Spark 自带的 Hive 客户端，去用集群上那套 3.1.3 的。"**
如果是关系 ①（Hive on Spark），根本不需要这三行——因为那时是 Hive 在跑，不是 Spark 在跑。

---

## 二、共享边界：三层里只有两层是共用的

```
┌──────────────────────────────────────────────────────────┐
│  ① 计算 / 执行层           ← 不共享，各用各的               │
│     Hive: 编译成 MapReduce 作业                            │
│     Spark: 自己的 DAG 引擎（不产生 MR 作业）                │
├──────────────────────────────────────────────────────────┤
│  ② 元数据层                ← 共享                          │
│     Hive Metastore 服务（lrh-1:9083），背后是 MySQL         │
│     存：库/表/字段/分区/表对应哪个 HDFS 路径                │
├──────────────────────────────────────────────────────────┤
│  ③ 存储层                  ← 共享                          │
│     HDFS 上的同一批文件（ORC / 文本）                      │
└──────────────────────────────────────────────────────────┘
```

**这个设计解决了一个很实际的问题**：

> 换计算引擎时，**不用改表、不用搬数据、不用重写元数据**。
> 今天用 Hive 跑，明天想试 Spark，只要配上 Metastore 就能直接读同一张表。

这正是你能做"同 SQL 双引擎对比"的**前提条件**——如果两边数据不共享，A/B 对比根本无从谈起。

---

## 三、物理链路：一条 SQL 在机器上怎么走

这是本篇最核心的部分。**跟着一个字节走一遍。**

```
① 你敲 spark-submit（或 spark-sql）
        ↓
   在客户端/边缘节点起 【driver 进程】
        ↓
② driver 读 $SPARK_HOME/conf/hive-site.xml
   （这份是从 Hive 的 conf 目录 cp 过去的）
   → 从里面读到：Metastore 地址 = thrift://lrh-1:9083，MySQL 连接串
        ↓
③ driver 通过 【Thrift 协议】向 lrh-1:9083 发请求：
   "gmall 库的 ods_event_log_di，字段有哪些？分区有哪些？"
        ↓
④ Metastore 进程收到请求 → 去 【MySQL】 查元数据库
   （库名通常叫 hive，表名 DBS / TBLS / COLUMNS_V2 / PARTITIONS）
        ↓
   返回给 driver：
     字段：dt, event_time, event_type, product_id, ...
     存储：HDFS 路径 /user/hive/warehouse/gmall.db/ods_event_log_di/dt=2019-10-01
     格式：TEXTFILE（你的 ODS 是文本）
        ↓
⑤ ★ driver 拿着 HDFS 路径，直接去 HDFS 找 NameNode 问数据块位置
   —— 【Metastore 全程没碰过一个字节的数据】
        ↓
⑥ driver 生成 DAG，向 YARN 申请 executor 容器
   → YARN 分配容器 → executor 进程在 DataNode 上启动
        ↓
⑦ executor 直接从 HDFS 读数据块（就近读，尽量不跨网络）
   → 在自己的内存里算（这就是上一份文档讲的堆内存）
        ↓
⑧ 写回时：先写 HDFS（临时目录 → 改名），再更新 Metastore 的分区记录
```

### 3.1 每一步的硬件映射

| 步骤 | 涉及哪个进程 | 走什么 | 消耗什么 |
|---|---|---|---|
| ② 读 hive-site.xml | driver | 本机磁盘 | 一次小文件读，可忽略 |
| ③ 问 Metastore | driver ↔ Metastore | **网络**（TCP 9083）+ Thrift 序列化 | 毫秒级，只有元数据没有数据 |
| ④ 查 MySQL | Metastore ↔ MySQL | 本机/网络 TCP 3306 | 一次 SQL 查询 |
| ⑤ 问 NameNode | driver ↔ NameNode | 网络（TCP 8020） | 只拿块位置，很快 |
| ⑦ 读数据 | executor ↔ DataNode | **网络或本地磁盘**（就近） | 主要开销在这 |
| ⑦ 计算 | executor | CPU + **堆内存** | 见 `Java教程/01`、`03` |
| ⑧ 写 HDFS | executor ↔ DataNode | 网络（三副本要写三台） | 磁盘 IO × 3 |

### 3.2 一个必须记住的类比（要有物理事实支撑）

> **Metastore 之于数据，就像 NameNode 之于数据块。**

两者都是"**目录服务**"：
- NameNode 知道"文件被切成哪些块、每块在哪台机器"，**但不存块本身**
- Metastore 知道"表有哪些字段、数据在哪个 HDFS 路径"，**但不存数据本身**

**这就是为什么**：`show tables` 很快（只问元数据），而 `select count(*)` 很慢（要读 1.3 GB 数据）。
**快慢的分界线，就是"碰不碰数据"这条线。**

---

## 四、那三行配置逐行拆解

### 4.1 `metastore.version 3.1.3`

告诉 Spark：**"你要连的这个 Metastore 服务，是 3.1.3 版的。"**

### 4.2 `metastore.jars path`

告诉 Spark：**"客户端实现不要从别处拿，用我指定的路径下的 jar。"**

可选值有三种：

| 值 | 含义 | 问题 |
|---|---|---|
| `builtin` | 用 Spark 打包时内置的 Hive 客户端 | **版本被锁死**（Spark 3.5.9 内置 2.3.9）|
| `maven` | 运行时从 Maven 仓库下载指定版本的 jar | 要能连外网；每次启动可能要校验下载 |
| **`path`** | **用你给的目录下的 jar** | 需要你保证这个目录就是集群在用的那套 |

### 4.3 `metastore.jars.path file:///opt/hive/apache-hive-3.1.3-bin/lib/*`

具体指向集群上 Hive 自己的 lib 目录。

**为什么选 `path` 而不是 `maven`**：
- `path` 用的是**集群上正在跑的那套 jar**，和服务端**物理上就是同一批文件** → 版本必然对齐
- `maven` 下的是"名义上同版本"的包，理论上也行，但在内网、镜像不全、依赖传递出错时会很麻烦

> **规律（可以推广到所有跨组件集成）**：
> **版本不一致时，优先"借对方的客户端"，而不是"自己照版本号下一个"。**

### 4.4 ⚠️ 一个必须诚实标注的地方

项目文档里给的理由是：**Spark 3.5.9 内置的 Hive 客户端是 2.3.9，而集群 Metastore 服务是 3.1.3，版本不匹配。**

但要说清楚：**这是"设计依据"，不是"实测的报错现场"。**

本项目是**先配好再跑**的——`metastore.version` 一开始就写对了，
所以**没有留下"不配会报什么错"的实测记录**。

> 按项目纪律：**没实测过的现象，不能当成"我知道会这样"来讲。**
> 面试被问"不配会怎样"，正确答法是：
> "我是按版本必须对齐来设计的，配之前没有留不匹配的现场；
> 从机制上推断会连不上或元数据解析错，但**这个现象我没有实测验证**。"

---

## 五、catalog：Spark 眼里的"表"从哪来

```
spark.sql.catalogImplementation = hive     ← 本项目用的
spark.sql.catalogImplementation = in-memory ← Spark 默认（纯文件，不接 Hive）
```

**catalog 是什么**：Spark 用来"找表"的一层抽象。相当于"目录的目录"。

| catalog | 作用 |
|---|---|
| `spark_catalog` | 默认 catalog，在 hive 模式下它就是 Hive Metastore 的入口 |
| `hive` | 显式指定用 Hive Metastore |

**表名解析顺序**：`catalog.database.table`

```sql
gmall.ods_event_log_di          -- 简写：默认 catalog + 库 + 表
spark_catalog.gmall.ods...      -- 全写
```

> 这解释了一个常见困惑：**为什么有时建的表"找不到"？**
> 因为可能建在了 `in-memory` catalog 里，而查询走的是 `hive` catalog——
> **两个 catalog 是两套命名空间，互相看不见。**

---

## 六、写的时候发生了什么（分区元数据）

你在项目里踩过的那条坑，本质就在这里：

```
INSERT OVERWRITE TABLE t PARTITION(dt) SELECT ...
```

| 模式 | 行为 |
|---|---|
| **static（默认）** | 不指定 `PARTITION` 子句时，**先清空整个表**再写 → 危险 |
| **dynamic** | 按 `SELECT` 里最后一列的值动态决定写哪个分区 → 只覆盖对应分区 |

所以项目里必须显式：

```
spark.sql.sources.partitionOverwriteMode = dynamic
```

### 6.1 关键：写完之后元数据怎么更新

```
① executor 把数据写成 HDFS 文件
   /user/hive/warehouse/gmall_bench.db/dws_user_event_day/dt=2019-10-01/part-00000-xxx
        ↓
② Spark 把"新增了哪些分区、每个分区的路径"登记到 Metastore
        ↓
③ 此后无论 Hive 还是 Spark 去查，都能看到这个分区
   —— 因为【元数据只有一份】
```

> 注意：这条是 Spark **主动**去登记。如果是**你手动 `hdfs dfs -put` 往分区目录里丢文件**，
> Metastore 就不知道 → 这时才需要 `MSCK REPAIR TABLE` 去补。
> **"绕过 Metastore 的操作，元数据就不会自动更新"**——这是所有这类系统的通病。

---

## 七、为什么"两边数据一致"是天然成立的

你报告里有这句：**Hive 与 Spark 8 张表行数逐项一致、GMV 逐位一致、分区 31=31。**

**物理原因**：两侧读的是**同一份 HDFS 文件**，写的是**同一份元数据**。
**根本不存在"两份数据"**，所以不存在"同步"问题。

这意味着你的对账**验证的不是数据同步，而是计算逻辑**：

| 对账验证了什么 | 没验证什么 |
|---|---|
| ✅ 同一份 SQL 在两种引擎下语义一致 | ❌ 数据有没有被正确加载（那是 ODS 校验的事）|
| ✅ 聚合、去重、窗口函数的边界行为一致 | ❌ 业务口径对不对（那是业务问题）|
| ✅ Spark 写分区没写错（31 个分区都在）| |

> 面试如果问"你怎么保证 Spark 算得对"，答"因为我跟 Hive 跑了同一份 SQL 做逐项对账，
> 而且两边读的是同一份文件，所以差异只可能来自引擎语义"——**这个答法比"我看了结果觉得对"强得多。**

---

## 八、能不能不要 Hive？

这是很多人没想过的问题。先把 Hive 拆开：

| Hive 的组成 | 端口 | 能不能不要 |
|---|---|---|
| **HiveServer2**（SQL 入口，JDBC/ODBC） | 10000 | ✅ 能，用 `spark-sql` / `spark-submit` 代替 |
| **Metastore**（元数据服务） | 9083 | ⚠️ **几乎不能**——它是"表"这个概念的唯一真相源 |
| **执行引擎**（默认 MR） | —— | ✅ 能，换成 Spark / Tez |

**生产上非常常见的一种架构**：

```
只用 Spark（或 Trino / Flink 做查询）
+ 一个独立的 Hive Metastore 服务（不启动 MR，不启动 HiveServer2）
```

也就是说：**Hive 的价值从"计算"退化成了"元数据标准"。**
这不是 Hive 落后了，而是它在这一层的地位太稳——整个生态（Spark、Flink、Trino、Iceberg、Hudi）
都认 Hive Metastore 这套接口。

### 8.1 所以你项目里的 Hive 到底在提供什么

**三个作用，没有一个是"跑计算"**：

1. **提供 Metastore** —— Spark 要靠它才能"看见表"
2. **提供建表规范** —— DDL、分区、ORC 格式、字段注释（你还专门修过中文注释乱码）
3. **提供对照基线** —— Hive 跑一遍的结果，用来验证 Spark 算得对不对

> 面试被问"你为什么还要 Hive，直接 Spark 不行吗"，
> 上面这三条就是答案，而不是"因为教程这么教的"。

---

## 九、可跑验证（需要集群在线）

> ⚠️ 以下命令要在集群上执行，本机没有 Hive 环境。

### 9.1 确认 Metastore 真的在 9083

```bash
ss -lnt | grep 9083
# 期望：LISTEN 0 ... 0.0.0.0:9083 或 *:9083
```

### 9.2 确认 Spark 读的是同一份元数据

```bash
# Hive 侧看表数
beeline -u jdbc:hive2://localhost:10000 -e "show tables in gmall;" --silent=true

# Spark 侧看表数（同一个库）
spark-sql -e "show tables in gmall;"
```

**看什么**：两边**表名和数量必须一致**。不一致 → 说明其中一边连的不是同一个 Metastore。

### 9.3 确认 Metastore 背后是 MySQL

```bash
mysql -uhive -phive -e "select count(*) from metastore.TBLS;" 2>/dev/null
```

**看什么**：返回的表数量应该和 9.2 里的数量对得上。
**这一条最有说服力**——它直接证明"元数据的真相源在 MySQL 里，不在 Spark 也不在 Hive 进程里"。

### 9.4 确认 Spark 的配置

```bash
grep -E "metastore|warehouse" /opt/spark-3.5.9-bin-hadoop3/conf/spark-defaults.conf
ls -la /opt/spark-3.5.9-bin-hadoop3/conf/hive-site.xml
```

**看什么**：三行 metastore 配置都在，且 `hive-site.xml` 确实被拷过来了。

---

## 十、面试官会怎么问

| 问题 | 该怎么答 |
|---|---|
| Spark 和 Hive 什么关系？ | **共享元数据与数据文件，不共享计算引擎**。"Spark 是 Hive 的引擎"只在 Hive on Spark 配置下成立 |
| Spark 为什么能直接查 Hive 表？ | 通过 Metastore 拿表结构和 HDFS 路径，自己读文件自己算 |
| Metastore 是什么？存在哪？ | 独立进程（9083），背后是 MySQL，只存元数据不存数据 |
| 为什么要配 `metastore.version` 和 `jars=path`？ | 内置客户端版本与服务端不一致；用 path 借集群那套 jar 保证版本物理一致 |
| 那 Spark 写表，Hive 能看到吗？ | 能。写的时候会更新 Metastore 分区信息，**元数据只有一份** |
| 为什么有时建完表查不到？ | catalog 不一致（`in-memory` vs `hive` 是两套命名空间） |
| **能不能不要 Hive？** | HiveServer2 可以不要，**Metastore 几乎不能**——它是"表"的唯一真相源 |
| 你怎么保证 Spark 算得对？ | 同 SQL 与 Hive 逐项对账；且两边读同一份文件，**差异只可能来自引擎语义** |

---

## 十一、术语卡

| 术语 | 一句话 |
|---|---|
| **Metastore** | 元数据服务，存"表长什么样、数据在哪"，不存数据本身 |
| **Thrift** | 一种跨语言 RPC 协议，Spark 用它和 Metastore 通信 |
| **catalog** | Spark 找表的抽象层；`hive` 模式下指向 Metastore |
| **HiveServer2** | Hive 的 SQL 入口（10000），提供 JDBC |
| **Hive on Spark** | Hive 主导、把执行换成 Spark 的配置方式（**不是本项目**） |
| **`MSCK REPAIR TABLE`** | 绕过 Metastore 直接放文件后，用它补分区元数据 |
| **`partitionOverwriteMode`** | Spark 写分区表的模式，**must be dynamic**，否则先清空整表 |

---

## 十二、自测（能答出来才算真懂）

1. **"Spark 是 Hive 的执行引擎"这句话为什么不对？** 什么配置下它才对？
2. **`show tables` 很快、`select count(*)` 很慢**——用本篇的机制解释这个差别。
3. **我把 Spark 的表写完了，Hive 里 `show partitions` 能看到吗？** 为什么？
4. **如果我把 Hive 的 `hive-server2` 停掉，Spark 还能查表吗？** 为什么？
   （提示：想清楚 Metastore 和 HiveServer2 是两个不同的进程）

---

## 下一篇

- 想跑起来 → `03_怎么装_本项目的真实部署过程.md`
- 想知道"该用哪个引擎、什么时候用" → `05_该用哪个_Hive与Spark怎么选.md`
- 想看"这些结论的原始数据" → `实验分析/实验10_Spark双引擎对比.md`
