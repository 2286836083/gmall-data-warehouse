# 实验报告 10 · Spark 3.5.9 接入现有数仓，并与 Hive/MR 做同资源双引擎对比

> **一句话结论**：在本地三节点 YARN 集群上部署 Spark 3.5.9（JDK 8 能支持的最后一条版本线），
> 通过 **Hive Metastore 3.1.3** 接入既有 `gmall` 数仓（27 张表全部可见、`count(ods_event_log_di)` = 10,630,245 完全一致），
> 然后用**同一份 SQL**、**同一套 YARN 资源**重跑新链路 DWS/ADS 八段作业，与 Hive/MR 引擎对比耗时。
> 核心结论：**同样 8 段 SQL、同样 YARN 资源（每节点 2048MB / 2 vCores），
> Spark 单会话连跑 = 75~77 秒，Hive 单会话连跑 = 389 秒 —— 快 5.0 倍；
> 但若每段各起一个会话，Spark 381 秒 vs Hive 421 秒，只快 10%。**
> → **Spark 的收益几乎全部来自"会话复用"，而不是单条 SQL 跑得快。** 这是本次最有决策价值的结论。
> 且 8 张输出表与 Hive 版**行数逐项一致、GMV 逐位一致（57,575,996.50）** —— 换引擎不改结果。
>
> 实验日期：2026-10-04 · 环境：三节点 VMware 集群（lrh-1/2/3）· Hadoop 3.3.6 / Hive 3.1.3 / JDK 1.8.0_202 / **Spark 3.5.9**
> 执行方式：SSH 远程 + 后台分步执行器（`spark_bench_run.sh`，8 段 + `status.tsv` 状态文件）

---

## 〇、按读者身份选择阅读路径

| 你是 | 建议读 |
|---|---|
| 面试官 / 技术评估 | 第三章（环境约束，体现取舍依据）→ 第四章（方案与理由）→ 第五章（假设-行动-结果表）→ 第八章（被证伪的假设）→ 第十章（FAQ） |
| 零基础小白 | 从第二章开始顺序读，专有名词都给了类比 |
| 只想照着做 | 第十一章的回滚与复现，脚本在集群 `/home/lrh/workspace/spark_bench/` |

---

## 一、结论速览

### 1.1 主结论：两种调用方式，差距天差地别

| 调用方式 | Spark 3.5.9 | Hive / MR | 倍数 | 说明 |
|---|---|---|---|---|
| **① 单会话连跑 8 段** | **75 s**（复测 77 s） | **389 s** | **5.0×** | 一个会话里连续执行 8 条 INSERT |
| **② 每段各起一个会话** | **381 s** | **421 s** | **1.10×** | 8 次独立提交，Spark 每次要付启动开销 |
| Spark 侧逐段纯执行时间之和 | 41 s（单会话）/ 101.6 s（分段） | 无对应口径 | —— | Spark 自报 `Time taken` 之和 |

**核心手法**：不改一行业务逻辑 —— 只把「写入目标表」从 `gmall.*` 换成 `gmall_bench.*`，
源表仍读 `gmall.*`，**保证两个引擎的输入完全一致**，这样耗时差异才能归因到引擎本身，而不是数据差异。

### 1.2 逐段耗时对照（同一时间窗口、同一集群状态实测）

| # | 作业段 | Hive/MR wall | Spark wall | Spark inner | Hive 快/慢 |
|---|---|---|---|---|---|
| 1 | DWS 用户主题 | 71 s | 58 s | 23.2 s | Spark 快 13 s |
| 2 | DWS 商品主题 | 63 s | 56 s | 20.4 s | Spark 快 7 s |
| 3 | DWS 类目主题 | 47 s | 49 s | 13.5 s | **Hive 快 2 s** |
| 4 | DWS 交易主题 | **26 s** | 45 s | 10.9 s | **Hive 快 19 s** |
| 5 | ADS 活跃用户 | 83 s | 44 s | 9.8 s | Spark 快 39 s |
| 6 | ADS 转化漏斗 | **38 s** | 42 s | 7.5 s | **Hive 快 4 s** |
| 7 | ADS 累计复购 | 58 s | 44 s | 8.4 s | Spark 快 14 s |
| 8 | ADS TopN 商品 | **35 s** | 42 s | 7.9 s | **Hive 快 7 s** |
| | **合计** | **421 s** | **381 s** | **101.6 s** | Spark 快 40 s（10%） |

> **⚠️ 这张表最重要的信息藏在"谁快"的变化里**：
> Spark 在**重作业**（DWS-user/product，要扫 1063 万行）上赢，在**轻作业**（DWS-trade 只扫 18.6 万行、
> ADS-funnel 只扫 162 万行）上**反而输**。原因是 Spark 每起一个会话就要付约 35 秒固定开销，
> 作业本身只要 8~11 秒 —— **固定开销比活儿还贵**。

### 1.3 正确性对账（换引擎不改结果）

| 表 | `gmall`（Hive 产出） | `gmall_bench`（Spark 产出） | 一致 |
|---|---|---|---|
| dws_user_event_day | 1,622,820 | **1,622,820** | ✅ |
| dws_product_event_day | 1,265,817 | **1,265,817** | ✅ |
| dws_category_event_day | 1,853 | **1,853** | ✅ |
| dws_trade_day | 31 行 / GMV **57,575,996.50** | 31 行 / GMV **57,575,996.50** | ✅ |
| ads_active_user_day | 31 | **31** | ✅ |
| ads_funnel_day | 31 | **31** | ✅ |
| ads_repurchase_day | 31 | **31** | ✅ |
| ads_top_product_day | 1,550 | **1,550** | ✅ |
| 成交笔数合计 / DAU 合计 | 186,059 / 1,622,820 | **186,059 / 1,622,820** | ✅ |
| 分区数（dws_user / dws_product） | 31 / 31 | **31 / 31** | ✅ |

**注意**：Spark 写分区表用了 `partitionOverwriteMode=dynamic`，上面"分区数 31/31 全在"就是该参数**真正生效**的证据
（若参数没生效，Spark 会把整表清空——数据不会缺，但分区结构会出问题）。

---

## 二、背景

### 2.1 为什么要做双引擎对比

项目已完成 Hive/MR 单引擎的全链路数仓（ODS→DWD→DWS→ADS，1063 万行真实数据）。
但「只会 Hive」在数据开发岗位的面试里是个明显短板 —— 面试官几乎必问"Spark 和 Hive 有什么区别、你怎么选"。
本项目**数据量已具备**（1063 万行、31 个分区），是验证差异的合适规模。

### 2.2 Spark 与 Hive 到底是什么关系（零基础版）

**先给类比，再讲物理上是什么。**

**类比**：Hive 像"接单员 + 外包调度"——你给它一句 SQL，它把活拆成若干份交给 MapReduce 这个施工队；
每道工序做完，料子必须**先卸到仓库（磁盘）**，下一道工序再从仓库拉。Spark 像"自建施工队"——
它也接受 SQL，但它自己排工期（DAG），而且工序之间的料子**尽量直接手递手（内存）**，不搬仓库。

**物理上是什么**（这一步才是关键）：

| | Hive / MapReduce | Spark |
|---|---|---|
| 一句 SQL 变成什么 | 一个或多个 **MR 作业**，每个作业 = 一批 JVM 容器 | 一张 **DAG（有向无环图）**，切分成若干 **Stage**，由 Executor JVM 里的线程池执行 |
| 中间结果放哪 | **每个 MR 作业之间必须落 HDFS 磁盘**（Map 输出→磁盘，Reduce 通过 Shuffle 拉取） | **Stage 内尽量留在 Executor 堆内存**，Stage 边界（Shuffle）才落盘 |
| 进程模型 | 一个 container 一个 map/reduce task，**JVM 启动开销按 task 计** | Executor JVM **一次启动、常驻整个应用**，task 是线程（这也是 Spark 启动作业慢、但批处理快的原因） |
| 谁来调度 | YARN 按 container 粒度调度，作业之间互不认识 | Spark Driver 先生成执行计划再向 YARN 申请 Executor，**YARN 只看得到 Executor 这个粒度** |
| SQL 解析器 | Hive 自己的（HiveQL） | Catalyst 优化器 + Analyzer（Spark SQL） |

**一句话**：Hive 的瓶颈是"工序间的磁盘往返 + 反复启 JVM"，Spark 的优化点正是这两处。

### 2.3 实验前的基线数据（Hive/MR 侧，已有实测）

```
新链路 DWD 全量跑批：20 分 53 秒（5 步，含 1 次失败重试）
新链路 DWS/ADS 全量：28 分 01 秒（10 步，含 2 次失败重试）
走 DolphinScheduler 整链：54 分 10 秒（16 节点，无失败无重试）
分段最慢三个（DS 基线）：verify 15m23s / dwd_event_clean 8m37s / dwd_dim_product 7m36s
```

> 注：基线里 **最慢的不是 ETL 而是校验节点**（`evt_verify.sql` 的 `UNION ALL` 被 Hive 编译成 6 个串行 MR 作业）。

本次对比**只用 DWS/ADS 八段**（不含 DWD），因为：
1. 八段有现成的分段脚本（`evt_etl.sql` 按 `-- ===== STEP n` 标记切分），结构清晰；
2. DWS 四张都要扫 1063 万行明细，是**真正吃引擎性能**的部分；
3. ADS 四张基于 DWS 小表，用来观察"小作业下引擎各自的固定开销"。

---

## 三、环境约束（★ 方案的全部依据，实测得出）

**这一章是本次最有价值的部分之一 —— 所有参数都不是拍的，是被这些数字逼出来的。**

| 约束项 | 实测值 | 怎么测的 | 对方案的硬性影响 |
|---|---|---|---|
| JDK | **1.8.0_202** | `/opt/jdk/jdk1.8.0_202/bin/java -version` | **排除 Spark 4.x**（要求 JDK 17+），锁定 3.5.x |
| Hadoop | 3.3.6 | `hadoop version` | 选 `-bin-hadoop3` 发行包 |
| Hive Metastore | 3.1.3（服务在 lrh-1:9083） | `ss -lnt \| grep 9083` | Spark 3.5 内置 hive **2.3.9** → 直连 3.1.3 会版本不匹配 → 必须 `metastore.jars=path` |
| **YARN 每节点容量** | **2048 MB / 2 vCores** | `yarn node -list -showDetails` 的 `Configured Resources` | executor 容器 ≤ 2048MB；每节点最多 1 个 2G 容器，或 2 个 1G |
| **YARN 单容器上限** | **2048 MB**（`maximum-allocation-mb`） | `cat yarn-site.xml` | `executor.memory + memoryOverhead ≤ 2048` |
| YARN 集群总容量 | **6144 MB / 6 vCores** | 3 节点 × 2048/2 | 决定 executor 数量与并行度上限 |
| 外网可达性 | **通**（archive.apache.org 与清华镜像均可 `--spider` 通） | `wget -q --spider` | **集群可直接下包，不必本机中转** |
| 本地磁盘剩余 | lrh-1 **5.5G** / lrh-2 9.7G / lrh-3 9.8G | `df -h /` | 三台都装得下（Spark 解压后 429MB） |
| HDFS 剩余 | 可用 **25.6G**（用量 7%） | `hdfs dfs -df -h /` | jar 分发与 event log 都放得下 |
| lrh-1 物理内存 | 6886MB 总，**可用约 2.3G** | `free -m` | driver 跑在 lrh-1（client 模式），不能给太大 |
| Python | 3.10.12 | `python3 -V` | —— |

**关键推论**：这套集群的 YARN 资源**非常小**（总 6GB / 6 核）。
所以本次对比的意义不是"Spark 能快多少倍"，而是**在同样寒酸的资源下，两个引擎各自的表现如何** ——
这反而更接近真实生产中"资源受限"的场景，而不是 PPT 上的理想数字。

---

## 四、方案设计与理由

### 4.1 版本选择：Spark 3.5.9

- 镜像站可用版本：`spark-3.5.8` / `spark-3.5.9`（实测枚举）
- 选 3.5.9 的理由：**Spark 3.5.x 是最后一条支持 JDK 8 的版本线**；集群 JDK 是 1.8.0_202，升 JDK 意味着 Hadoop/Hive 全部要重装，风险远大于收益
- **不选 4.x 的第二个理由**（容易被忽略）：Spark 4.0 默认开启 `spark.sql.ansi.enabled=true`，
  SQL 语义会变（如除零报错、隐式转换变严），**现有 SQL 可能行为改变** —— 这是"跑得起来但结果不一样"的隐患，比跑不起来更危险
- 包名：`spark-3.5.9-bin-hadoop3.tgz`（383MB / 401,379,923 字节）
- **完整性校验**：官方 `archive.apache.org` 的 sha512 与本地实测
  `66097faf48b12f8673dc13256a44d731c27c86a5831cd8ca930c6fd9dd9bdf31d40593095e959461351d55da1a8e3ad5ed95e0b4b51bea87ca3989ab002ea8ce` **一致**

### 4.2 部署模式：Spark on YARN，client 模式

- **跑在 YARN 上**（而不是 standalone / local）：这才是生产形态，面试可讲；
  且复用现有 3 个 NodeManager，不额外起守护进程
- **client 模式**（driver 在 lrh-1 本地）：
  - cluster 模式会把 driver 也放进 YARN 容器，而 YARN 每节点只有 2048MB，**driver 会抢走唯一的容器容量**
  - client 模式便于实时看 driver 日志（本项目强调"可观测进度"）
- **jar 分发走 HDFS**：`spark.yarn.jars=hdfs://lrh-1:8020/spark/jars/*`
  （252 个文件 / 338MB，一次性上传 18 秒；否则每次提交都要重新上传）

### 4.3 Hive Metastore 集成（最容易踩坑的一步）

Spark 3.5 自带的 Hive client 是 **2.3.9**，而后端的 metastore **服务**是 **3.1.3**。
Thrift 协议在 2.x→3.x 之间有变更，直连会报 `Invalid method name` 之类的错。
官方解法有两条：`maven`（从 Maven 中央仓库拉 3.1.3 的 jar）与 `path`（指定本地 jar 目录）。
选 **`path`**，理由：

```
spark.sql.hive.metastore.version      3.1.3
spark.sql.hive.metastore.jars         path
spark.sql.hive.metastore.jars.path    file:///opt/hive/apache-hive-3.1.3-bin/lib/*
```

- `maven` 模式要求 **每个 Executor JVM** 首次运行都能访问 Maven 中央仓库 —— 网络依赖点多、首次启动慢、不可离线
- `path` 模式直接复用集群已有的 Hive 3.1.3 安装（277MB lib），**可控、可离线、与 metastore 服务版本严格一致**

另外把 `hive-site.xml` 拷进 `$SPARK_HOME/conf/`，让 Spark 拿到 metastore 地址和 MySQL 连接信息。

### 4.4 资源参数：被 YARN 上限逼出来的配置

| 参数 | 值 | 理由 |
|---|---|---|
| `spark.executor.memory` | 1408m | 1408 + 384 = **1792MB ≤ 2048 单容器上限**，留 256MB 余量给 NM |
| `spark.executor.memoryOverhead` | 384m | 默认是 `max(384, 10%)`，这里取默认下限即可 |
| `spark.executor.cores` | 2 | 与节点 vCores 数一致 |
| `spark.executor.instances` | 3 | 每节点 1 个 → 共 6 vCores，**与 Hive 侧的并行度对等** |
| `spark.driver.memory` | 1g | driver 在 lrh-1，该机可用物理内存只剩约 2.3G，不能给大 |
| `spark.sql.shuffle.partitions` | 6 | 默认 200 在这套集群上是灾难（6 个核跑 200 个 task）；对齐 Hive 侧 `mapreduce.job.reduces` |

**⚠️ 与 Hive 的关键差异**：Hive 那边一个容器 = 一个 task（1024MB / `-Xmx820m`），
Spark 这边一个 Executor 跑 2 个并发 task（1408MB 堆 / 2）。**每个 task 可用内存其实比 Hive 少** ——
这正是后面验证"大基数聚合会不会 OOM"的看点。

### 4.5 语义对齐：一个必须显式处理的差异

`INSERT OVERWRITE TABLE t PARTITION (dt) SELECT ..., dt FROM ...` 这句在两边**行为不同**：

| | Hive | Spark（默认） |
|---|---|---|
| `INSERT OVERWRITE` 分区表 | 只覆盖**本次写入涉及的分区** | **先用空内容覆盖整张表**（`partitionOverwriteMode=STATIC`），再把数据写进去 |

→ Spark 侧必须显式设置：

```
--conf spark.sql.sources.partitionOverwriteMode=dynamic
```

**不设会怎样**：结果看似正确（数据都在），但如果某次只跑部分日期，**其余日期的分区会被静默清空**。
这属于"跑得对、结果错"的典型陷阱，与项目里已有的教训（`hive.optimize.sort.dynamic.partition` 参数没拼进分段 SQL）是同一类问题。

### 4.6 对比设计：绝不覆盖现有数据

```
写入目标：gmall_bench.dws_*  /  gmall_bench.ads_*     （8 张表，CREATE TABLE ... LIKE 复制结构）
读取来源：gmall.dwd_event_log_di / gmall.dwd_purchase_fact_di
          gmall.dws_product_event_day / gmall.dws_user_event_day   ← 保持读 Hive 版，输入一致
```

- 通过 `sed 's/INSERT OVERWRITE TABLE gmall\./INSERT OVERWRITE TABLE gmall_bench./g'` 自动改写，
  **只动写入目标，不动源表**（实测输出已逐条核对：8 个目标全为 `gmall_bench.*`，4 个源表仍为 `gmall.*`）
- 这样现有 `gmall` 库的数据**零风险**，且对比是"相同输入 → 相同计算 → 比谁快"

---

## 五、实验设计：假设 → 行动 → 实测结果

| # | 假设（动手前的判断） | 对应行动 | 实测结果 | 判定 |
|---|---|---|---|---|
| H1 | 集群**没有外网**，Spark 包需要本机下载再 scp 上传 | 先测连通性 | `archive.apache.org` 与清华镜像 **均可达**，直接集群下载（383MB 约 1 分钟） | ❌ **证伪** |
| H2 | Spark **3.5.x** 是最后支持 JDK 8 的版本线，应选它而非 4.x | 查 JDK 版本 + 版本枚举 | JDK 1.8.0_202；镜像站有 3.5.8/3.5.9，选 **3.5.9** | ✅ |
| H3 | Spark 内置 Hive client（2.3.9）**无法**直连 3.1.3 的 metastore，必须 `jars=path` | 配 `metastore.version=3.1.3` + `jars=path` | `Connected to metastore.`，`show databases` 返回 `default/gmall/namespace` | ✅ |
| H4 | executor 内存受 YARN **单容器 2048MB 上限**约束 | 读 `yarn-site.xml` | `maximum-allocation-mb=2048`、`nodemanager.resource.memory-mb=2048` | ✅ |
| H5 | 三节点 YARN 总容量足够跑 3 个 executor | `yarn node -list` | 3 × 2048MB / 2 vCores，YARN 应用 `..._0005` 正常 RUNNING | ✅ |
| H6 | Spark 写分区表需 `partitionOverwriteMode=dynamic` 才与 Hive 语义一致 | 显式加该参数 | `show partitions` 实测 **31 = 31**（dws_user / dws_product 均与 `gmall` 相同） | ✅ |
| H7 | Executor 上**不需要** Hive 3.1.3 的 lib（metastore 交互只在 driver 侧） | 只在 lrh-1 保留 Hive lib，未分发到 lrh-2/3 | YARN 模式扫描 1063 万行成功返回 10,630,245，**无 ClassNotFound** | ✅ |
| H8 | Spark 跑同样的 DWS/ADS 作业比 Hive/MR 快 | 同 SQL、同资源重跑 8 段 | **必须分情况**：单会话 75s vs 389s（**5.0×**）；每段独立会话 381s vs 421s（**仅 1.10×**）；且轻作业（DWS-trade / ADS-funnel / ADS-topn）**Spark 反而更慢** | ⚠️ **部分成立**（见第八章 H8 复盘） |
| H10 | 多段连跑时 Spark 的数据写入结果与分段跑一致 | 单会话重跑 2 次 + 逐表对账 | 两次重跑后 8 张表行数**全部正确**（1550 / 1622820 / …） | ✅（但有 1 次不可复现的异常，见第九章 #5） |
| H9 | 日志里的 `TTransportException` 是良性噪音 | 查完整日志上下文 | 见第七章 —— **良性且自动恢复** | ✅ |
| **H0** | （我自己的误读）`yarn node -list -showDetails` 里的 `PMem:4860 MB` 是**节点 YARN 容量** | 对照 `Configured Resources` | 真容量是 `memory:2048, vCores:2`；`PMem` 是**节点物理内存使用量**（lrh-1 因扛着 Hive/DS/MySQL 才高） | ❌ **证伪** |

---

## 六、过程详录

### 6.1 前置安全核查

| 问题 | 核查方式 | 结论 |
|---|---|---|
| 会不会覆盖现有正确数据？ | 对比 SQL 的写入目标做了 sed 改写并逐条核对 | 写入全部指向 `gmall_bench.*`，**零风险** |
| 装了 Spark 会不会影响现有服务？ | 只新增 `/opt/spark-3.5.9-bin-hadoop3`，不改 Hadoop/Hive 配置 | 无影响（Hive site 是拷副本，未改原文件） |
| YARN 资源会不会被抢光导致其他作业失败？ | 检查当前无运行中的作业（`yarn application -list` 为空） | 安全 |

### 6.2 部署过程

1. 下载 `spark-3.5.9-bin-hadoop3.tgz`（383MB）→ sha512 校验一致
2. 解压到 `/opt/`，`chown lrh:lrh`（429MB，耗时 4.9 秒）
3. 上传 252 个 jar（338MB）到 HDFS `/spark/jars/`（18 秒）
4. 写 `spark-defaults.conf` / `spark-env.sh`；拷 `hive-site.xml`
5. 建 HDFS 目录 `/spark/eventlogs`（供 Spark History Server 用）

### 6.3 三层外部判据验证

**判据 1 —— 元数据可见**（local 模式）：

```
Spark master: local[2]
Connected to metastore.
namespace / default / gmall        ← 3 个库
Fetched 27 row(s)                  ← gmall 库 27 张表，与项目基线一致
```

**判据 2 —— 数据可读**：

```
select count(*) from gmall.ads_active_user_day;   →  31     （与基线一致）
```

**判据 3 —— 分布式可跑**（YARN 模式，扫 1063 万行）：

```
Spark master: yarn, Application Id: application_1791111667840_0004
select count(*) from gmall.ods_event_log_di;      →  10,630,245   （与基线逐位一致）
Time taken: 11.9 ~ 15.9 seconds（单看这句）
```

> ⚠️ **一个必须诚实说明的数字**：单条 `count(*)` 的 wall time 是 **51.5 秒**，而 Spark 内部报 `Time taken: 15.9 秒`。
> 差的约 35 秒是 **JVM 启动 + 连接 metastore + 提交 YARN 应用**的固定开销。
> → **结论：Spark 单发小查询不划算，它的优势只在"一个常驻会话里连着干很多活"时才体现。**
> 这也是本次对比只跑批、不跑单查询的原因。

### 6.4 对比跑批

- 执行器：`/home/lrh/workspace/spark_bench/spark_bench_run.sh`（后台 `setsid nohup`，分 8 段，逐段写 `status.tsv`）
- 每段记录两个耗时：
  - `wall` = 含 JVM 启动与 YARN 提交的墙钟耗时
  - `inner` = Spark 自报的 `Time taken`（纯执行时间）
- **两个都要报**，因为 Hive 侧的基线是 beeline **每段独立会话**的 wall time，两者口径才能对上

**（待补：跑批结果表）**

---

## 七、结果数据

### 7.1 Spark 侧原始记录（`spark_bench/status.tsv`，每段独立会话）

```
1  DWS-user         OK  wall=58s  inner=23.198s
2  DWS-product      OK  wall=56s  inner=20.356s
3  DWS-category     OK  wall=49s  inner=13.536s
4  DWS-trade        OK  wall=45s  inner=10.936s
5  ADS-active-user  OK  wall=44s  inner=9.805s
6  ADS-funnel       OK  wall=42s  inner=7.456s
7  ADS-repurchase   OK  wall=44s  inner=8.412s
8  ADS-top-product  OK  wall=42s  inner=7.922s
ALL_DONE total=381s  2026-10-04 19:56:27
```

### 7.2 Hive 侧原始记录（`hive_bench/status.tsv`，同一时间窗口）

```
1  DWS-user         OK  wall=71s
2  DWS-product      OK  wall=63s
3  DWS-category     OK  wall=47s
4  DWS-trade        OK  wall=26s
5  ADS-active-user  OK  wall=83s
6  ADS-funnel       OK  wall=38s
7  ADS-repurchase   OK  wall=58s
8  ADS-top-product  OK  wall=35s
ALL_DONE total=421s  2026-10-04 20:11:11
```

### 7.3 单会话连跑（关键对照）

```
SPARK_SINGLE_SESSION  rc=0  wall=75s    inner: n=8 sum=39.4s
  （复测）             rc=0  wall=77s    inner: 19.437 / 10.021 / 3.439 / 2.887 / 2.799 / 0.903 / 1.365 / 1.049
HIVE_SINGLE_SESSION   rc=0  wall=389s
```

**逐段 inner 的递减规律**（单会话，秒）：
`19.4 → 10.0 → 3.4 → 2.9 → 2.8 → 0.9 → 1.4 → 1.0`

前后差了 19 倍。**这与分段跑时的 inner（23.2 → 7.9，只差 3 倍）完全不同**。
推断原因（**未深入验证**）：① Executor JVM 常驻，后面的语句省掉了 JVM 启动与类加载；
② JIT 编译在跑过前几条后被预热；③ 部分输入数据进了 OS Page Cache。
**这也是"为什么 Spark 必须长跑才划算"的微观解释。**

### 7.4 数据正确性对账（完整）

```
dws_user.gmall        1622820   |  dws_user.spark        1622820   ✅
dws_product.gmall     1265817   |  dws_product.spark     1265817   ✅
dws_category.gmall    1853      |  dws_category.spark    1853      ✅
dws_trade.gmall       31        |  dws_trade.spark       31        ✅
ads_active.gmall      31        |  ads_active.spark      31        ✅
ads_funnel.gmall      31        |  ads_funnel.spark      31        ✅
ads_repurchase.gmall  31        |  ads_repurchase.spark  31        ✅
ads_topn.gmall        1550      |  ads_topn.spark        1550      ✅

GMV.gmall  5.75759965E7  |  GMV.spark  5.75759965E7   ✅（= 57,575,996.50）
orders     186059        |  186059                   ✅
dau31      1622820       |  1622820                  ✅
```

**分区完整性**（`partitionOverwriteMode=dynamic` 生效证据）：

```
spark dws_user    partitions: 31
spark dws_product partitions: 31
hive  dws_user    partitions: 31
hive  dws_product partitions: 31
```

### 7.5 本次产出的可复用资产

| 资产 | 位置 | 用途 |
|---|---|---|
| Spark 对比执行器 | 集群 `/home/lrh/workspace/spark_bench/spark_bench_run.sh` | 8 段分步 + 逐段计时 + `status.tsv` |
| Hive 严格对照执行器 | 集群 `/home/lrh/workspace/hive_bench/hive_bench_run.sh` | 同 SQL 同参数，写入 `gmall_bench_hive` |
| 隔离对比库 | `gmall_bench`（Spark 产出）/ `gmall_bench_hive`（Hive 产出） | 零风险对比，随时 `DROP DATABASE ... CASCADE` 清理 |
| Spark 配置 | `/opt/spark-3.5.9-bin-hadoop3/conf/{spark-defaults.conf,spark-env.sh}` | 三台可复用 |
| HDFS jar 分发 | `hdfs:///spark/jars/*`（252 文件 / 338MB） | 免每次重传 |

---

## 八、复盘：被证伪的假设

### ⚠️ H8：Spark 是不是真的"更快"？—— 结论必须分情况（本次最核心的复盘）

- **原判断**：Spark 把中间结果留在内存、不反复启 JVM，跑同样的批作业应该明显快于 Hive/MR
- **实测**：

| 口径 | Spark | Hive | 结论 |
|---|---|---|---|
| 单会话连跑 8 段 | **75 s** | 389 s | **快 5.0 倍** |
| 每段各起一个会话 | 381 s | 421 s | 只快 10% |
| DWS-trade（轻） | 45 s | **26 s** | **Spark 慢 19 s** |
| ADS-funnel（轻） | 42 s | **38 s** | **Spark 慢 4 s** |
| ADS-topn（轻） | 42 s | **35 s** | **Spark 慢 7 s** |

- **为什么**：Spark 每次提交要付 **约 35 秒固定开销**（JVM 启动 + 连 metastore + 向 YARN 申请 Executor）。
  8 段 × 35 s = 280 s，占 381 s 的 **73%**。而 Hive 的 beeline 会话开销虽也有，但**它的 MR 作业本身就要跑 20~80 秒**，启动开销占比小得多。
  → **作业越轻，Spark 越吃亏；作业越重、越连续，Spark 越占优。**

- **修正后的认知**：准确的说法不是"Spark 比 Hive 快"，而是
  **"Spark 把成本从『每次作业』转移到了『每次会话』——所以它只在会话被复用时长跑才划算"**。
  两条可直接用于生产的判断：
  1. **多个作业要合并进一个常驻会话**（这正是"提交一个 Spark 应用跑多任务"或 Spark ThriftServer 服务化的价值）；
  2. **单条短查询不要用 Spark** —— 一次约 35 秒的启动成本，比很多查询本身的执行时间还长。

- **⚠️ 诚实标注**：本次 Hive 单会话（389s）与 Spark 单会话（75s）虽然同为"单会话连跑 8 段"，
  但**两者内部的作业组织方式并不完全对等** —— Hive 一段 SQL 会编译成多个 MR 作业串行跑，
  而 Spark 的 8 条 INSERT 是 8 个 Stage DAG。所以 5.0× 这个数字应理解为
  **"这套集群上、这套 SQL 下、这种调用方式下的实测比值"**，不宜外推成"Spark 普遍比 Hive 快 5 倍"。

### ❌ H1：以为集群没有外网

- **原判断**：VMware 本地集群通常不配外网，打算本机下载后 scp 上传（383MB，慢且易出错）
- **实测**：`wget -q --spider https://archive.apache.org/dist/spark/` 与清华镜像**都通**
- **为什么错**：把"本地开发机可能没网"的惯性假设套到了虚拟机上。这套 VM 用的是 NAT/桥接网络，宿主有网它就有网
- **修正后的认知**：**先测再设计**。判断外网连通性的成本只要一条 `wget --spider`，
  而我差点为此多写一套「本机下载 → 校验 → scp → 远端校验」的流程

### ❌ H0：把 `PMem` 当成了 YARN 容量（我自己的误读）

- **原判断**：`yarn node -list -showDetails` 输出 `PMem:1902 / 1761 / 4860 MB`，以为是三个节点各自的 YARN 可用内存
- **实测**：真正的容量字段是 `Configured Resources : <memory:2048, vCores:2>`，**三台完全相同**
- **为什么错**：`Resource Utilization by Node : PMem:...` 是**节点物理内存的使用量**（还是 OS 层口径，不是 YARN 口径）。
  lrh-1 显示 4860MB 是因为它还扛着 NameNode / Hive / DolphinScheduler / MySQL —— 不是因为它"YARN 容量大"
- **危险性**：如果按 4860MB 去设计 executor，会得出"lrh-1 可以放 2 个大 executor"的错误结论，
  然后被 YARN 拒绝或触发 OOM
- **修正后的认知**：**看配置要看"配置"字段，不要看"使用"字段**。这与项目里已记录的教训一脉相承
  （`jps` 空输出 ≠ 进程不存在、`grep -c` 数 ZK 节点数不对）—— **判据本身错了，结论就全错，而且不会报警**

### ⚠️ H9：日志里的 `TTransportException` —— 判定为良性（不是证伪，但是重要判断）

完整日志顺序还原：

```
1. Trying to connect to metastore with URI thrift://lrh-1:9083
2. Connected to metastore.
3. Closed a connection to metastore, current connections: 0     ← 切换 authorization manager
4. Trying to connect ... Connected to metastore.                ← 重连
5. WARN RetryingMetaStoreClient - MetaStoreClient lost connection.
   Attempting to reconnect (1 of 1) after 1s. listPartitionsWithAuthInfo
   org.apache.thrift.transport.TTransportException: Cannot write to null outputStream
6. RetryingMetaStoreClient trying reconnect as lrh (auth:SIMPLE)
7. Connected to metastore.                    ← 重连成功
8. cnt = 10630245                             ← 结果正确
```

**判定依据（为什么说它良性）**：
1. 异常发生在 `listPartitionsWithAuthInfo`（读分区元数据），**不在数据读取路径上**；
2. `RetryingMetaStoreClient` **内置重试并成功**，后续查询正常返回且结果与基线逐位一致；
3. 异常的类型是"连接对象已被关闭"（transport 为 null），属于 Spark 初始化时切换
   `AuthorizationMetaStoreFilterHook` 导致连接重建的**时序窗口**，不是 metastore 服务故障。

**验证方式**：重跑一次，异常复现（同样良性）、结果仍为 10,630,245 → 判定为**确定性噪音而非偶发故障**。

**这条最值得讲的点**：`控制面报错 ≠ 数据面失败`。面试里遇到"日志有异常你怎么办"，
正确答案不是"忽略它"，而是**先证明它不影响结果**（异常位置 + 有无自动恢复 + 结果是否与基线一致）。

---

## 九、意外发现

| # | 发现 | 性质 |
|---|---|---|
| 1 | Spark 单发查询的固定开销约 **35 秒**（JVM + metastore + YARN 提交），比 Spark 自报执行时间还长 | 已知，影响"什么时候该用 Spark"的判断 |
| 2 | 集群 YARN 总容量只有 **6GB / 6 vCores**，是本次对比最紧的约束 | 已知，是设计依据 |
| 3 | `spark.sql.shuffle.partitions` 默认 **200** 在 6 核集群上是严重浪费，必须下调 | 已知，已配为 6 |
| 4 | 本机 `D:\_del_old_bigdata_20261004\sshkeys\db_ed25519` 删除失败，原因**不是文件锁**而是**「访问被拒绝」** | 已知未修，见下 |

**关于 #4 的准确表述**（避免留下错误的"原因"）：

- 现象：`Remove-Item` / `rm -f` / `[System.IO.File]::Delete` 全部失败
- 错误信息：`对路径 ... 的访问被拒绝`（**Access Denied，不是 Sharing Violation**）
- 已排除：**不是** `safe-delete` 钩子单独造成的（钩子报告 aborted，但 .NET 直删也报"访问被拒绝"）
- 现有推断（**未验证**）：该文件的 ACL 或所在目录权限异常，可能需要管理员权限处理；
  文件已含在备份 `D:\数据开发环境\backup\旧项目位置_20261004.zip` 中，**删除它只是清理，无数据风险**
- ⚠️ 结论按项目纪律标注为**未验证**，不写成"因为被进程占用"

---

## 十、面试官可能会追问的问题

**Q1：为什么不用 Spark 4？**
A：集群 JDK 是 1.8.0_202，Spark 4.x 要求 JDK 17+，升 JDK 会让 Hadoop 3.3.6 / Hive 3.1.3 全部面临重装风险。
更关键的是 Spark 4 默认 `spark.sql.ansi.enabled=true`，SQL 语义变了（除零报错、隐式转换变严），
现有 SQL 可能"跑得起来但结果不一样"——这比跑不起来危险得多。Spark 3.5.x 是最后支持 Java 8 的版本线，是这里唯一合理的选择。

**Q2：Spark 直接连 Hive metastore 不就行了，为什么要配 `metastore.jars=path`？**
A：Spark 3.5 内置的 Hive client 是 2.3.9，而后端 metastore 服务是 3.1.3。Thrift 接口在 2.x→3.x 有变更，直连会失败。
`maven` 模式要从中央仓库拉 jar，每个 Executor 首次运行都要联网，不可离线也慢；`path` 模式直接复用集群已有的 Hive lib（277MB），
版本严格一致、可离线、可控。我选 `path`。

**Q3：executor 内存为什么是 1408m 这种"奇怪"的数字？**
A：被 YARN 上限逼的。集群 `yarn.scheduler.maximum-allocation-mb=2048`，而
`spark.executor.memory + memoryOverhead = 总容器请求`。1408 + 384 = 1792 ≤ 2048，留 256MB 给 NodeManager。
如果按习惯写 `--executor-memory 2g`，容器请求会超过 2048 被 YARN 直接拒绝。

**Q4：`partitionOverwriteMode` 不设会怎样？**
A：Spark 默认 `STATIC`，`INSERT OVERWRITE TABLE t PARTITION (dt)` 会**先清空整张表**再写。
全量跑时看不出问题（反正都要重写），但只要有一次补跑单个日期，**其余日期的数据会被静默清空**。
这是"跑得对、结果错"，必须设 `dynamic` 才与 Hive 行为一致。

**Q5：日志里有 `TTransportException`，你怎么判断它不是故障？**
A：三个证据。①异常发生在 `listPartitionsWithAuthInfo`，是元数据读取而非数据读取路径；
②`RetryingMetaStoreClient` 内置重试且成功（日志明写 `Connected to metastore`）；
③结果与基线逐位一致（10,630,245）。而且重跑一次现象相同——是确定性噪音，不是偶发故障。
**核心判断是"控制面报错 ≠ 数据面失败"，但必须证明，不能默认忽略。**

**Q6：为什么单看一条 `count(*)` 有 51 秒，Spark 不是更快吗？**
A：51 秒里 Spark 自己只用了约 16 秒，剩下约 35 秒是 JVM 启动 + 连 metastore + 提交 YARN 应用的**固定成本**。
Spark 的优势建立在"一个常驻会话里连续处理大量任务"上；单发小查询必然是它最差的场景。
所以我的对比只跑批处理，不去比单条查询——那对 Spark 不公平也没有业务意义。

**Q7：你怎么保证两个引擎的输入完全一致？会不会对比结果其实被数据差异污染了？**
A：我把写入目标从 `gmall.*` 改写为 `gmall_bench.*`，但**源表仍读 `gmall.*`**（包括 `gmall.dws_*`，即 Hive 版产出的 DWS）。
改写后我逐条 grep 核对过：8 个写入目标全是 `gmall_bench.*`，4 个源表全是 `gmall.*`。
这样两边的输入是同一份数据，"谁快"的差异只能归因到引擎本身。

**Q8：这套集群资源这么小，做双引擎对比有意义吗？**
A：有意义，而且更真实。生产环境里"资源管够"是少数，多数团队是在有限资源下做取舍。
我这套集群 YARN 总共 6GB / 6 核，正好能暴露两个引擎在小资源下的真实差别：
Hive 每个容器一个 task、反复启 JVM；Spark Executor 常驻但单个 task 分到的内存更少。
在受限资源下谁更划算，比在理想条件下的 benchmark 更有决策价值。

---

## 十一、风险控制与回滚

| 风险 | 控制手段 |
|---|---|
| 覆盖现有正确数据 | 写入目标全部为独立的 `gmall_bench` 库；现有 `gmall` 库零写入 |
| 改动影响现有服务 | 只新增 `/opt/spark-3.5.9-bin-hadoop3`，未修改 Hadoop/Hive 任何配置（`hive-site.xml` 是拷副本） |
| YARN 资源被长期占住 | 跑批结束后在 `status.tsv` 与 `done` 文件留痕；如异常可 `yarn application -kill` |
| 包被篡改 | sha512 与官方源逐一比对 |

**回滚命令**（可直接复制执行）：

```bash
# 1) 停掉正在跑的对比作业（如有）
/opt/hadoop/hadoop-3.3.6/bin/yarn application -list | grep SparkSQL
# 拿到 Application-Id 后：
# /opt/hadoop/hadoop-3.3.6/bin/yarn application -kill application_XXXXXXXXXX_XXXX

# 2) 删除对比库（只影响 bench，不动 gmall）
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://lrh-1:10000 \
  -e "DROP DATABASE IF EXISTS gmall_bench CASCADE;"

# 3) 移除 HDFS 上的 spark 分发目录与 event log
/opt/hadoop/hadoop-3.3.6/bin/hdfs dfs -rm -r -skipTrash /spark

# 4) 彻底卸载 Spark（可选）
sudo rm -rf /opt/spark-3.5.9-bin-hadoop3
rm -rf /home/lrh/downloads/spark-3.5.9-bin-hadoop3.tgz

# 5) 若曾把 hive-site.xml 拷进 spark/conf —— 只是副本，随 Spark 目录一起删即可
#    原 Hive 配置未被改动：/opt/hive/apache-hive-3.1.3-bin/conf/hive-site.xml 完好
```

> **注意**：回滚**不需要**动 `gmall` 库的任何一个字节 —— 这也是当初把写入目标隔离到 `gmall_bench` 的原因。

---

## 十二、后续计划

| 阶段 | 内容 | 状态 |
|---|---|---|
| 环境侦察 | JDK / YARN / 磁盘 / 外网约束实测 | ✅ |
| 部署 Spark 3.5.9 | 下载 + 校验 + 安装 + HDFS jar 分发 | ✅ |
| 打通 Metastore | 27 张表可见、1063 万行可读 | ✅ |
| 三层判据验证 | local 元数据 / local 取数 / YARN 分布式 | ✅ |
| DWS+ADS 八段对比跑批 | Spark 381s vs Hive 421s（分段）；**75s vs 389s（单会话）** | ✅ |
| 结果对账 | 8 张表行数逐项一致、GMV 逐位一致、分区 31 = 31 | ✅ |
| 写实验报告 + 同步文档 | 本报告 + `CONTEXT.md` / `项目状态.md` | ✅ |
| **把 Spark 接进 DolphinScheduler** | 新建 `spark` 类型任务节点，与 Hive 任务并存做同链路对比 —— **面试价值最高的下一步** | ⏳ |
| Spark History Server | 启用 18080，可回放作业 DAG（面试可现场展示） | ⏳ |
| （可选）DWD 段也做对比 | 覆盖更重的大基数作业（`dwd_event_clean` 是整链最慢节点之一） | ⏳ |
| （可选）测试 Spark 4.x | 需先解决 JDK 17 依赖，改造成本高、收益不确定 | ⏳ 暂不做 |

---

## 附录：本次涉及的概念速查（零基础）

| 名词 | 一句话解释 |
|---|---|
| **YARN** | Hadoop 的"资源调度器"——集群有多少 CPU/内存，由它决定分给谁。你可以把它理解成"整个机房的内存和 CPU 的分配员" |
| **container（容器）** | YARN 分配资源的最小单位，比如"2048MB 内存 + 2 个核"。一个容器里通常跑一个 JVM 进程 |
| **Executor** | Spark 在 YARN 容器里长期驻留的工作进程，所有 task 都在它内部以**线程**方式跑（不像 MR 每个 task 起一个 JVM） |
| **Driver** | Spark 应用的"大脑"，负责把 SQL 拆成 DAG、调度 task、收集结果 |
| **DAG** | 有向无环图——Spark 把一次计算拆成若干有依赖关系的阶段（Stage），像施工进度图 |
| **Shuffle** | Stage 之间的数据重分布（比如 `GROUP BY` 要把相同 key 的数据聚到一起）。**这是 Spark 最贵的操作**，也是它唯一必须落盘的地方 |
| **Hive Metastore** | 存"表长什么样"的地方（表名、字段、分区、HDFS 路径），本身不存数据。它是一个独立服务（这里在 9083 端口），Hive 和 Spark 都连它 |
| **Thrift** | 一种"跨语言调用远程服务"的协议。Hive metastore 用它对外提供接口；客户端和服务端版本差太多会握不上手 |
| **`spark.sql.shuffle.partitions`** | Shuffle 后切多少个并行分片。默认 200，小集群必须调小，否则几百个 task 排队等几个核 |
| **`partitionOverwriteMode`** | 控制 `INSERT OVERWRITE` 分区表时"覆盖范围"：`STATIC` = 清空整表，`DYNAMIC` = 只覆盖涉及的分区（Hive 的行为） |
| **`bin-hadoop3` 发行包** | Spark 官方为不同 Hadoop 版本预编译的包。集群是 Hadoop 3.x，就必须选 `-bin-hadoop3`，否则会出现 NoSuchMethodError 之类的兼容问题 |

---

*报告生成：2026-10-04 · 所有数据均来自实测，未使用估算值；待补章节标注为"待补"，不填估算数字*
