# 05 · YARN 与 MapReduce：活是怎么干完的

> **这章解决什么**：你敲下一条 SQL、按下回车，到结果出来——中间物理上发生了什么？
> 为什么有的任务 3 秒、有的 3 小时？为什么总有任务在"99%"卡住？
>
> **这是整套教程里最值钱的一章。** 建立这个心智模型之后，报错信息对你来说才开始有意义。

---

## 一、先说 YARN 是干嘛的（它不计算）

**常见误解**：YARN 是个计算框架。**错。YARN 只管分资源，不碰数据。**

```
YARN  = 车间调度员  → 决定"这个活派给谁、给它多大地方"
MapReduce/Spark = 工人 → 真正干活
HDFS  = 仓库      → 存原料和成品
```

### 1.1 四个角色

| 角色 | 在哪 | 干什么 |
|---|---|---|
| **ResourceManager (RM)** | 主节点，一台 | 全局资源账本：谁有多少空闲内存/CPU，接单、派单 |
| **NodeManager (NM)** | 每台一台 | 上报本机资源，接收任务，在本机起"容器"干活，监控它死没死 |
| **ApplicationMaster (AM)** | **每个作业一个**，随机某台机器 | 这个作业的"项目经理"：向 RM 要资源、盯着自己的任务、失败重试 |
| **Container（容器）** | 某台机器上 | 一块被划出来的资源（比如 1GB 内存 + 1 个核），任务就跑在里面 |

**物理上 Container 是什么**：就是一个被限制了资源额度的 **JVM 进程**。
它不是 Docker 容器，没有镜像，本质就是"一个 java 进程 + 一组内存/CPU 限额参数"。
NM 会盯着它，超过限额就杀掉。

> 📌 **本项目对照**：案例集群每节点 `yarn.nodemanager.resource.memory-mb=2048`
> → **单个容器最多只能申请到 2048MB**。这是最硬的一条约束：
> 后面 Hive 的所有 `mapreduce.map.memory.mb`、`-Xmx` 参数都被这个数卡死。
> **你调不出比节点容量更大的容器**（第 11 章详谈）。

---

## 二、顺序：提交一个作业的完整生命周期

**场景**：你在 lrh-1 上执行一条 Hive SQL（比如那种"带 `GROUP BY`、又带去重指标"的统计）。
Hive **不会直接开始算**——它先把这条 SQL **编译成若干个** MapReduce 作业，再逐个提交给 YARN。

> ⚠️ **"几个"不是固定值，由 SQL 的写法决定**（这点最容易被误解，所以先说清）：
> Hive 会为每一个"**必须让数据跨机器重新分布**"的环节切一刀，**切几刀就是几个作业**。
>
> | SQL 形态 | 作业数 | 为什么 |
> |---|---|---|
> | `SELECT COUNT(1) FROM t` | **1** | 塞进一个作业的 map 端就做完了 |
> | `GROUP BY 小基数` | **1** | 一次 shuffle 就够 |
> | **`GROUP BY` + `COUNT(DISTINCT)`** | **2~3** | 去重没法在 map 端部分完成，要多一轮 |
> | 大表 JOIN 大表 | 2~3 | 先各自重分布，再 join |
> | 小表 JOIN 大表（开了 map-join） | **1** | 小表广播下去，不用 shuffle |
> | `UNION ALL` 六段（案例的校验 SQL） | **6** | 每段独立编译，而且**串行**跑 |
>
> 下面按 **"编译出 3 个"** 来讲 —— 因为 `COUNT(DISTINCT)` 这类**最常见的统计写法**就是 2~3 个。
> 这样安排是为了让你看清：**每一个**作业，都要**独自**完整走一遍这七步。
> 想知道**你自己那条 SQL** 会切成几个，用 `EXPLAIN` 数 `STAGE`，见 §四。

**先看清"谁在哪、有几个"**——这是理解下面七步的前提：

```
       客户端 (beeline)
            │ 提交作业
            ▼
   ┌────────────────────────┐
   │  ResourceManager (RM)  │  ← 全集群只 1 个，固定在 lrh-1
   │  全局账本：谁有空闲资源 │     网页 8088 能看到它
   └───────────┬────────────┘
               │ 划一个容器出来，在里面启动 AM
               ▼
 ╭─────────────────────────────────────────────────────────────╮
 │ 集群：每台机器跑 1 个 NodeManager，容器跑在 NodeManager 里面  │
 │                                                             │
 │    lrh-1              lrh-2              lrh-3              │
 │    NodeManager        NodeManager        NodeManager        │
 │    ├─ Container  ◀──  ├─ ★ AM  ──▶       ├─ Container        │
 │    └─ Container       └─ Container       └─ Container        │
 ╰─────────────────────────────────────────────────────────────╯
```

**四个角色的数量与位置**（最容易搞混的地方）：

| 角色 | 全集群几个 | 在哪台机器 | 常驻还是临时 | 干什么 |
|---|---|---|---|---|
| **ResourceManager (RM)** | **1 个** | 固定（案例 lrh-1） | 常驻 | 全局资源账本，决定"派给谁" |
| **NodeManager (NM)** | **每台 1 个** | 每台机器 | 常驻 | 上报本机资源、起容器、盯着容器 |
| **ApplicationMaster (AM)** | **每作业 1 个** | 集群里**随机**某台（≠ 主节点） | **临时**（作业结束就注销） | 这个作业的"项目经理" |
| **Container** | 若干 | 跑在 NM 里面 | 临时 | 一块被限额的资源，任务真正执行的地方 |

> ⚠️ **两个最容易混的点**：
> 1. **NM ≠ AM**：NM 是**常驻**的（机器活着它就在，管的是"这台机器的资源"），
>    AM 是**临时**的（作业跑完就没了，管的是"这一个作业"）；
> 2. **AM 本身就是一个 Container**——它不是特殊进程，只是"跑着 AM 程序的那个容器"。

**接着看这七步**（每一步都能在上图里找到位置）：

```
① 【客户端 → RM】"我要提交一个作业"
     RM 记一笔，回一个 application id（application_1699..._0001）

② 【RM → 某台 NM】"你起一个 Container，跑这个作业的 AM"
     AM 进程在集群里某台机器上诞生（不一定是 lrh-1）

③ 【AM → RM】"我需要 8 个 map 容器 + 2 个 reduce 容器，每个 1GB"
     AM 按数据本地性提要求："最好能在存着这块数据的那台机器上跑"

④ 【RM → AM】"给你这几个地址"（资源分配）
     RM 只能承诺"尽力就近"，资源紧张时就得跨机器

⑤ 【AM → 各 NM】"在我给你分配的容器里跑这个任务"
     NM 在本地 fork 出一个 JVM（YarnChild）

⑥ 【容器干活】读 HDFS → 算 → 写本地磁盘（shuffle 阶段还走网络）
     容器定期向 AM 汇报进度（0%~100%）

⑦ 【AM 收齐】所有任务成功 → 向 RM 注销 → 作业完成
     任何任务失败 → AM 重新向 RM 要一个容器，重跑那一个任务（最多 4 次）
```

**关键认知**：

| 认知 | 说明 |
|---|---|
| **AM 是每作业一个** | 所以大集群里 AM 本身也占资源 |
| **失败重试是自动的** | 一个 map 挂了不会让整个作业挂，只重跑那一个——**这是分布式容错的核心** |
| **"卡在 99%"** | 通常是少数几个 reduce 在跑（数据倾斜），见第 11 章 |

---

## 三、MapReduce：一个作业内部的三段

拿最经典的 WordCount（数每个词出现几次）来看：

```
输入：3 个块（分布在三台机器上）
  块1: "hello world hello"
  块2: "world hadoop"
  块3: "hello hadoop world"
```

### 阶段一：Map（各干各的，零通信）

```
块1 → map → (hello,1) (world,1) (hello,1)
块2 → map → (world,1) (hadoop,1)
块3 → map → (hello,1) (hadoop,1) (world,1)
```

**物理上**：每个 map 任务读一个块（**尽量在本机读，不跨网络** —— 这叫数据本地性），
输出先写在**内存缓冲区**，满了就溢写到**本机磁盘**。

### 阶段二：Shuffle（★ 最贵的一步）

**问题**：要统计 `hello` 的总数，必须把所有 `hello` 弄到一起去。

```
map 端：
  按 key 分区（hash(key) % reduce数）→ 每个分区内按 key 排序 → 写本地磁盘
  块1: 分区0=[hello,1 hello,1]  分区1=[world,1]
  块2: 分区0=[]                 分区1=[hadoop,1 world,1]
  块3: 分区0=[hello,1]          分区1=[hadoop,1 world,1]

                    ↓↓↓  网络传输（reduce 去各台机器上"拉"属于自己的分区）  ↓↓↓

reduce 端：
  reduce0 收到所有分区0 → 归并排序 → [hello,1 hello,1 hello,1]
  reduce1 收到所有分区1 → 归并排序 → [hadoop,1 hadoop,1 world,1 world,1 world,1]
```

**Shuffle 为什么贵——物理账**：

| 代价项 | 说明 |
|---|---|
| **磁盘 IO** | map 端要排序并写本地磁盘（可能写多轮） |
| **网络** | 所有中间数据要跨机器传输，量级常常和输入数据相当 |
| **磁盘 IO（再来一次）** | reduce 端收到后要归并，又要落盘 |

> **一句话**：MapReduce 慢，主要不是因为"算"慢，而是因为**中间结果要落盘 + 跨网络搬**。
> 这也是 Spark 快的主要原因——它尽量把中间结果留在内存里（但内存装不下时一样要落盘）。

### 阶段三：Reduce（合并）

```
reduce0: hello → 3
reduce1: hadoop → 2,  world → 3
```

**物理上**：reduce 的输出**写回 HDFS**（又是一份 3 副本的网络 + 磁盘开销）。

---

## 四、一条 Hive SQL = 几个 MapReduce 作业？

这是理解"为什么我的 SQL 这么慢"的关键。**看执行计划**：

```sql
EXPLAIN
SELECT dt, COUNT(DISTINCT user_id) FROM gmall.dwd_event_log_di GROUP BY dt;
```

输出是一棵树（简化）：

```
STAGE DEPENDENCIES:
  Stage-1 is a root stage
  Stage-0 depends on stages: Stage-1

STAGE PLANS:
  Stage: Stage-1
    Map Reduce
      Map Operator Tree:
          TableScan → Select Operator → Group By Operator (partial) → Reduce Output Operator
      Reduce Operator Tree:
          Group By Operator (final) → File Output Operator
```

**数 Stage 就知道要跑几个作业。** 规律：

| SQL 写法 | 作业数 | 为什么 |
|---|---|---|
| `SELECT COUNT(1) FROM t` | 1 | 只有 map 端聚合 + 一个汇总 |
| `GROUP BY 小基数` | 1 | 一次 shuffle |
| `GROUP BY 大基数 + COUNT(DISTINCT)` | 2~3 | 去重需要额外的 shuffle |
| `大表 JOIN 大表` | 2~3 | 分别打标签 → shuffle → join |
| `UNION ALL 六段校验` | **6** | 每段独立编译成一个作业，**串行跑** |

> 📌 **本项目对照**：案例项目的 DS 工作流里，最慢的节点不是 ETL 而是**校验节点**（15 分 23 秒，占整链 28%）。
> 原因就是校验 SQL 里的 `UNION ALL` 被 Hive 编译成 **6 个串行 MR 作业**。
> ——**慢不一定在"算"，可能在"作业数量多、逐个排队"。**

**★ 从集群侧验证（比看 EXPLAIN 更硬的判据）**：

**Hive 的每一个 Stage，都会以一个独立的 YARN 应用出现在 8088 页面上。**
所以跑 SQL 的时候打开 `http://lrh-1:8088`，看应用列表里**一条接一条地冒出来**——
数一下就知道你这条 SQL 实际切成了几个作业，不需要相信任何人的说法。

```bash
# 命令行版（更直观：能直接看到同一时刻有几个在跑）
yarn application -list
```

> 这比 EXPLAIN 更可信：EXPLAIN 是"Hive **打算**怎么跑"，8088 是"**实际**跑成了几个"。
> 两者不一致时（比如优化器改写了计划），**以实际为准**。

---

## 五、怎么看一个正在跑的作业（YARN Web UI）

打开 `http://<主节点>:8088`，你会看到：

| 页面信息 | 含义 |
|---|---|
| **State** | ACCEPTED（在排队等资源）/ RUNNING / FINISHED / FAILED |
| **Progress** | 百分比（注意：这是 map 和 reduce 的加权平均，不准） |
| **Tracking UI** | 点进去看每个任务的明细 |
| **Diagnostics** | ★ 失败原因写在这里 |
| **Num containers** | 用了几个容器 |

### 5.1 命令行方式（更常用）

```bash
yarn application -list                          # 正在跑的作业
yarn application -status application_xxx_0001   # 某个作业的详情
yarn application -kill application_xxx_0001     # 杀掉它
yarn logs -applicationId application_xxx_0001   # ★ 看完整日志（报错必用）
yarn node -list                                 # 节点列表与资源
```

### 5.2 Counters：判断"这个作业死在哪"的体检表

在作业的详情页往下翻，有一大堆 Counter。**只看四个就够定位大部分问题**：

| Counter | 说明 | 怎么用 |
|---|---|---|
| `HDFS: Read` | 读了多少 HDFS 数据 | **远小于输入量 → 死得很早** → 内存/初始化问题 |
| `HDFS: Write` | 写了多少 | **Read 正常但 Write=0 → 算完了写不出去** → 写入并发问题 |
| `Cumulative CPU` | 累计 CPU 时间 | 很小却耗时很长 → **在等 IO/网络，不是在计算** |
| `GC time elapsed` | 花在垃圾回收上的时间 | 占比高 → **内存不够，在反复 GC** |

> 📌 **本项目对照**：这是案例项目总结出的定位法，非常实用——
> `GC overhead limit exceeded` **不在 `Caused by` 异常链里**，只在 task 的 Diagnostic Messages 里。
> 所以你要靠**量级不匹配**来判断，而不是靠搜报错关键字。

---

## 六、动手实验室

### 实验 5-1：跑一个 WordCount（亲手看三阶段）

```bash
# 准备数据
hdfs dfs -mkdir -p /wc/input
echo "hello world hello hadoop world hello" > /tmp/wc.txt
hdfs dfs -put /tmp/wc.txt /wc/input/

# 跑官方示例（路径按你的版本改）
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar \
  wordcount /wc/input /wc/output

# 看结果
hdfs dfs -cat /wc/output/part-r-00000
```

**观察**：打开 8088 页面，找到这个作业，看：
- 有几个 map 任务？（= 输入块数）
- 有几个 reduce 任务？（默认 1，可以在命令行用 `-D mapreduce.job.reduces=3` 改）
- Counters 里的 HDFS Read/Write 各是多少？

### 实验 5-2：看 reduce 数的影响

```bash
hadoop jar ...examples.jar wordcount -D mapreduce.job.reduces=3 /wc/input /wc/output2
hdfs dfs -ls /wc/output2        # 应该有 3 个 part 文件
```

**想一想**：reduce 越多越快吗？

> 不一定。reduce 多 → 并行度高，但：
> ① 每个 reduce 都要从所有 map 拉数据 → **网络连接数 = map数 × reduce数**，太多会压垮网络；
> ② reduce 太多会产生很多小文件。
> 经验：reduce 数 ≈ 集群总核数的 1~2 倍，或按输出大小控制（每个 reduce 输出 1~2 个块大小）。

### 实验 5-3：让一个任务失败，看它怎么重试

```bash
# 故意用一个不存在的输入路径
hadoop jar ...examples.jar wordcount /wc/not_exist /wc/out3
yarn application -list            # 看状态
# 打开 8088，看 Diagnostics 里写了什么
```

**这就是你以后排错的日常动作**：状态 → Diagnostics → `yarn logs`。

---

## 七、常见坑

| 现象 | 根因 | 解法 |
|---|---|---|---|
| 作业一直 ACCEPTED 不 RUNNING | 资源被占满（队列满/内存不够） | 看 8088 的集群资源；杀掉不必要的应用 |
| `Container killed by YARN for exceeding memory limits` | 容器实际用量超过申请值 | 调大 `mapreduce.map.memory.mb` 和 `-Xmx`（第 11 章） |
| `GC overhead limit exceeded` | 内存不够，JVM 98% 时间在 GC | **不是加 CPU，是加内存**。看 Diagnostic Messages |
| `Container is running beyond virtual memory limits` | 虚拟内存超限 | 虚拟机环境常配 `yarn.nodemanager.vmem-check-enabled=false` |
| reduce 卡在 99% | 数据倾斜：某个 key 特别大 | 见第 11 章（加盐/两阶段聚合） |
| `Exit code 137` | SIGKILL，通常是 OOM 被系统杀 | 查内存 |
| `Exit code 143` | SIGTERM，**被人主动停止** | 别当成程序错误（第 11 章） |
| 输出目录已存在 | MapReduce 规定输出目录不能事先存在 | 换目录或先删 |

> ⚠️ **一条重要的方法论**：**退出码先于日志文本。**
> 看到 `143`（SIGTERM）就该想到"是不是被谁停了"，而不是去日志里找 SQL 语法错误。
> 案例项目就吃过这个亏：日志尾部看起来像 SQL 报错，实际是**被人主动停掉的**。

---

## 八、自测

**每题标了对应的正文小节——答不出来就回去看那节，不要硬想。**

1. YARN 和 MapReduce 的分工是什么？一句话说清。—— 对应 **§一**
2. Container 物理上是什么？—— 对应 **§1.1**
3. 一个 map 任务挂了，整个作业会失败吗？为什么？—— 对应 **§二**
4. Shuffle 阶段有哪三个主要物理代价？—— 对应 **§三（阶段二）**
5. 给你一条 SQL，你怎么知道它会跑成几个作业？—— 对应 **§四**
6. 作业失败，你会看哪四个 Counter 来判断它死在哪？—— 对应 **§5.2**

<details>
<summary><b>参考答案要点</b>（先自己答一遍，再点开核对）</summary>

**1. YARN 与 MapReduce 的分工**
- **YARN 管资源**：决定"派给谁、给它多大地方"——它**不计算，也不碰数据**
- **MapReduce 干活**：在容器里真正读数据、算、写结果
- 一句话：YARN 是车间调度员，MapReduce 是工人，HDFS 是仓库

**2. Container 物理上是什么**
- **一个被限额的 JVM 进程**（内存 / CPU 额度），**不是 Docker 容器**（没有镜像、没有命名空间）
- 超额会被 NodeManager 杀掉
- 案例约束：每节点容量 2048MB → **单容器最多 2048MB**，这是最硬的一条

**3. 一个 map 挂了会怎样**
- **不会**导致整个作业失败。AM 发现后向 RM 再要一个容器，**只重跑那一个任务**（默认最多重试 4 次）
- 这是分布式容错的核心：把"大作业"拆成"可独立重试的小任务"
- 除非：AM 自己挂了、或重试次数用尽 → 整个作业才失败

**4. Shuffle 的三个物理代价**
- ① **map 端排序并写本地磁盘**（磁盘 IO，可能多轮溢写）
- ② **跨网络拉取**（reduce 去各台机器拉属于自己的分区）——最贵的一块
- ③ **reduce 端归并落盘**（又一次磁盘 IO）
- 加分项：中间结果量级常与输入数据**相当**（1TB 输入可能近 1TB 过网络）；这也是 Spark 快的原因（尽量留内存）

**5. 怎么知道会跑几个作业**
- `EXPLAIN <你的 SQL>`，数 **`STAGE DEPENDENCIES`** 里有几个 Stage
- 经验：简单聚合 1 个；大基数 GROUP BY + DISTINCT 2~3 个；大表 JOIN 大表 2~3 个
- 案例：校验 SQL 里的 `UNION ALL` 六段 → **6 个串行作业**，成了整链最慢的节点（15m23s，占 28%）

**6. 四个 Counter**
| Counter | 说明 | 判读 |
|---|---|---|
| `HDFS: Read` | 读了多少 | **远小于输入 → 死得很早 → 内存/初始化问题** |
| `HDFS: Write` | 写了多少 | Read 正常但 Write=0 → 算完了写不出去 → 写入并发 |
| `Cumulative CPU` | 累计 CPU | 很小却耗时长 → 在等 IO/网络，不是在算 |
| `GC time elapsed` | GC 耗时 | 占比高 → **内存不够，在反复 GC** |
</details>

---

## 下一章

[06 · Hive 与 HQL：用 SQL 指挥一群机器](./06_SQL_Hive与HQL.md) —— 不用写 Java，也能让三台机器替你算。
