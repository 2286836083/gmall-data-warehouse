# 项目升级规划：内存扩容 · 可视化 · Spark 双引擎

> 制定日期：2026-09-27
> 状态：**P1 / P2 已完成**（P2 待换新数据源重出）；**P3 未开始**
> 适用环境：VMware 三节点集群 lrh-1 / lrh-2 / lrh-3
> 已定决策：① 顺序＝内存 → 可视化 → Spark ② 可视化＝ECharts 静态大屏 ③ Spark＝ETL 双引擎 + 性能对比

---

## 〇、为什么是这三个顺序（依赖关系）

```
内存扩容  ──►  可视化(ECharts)  ──►  数据量放大  ──►  Spark 双引擎  ──►  性能对比
  ↑ 一切的前提      ↑ 独立、零风险        ↑ 对比成立的前提      ↑ 高风险高价值
```

三条硬逻辑：

1. **内存是所有事的前提**。Spark on YARN 的 executor 要落在 lrh-2/lrh-3 上，lrh-1 的 driver 也要内存；现在 lrh-1 已吃到 swap，直接上 Spark 必 OOM。
2. **ECharts 不依赖前两者**，零集群负担、半天见效，先把"能演示"落地，避免项目长期停在半成品状态。
3. **不做数据量放大，Spark 的性能对比结论不成立**。现数据只有 5 万订单 + 10 万日志，Spark 的启动开销可能比省下的计算时间还大，甚至跑得比 MapReduce 慢。必须先放大数据量，对比才有说服力。

---

## 一、Phase 1：内存扩容与组件瘦身

### 1.0 目标
让 lrh-1 空载时 `free -m` 的 available ≥ 3000MB，swap 使用量 < 500MB（当前约 1.7GB），为 Spark 让出空间。

### 1.1 已核实的资源现状

| 项 | 实测值 | 来源 |
|---|---|---|
| 宿主机物理内存 | **31.7 GB**（2×16G DDR5-5600） | Win32_PhysicalMemory |
| 宿主机逻辑核 | 32 | Win32_ComputerSystem |
| 宿主机当前可用 | 13.5 GB（**集群关机状态**下） | FreePhysicalMemory |
| D 盘可用 | 349 GB | df |
| lrh-1 配置内存 | 4096 MB / 2 vCPU | `D:\虚拟机\Ubuntu 64-bit.vmx` |
| lrh-2 配置内存 | 4392 MB / 2 vCPU | `D:\虚拟机2\Ubuntu 64-bit.vmx` |
| lrh-3 配置内存 | 4096 MB / 2 vCPU | `D:\虚拟机3\Ubuntu 64-bit.vmx` |

**关键结论**：三台 VM 合计只占 12.3GB，宿主机还有约 19GB 理论余量。问题不是总量不足，而是**最吃紧的 lrh-1 与最清闲的 lrh-3 分到了同样多的内存**，而 lrh-1 要跑 9 个 Java 进程。

> ⚠️ **反向提醒**：集群关机时宿主机 `FreePhysicalMemory` 只有 13.5GB，意味着 Windows 自身已占用约 18GB。扩容前建议先开任务管理器看一眼是谁在吃（浏览器多标签、OneDrive 同步、Docker Desktop 等都是大头），否则扩完可能反而让宿主机开始换页。

### 1.2 步骤

**Step 0 · 确认节点身份（不可跳过）**

三台 `.vmx` 的 `displayName` 全部是 `"Ubuntu 64-bit"`，且 `D:\虚拟机` 与 `D:\虚拟机2` 两个文件夹在 2026-09-21 被互换过名字。**改内存前必须先开机逐台确认**：

```bash
hostname            # 看是 lrh-1 / lrh-2 / lrh-3
ip addr | grep 192.168.87
```
把 `hostname` 与 `192.168.87.131/130/132` 对上，才能确定该改哪个文件夹的 vmx。**改错等于把 8GB 送给闲着的节点。**

**Step 1 · 采集基线（扩容前的证据）**

```bash
free -m                          # 内存 + swap
jps                              # 进程清单
ps -eo pid,comm,rss --sort=-rss | head -20   # 谁最吃内存（RSS 单位 KB）
swapon --show                    # swap 设备
df -h /                          # 各节点根分区余量
```
把三台的结果存档，作为"扩容前"的对照数据。

**Step 2 · 关闭虚拟机，修改 vmx**

在 VMware 里对三台执行 Shut Down Guest（不要直接关窗口）。关机顺序：先 `sudo systemctl stop hadoop` 避免 HDFS 元数据损坏。然后编辑 vmx：

| 节点 | 当前 | 建议值 | 说明 |
|---|---|---|---|
| lrh-1 | 4096 | **8192** | 主力扩容对象，9 个进程 + Spark driver |
| lrh-2 | 4392 | **4096** 或 6144 | 顺带把 4392 这个奇怪值规范化 |
| lrh-3 | 4096 | **4096** 或 6144 | 工人节点，需容纳 Spark executor |

改 `D:\虚拟机*\Ubuntu 64-bit.vmx` 里的 `memsize = "4096"` 一行。
**改前先复制一份 `.vmx.bak`**，这是本步骤的回滚手段。

分配方案二选一：
- **保守方案（推荐先上）**：8192 / 4096 / 4096 = 16.4GB，宿主机留约 15GB
- **充足方案**：8192 / 6144 / 6144 = 20.5GB，宿主机留约 11GB

**Step 3 · 配置瘦身（配套，可选但建议）**

| 项 | 位置 | 调整 | 预计收益 |
|---|---|---|---|
| SecondaryNameNode 迁到 lrh-2 | `hdfs-site.xml` 的 `dfs.namenode.secondary.http-address` | 改指 lrh-2 并在 lrh-2 起 2NN | ~200–300MB（lrh-1） |
| YARN 容器上限 | `yarn-site.xml`：`yarn.nodemanager.resource.memory-mb` / `yarn.scheduler.maximum-allocation-mb` | 按节点实际内存下调，避免 over-allocation 被 NM 杀容器 | 防 OOM |
| MapReduce 容器 | `mapred-site.xml`：`mapreduce.map.memory.mb` / `reduce.memory.mb` | 调到 512MB（当前默认 1024MB 对本数据量过大） | 可并发更多容器 |
| MR JVM 重用 | `mapreduce.job.jvm.numtasks` | 设为 10 | 省 JVM 反复启动开销 |
| ZooKeeper 堆 | `zkServer.sh` 的 `SERVER_JVMFLAGS` | 512m → 256m（ZK 数据量极小） | ~250MB × 3 台 |
| DolphinScheduler | 保持 `-Xms512m -Xmx512m` | **不要改大** | 防 OOM |
| alert-server | 演示时不启动 | 只启 api + master + worker | ~200–300MB |
| 内核参数 | `/etc/sysctl.conf`：`vm.swappiness=10` | 降低无谓换出 | 稳定性 |
| 内存软链 | 关掉不需要的自启服务（如无用的 snapd/cloud-init 残留） | `systemctl list-unit-files --state=enabled` 逐个体检 | 视情况 |

### 1.3 验收标准
- [ ] 三台节点身份与 IP 对应关系已书面确认
- [ ] 扩容前基线数据已存档（free / jps / RSS Top20）
- [ ] lrh-1 空载 `free -m` 的 **available ≥ 3000MB**
- [ ] 空载 `swapon --show` 用量 **< 500MB**
- [ ] `hdfs dfsadmin -report` 显示 `Live datanodes (3)`
- [ ] `yarn node -list` 三个节点 RUNNING
- [ ] 跑通一次完整链路（`run_pipeline.sh` 或至少 ADS 查询），确认无回归

### 1.4 回滚
- vmx 改坏 → 用 `.vmx.bak` 覆盖还原
- 配置改坏 → 配置文件改前统一备份到 `/opt/backup/<日期>/`
- 服务起不来 → `sudo systemctl restart hadoop` / `zookeeper` / `hive-metastore` / `hive-server2`

### 1.5 执行记录（2026-09-27）

**已完成**（脚本：`运维脚本/desktop_switch.sh`、`运维脚本/node_memory_tune.sh`）

| 动作 | 结果 |
|---|---|
| 关闭三节点桌面（graphical → multi-user） | 实测释放约 **600MB/台**，服务零影响 |
| Hadoop 各 daemon 堆上限 512m | 全部生效（`-Xmx512m` 已确认） |
| YARN RM/NM 堆 512m | 生效 |
| YARN 容器上下限 + vcore=2 | 已写入并通过 MR 作业验证 |
| MapReduce 容器 512m / JVM 重用 10 / 关推测执行 | 已写入并通过 MR 作业验证 |
| ZooKeeper 堆 1000m → 512m | 三台逐台重启，quorum 正常 |
| `vm.swappiness` 60 → 10 | 三台已生效 |
| MySQL 关闭 performance_schema | 已写入并重启成功 |
| swap 回收 | lrh-1 由 419MB → **0**，三台 swap 均归零 |

**内存实况对比**

| 节点 | 起始 available | 起始 swap | 现状 available | 现状 swap |
|---|---|---|---|---|
| lrh-1 | 601 MB | 419 MB | **约 1000–1250 MB** | **0** |
| lrh-2 | 2414 MB | 0 | **约 3000 MB** | 0 |
| lrh-3 | 2086 MB | 0 | **约 2700 MB** | 0 |

**健康验证**：HDFS `Live datanodes (3)`、YARN 三节点 RUNNING、真实 MR 作业
（`SELECT COUNT(*) FROM gmall.dwd_user_behavior_di` → 100000）正常返回、ADS 查询正常。

> ⚠️ **两处需要修正原规划的预期**：
> 1. **调小 JVM 堆不会立刻释放等量内存。** 原估"省 400–600MB"偏高——JVM 的 RSS 主要由 metaspace、
>    code cache、线程栈等非堆部分构成，之前也没吃满 1GB 堆。调小 `-Xmx` 的真实价值是**限制上限、
>    防止负载上来后膨胀**（原来 6 个 JVM 各可长到约 1GB，合计 6GB，对 3.8GB 节点是隐患）。
>    这也是为后续 Spark 做的必要准备。
> 2. **Hive 的堆上限被顺带从 256m 提到了 512m**（`HADOOP_HEAPSIZE_MAX` 也会被 Hive 读到），
>    MetaStore / HiveServer2 的 RSS 各涨约 125MB。考虑到后面要用 100 万条数据跑全流程，
>    512m 更稳妥，故保留。

**尚未执行**：vmx 内存变更（lrh-1 → 7168MB、lrh-2 → 3392MB），需三台关机后操作。

### 1.6 P1-b 执行记录：vmx 内存扩容（2026-09-27 12:0x～12:2x）

**执行方式**：全程自动化，用 VMware 命令行工具 `vmrun.exe` 完成（**本机 VMware 装在 `D:\VMware\`，不在默认的 Program Files**，一开始漏查了）。

完整流程与结果：

| 步骤 | 操作 | 结果 |
|---|---|---|
| 1 | 发现用户说的"已关机"实为**挂起**（判据：`.vmss`/`.vmem` 存在） | 停手，不冒险改配置 |
| 2 | `vmrun start <vmx> nogui` 恢复三台 | 30 秒后三个 IP 全通 |
| 3 | 确认服务完好（挂起 1:40 后恢复） | 5 个服务 active、HDFS 3 DN、YARN 3 节点 |
| 4 | 先停 hadoop（保护 HDFS 元数据）再 `systemctl poweroff` | 优雅关机 |
| 5 | 校验 `.lck`/`.vmem`/`.vmss` **三者全部为 0** | **真关机确证**（唯一可靠凭据） |
| 6 | 备份三份 `.vmx.bak` | 回滚路径就绪 |
| 7 | 改 `memsize`：lrh-1 4096→**7168**、lrh-2 4392→**3392** | 文件完整性校验通过（116 行、末行正常） |
| 8 | `vmrun start <vmx> gui` 开机 | 服务全部自启成功 |

**最终内存对比**

| 节点 | 修改前 available | 修改后 available | 提升 |
|---|---|---|---|
| **lrh-1** | 601 MB（原始基线） | **4123 MB** | **6.9 倍** |
| lrh-2 | 2414 MB | 2121 MB | 减配 1GB 后仍充裕 |
| lrh-3 | 2086 MB | 2792 MB | 关桌面 + 调优的净收益 |

三台 swap 均已归零。

### ⚠️ 1.7 暴露出的遗留缺陷：worker 节点开机自启不可靠（**待修**）

**现象**：三台虚拟机冷启动后，HDFS 只有 `Live datanodes (1)`、YARN 只有 `Total Nodes:1`——**只有 lrh-1 起来了，lrh-2/lrh-3 的 DataNode/NodeManager 没启动**。

**根因**（与 4.1 节的发现同源）：lrh-2 / lrh-3 上**没有 `hadoop.service`**，它们的守护进程是 lrh-1 的 `hadoop.service` 通过 SSH 远程拉起的。三台同时冷启动时，lrh-1 执行 `start-all.sh` 的那一刻 worker 还没准备好接受 SSH，于是**静默失败、不报错**。

**当前处置**：待三台都就绪后，在 lrh-1 上 `sudo systemctl restart hadoop` 即恢复正常（已验证：3 DN + 3 NM 全部回来，Missing blocks 为 0）。

**建议的根治方案**（待用户决定）：
- 方案 A（推荐）：在 lrh-2 / lrh-3 上各建一个只启动本机 DataNode + NodeManager 的 systemd 单元，让 worker 自力启动，彻底消除孤儿进程与启动时序依赖
- 方案 B：改造 lrh-1 的 `hadoop.service`，加等待所有节点可达（ping/SSH 探测）再执行 `start-all.sh`


---

## 二、Phase 2：ECharts 静态数据大屏

### 2.1 目标
产出一个**双击即可打开**的全屏数据大屏，展示 ADS 层核心指标，用于面试演示与答辩。

### 2.2 必须先实测的一个疑点

`Day17_Web展示页ECharts.md` 里写的导出方式是：

```bash
beeline -u jdbc:hive2://localhost:10000 -e "..." --outputformat=json > /tmp/ads_dau.json
```

**`--outputformat=json` 很可能不存在**。beeline 常见取值为 `table / vertical / xmlattr / xmlelements / csv2 / tsv2 / separated-value`，文档中并未列出 json。照抄大概率先报错。

**动工第一步就是实测**：
```bash
/opt/hive/apache-hive-3.1.3-bin/bin/beeline --help | grep -A3 outputformat
```
- 若支持 json → 直接用
- **若不支持（大概率）→ 走 csv2 路线**：`--outputformat=csv2 --silent=true` 导出 CSV，再用 Python 转 JSON

### 2.3 产出物清单

| 文件 | 作用 |
|---|---|
| `export_dashboard_data.sh` | 一键导出：查 5 张表 → CSV → JSON |
| `csv2json.py` | CSV → JSON 转换，处理表头与空值 |
| `dashboard_data.js` | 生成的**数据文件**（`window.DASHBOARD_DATA = {...}`） |
| `dashboard.html` | 大屏页面（单文件 + ECharts CDN） |
| `指标口径说明.md` | 每个指标的定义与计算逻辑，演示时讲得清 |

### 2.4 数据来源（全部是已跑通的 ADS 表）

| 图表 | 来源表 | 说明 |
|---|---|---|
| DAU 趋势折线 | `ads_dau` | 5 天（放大数据量后变 30 天） |
| 转化漏斗 | `ads_conversion_funnel` | 浏览 → 加购 → 下单 → 支付 |
| TopN 商品 | `ads_top_sku` | 横向柱状图 |
| 复购率 | `ads_repurchase_rate` | KPI 卡片 |
| GMV 趋势 | `dws_order_day` | 折线 / 面积图 |
| 客单价 | `dws_order_day` 派生 | KPI 卡片 |

### 2.5 一个关键的实现细节：不要用 fetch 读本地 JSON

浏览器在 `file://` 协议下会以 CORS 策略拦截 `fetch('data/dau.json')`，双击打开会**白屏**。正确做法是让导出脚本直接生成 **`dashboard_data.js`**：

```javascript
window.DASHBOARD_DATA = { "dau": [...], "funnel": [...], "top_sku": [...] };
```

页面用 `<script src="dashboard_data.js"></script>` 加载——`<script>` 不受 CORS 限制，双击打开即可正常渲染。这一步决定了"演示时能不能一键打开"，务必按此实现。

### 2.6 验收标准
- [ ] 双击 `dashboard.html` 直接出图，**不需要起任何服务**
- [ ] 5 个图表 + KPI 卡片全部有数据，无空白
- [ ] 页面数据与 `beeline` 直查 ADS 表的结果**逐项一致**
- [ ] 断网情况下页面仍可打开（ECharts 改本地引入版本，或接受 CDN 依赖并备一份离线包）
- [ ] 附指标口径说明，能口头解释每个数字怎么算出来的

---

## 三、Phase 3：Spark 双引擎改造与性能对比

### 3.1 版本决策：Spark 3.5.x（不是 4.2.0）

已查证的事实：

| 项 | Spark 3.5.x | Spark 4.x（最新 4.2.0） |
|---|---|---|
| JDK 要求 | Java 8 / 11 / 17 | **Java 17+（JDK 8/11 已移除）** |
| Scala | 2.12（3.5 主版本） | **2.13** |
| ANSI SQL 模式 | 默认关闭 | **默认开启**——除零/非法转换从返回 NULL 变为抛异常 |
| 生命周期 | 2026-04 已 EOL | 当前长期支持 |

**选 3.5.x 的理由**：集群是 JDK 8 + Hadoop 3.3.6，Hadoop 3.3 对 JDK 17 支持不完善；Spark 4 的 ANSI 默认开启会让现有 Hive SQL 出现难以定位的运行时异常。
> 面试加分点：能答出"因为集群 JDK 8、且 Spark 4 默认开 ANSI 会改变 NULL 语义，所以选 3.5 LTS"——比"随手装最新版"专业得多。

### 3.2 前置：数据量放大（不做这步，性能对比无效）

| 项 | 现状 | 目标 |
|---|---|---|
| 行为日志 | 10 万条 / 5 天 | 100 万+ 条 / 30 天 |
| 订单 | 5 万条 | 30 万+ 条 |
| 商品 | 500 条 | 保持不变或扩到 5000 |

**操作**：调整 `mock/generate_mock_data.py` 的参数 → 重新生成 → `split_by_date.py` 按天拆分 → `batch_load.sql` 装载 → 重跑全链路。
**磁盘评估**：100 万条日志约 200MB，HDFS 3 副本约 600MB；订单同理。三台各 30GB 磁盘，**空间充足**。若后续想扩到千万级需重新评估。

**同步产出**：以放大后的数据量重跑一遍 **Hive(MapReduce) 全链路，记录每步耗时**，作为对比基线。

### 3.3 安装与元数据打通（最大的技术风险点）

**安装**：Spark 3.5.x 预编译 Hadoop3 版本，解压到 `/opt/spark/`。先在 lrh-1 装（driver 节点），需要本地模式跑时再考虑分发到 lrh-2/3。

**元数据兼容配置**（已查证，Hive **3.1.3 被 Spark 原生支持**，无需重编译）：

```properties
# $SPARK_HOME/conf/spark-defaults.conf
spark.sql.hive.metastore.version    3.1.3
spark.sql.hive.metastore.jars       path
spark.sql.hive.metastore.jars.path  file:///opt/hive/apache-hive-3.1.3-bin/lib/*
```

**已知坑（务必处理）**：
1. 需把 Spark 自带的 `commons-collections-3.2.2.jar` 复制到 `/opt/hive/apache-hive-3.1.3-bin/lib/`，否则报 `ClassNotFoundException: org.apache.commons.collections.CollectionUtils`
2. 启动日志出现 `Version information not found in metastore ... recording the schema version 3.1.0` 属**正常现象**，不是错误
3. 另需确认 `hive-site.xml` 能被 Spark 读到（复制到 `$SPARK_HOME/conf/` 或显式 `--conf` 指定）
4. **不要用 `spark.sql.hive.metastore.jars=maven`**——需要联网拉包，且官方不推荐用于生产

**冒烟验收（Phase 3 的分水岭）**：
```bash
spark-sql -e "SELECT count(*) FROM gmall.ads_dau;"
spark-sql -e "SHOW TABLES IN gmall;"
```
**这两条跑通 = 最大风险已排除**，可以继续往下；跑不通就先停下来排查，不要硬推。

### 3.4 ETL 双引擎改造

**原则：不覆盖 Hive 链路的结果**，两条链路并存以便对比与回滚。

- 方案 A（推荐）：Spark 输出到独立库 `gmall_spark`
- 方案 B：表名加后缀，如 `dwd_user_behavior_di_spark`

**改造范围**：`clean_dwd.sql`、`etl_dwd_dim.sql`、`etl_dwd_fact.sql`、`etl_dws_user.sql`、`etl_dws_sku.sql`、`etl_dws_order.sql`、`etl_ads_opt.sql`（共 7 个）

**改造要点**（Hive SQL → Spark SQL 并非 100% 照抄）：
- Hive 专有函数（如部分 `get_json_object` 写法、`collect_list` 的差异、正则函数）需逐个验证
- 分区表写入语法、`INSERT OVERWRITE ... PARTITION` 行为差异
- 数据类型隐式转换：Hive 更宽松，Spark 更严格，容易在 join/聚合处报错
- 建表语句：Spark 侧建议显式指定 `USING parquet` 或沿用 Hive SerDe

### 3.5 性能对比（核心交付物）

在同一批放大后的数据上，逐步骤记录耗时：

| 步骤 | Hive (MapReduce) 耗时 | Spark SQL 耗时 | 加速比 |
|---|---|---|---|
| dwd_clean | | | |
| dws_user | | | |
| dws_sku | | | |
| dws_order | | | |
| ads_opt | | | |
| **端到端** | | | |

**同时必须核对结果一致性**：行数、关键指标值（DAU、漏斗各环节、GMV）两条链路必须对齐，否则加速比没有意义。

**结论表述建议**：区分开"小数据量启动开销占主导"与"数据量上来后 Spark 的优势体现"，这比简单说"快 N 倍"更有说服力，也是面试官爱问的深挖点。

### 3.6 可选扩展（先不做，视余力）
- 挑 1–2 个作业用 **PySpark DataFrame API** 重写，展示 Python 能力
- 把 Spark 作业接入 **DolphinScheduler**（用 Spark 节点或 shell 调 `spark-submit`），形成双链路可切换
- Structured Streaming 做准实时 —— **不建议现在做**，内存吃紧且范围会失控

### 3.7 验收标准
- [ ] `spark-sql` 能查询 gmall 库（元数据打通）
- [ ] 7 个 ETL 脚本全部改造完成，Spark 链路端到端跑通
- [ ] Spark 链路产出与 Hive 链路结果**逐项一致**
- [ ] 产出 Hive vs Spark 逐步骤耗时对比表
- [ ] Hive 链路未被破坏，仍可独立运行

---

## 四、风险登记表

| 风险 | 概率 | 影响 | 应对措施 |
|---|---|---|---|
| 改错 vmx 导致内存给错节点 | 中 | 中 | Phase 1 Step 0 强制确认节点身份 |
| 宿主机内存不足引发换页 | 中 | 高 | 先用保守方案 8192/4096/4096；扩容前清理宿主机占用 |
| lrh-1 内存仍不够跑 Spark driver | 中 | 高 | 显式限制 driver 内存；必要时 Spark 改 local 模式或只在 lrh-1 单机跑对比 |
| Spark 读不到 Hive 元数据 | 中 | 高 | jars=path 方案 + 补 commons-collections；先做冒烟验证再推进 |
| `beeline --outputformat=json` 不可用 | **高** | 低 | 已预案：改走 csv2 + Python 转换 |
| 本地 JSON 被 CORS 拦截导致大屏白屏 | **高** | 中 | 已预案：导出为 `dashboard_data.js` 用 script 标签加载 |
| 数据量放大后跑批时间过长 | 高 | 中 | 先用单天数据验证；长任务后台 `setsid nohup` 跑 |
| Hive SQL 直接搬 Spark 报语法错 | 中 | 中 | 建立语法差异清单，逐条验证 |
| 虚拟机磁盘被放大后数据写满 | 低 | 中 | 每台 30GB，百万级数据约 600MB×2，空间充足；跑前 `df -h` 确认 |
| 长时间改动期间环境不可用 | 中 | 低 | 每个 Phase 结束都保证环境可跑通、可演示 |

---

## 五、开工前需要你拍板的 5 个问题

1. **lrh-1 扩到 8GB 可以吗？** worker 是保持 4GB（保守，宿主机留 15GB）还是同时提到 6GB（充足，宿主机留 11GB）？
2. **SecondaryNameNode 要不要迁到 lrh-2？** 收益约 300MB，但要改 `hdfs-site.xml` 并同步配置，有一定操作量和出错面。不做也行。
3. **宿主机那 18GB 占用是谁？** 需要你开机后看一眼任务管理器——如果宿主机本身已经很满，扩容方案要相应收敛。
4. **Spark 的输出写到哪？** 独立库 `gmall_spark`（推荐，干净）还是表名加 `_spark` 后缀？
5. **数据量放大到什么规模？** 100 万条 / 30 天（推荐，稳妥）还是更激进？

---

## 六、Phase 与交付物总览

| Phase | 内容 | 交付物 | 谁来定 |
|---|---|---|---|
| **P1** | 内存扩容 + 组件瘦身 | 扩容前后的资源对比数据、稳定的集群 | 需你确认问题 1–3 |
| **P2** | ECharts 数据大屏 | `dashboard.html` + 导出脚本 + 指标口径说明 | 需你确认是否接受 CDN 依赖 |
| **P3-pre** | 数据量放大 | 放大后的数据集 + Hive 基线耗时 | 需你确认问题 5 |
| **P3** | Spark 双引擎 | Spark 链路 + Hive vs Spark 对比表 | 需你确认问题 4 |
| 收尾 | 文档更新 | 更新 `项目介绍与环境架构.md`（已过时）、新增 Spark 章节 | — |

---

*本规划中标注「已核实」的数据均来自 2026-09-27 的实际读取（vmx 文件、宿主机硬件查询）或公开文档查证；标注「待实测」的项需在动工时验证。*
