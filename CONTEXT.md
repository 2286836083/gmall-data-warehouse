# CONTEXT · 项目上下文包

> **这个文件是给「任何 AI 助手」看的** —— 不限于某一个工具。
> **新会话第一句话，请让我读这个文件 + `项目状态.md`**，我就能接上，不需要你重新解释一遍项目。
>
> 维护约定：**项目"是什么样"变了才改这里**（不是每次干活都改）。进度变化的记录放 `项目状态.md`。
> 最后更新：2026-10-05（对标 Olist 补齐建模短板：30 张表粒度声明 / dim_date 时间维 / 分层消费修正）

---

## 〇、三十秒了解这个项目

**gmall 电商数仓** —— 一个跑在**本地三节点 VMware 集群**上的完整离线数仓项目，用来支撑求职简历与现场演示。

**数据链路（双链路并存）**：

```
【旧链路】模拟数据 15 张表
  → 全流程跑通，旧版大屏 dashboard.html 的数据源（已保留，可切换对照）
  → 问题：ads_dau 每天恒为 1000、复购率恒在 0.99 以上，折线画出来是直线

【新链路】真实数据 15 张表（REES46，1063 万行）
  ODS ods_event_log_di
   → DWD dwd_event_log_di / dwd_purchase_fact_di + DIM dim_product / dim_date
    → DWS dws_user_event_day / dws_product_event_day / dws_category_event_day / dws_trade_day
     → ADS ads_active_user_day / ads_funnel_day / ads_repurchase_day / ads_top_product_day
           + ads_category_day / ads_trade_day      ← 后 2 张 2026-10-05 补建（修跨层消费）
  → 已全部跑通（31 分区）
  → **已接入 DS 工作流 `gmall_evt_pipeline`（16 节点）并跑通；新版大屏 dashboard_real.html 用的就是它**
```

**分层消费规范（2026-10-05 修正后）**：大屏 → ADS → DWS/DIM → DWD → ODS，**应用层只消费 ADS**。
（修正前大屏的类目图与交易图直接读 DWS，跳过 ADS，已补表改正）

**一句话现状**：真实数据链路已打通到 ADS，并接入 DolphinScheduler 与新版大屏；**Spark 3.5.9 双引擎对比已于 2026-10-04 完成** —— 剩下的是「把 Spark 接进 DS」这类延伸项，主线三步（内存 → 可视化 → Spark）**全部走完**。

---

## 一、必须遵守的硬约定（用户明确要求，不可协商）

### ① 进度条约定

**跑耗时程序（Hive ETL / MR / Spark / 大数据导入）必须给进度条，不能闷头跑。**

- 进度必须来自**真实可观测信号**（步骤状态文件 + MR 百分比），**不许拍脑袋估**
- 长任务一律 `setsid nohup ... &` 放后台，远端写状态文件，本机轮询读取
- 现成工具：
  - 集群侧 `dwd_run.sh` / `evt_run.sh`（分步执行 + 写 `<run>/status.tsv`）、`dwd_progress.sh`（探针）
  - 本机侧 `项目产出/进度看板/serve.py [端口] [dwd_run|evt_run]` → `http://localhost:8765/`
- 阻塞等待用**值班循环**（探测到状态变化立即返回），不要空 `sleep`

### ② 文档同步约定

**环境变更的收尾步骤里，必须包含「同步改说明书类文档」。**

- **说明书类**（`项目介绍与环境架构.md`、`虚拟机集群使用指南.md`、`项目产出/项目架构图.html`）描述"现在是什么样"，**会随每次变更持续失真**；实验报告/讲义属**快照类，允许过时**
- **绝不凭记忆改文档** —— 每处订正必须指向独立证据（`.vmx` 文件 / `fix_*.sh` / 架构图端口表 / 实验报告实测输出）
- **错误的"原因"比错误的"现象"更危险**：现象错了照做会失败、能立刻发现；原因错了会带着错误世界观排查其他问题且永远发现不了。**写原因必须能回答「我怎么证明这个因果」**
- 判断一处错误要不要修，问的不是"准不准"，而是"**有没有人会根据它做决定或执行操作**"
- 改完做**交叉自洽检查**：同一事实（内存/端口/表数/unit 名）在总览、手册、架构图里必须一致

### ③ 实验报告约定

**每次实验或环境改动后，必须写报告到 `实验分析/实验NN_主题.md`**（模板 `_实验报告模板.md`）。

硬性要求：

1. 读者是**面试官 + 零基础小白** —— 专有名词首次出现必须给类比，不能假设读者有基础
2. 每个判断都要写成「**假设 → 行动 → 实测结果 → 成立与否**」的可验证表格
3. **被证伪的假设必须写进报告**并解释为什么会错 —— 只写成功的报告等于宣传材料
4. 必须跑**真实业务动作**做端到端验证，不能只看服务状态是 active
5. 必须包含「面试官可能会追问的 FAQ」和「回滚命令」
6. 报告开头要有「按读者身份选择阅读路径」的指引表

### ④ 文件存放约定

| 目录 | 放什么 | 规则 |
|---|---|---|
| `项目产出/` | **AI 生成的交付文件** | 索引见 `项目产出/说明.md`；子目录：`运维脚本/`、`数据接入/`、`可视化/`、`进度看板/`、`组件详解/` |
| `实验分析/` | **实验报告** | 报告 `实验NN_主题.md`；**辅助材料用 `_` 前缀**（不占编号，如 `_实验报告模板.md`） |
| `其他/` | **杂项**（调研、笔记、问答） | 不占编号、不进 `项目产出/` |
| `D:\数据集\` | 大数据文件（原始数据集、采样产物） | **故意放项目外**，避免 OneDrive 同步数 GB |
| `D:\OneDrive\桌面\知识库\数据开发\` | **学习材料** | 补"原理类"内容优先放这里，**不放 `项目产出/`** |

⚠️ `项目产出/可视化/` 里 `dashboard.html` / `dashboard_data.js` / `echarts.min.js` **必须始终同目录**（页面用相对路径引用）

### ⑤ 讨论 vs 决策

**没落盘的结论等于没发生。** 对话里讨论的内容，如果没写进文件，**视为未决定**。
凡是"要改项目"的决定，必须落到 `实验分析/` 或记忆里，否则下次会被当成没说过。

---

## 二、关键路径

| 用途 | 路径 |
|---|---|
| **项目工作区** | `D:\数据开发\` |
| **环境配套**（SSH 密钥 + 运维脚本 + ISO） | `D:\数据开发环境\VMware\` |
| 旧位置 | ✅ **2026-10-04 已永久删除**（删除前已打包备份至 `D:\数据开发环境\backup\旧项目位置_20261004.zip`） |
| 学习知识库（**在 OneDrive，未迁出**） | `D:\OneDrive\桌面\知识库\数据开发\` |
| 大数据文件 | `D:\数据集\` |

**SSH 进集群**（三台互信，免密直达）：

```bash
ssh -i "D:\数据开发环境\VMware\sshkeys\cluster_ed25519" lrh@192.168.87.131
```

**集群内路径**（三台一致，MySQL/Hive 仅 lrh-1）：

```
JDK         /opt/jdk/jdk1.8.0_202
Hadoop      /opt/hadoop/hadoop-3.3.6
Hive        /opt/hive/apache-hive-3.1.3-bin
ZooKeeper   /opt/zookeeper/apache-zookeeper-3.8.7-bin
DS          /home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin
配置备份     /opt/backup/<日期>/
脚本目录     /home/lrh/workspace/new_chain/  ← 新链路（真实数据），全部用 beeline -f 提交
             /home/lrh/workspace/old_chain/  ← 旧链路（mock 数据，2026-09-23 后停用）
             /home/lrh/workspace/            ← 只留通用工具：verify.sh / verify_report*.txt / 中文注释修复三件套
             ⚠️ 2026-10-20 分家，清单见 `项目产出/workspace文件清单_集群端.md`
```

---

## 三、集群事实

| 文件夹 | MAC | 节点 | IP |
|---|---|---|---|
| `D:\虚拟机` | 00:0c:29:f1:0f:d0 | lrh-1 | 192.168.87.131 |
| `D:\虚拟机2` | 00:0c:29:2e:b5:05 | lrh-2 | 192.168.87.130 |
| `D:\虚拟机3` | 00:0c:29:66:8c:42 | lrh-3 | 192.168.87.132 |

- 版本：Hadoop 3.3.6 / Hive 3.1.3 / **Spark 3.5.9** / MySQL 8 / ZooKeeper 3.8.7 / DS 3.1.8 / JDK 8 / Ubuntu 24.04 Desktop（桌面已关，省 600MB/台）
- **Spark 3.5.9**（`-bin-hadoop3`，2026-10-04 装）在三台 `/opt/spark-3.5.9-bin-hadoop3`（429MB）
  - 配置：`master=yarn` + `deployMode=client` + **`spark.sql.sources.partitionOverwriteMode=dynamic`**（不设会清空整表！）+ `shuffle.partitions=6`
  - Hive 集成：`spark.sql.hive.metastore.version=3.1.3` + `jars=path` 指向 `/opt/hive/apache-hive-3.1.3-bin/lib/*`（内置 hive 2.3.9 直连不兼容）
  - jar 分发走 HDFS `hdfs://lrh-1:8020/spark/jars/*`（252 文件 / 338MB），event log 在 `/spark/eventlogs`
  - ⚠️ **YARN 每节点容量只有 2048MB / 2 vCores，单容器上限 2048MB** —— Hive 与 Spark 的所有内存参数都被这个卡死
  - 对比用隔离库：`gmall_bench`（Spark 产出）/ `gmall_bench_hive`（Hive 产出），**用完可整库 `DROP DATABASE ... CASCADE`**，`gmall` 库零写入
- 三台 VM 的 displayName **全叫** `Ubuntu 64-bit`，只能靠文件夹名或 MAC 区分
- 内存配置：lrh-1 **7168MB** / lrh-2 **3392MB** / lrh-3 **4096MB**（扩容后，`available` 从 601MB 涨到 4123MB）
- 数仓 `gmall` 库共 **30 张表** = 旧链路 15 张（模拟数据，旧版大屏 `dashboard.html` 的数据源）+ 新链路 15 张（真实数据 12 张 + 2026-10-05 新增 `dim_date` / `ads_category_day` / `ads_trade_day`）
  - ⚠️ `tmp_order_user_cnt` **不是垃圾**（被 `etl_ads_opt.sql` 引用，勿删）
  - ★ **全部 30 张表都已带统一格式的粒度声明** —— `desc` 一打就能看到 `【链路】新·真实 ｜ 【粒度】用户 × 天 ｜ …`
  - ★ 表清单 / 粒度 / 业务问题映射见 `项目产出/数仓表清单与业务问题映射.md`（**做指标前先看它第六节「口径易错点」**）
- DS 工作流共 **4 个**：
  - `gmall_evt_pipeline` —— **16 节点，真实数据链路**（ODS 就绪 → DWD DDL → DWD 三张表 → DWS×4 → ADS×4 → 三层校验）**已上线** ← **正式链路，全 Hive 跑，写 `gmall` 库**
  - `gmall_evt_pipeline_spark` —— **11 节点 Spark 版**（2026-10-15 新建，preflight → bench_ddl → DWS×4 → ADS×4 → reconcile）**已上线**，首次运行 11/11 SUCCESS（14m46s）
    ★ 它是**旁路/对照组**：写隔离库 `gmall_bench`，**源表仍读 `gmall`**（输入一致）；**正式数据零改动**
    ⚠️ **不是"前段 Hive 后段 Spark"的分工** —— 正式链路从头到尾都是 Hive 产数据
  - `gmall_dw_pipeline`（9 节点，**旧模拟链路**）**已下线**；`gmall_dw_pipeline_tail`（5 节点，旧链路尾段）已上线
  - ⚠️ `t_ds_schedules` **仍然是空的** —— 文档里写的"按天定时跑数"**从未配置过**
- **DS 不自启**，重启后手动拉起 master→worker→api
- DS 工作流的**分段 SQL** 在 `/home/lrh/workspace/new_chain/ds_steps/`，由 `ds_prepare_steps.sh` 从主 SQL 切分、参数从 runner 抽取（勿手改，会重生成）
- 大屏（两版并存，双击即开、离线可用）：
  - `项目产出/可视化/dashboard_real.html` —— **真实数据链路**（当前推荐演示用）
  - `项目产出/可视化/dashboard.html` —— 旧模拟链路，原样保留可对照
  - ⚠️ 各自的 html / `dashboard_data*.js` / `echarts.min.js` **必须同目录**（页面用相对路径引用）

---

## 四、已知陷阱（踩过的坑，别再踩）

### 集群与运维

1. **lrh-2/3 上没有 `hadoop.service`** —— 只有 `hadoop-worker`。那两台的 DataNode/NM 是 lrh-1 的 hadoop.service 经 SSH 拉起的**孤儿进程**
   - ⚠️ **别把这个推成"停 lrh-1 的 `hadoop` 就会停掉它们"**（2026-10-04 实测证伪）：停完 lrh-1 `hadoop` 后，lrh-2/3 **仍各有 1 个 DataNode + 1 个 NodeManager**，直到 `stop hadoop-worker` 才退出 → 它们实由**各节点自己的 `hadoop-worker` 单元**管理。**关机时两处都要停，缺一不可**
2. **`systemctl is-active` = active ≠ 服务可接受连接**：HiveServer2 被标记 active 后**至少 50 秒**才真正监听 10000 端口。判可用必须用**外部判据**：`ss -lntp | grep 10000` + 发一次真实查询
3. **区分「挂起」与「真关机」只看 `.vmss`/`.vmem`，不看 `.lck`**：`.vmss` 在→挂起；两者都不在→真关机。`.lck` 常在（VMware 窗口还开着）；ping 不通**不能**作判据
4. **优雅关机顺序**（不可颠倒）：
   lrh-1 `stop hadoop` → lrh-2/3 `stop hadoop-worker` → lrh-1 `stop hive-server2 hive-metastore mysql` → 三台 `stop zookeeper`（lrh-3→2→1）→ `poweroff`
   - 停 ZK/Hive 后 `is-active` 显示 **`failed`** 是 systemd 对 SIGTERM 的记账方式，**不是故障**
   - ⚠️ **关机前必须先停本机侧轮询脚本**（进度看板）：RM 停后 `mapred job -status` 会挂住不退出，5 秒轮询会堆积僵尸进程
5. **改 vmx 必须三台关机**；关机前先 `stop hadoop` 保护 HDFS 元数据
6. 重启后 **DS 不自启**；顺手确认 `hdfs dfsadmin -report` 有 **3 个 Live datanodes**

### 命令与调试

7. **中文不要塞进 `ssh` 命令行**（Git Bash 按本地代码页编码会破坏参数 → 静默返回空）。**只有输入方向坏、输出正常**，极易误判为远端故障。同理**中文路径不要作为命令行参数传给 Windows 程序**（`python -m py_compile "D:\中文路径\x.py"` 会报"文件不存在"）。用 `scp` / heredoc / 脚本内部构造路径
8. **`rows` 是 Hive 保留字**，作别名会让语句解析失败；stderr 被丢时表现为**静默返回空**，会被误判成"数据丢了"。用 `row_cnt`
9. **非交互 SSH 里裸写 `yarn` 会 command not found**，加 `2>/dev/null` 就变成"看起来合法的空答案"。**一律用全路径**
10. **`beeline --outputformat=json` 不可用**（合法值只有 `table/vertical/csv2/tsv2/dsv/csv/tsv`）→ 用 `csv2` + Python 转 JSON
11. **用 `mysql` 客户端必须加 `--default-character-set=utf8mb4`**

### Hive 执行（★ 最重要）

12. **★ 调优参数必须按「作业类型」分层，不能一套走天下**
    - 判断依据不是作业类型名，而是「**这个作业有没有要同时装住大量数据的结构**」
    - **不囤数据的**（写入型/清洗型）：容器压小**不会直接失败**（瓶颈在磁盘 IO）→ 可以压
    - **囤数据的**（大基数 `GROUP BY` / `DISTINCT` / 非 MapJoin / 全表 `ORDER BY`）：容器小**必然失败** → 必须放大
      （实测：`GROUP BY product_id, dt` 达 430 万组合，512MB/`-Xmx410m` → `GC overhead limit exceeded`；调到 **1024MB/`-Xmx820m`** 后 **637s 失败 → 63s 成功**）
    - ⚠️ 旧 DWS 脚本头部本来写着 `-Xmx768m`（标了 `-- ds-tuned`），被"统一压到 512/410"时**抹掉了分层设计**。**统一配置看起来整洁，实际是丢信息**
13. **`GC overhead limit exceeded` 不在 `Caused by` 链里**，只在 task 的 `Diagnostic Messages`。**定位靠量级不匹配**——看 `HDFS Read` 与 `Cumulative CPU` 是否符合预期：
    - Read 远小于输入量 → 死得很早 → **内存/初始化**问题
    - Read 正常但 Write = 0 → 算完了写不出去 → **写入并发**问题
    - Read/Write 都正常但耗时长 → **搬运/倾斜**问题
14. **"参数文件生成了" ≠ "参数生效了"**：公共 SET 写在 `setup.sql` 必须**拼进每个分段 SQL** 才生效（曾因此报 `Error 10096`）。**配置类动作都要在启动前 `grep` 自检，查不到就终止**
15. **跨表写 SQL 前先 `DESC` 所有涉及的表**，尤其**自己几天前设计的表**（曾因写不存在的 `user_session` 报 `Error 10004`）
16. **长任务运行器一律支持断点续跑**（`bash evt_run.sh <起始步骤号>`）
17. **`hdfs dfs -du -h <目录>` 两列 = 实际大小 / 含副本占用**（后者 = 前者 × 副本数）→ 看**写入放大**最直观
18. **抓容器要抓 `java` 那一层**：容器进程树是 `bash -c → java`，`ps -ef | grep YarnChild` 会先抓到外层 bash（`VmRSS` 仅几 MB）。用 `jps -l | grep YarnChild`
19. **`dfsadmin -report` 的并发流字段是 `Xceivers`**（不是 `XceiverCount`）
20. **报错里 `0 of the 1 minReplication nodes` 的那个 `1`** = `dfs.namenode.replication.min`（实测值 **1**），是写入**底线**，不是副本**目标**（3）

### 已根治（别再怀疑）

21. **Hive 元数据中文注释乱码**：根因是官方 metastore schema 把注释列声明为 **latin1**，而 JDBC 是 utf8mb4 → 中文被静默替换成 `?`（0x3F）。已迁移 12 个列。诊断法：**先 `HEX()` 判断「显示型」还是「存储型」（`3F3F`）**。修复脚本 `项目产出/运维脚本/fix_metastore_charset.sh`。已排除的嫌疑：JVM `file.encoding`、JDBC 连接串、MySQL 服务端
22. **大屏不能用 `fetch` 读本地 JSON**（file:// 下 CORS 拦截白屏）→ 导出为 `dashboard_data.js` 用 `<script src>` 加载

### DolphinScheduler 编排（2026-10-03 新增）

23. **★ DS 停任务杀不掉远端 `beeline`** —— 这是老坑 4 的再次发生，而且更隐蔽。停任务后 DS 里 `state=9 (KILL)`、`exitStatusCode:143`，但 **lrh-1 上的 beeline 还活着**，实测跨了 3 天把 `etl_dws_order.sql` 5 天数据全跑完了。根因：任务脚本是 `ssh lrh-1 'beeline …'`，**非交互会话没有终端**，ssh 断开时 sshd **不发 SIGHUP**，远端进程变孤儿；DS 只杀得掉 worker（lrh-2/3）上的进程树。
    - **判据**：DS 状态是终态**不等于**任务停了。必须补 `ps -ef | grep jdbc:hive2` + `yarn application -list` 从**数据面**确认（"控制面"与"数据面"要分开验证）
    - **处置**：`pkill -f 'jdbc:hive2'`（确认没别人在跑）
    - **预防**：新工作流的第 1 个节点 `preflight` 就检查残留，有就拒绝开工
24. **退出码先于日志文本**：判"被杀"还是"跑错"先看退出码 —— `143`=SIGTERM（被叫停）、`137`=SIGKILL（常见 OOM）。09-30 那次日志尾部像 SQL 报错，实际是**被人主动停的**（state=5 STOP / task state=9 KILL）
25. **DS REST 接口有 3 个文档没写的坑**（建工作流必踩，详见 `手把手教程/DolphinScheduler全流程排障汇总.md` §一.1）：
    - `releaseState` 要传**枚举名** `ONLINE`/`OFFLINE`，传 `1`/`0` 只回笼统的 `10108`
    - `timeout` 必须落在 **(0, 86400]**，传 `0`（"不超时"的直觉写法）回 `10002` —— 取值范围是 `javap -c` 反编译 `ExecutorServiceImpl` 读出来的
    - `query-by-name` 接口**是坏的**（对存在的定义也回 50003）→ 改用分页列表接口自己按名字找
    - 另：**请求里指定的 `code` 会被忽略**，必须回读真实 code（老坑 14 再次确认）
26. **时段/时长别用 `uptime` 和 `ps lstart` 推因果**：这台 VM 是"开机 → 挂起 → 多日后恢复"，恢复时**墙钟跳了约 3 天**，但 `/proc/uptime` 不含挂起时间 —— 表现是 `uptime` 说"up 33 min"、`ps` 把**所有**进程启动时间都往后推了 3 天。判"挂起 vs 重开机"要看 `journalctl --list-boots` 的 boot 区间（补充 §四.3）
27. **★★ 千万别用"从指定节点重跑"**（接口的 `startNodeList`，网页上是「从当前节点开始重跑」）：实测 **3/3 必挂** —— master 进入 `DagHelper.parsePostNodes(DagHelper.java:306)` NPE **死循环**，CPU 打满、日志每秒涨几千行（2 分钟 179MB）、实例永远 RUNNING 且**一个任务都不下发**；**重启 master 修不好**，必须停 master + 从库里删掉那条实例。**要重跑就整条重跑**（ETL 幂等，约 54 分钟）。`ds_evt_pipeline.py` 已移除该功能。详见排障汇总 §1.6
    - ⚠️ 这条**证伪了排障汇总 §3.1 原来的结论**（"重启即可清除"是错的）—— 当时起作用的是"删实例"，不是"重启"
28. **★★ 画布上 16 个节点只剩 1 个 → 查 task code 是不是超过 2^53**（这条我当天先写错过一次，更正如下）：
    - **真根因**：DS 接口把 code 当 **JSON 数字**返回，浏览器用 JS 双精度浮点解析，**只能精确到 2^53 = 9007199254740992**。超过之后**多个不同 code 塌缩成同一个数** → DAG 只剩 1 个节点（且没有连线）。判据：`select max(code) from t_ds_task_definition` **> 2^53** 就是它
    - ⚠️ **数据库/master 是 64 位整数，所以执行完全正常**（16/16 SUCCESS、行数全对）—— **跑得对、显示全错**，所有自动化测试都发现不了，只能人眼看画布
    - **解法**：**在网页上拖节点建，让 DS 自己生成 code**（实测 2.3e13，安全）；程序化建时自己指定的 code **必须 ≤ 2^53**（如 14 位 `33000000000001`），`ds_evt_pipeline.py` 已加 assert + `info` 自检
    - **历史遗留**：旧工作流 `gmall_dw_pipeline` / `gmall_dw_pipeline_tail` 的 code 也是 19 位手写值（1.1e18 / 2.2e18），**所以它们一直只显示 1 个节点**，只是没人打开看
    - **次要问题（同一天也踩了）**：节点坐标间距必须 **≥ 220px**（DS 节点宽 180~200px），否则即使 code 修好也会挤成一团。`NODE_DX = 220`
    - **教训**：跨"执行链路(Java/Python 64位整数) ↔ 显示链路(浏览器双精度)"传 ID 时，**要么用字符串，要么确认范围**
29. **`PUT` 更新工作流定义会往 `*_log` 版本历史表再写一份**，导致同一 task code 出现两个版本 → API 返回的 `taskDefinitionList` **节点数翻倍（16 变 32）**。清库时必须**连 `t_ds_task_definition_log` / `t_ds_process_definition_log` 一起清**。这也解释了 `gmall_dw_pipeline`（PUT 更新过一次）为何一直跑不起来：**本 build 下改工作流的可靠做法是"删掉重建"，不是"更新"**

### 诊断手法（★ 会让你得出"看起来很正式的错误结论"，2026-10-04 新增）

30. **★ `jps` 返回空 ≠ 进程不存在**：lrh-1 上 systemd 用 **root** 启动的 JVM（含 DS 全部服务）**不在 lrh 用户的 `hsperfdata` 里**，`jps -l` 会返回空。曾差点据此报"DS 没跑"（实际没跑是巧合，判据本身是坏的）。
    - **真判据**：`ps -ef | grep "[d]olphinscheduler"`（模式串用方括号防自匹配）＋ 端口 `ss -lnt` ＋ 日志 mtime
31. **★ 用 `grep` 数 `ps` 输出时，你这条命令自身的命令行文本也被 `ps` 算进去** → 计数虚高。曾这样数出 `MasterServer=3`、`WorkerServer=3`，而同一时刻精确查询是**空**。用 `[M]asterServer` 打断字面量
32. **★ ZK 里数 DS 注册节点，别用 `grep -c "server-"`**：DS 3.1.8 的注册节点名是 **`ip:port`**（不是 `server-xxxxx`），且 `zkCli.sh` 的 `ls` **把整个列表打在一行** → `grep -c` 数的是行数，**无论几个节点都返回 1**。曾据此误报"worker 只注册上 1 个"（实际 2 个都在）。
    - 数法：`... | grep "^\[" | tr ',' '\n' | grep -c ':'`；**更优解**：走 API `/monitor/workers`（结构化 `host` 字段）
33. **★ HDFS 报 "Under-replicated blocks" 先看副本因子，别急着查损坏**：本集群实测的 13 个"欠副本"全是**副本因子 = 10** 的作业暂存文件（`/tmp/hadoop-yarn/staging/**/job.jar`、`/tmp/hive/**/map.xml`），而集群只有 **3 台 DataNode → 永远凑不齐**。根因是 `mapreduce.client.submit.file.replication` **默认 10**。
    - **判据**：`fsck <路径> -files -blocks | grep -i "under replicated"` —— 若消息是 `Target Replicas is 10 but found 3`，就是**配置造成的假警报**，与数据损坏无关
    - **处置**：删掉陈旧暂存即可归零。**本集群已决定不改配置** → 每次跑批后该告警**会再次出现，属预期现象，不必再排查**
34. **本机删文件走的是 `safe-delete` 保护钩子**（`Remove-Item` 被接管 → 送回收站），行为要记牢：
    - **中文路径 + 目录内有文件被占用** → 报 `trash operation ... Some operations were aborted` 并 **fail-closed（拒绝删除）**。报错信息里的路径会显示成乱码（`桌面`→`妗岄潰`），**那是显示问题，不一定是真因**
    - **ASCII 路径能删，但钩子会把成功误报成 `trash-failed`**（detail 却写着 `OK`）→ **判成功与否要看 `Test-Path`，不要看它的报错**
    - **OneDrive 下的目录搬迁/删除极易被句柄打断**：`Move-Item` 可能只搬一半、`Permission denied`（可读但不可写不可删 = 被进程独占）。绕法：**先搬内容到纯 ASCII 中转目录，再删中转目录**
    - 环境**同时禁用了 `Add-Type` 与 COM 实例化** → 拿不到 `Microsoft.VisualBasic` 的回收站 API，也没法用 Shell 的 `delete` 动词
    - 补充（2026-10-05）：`D:\_del_old_bigdata_20261004\sshkeys\db_ed25519` 至今删不掉，报的是**「访问被拒绝」(Access Denied)** —— 即**不是** Sharing Violation、**不是**文件被进程锁；`rm -f` / `Remove-Item` / `[System.IO.File]::Delete` 全失败。**真因未查明**（猜测 ACL 或需管理员权限，**未验证**），文件已含在备份 zip 中
    - 另：`cmd.exe /c ...` 会被安全策略直接拦截（"Invoking cmd.exe from Bash bypasses all command validation"），想用 cmd 绕开 Git Bash 的路径转换**行不通**
35. **★ 写动态分区 INSERT 必须显式带 `SET hive.exec.dynamic.partition.mode=nonstrict`** —— 2026-10-05 **明知陷阱 14 还是踩了**：手工写 `INSERT OVERWRITE ... PARTITION (dt)` 时报 `Error 10096`。
    - **根因不是"不懂"，是"流程"**：凭记忆写 SQL，而没走既有执行器（`evt_run.sh` 会自动把 `setup.sql` 拼进每一段）
    - **正确做法**：涉及动态分区写入的 SQL **一律走执行器**；万不得已手写时，文件头**固定带 4 行 SET**（dynamic.partition / mode / max.dynamic.partitions / pernode）
    - **教训**：**靠记忆防坑不可靠，坑要被工程化解决**（固化进模板/脚本），不能靠"记住"
36. **`sequence()` 是 Spark SQL 的函数，Hive 没有** → 在 Hive 里写 `explode(sequence(0,364))` 报 `Error 10011 Invalid function sequence`。
    - **Hive 的替代写法**：`SELECT posexplode(split(repeat(',', 364), ',')) AS (i, v)` —— 造出 365 行、序号 0..364
    - **教训**：Spark SQL 与 HiveQL **高度相似，相似性本身就是陷阱**；跨引擎迁移语法假设**必须先验证再写进脚本**
    - 附带：Hive 的 `dayofweek()` 原生是 **1=周日**（不是 1=周一），做星期维度**必须换算**
37. **★ 金额字段用 `double` → 跨引擎对账必然出现"假不一致"** —— 2026-10-15 接 Spark 进 DS 时发现：
    - 同一份 SQL、同一份输入，`sum(gmv)` 两个引擎算出
      `5.757599649999998E7`（Hive） vs `5.757599650000003E7`（Spark）—— **第 15 位有效数字之后不同**
    - **根因**：`dws_trade_day.gmv` / `avg_order_amount` 是 **double**（约 15 位有效数字），
      且**两个数相加时顺序不同，累积误差路径就不同**（Hive 按 MR 作业顺序聚合、Spark 按 shuffle 分区并行再汇总）
    - **正确做法**：涉及浮点/金额的跨引擎对比，**一律先 `round(x, 2)` 再比**（实测 round 后两边是同一个字符串）
    - **根本修法**：金额改 `DECIMAL(18,2)`（定点数，加法精确）—— **已识别但未修**（会牵涉表重建，需单独评估，见 `实验分析/实验12` §七）
    - **反面做法（不要做）**：直接把原始值不一致当成"引擎算错了"，那是**制造假 bug**
38. **Hive 侧多路 `UNION ALL` 会编译成等量的串行 MR 作业** —— 16 路 UNION ALL 的对账 SQL 在 Hive 上跑到工具超时；
    同是 UNION ALL，6 路就让校验节点吃了 15 分 23 秒（占当次整链 28%）。
    - **做法**：大 UNION ALL 型的校验/对账查询改用 **Spark 跑**（同一条 SQL 约 80 秒 ~2 分钟）
    - **教训**：**慢不一定在"算"，可能在"作业数量多、逐个排队"**

39. **★ 移动工作区文件后，DS 工作流必须重建 —— 节点路径存在 MySQL 里，不跟着磁盘走**（2026-10-20 分家时实测）。
    删 definition **不级联删 `t_ds_task_definition`**；若 `TASK_CODE_BASE` 仍是固定值，重建会撞出双份节点
    （症状：`info` 打印「节点 22 个；画布坐标 11 个」，每个节点名出现两遍）。
    - **做法**：改结构后 `delete` → `create` → `online`，并**换一段新的 `TASK_CODE_BASE`**
    - 详见 `项目产出/workspace文件清单_集群端.md` 第五、六节

40. **★ 脚本内定位同目录文件，一律用 `HERE="$(cd "$(dirname "$0")" && pwd)"`，别写死绝对路径**
    —— 同一批脚本里两种写法在移动目录后命运完全不同：写死的（`WS=/home/lrh/workspace`）全崩，
    自适应的自动跟着走。2026-10-20 分家时逐个改路径就是为这个付的账。

41. **★ `DSClient` 里同名方法不报错，只会让某条路径永远走不通**
    —— `def delete(self, path)`（底层 HTTP）被后定义的 `def delete(self)`（业务动作）**覆盖**，
    业务方法里 `self.delete(path)` 抛 `TypeError: takes 1 positional argument but 2 were given`。
    这条 `delete` 路径从没走过，所以建工作流一直"正常"。**"从没报错"不等于"没有问题"。**

---

## 五、常用命令

```bash
# ── 连集群 ──
ssh -i "D:\数据开发环境\VMware\sshkeys\cluster_ed25519" lrh@192.168.87.131

# ── 集群健康一览（在 lrh-1 执行）──
H=/opt/hadoop/hadoop-3.3.6/bin
for s in hadoop zookeeper mysql hive-metastore hive-server2; do echo "$s: $(systemctl is-active $s)"; done
$H/hdfs dfsadmin -report | grep -E "Live datanodes|Missing blocks"
$H/yarn node -list | grep "Total Nodes"
free -m | sed -n 2p; df -h / | tail -1

# ── 提交 Hive 脚本 ──
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://lrh-1:10000 -f <脚本.sql>

# ── 跑 Spark SQL（YARN client，配置已在 spark-defaults.conf）──
SP=/opt/spark-3.5.9-bin-hadoop3/bin/spark-sql
$SP -e "select count(*) from gmall.dws_trade_day;"        # 单条查询（注意：一次约 35s 启动开销）
$SP -f <多段合并.sql>                                      # ★ 多段务必放同一会话，分段各起一次白付 8 次启动

# ── 桌面开关（省 600MB/台）──
bash /home/lrh/desktop_switch.sh off|on|status

# ── 本机起进度看板 ──
python "D:\数据开发\项目产出\进度看板\serve.py" 8765 [dwd_run|evt_run]
```

**开机后必做**：确认 `hdfs dfsadmin -report` 有 **3 个 Live datanodes**（冷启动有竞态，先起 worker 再起 master 更稳）。

---

## 六、文档地图（东西在哪）

```
D:\数据开发\
├─ CONTEXT.md                    ← 本文件（跨工具上车包）
├─ 项目状态.md                    ← 当前进度锚点【每次阶段变化更新】
├─ 项目介绍与环境架构.md           ← 说明书类：项目总览（会随变更失真）
├─ 虚拟机集群使用指南.md           ← 说明书类：集群操作手册
├─ 目录结构说明.md                ← ⚠️ **WSL 时代旧文档，已失真**（正文写"WSL2 + 伪分布式 + /home/hadoop"，
│                                   而现集群是 VMware 三节点 + `/home/lrh`）。**别照它执行**，以本文件 §二/§三为准
├─ 实验分析/                      ← 【实验报告】实验01~11 + _模板 + _提问框架
│                                   （实验09 = 全量重启与冷启动验收；实验10 = Spark 3.5.9 双引擎对比；
│                                    **实验11 = 对标 Olist 补齐建模短板：粒度声明 / dim_date / 分层修正**）
├─ 项目产出/                      ← 【交付物】索引见 说明.md
│   ├─ 数仓表清单与业务问题映射.md   ★ **表清单 / 粒度 / 业务问题 / 口径易错点**（做指标前先看它第六节）
│   ├─ 数据接入/                   DDL + ETL + 分步执行器（dwd_run.sh / evt_run.sh）+ **DS 编排脚本**
│   ├─ 数据链路精讲/               ★ 3 份实现级讲解：原始数据→ODS / ODS→DWD / DWD→ADS（2026-10-15 建）
│   │                                数字全部实测；用真实 user_id=513781527 贯穿全程（5 行→1 行→1 个计数）
│   │                                **03 含三个真实数据撞出的陷阱**：首日效应 / 转化率>100% / 浮点尾巴
│   ├─ 代码学习路径_从AI代码到自己能讲.md
│   │                              ★ 学「AI 代写代码」的路径：3600 行的减法表 + 三站验收题 + 进展记录
│   ├─ 运维脚本/                   修复脚本
│   ├─ 可视化/                     ECharts 大屏（双击即开；**6 支查询全部只读 ADS 层**）
│   ├─ 进度看板/                   长任务实时进度条
│   └─ 组件详解/                   6 份：给零基础讲每个组件
├─ 手把手教程/                    ← 【教程】从零跟做（原文档地图漏了这个目录）
│   ├─ DolphinScheduler零基础操作教程.md   ⭐ **DS 专篇**：零基础学 DS，命令全部实测过
│   ├─ gmall数仓全流程手把手教程.md        数仓侧全流程（路径A手工 / 路径B走DS）
│   └─ DolphinScheduler全流程排障汇总.md    19 个坑：现象 → 根因 → 解法
├─ 零基础教程/                    ← 【课程】面向"没接触过大数据"的小白，读完能独立开发（2026-10-06 建）
│                                   README（总纲/路线图）+ 01导论 02Linux 03搭集群 04HDFS 05YARN
│                                   06Hive 07建模★ 08实战★ 09调度 10可视化 11调优排错★ 12毕业项目 13速查
│                                   与 组件详解/（科普组件）、手把手教程/（本集群操作手册）三者分工不同
├─ Spark教程/                     ← 【课程】Spark 专题（2026-10-11 建，8 份）
│                                   ★ 写作约定：**每个专业名词首次出现都给中文解释**（用户明确要求）
│                                   README + 01是什么 02核心概念 03怎么装 04怎么写
│                                   05该用哪个★ 06调优踩坑 07名词对照表
│                                   素材全部出自 `实验分析/实验10`（实测），前置是 零基础教程/05
├─ 项目流程详解/                  ← 【讲"这个项目"】面向"想建立整体掌控感"的主人视角（2026-10-11 建，9 份）
│                                   ★ 统一模板：**目标 → 做什么 → 产物 → 谁在干 → 怎么知道干对了 → 不懂会怎样**
│                                   00导读 01全景图★（9 站旅程）02四层楼 03五个角色
│                                   04一次计算 05自动化 06你在项目里的角色★ 07术语表
│                                   **08分段与观测**（8 段怎么切的/怎么看进度 —— 出自 evt_run.sh + ds_prepare_steps.sh + serve.py）
│                                   ★ 与 零基础教程/ 的区别：那份教"大数据这门手艺"，这份讲"**你这个项目**"
├─ 其他/                          ← 杂项：调研、笔记、待问问题、问答记录
├─ mock/                          模拟数据生成脚本（含旧链路 ETL 脚本，注意与集群对账）
└─ .workbuddy/memory/             ← AI 的工作记忆（日志 + MEMORY.md）

~~环境搭建/~~  ← **2026-10-18 已删除**（WSL 时代的 62 个搭建脚本，与 VMware 三节点无继承关系）
                 删除清单与证据见 `其他/WSL遗留文件_删除清单_20261018.md`

D:\OneDrive\桌面\知识库\数据开发\   ← 学习材料（在 OneDrive）
├─ Day2~Day21 教程                操作型：讲"怎么写出能跑的"
├─ 执行原理/                       ⭐ 7 份：讲"作业在机器上怎么跑、为什么炸"
└─ ssh-remote-command-beginner-tutorial.md
```

**如果要补"原理类"内容 → 放知识库 `执行原理/`；如果是本项目的交付物 → 放 `项目产出/`。**

---

## 七、怎么用这个文件

| 场景 | 做法 |
|---|---|
| **新会话接上项目** | 让 AI 读 `CONTEXT.md` + `项目状态.md` |
| **换工具（Trae 等）** | 同上 —— 这两个文件就是全部必需的上下文 |
| **只想问问题** | 没问题，但结论若影响项目，请让 AI 写进 `实验分析/` 或记忆 |
| **问题很多** | 先记到 `其他/待问问题.md`，攒一批一次处理 |
| **发现项目"是什么样"变了** | 改本文件 + 相关说明书文档（按约定②） |

> **一条纪律**：**没落盘的结论等于没发生。**
