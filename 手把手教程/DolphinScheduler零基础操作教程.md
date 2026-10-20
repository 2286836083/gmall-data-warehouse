# DolphinScheduler 零基础操作教程

> **这份教程教什么**：怎么用 DolphinScheduler（下称 **DS**）把这套数仓的全流程跑起来 —— 从"DS 是什么"讲到"我能自己改一个节点、配一条定时"。
>
> **为什么可信**：里面的命令**全部来自 2026-10-03 的一次真实运行** —— 那天用 DS 把真实数据链路（1063 万行）的 **16 个节点全部跑成功**，整链 54 分 10 秒。凡是**我没实测过**的（主要是网页操作的具体按钮位置），都会明确标注。
>
> **配套文档**（遇到报错先翻第一份）：
> - `DolphinScheduler全流程排障汇总.md` —— 19 个坑的现象→根因→解法
> - `gmall数仓全流程手把手教程.md` —— 数仓侧（不用 DS 也能跑的那条路）
> - `../实验分析/实验08_真实链路接入DolphinScheduler.md` —— 这套工作流是怎么设计出来的、证据在哪
>
> 环境基线：三节点 VMware（lrh-1/2/3 = 192.168.87.131/130/132）· DS 3.1.8 · Hive 3.1.3 · Hadoop 3.3.6 · JDK 8

---

## 〇、按你的目的选一条路读

| 你是 | 直接看 |
|---|---|
| **完全没概念**，想先听懂 DS 在干嘛 | 第 1 章（15 分钟，全是类比） |
| **想马上跑一次**看效果 | 第 2 章（拉起来）→ 第 4 章（命令行跑）——最快路径 |
| **想用网页**操作、演示给别人看 | 第 2 章 → 第 3 章 |
| **想改 SQL / 加节点** | 第 5 章（**改之前一定要读**，改错地方会白改） |
| **想让它每天自动跑** | 第 6 章（已实测验证） |
| **跑挂了 / 卡住了** | 第 7 章 → 再翻《排障汇总》 |
| **想证明自己真跑通了** | 第 8 章验收清单 |

---

## 一、先搞懂 DS 是什么

### 1.1 一句话类比

**DolphinScheduler 是工厂的"自动排班系统"。**

你的数仓有 16 道工序（建表、清洗、汇总、算指标、校验……），每道工序就是一条 SQL。如果没有排班系统，你得**守在电脑前**，算完一步手动敲下一条命令，敲 16 次。

DS 的作用就是：**把 16 道工序画成一张流程图，点一下"运行"，它自己按顺序一道道做完；哪道挂了停下来告诉你；还能设成每天凌晨 3 点自动开工。**

⚠️ **最关键的区分**（初学最容易混）：

| 谁 | 管什么 | 类比 |
|---|---|---|
| Hive / YARN / HDFS | **算得对不对** | 车间里的机器和工人 |
| **DolphinScheduler** | **什么时候算、按什么顺序算、挂了怎么办** | 排班表 + 工头 |

所以"DS 显示成功"**不等于**"数据是对的"。DS 只保证"这条命令跑完没报错"；数据对不对要靠**校验 SQL**（本工作流最后那个 `verify` 节点就是干这个的）。

### 1.2 五个必须分清的词

| 词 | 类比 | 在本项目里 |
|---|---|---|
| **工作流定义**（workflow definition） | **图纸 / 排班表** | `gmall_evt_pipeline`（16 个格子） |
| **节点 / 任务定义**（task） | 图纸上的**一个格子** | `dws_user` 这个格子 = 跑一条 SQL |
| **依赖边**（relation） | 格子之间的**箭头** | 16 个格子串成一条直线，前面的不成功后面不动 |
| **工作流实例**（process instance） | **今天这一趟实际执行** | 2026-10-03 10:22 那次，编号 6 |
| **任务实例**（task instance） | 这一趟里**某个格子的实际执行** | 那次里的 `dws_user`，耗时 2 分 48 秒 |

> **一句话**：**图纸只有一份，执行可以有很多趟。** 同一个工作流定义，今天跑一次是实例 6，明天跑一次是实例 7。
>
> 明白了这个，你就明白为什么"改了定义"和"跑了实例"是两件事 —— **改完图纸必须重新上线，才生效。**

### 1.3 "上线 / 下线"是什么（初学者最常卡的点）

DS 里的工作流有两个状态：

- **下线（OFFLINE）**：图纸归档了。**列表页那一行的"运行"按钮是灰的，点不动。**
- **上线（ONLINE）**：可以运行。

> 类比：**上线 = 把排班表贴到车间墙上**。没贴上去，工人看不到，自然不会干活。
>
> ⚠️ **本项目现在三个工作流的状态（2026-10-03 实测）**：

| 工作流名 | 节点数 | 状态 | 用途 |
|---|---|---|---|
| **`gmall_evt_pipeline`** | **16** | ✅ **ONLINE** | **真实数据链路（1063 万行）—— 演示用这个** |
| `gmall_dw_pipeline` | 9 | ⛔ **OFFLINE** | 旧模拟链路全流程（跑不了，按钮是灰的） |
| `gmall_dw_pipeline_tail` | 5 | ✅ ONLINE | 旧模拟链路尾段 |

### 1.4 状态码速查（看日志/查库时天天用）

**工作流实例状态**（`t_ds_process_instance.state`）：

| 值 | 含义 | 初学怎么理解 |
|---|---|---|
| 1 | RUNNING_EXECUTION | 正在跑 |
| 5 | **STOP** | **被人叫停了**（不是自己跑挂） |
| 6 | FAILURE | 真跑挂了 |
| 7 | **SUCCESS** | ✅ 成功了 |
| 9 | KILL | 被杀掉了 |

**任务实例状态**（`t_ds_task_instance.state`）：同样 1/5/6/7/9，另有 2=DISPATCH 等中间态。

**退出码**（初学判"被杀"还是"跑错"的第一眼）：

| 退出码 | 含义 |
|---|---|
| `0` | 正常结束 |
| `143` | = 128+15 = 收到 **SIGTERM**，**被人叫停的** |
| `137` | = 128+9 = 收到 SIGKILL，常见于**内存不够被系统杀** |

> ★ **重要经验**：**先看退出码，再看日志文本。**
> 09-30 那次 `dws_order` 的日志尾部看起来像 SQL 报错，其实退出码是 `143` —— 是**被人主动停的**。
> 如果按"SQL 有问题"去查，方向完全错。详见《排障汇总》§一.1。

---

## 二、开工前：把 DS 拉起来，并确认它真的能用

### 2.1 三个反常识（不知道会白折腾半小时）

**① DS 不会开机自启。**
Hadoop / Hive / ZooKeeper / MySQL 都是 systemd 服务，开机自动跑；**但 DS 没有注册成 systemd**（这是它和别的组件最大的区别），每次虚拟机重启后**必须手动拉起**。

**② `systemctl is-active` 说 active ≠ 服务能用。**
HiveServer2 被标记 `active` 之后**至少还要几十秒**才真正监听 10000 端口。所以判"能不能用"必须用**外部判据**：端口在不在 + 发一次真实查询有没有返回。**这条已经写进了工作流的第 1 个节点**（见 3.2）。

**③ 内存很紧，别乱开。**
lrh-1 只有约 7GB 内存，DS 的每个 JVM 堆都锁死在 512MB。**DS 三个进程 + HiveServer2 + ResourceManager 已经把它们自己塞满了**，所以别同时跑两个大作业。这也是为什么工作流第 1 个节点会检查"有没有残留进程"。

### 2.2 拉起 DS（顺序不能反）

```bash
# 从你的 Windows 机器连进集群（本教程所有命令都在 lrh-1 上执行）
ssh -i "D:\数据开发环境\VMware\sshkeys\cluster_ed25519" lrh@192.168.87.131
```

登录后，在 lrh-1 上执行：

```bash
DS=/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin

# 1) lrh-1：先 master，再 api
$DS/bin/dolphinscheduler-daemon.sh start master-server
$DS/bin/dolphinscheduler-daemon.sh start api-server

# 2) lrh-2 / lrh-3：各自拉起 worker（worker 必须在这两台，它们才是干活的）
ssh lrh-2 "$DS/bin/dolphinscheduler-daemon.sh start worker-server"
ssh lrh-3 "$DS/bin/dolphinscheduler-daemon.sh start worker-server"
```

**为什么顺序是这个？** 三个角色都靠 **ZooKeeper**（值班室的对讲机）互相发现：
**master** 是工头（派活）、**worker** 是工人（干活）、**api** 是你登录网页的后台。
工人和工头都得先在对讲机里"报到"，网页才看得到它们。所以 **api 要放在最后** —— 实测过：ZooKeeper 刚起来时立刻启 api-server，它会因为 `zookeeper connect timeout` 直接启动失败。

> 内存特别紧的时候（比如只想验证 SQL），可以**不启 alert-server**（告警服务，调度链路用不到它），省一份堆内存。

### 2.3 30 秒体检（复制粘贴就能跑）

```bash
H=/opt/hadoop/hadoop-3.3.6/bin

# ① DS 三个进程在不在（jps 要全路径，非交互 SSH 里 PATH 是空的）
export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH
jps -l | grep -E "MasterServer|WorkerServer|ApiApplicationServer"

# ② ZooKeeper 里认到了几个 master / 几个 worker（期望 1 个 master、2 个 worker）
printf 'ls /dolphinscheduler/nodes/master\nls /dolphinscheduler/nodes/worker\nquit\n' \
  | timeout 25 /opt/zookeeper/apache-zookeeper-3.8.7-bin/bin/zkCli.sh -server lrh-1:2181 2>/dev/null | grep '^\['

# ③ HiveServer2 是不是"真能用"（端口 + 真实查询，两个都要）
ss -lntp | grep ':10000'
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://lrh-1:10000 --silent=true -e "select 1;"

# ④ 集群规模（期望各 3）
$H/hdfs dfsadmin -report | grep "Live datanodes"
$H/yarn node -list | grep "Total Nodes"
```

**期望输出**：

```
3813 ...MasterServer     3793 ...ApiApplicationServer
[192.168.87.131:5678]                      ← 1 个 master
[192.168.87.130:1234, 192.168.87.132:1234] ← 2 个 worker
LISTEN ... :10000
Live datanodes (3):
Total Nodes:3
```

> ⚠️ **worker 上不要敲 `systemctl status hadoop`** —— lrh-2/3 上**没有 `hadoop.service`**，只有 `hadoop-worker`。

### 2.4 打开网页

浏览器访问 **<http://192.168.87.131:12345/dolphinscheduler>**
账号 `admin` / 密码 `dolphinscheduler123`

**打不开怎么办**（按可能性排序）：

| 现象 | 先查什么 |
|---|---|
| 浏览器连不上 | 在 lrh-1 上 `ss -lntp \| grep 12345` —— 没监听就是 api-server 没起来 |
| 12345 在监听但页面报错 | `tail -40 $DS/api-server/logs/api-server-lrh-1.out` |
| 登录后一直转圈 | 多半是 ZooKeeper 有问题（DS 靠它），查 `ls /dolphinscheduler/nodes/*` |

---

## 三、网页上跑一次：看懂那 16 个节点

> 本章的**菜单路径**来自本项目既有教程《gmall数仓全流程手把手教程》§7.2（那份是照着网页操作写的）；
> **节点清单和耗时**是我 2026-10-03 的实测结果。

### 3.1 找到工作流

```
项目管理 → 项目 gmall → 左侧「工作流定义」
```

列表里你会看到 **3 个工作流**（见 1.3 的表）。**要找的是 `gmall_evt_pipeline`。**

⚠️ **只有"已上线"的才能运行。** 如果你点的是 `gmall_dw_pipeline`，那个"运行"图标是**灰的** —— 不是坏了，是它在下线状态。

### 3.2 打开图纸：16 个节点分别在干什么

点 `gmall_evt_pipeline` 的名字进编辑器，你会看到一条**从左到右的直线**，16 个 SHELL 节点：

| # | 节点名 | 干什么 | 实际执行的文件 | 实测耗时 |
|---|---|---|---|---|
| 1 | `preflight` | **开工前体检**：服务 / 3 DataNode / 3 NodeManager / HS2 真可用 / **有没有残留进程** / 生成分段 SQL | `ds_preflight.sh` | 24s |
| 2 | `ods_ready` | ODS 就绪检查（31 分区 / 10,630,245 行） | `evt_ods_ready.sh` | 1m59s |
| 3 | `dwd_ddl` | 建 DWD/DIM 三张表 | `dwd_ddl.sql` | 19s |
| 4 | `dwd_event_clean` | 清洗 1063 万行事件明细（**最重的 ETL**） | `ds_steps/dwd_step2.sql` | 8m37s |
| 5 | `dwd_purchase_fact` | 派生购买事实 | `ds_steps/dwd_step3.sql` | 1m15s |
| 6 | `dwd_dim_product` | 派生商品维度 | `ds_steps/dwd_step4.sql` | 7m36s |
| 7 | `evt_ddl` | 建 4 张 DWS + 4 张 ADS | `evt_ddl.sql` | 17s |
| 8 | `dws_user` | DWS·用户主题 | `ds_steps/evt_step1.sql` | 2m48s |
| 9 | `dws_product` | DWS·商品主题 | `ds_steps/evt_step2.sql` | 2m36s |
| 10 | `dws_category` | DWS·**类目**主题 | `ds_steps/evt_step3.sql` | 2m00s |
| 11 | `dws_trade` | DWS·交易主题 | `ds_steps/evt_step4.sql` | 1m09s |
| 12 | `ads_active_user` | ADS·活跃用户 | `ds_steps/evt_step5.sql` | 3m29s |
| 13 | `ads_funnel` | ADS·转化漏斗 | `ds_steps/evt_step6.sql` | 1m47s |
| 14 | `ads_repurchase` | ADS·复购率 | `ds_steps/evt_step7.sql` | 2m27s |
| 15 | `ads_topn` | ADS·TopN 商品 | `ds_steps/evt_step8.sql` | 1m48s |
| 16 | `verify` | **三层校验**（行数 / GMV 交叉对账 / 方向性） | `evt_verify.sql` | **15m23s** |
| | | | **合计** | **54m10s** |

**双击任意一个节点**，能看到它的脚本内容。所有节点都是同一个模板，只换 `-f` 后面的文件名：

```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'export JAVA_HOME=/opt/jdk/jdk1.8.0_202; \
  export PATH=$JAVA_HOME/bin:$PATH; \
  /opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 \
  --silent=true -f /home/lrh/workspace/new_chain/ds_steps/evt_step1.sql </dev/null 2>&1'
```

**这行脚本有三处"不能动"，每一处都是踩坑踩出来的**：

| 片段 | 为什么不能省 |
|---|---|
| `ssh ... lrh-1 '...'` | DS 的 worker 在 lrh-2/lrh-3 上，**那两台没装 Hive 客户端**，任务必须跳回 lrh-1 执行 beeline |
| `</dev/null` | ★ beeline 是交互式程序，**没有终端又没重定向 stdin 时会被 SIGTTOU 冻住**（进程状态 `Tl+`）。现象是：DS 里显示"运行中"，**但永远不提交任何作业，日志里也看不到报错** —— 极难查 |
| `2>&1` | 让 beeline 的报错也进 DS 任务日志，不然失败了看不到原因 |

> ★ **为什么最慢的是"校验"而不是 ETL？**
> 因为 `evt_verify.sql` 用 `UNION ALL` 把 8 张表的统计合成一条语句，Hive 把它编译成 **6 个串行 MR 作业**（日志里能看到 `Launching Job 1 out of 6`），每个作业光调度开销就几十秒。
> **"读多张表做校验"在 Hive-on-MR 上比想象中贵。** 这是本次实测的意外发现。

### 3.3 点"运行"之后看什么（三级下钻）

**第一级 · 工作流实例**：`左侧「工作流实例」` → 最上面那条。看**整体状态刷成绿色**。
**第二级 · 甘特图**：能看到 16 个节点**哪个最占时间**（你会发现 `verify` 那条最长）。
**第三级 · 节点日志**：点开某条实例 → 找到某个节点 → 点「日志」。

**日志里怎么读？** 找这几类行：

```
INFO  : Starting Job = job_1790734666365_0097     ← 提交了一个 MR 作业
INFO  : Stage-4 map = 100%, reduce = 100%         ← 这个作业跑完了
[INFO] ... Send task execute result to master, the current task status: TaskExecutionStatus{code=7, desc='success'}
                                                  ← ★ 这一行是结论：code=7 成功
```

### 3.4 怎么判断"卡住了"还是"正常推进"

**最可靠的办法：去 YARN 看作业是不是在换。**

浏览器开 <http://192.168.87.131:8088>，或者在 lrh-1 上：

```bash
/opt/hadoop/hadoop-3.3.6/bin/yarn application -list
```

- **`Stage-N` 的 N 在变 / 应用 ID 后缀在变** → 正常推进，耐心等
- **同一行 5 分钟没动、进度 0%** → 可能是资源排队（正常），也可能是真卡了

**"两个阶段之间的空档"是正常的**：beeline 提交完一个 MR 作业、准备下一个的那几十秒，YARN 里可能一个应用都没有 —— 这时候看 DS 节点状态还是"运行中"就说明没事。

### 3.5 失败了怎么办

**第一步：别急着重跑，先看节点日志的最后 30 行 + 退出码。**

```
退出码 143 → 被叫停（不是 SQL 问题）
退出码 137 → 内存被杀
其他非 0   → 去看 beeline 的报错（Error 10004/10096 之类）
```

**第二步：按节点状态决定动作**

| 你想干什么 | 怎么做 |
|---|---|
| **整个流程重跑** | 工作流定义 → 点「运行」；或命令行 `python3 ds_evt_pipeline.py run` |
| **重新跑失败的那个节点** | 实例页 → 右键该节点 → 「重跑」 |
| **从失败处往后接着跑** | ⛔ **本环境下别用！** 网页上的「从当前节点开始重跑」和接口的 `startNodeList` 走的是同一段后端逻辑，实测会触发 master NPE 死循环（详见 **§4.5**）。**要重跑就整条重跑**（ETL 幂等，约 54 分钟） |
| **停止整条流程** | 实例页 → 点「停止」；或 `execute?executeType=STOP` |

> ⚠️ 上面"重跑单个节点"这一条我也**没有实测过**（它同样可能走 `startNodeList` 那条路）。
> 想用之前，**先确认 master 日志没有开始刷 NPE**（`tail -f .../dolphinscheduler-master.log`）。
> 最稳的仍然是：**整条重跑**。

> ⚠️ ★ **停止有着一个很坑的副作用，必须知道**：
> **DS 的"停止"杀不掉真正在干活的进程。**
> 因为任务是 `ssh lrh-1 'beeline …'` —— 命令跑在 lrh-1 上，而 **DS 只能杀掉 worker 机器（lrh-2/3）上的进程树**。
> 实测过：09-30 停掉的任务，它的 beeline **在 lrh-1 上又跑了 3 天，把 5 天数据全写完了**。
> **所以停完之后一定要补一刀确认**：
> ```bash
> ps -ef | grep jdbc:hive2 | grep -v grep   # 还有输出 = 没停干净
> /opt/hadoop/hadoop-3.3.6/bin/yarn application -list
> pkill -f 'jdbc:hive2'                     # 确认没别人在跑再执行
> ```

---

## 四、命令行跑一次（我更推荐这条路）

### 4.1 为什么推荐命令行

因为 **DS 的全部状态都老老实实躺在 MySQL 里**。一条 SQL 就能问清"现在到底怎么样"，**比在网页上点来点去更快、也更适合反复练**。而且命令行可以脚本化、可以复现。

（网页适合**演示**：给别人看甘特图和节点颜色，命令行适合**干活和排查**。）

### 4.2 用 4 条 SQL 看懂全部状态

在 lrh-1 上执行（`sudo -n mysql` 走 root socket 认证，不需要密码）：

```bash
# ① 有哪些工作流？上线了吗？（release_state: 1=上线, 0=下线）
sudo -n mysql dolphinscheduler -t -e \
 "select code,name,version,release_state from t_ds_process_definition;"

# ② 最近跑了哪几趟？什么结果？（state: 1跑着/5被停/6失败/7成功/9被杀）
sudo -n mysql dolphinscheduler -t -e \
 "select id,name,state,start_time,end_time from t_ds_process_instance order by id desc limit 5;"

# ③ 这一趟里 16 个节点各自怎么样？
sudo -n mysql dolphinscheduler --default-character-set=utf8mb4 -t -e \
 "select name,state,start_time,end_time from t_ds_task_instance
  where process_instance_id=<把②里的id填进来> order by id;"

# ④ 有没有定时调度？（0 = 从未配过）
sudo -n mysql dolphinscheduler -N -B -e "select count(*) from t_ds_schedules;"
```

> 💡 **小技巧**：`-t` 是表格输出（好看），`-N -B` 是纯数字输出（好给脚本用）。
> 💡 MySQL 客户端**必须加 `--default-character-set=utf8mb4`**，否则中文节点名会乱码。

### 4.3 触发一次运行

工作流定义、上线、触发，我都封装成了一个脚本（在集群 `/home/lrh/workspace/new_chain/`）：

```bash
cd /home/lrh/workspace/new_chain

python3 ds_evt_pipeline.py info      # 先看：定义叫什么、几个节点、上线没有
python3 ds_evt_pipeline.py online    # 上线（下线状态跑不了）
python3 ds_evt_pipeline.py run       # 触发一次
```

`run` 成功会返回：

```
已触发: {'processDefinitionCode': 23240142767136, ...}
```

然后立刻用 §4.2 的 ② 查，能看到新的一趟实例、`state=1`。

> ⚠️ **这个脚本踩过 3 个 DS 接口的坑，已经绕好了**（你自己写接口时一定会遇到，详见《排障汇总》§一.1）：
> 1. `releaseState` 必须传**枚举名** `ONLINE`/`OFFLINE`，传 `1`/`0` 只回一句笼统的 `10108`
> 2. `timeout` 必须落在 **(0, 86400]**，传 `0` 回 `10002`（"不超时"的直觉写法其实是非法值）
> 3. `query-by-name` 接口**是坏的** —— 对确实存在的工作流也会回 `50003 does not exist`

### 4.4 盯着进度跑（不用一直刷网页）

集群侧有个监视器，每分钟把状态写进一个文件：

```bash
# 启动（<实例id> 从 §4.2 的 ② 里拿）
cd /home/lrh/workspace/new_chain
setsid nohup bash ds_monitor.sh 6 >/dev/null 2>&1 < /dev/null &

# 随时看
tail -20 /home/lrh/workspace/new_chain/ds_steps/monitor.log
```

输出长这样（**这就是判断"通不通畅"最直接的样子**）：

```
=============== 2026-10-03 10:50:47 ===============
INSTANCE id=6 state=1
  preflight            state=7 start=... 10:22:03 end=2026-10-03 10:22:27
  ods_ready            state=7 start=... 10:22:28 end=2026-10-03 10:24:27
  dwd_event_clean      state=7 start=... 10:24:47 end=2026-10-03 10:33:24
  ...
  dws_trade            state=1 start=... 10:50:03 end=-
  YARN running apps = 1
  beeline on lrh-1   = 2
```

`state=7` 就是成功。**`YARN running apps` 和 `beeline` 这两行是"它真的在干活"的证据** —— 如果节点显示运行中但这两行都是 0，那才是真卡住了。

### 4.5 ⛔ "从指定节点重跑"——**本环境下千万别用**

DS 的接口里有一个 `startNodeList` 参数，意思是"只从某个节点开始跑"：

```
POST /projects/{projectCode}/executors/start-process-instance
    processDefinitionCode=<工作流code>
    startNodeList=<节点名>            ← 例如 dws_product
    taskDependType=TASK_ONLY          ← 只跑这个节点
```

**看起来很有用（前面都成功了，只想重跑后面几个），但在本项目这套 DS 3.1.8 上，它会把 master 搞死。**

**实测证据（2026-10-03，三次复现、零例外）**：

| 实例 | 启动方式 | 结果 |
|---|---|---|
| 6 | **普通全量**运行 | ✅ 16/16 SUCCESS |
| 9 | **普通全量**运行（2 节点测试流） | ✅ SUCCESS |
| **7、8、11** | **带 `startNodeList`** | ❌ **master NPE 死循环**（一个任务都不下发） |
| 12 | **普通全量**运行 | ✅（重建后重跑正常） |

**死循环长什么样**（认出来就别犹豫，立刻处置）：

```
[ERROR] WorkflowExecuteRunnable:[709] - Start workflow error
java.lang.NullPointerException: null
    at ...service.utils.DagHelper.parsePostNodes(DagHelper.java:306)
[ERROR] WorkflowStartEventHandler:[72] - Failed to submit the workflow instance, will resend...
```

症状是**三个一起出现**：① master CPU 打满；② 日志每秒涨几千行（实测 2 分钟涨到 **179 MB**，会把磁盘吃干）；③ 工作流实例永远停在 RUNNING，**一个任务都不下发，而且界面上看不到任何任务级报错**。

> ★ **最坑的一点**：**重启 master 没用。** 我试过 —— 重启后它会重新捡起那条实例，继续循环。
> （本项目《排障汇总》§3.1 里原来写"重启即可清除"，那个结论**是错的**，已在那边更正。）

**正确做法**：**要重跑就整条重跑。** 本链路的 ETL 全是 `INSERT OVERWRITE` / `DROP+CREATE`，**幂等**，整条重跑约 54 分钟，不会污染数据。慢一点，但不会把调度平台搞挂。

**如果你已经踩了，怎么救**（按顺序，别颠倒）：

```bash
DS=/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin
export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH

# ① 先停 master —— 必须先停！否则清了库，master 内存态还是脏的
$DS/bin/dolphinscheduler-daemon.sh stop master-server
for i in $(seq 1 12); do sleep 5; jps -l | grep -q MasterServer || break; done

# ② 从库里删掉那条卡死的实例（先查 id：它是 state=1 且 task_instance 数为 0 的那条）
sudo -n mysql dolphinscheduler -t -e \
 "select id,name,state from t_ds_process_instance order by id desc limit 3;"
sudo -n mysql dolphinscheduler -e "
 delete from t_ds_task_instance    where process_instance_id=<卡死的id>;
 delete from t_ds_process_instance where id=<卡死的id>;
 delete from t_ds_command          where process_instance_id=<卡死的id>;
 delete from t_ds_error_command    where process_instance_id=<卡死的id>;"

# ③ 截断暴涨的日志，把磁盘收回来
: > $DS/master-server/logs/dolphinscheduler-master.log

# ④ 重启 master，然后用**普通全量**方式重跑
$DS/bin/dolphinscheduler-daemon.sh start master-server
```

> 本项目 `ds_evt_pipeline.py` **已经把这个功能移除了** —— 就是为了防止有人顺手用了把平台搞挂。

---

## 五、这套工作流的文件长什么样（改之前必须懂）

### 5.1 关系图：一句话说不清，一定要看图

```
                    【唯一的真源】
   /home/lrh/workspace/new_chain/etl_ods_to_dwd.sql   (DWD 的 3 条 INSERT)
   /home/lrh/workspace/new_chain/evt_etl.sql          (DWS/ADS 的 8 条 INSERT)
                    │
                    │  ① 按注释标记切分
                    │     （"-- 第 N 步" / "-- ===== STEP N"）
                    ▼
   /home/lrh/workspace/new_chain/ds_prepare_steps.sh
                    │
                    │  ② 每段前面拼上"公共 SET 参数"
                    │     参数是从 dwd_run.sh / evt_run.sh 里【抽取】的
                    ▼
   /home/lrh/workspace/new_chain/ds_steps/
        dwd_step2.sql  dwd_step3.sql  dwd_step4.sql
        evt_step1.sql  ...  evt_step8.sql
                    │
                    │  ③ DS 的 16 个节点各自 beeline -f 其中一个
                    ▼
              DS 工作流 gmall_evt_pipeline
```

### 5.2 为什么参数要从 runner 里"抽"，而不是直接写进去

因为**同一份配置存在两处，就一定会漂移**。本项目已经因此踩过两个坑：

- 调优参数被"统一压到 512MB"，**抹掉了分层设计** → DWS 大基数 `GROUP BY` 直接 `GC overhead limit exceeded`
- 参数文件**生成了但没拼进分段 SQL** → 报 `Error 10096`（动态分区模式还是 strict）

所以 `ds_prepare_steps.sh` 用 `sed` 从 `dwd_run.sh` / `evt_run.sh` 的 heredoc 里**把参数抠出来**，而不是重抄一遍。
**并且加了"启动前自检"**：生成后 `grep` 每个分段文件里有没有 `hive.exec.dynamic.partition.mode` 和 `mapreduce.job.reduces`，**查不到就直接非 0 退出**。

> **设计原则**：让配置漂移变成**"立即失败"**，而不是**"静默跑错"**。
> （"静默跑错"是这类项目最贵的 bug —— 你以为在跑 A，其实在跑 B。）

### 5.3 ★ 改 SQL 的正确姿势

| 你想改什么 | 改哪个文件 | 改完要做什么 |
|---|---|---|
| DWD 的清洗逻辑 | `etl_ods_to_dwd.sql` | 重新执行 `ds_prepare_steps.sh`（或直接重跑工作流，第 1 个节点会自动重新生成） |
| DWS / ADS 的算法 | `evt_etl.sql` | 同上 |
| 公共调优参数 | `dwd_run.sh` / `evt_run.sh` 里的 `setup.sql` heredoc | 同上 |
| 校验口径 | `evt_verify.sql` | 直接生效（verify 节点直接读它） |

⛔ **绝对不要直接改 `/home/lrh/workspace/new_chain/ds_steps/*.sql`** —— 它是**生成物**，下次生成就被覆盖了。改它等于白改。

> 这条规矩和本项目的另一个教训是同一个道理：**"参数文件生成了" ≠ "参数生效了"**。改对地方，比改得快重要。

### 5.4 想加一个节点怎么办

**网页方式**（适合初学者，直观）：

```
工作流定义 → 点 gmall_evt_pipeline 的名字进编辑器
→ 从左侧工具栏拖一个 SHELL 节点到画布
→ 双击它，填【节点名】和【脚本内容】（照抄 3.2 的模板，只换文件名）
→ 把上一个节点的圆点连到它、它的圆点连到下一个
→ 右上角【保存】→ 回列表【上线】（★ 改完必须重新上线才生效）
```

**接口方式**（适合脚本化）：用 `ds_evt_pipeline.py` 里的 `NODES` 列表加一行，删掉旧定义再 `create`。
每个节点要有**全局唯一的 `code`**，依赖关系写成 `taskRelationJson`。

> ⚠️ **加节点的两个坑**：
> 1. **节点名要唯一** —— 重名会让日志和状态查询分不清谁是谁
> 2. **别忘了重新上线** —— 保存只是改了图纸，上线才是贴到墙上

---

## 六、定时调度：让"按天自动跑"真的成立

### 6.1 现状：**一条调度都没有**

```bash
sudo -n mysql dolphinscheduler -N -B -e "select count(*) from t_ds_schedules;"
# → 0
```

所以文档里如果写"按天定时跑数"，那是**能力描述**（DS 支持），不是**状态描述**（我们配了）。
**能力 ≠ 状态** —— 判据就是上面这条 SQL 有没有行。

### 6.2 配之前必须想清的三件事（比配置本身重要）

| 要想清 | 为什么 | 本项目现状 |
|---|---|---|
| **跑的是"哪一天"？** | 定时跑的前提是**增量**：每天处理昨天那份数据。而本项目现在的 ETL 是**全量覆盖 31 天**（2019-10-01~31 的历史快照）—— 每天重跑一次要 54 分钟，且产出完全一样 | ⚠️ **口径没定，所以先别配** |
| **两趟会不会撞车？** | 上一趟还没跑完，下一趟又启动 → 两个 beeline 抢 HiveServer2、同时 `INSERT OVERWRITE` 同一张表 → **结果不可预测** | 需要加互斥（`runMode=RUN_MODE_SERIAL` 或前置检查） |
| **失败了谁知道？** | 现在 `warningType=NONE`，**失败了没有任何通知** | 需要先配告警组 |

> **所以我的建议是：口径、互斥、告警这三件事没解决之前，不要配定时。**
> 配一条"每天自动全量重跑一遍历史数据"的调度，除了占集群资源，不会让项目多任何东西。

### 6.3 真要配：实测验证过的完整步骤

> **本节内容我实测过**：创建 → 确认它是"下线" → 查询 → 删除，全链路走通。
> **但有一点我故意没做**：`/online` 那一步（见第 3 步），因为一旦上线它就**真的会开始按点触发**。

**★ 第 0 步：先记住 `schedule` 参数不是 cron 字符串，是 JSON 对象**

这是最容易踩的坑。Swagger 文档把 `schedule` 标成 `type: string`，于是很自然会传 `0 0 3 * * ? *`，结果报：

```
{"code": 10076, "msg": "create schedule error"}     ← 笼统得看不出问题
```

真因在 api-server 日志里：

```
Parse object exception, jsonStr: 0 0 3 * * ? *, class: ...api.dto.ScheduleParam
Cannot construct instance of `ScheduleParam` ... from Number value (0)
```

**正确传法**是一个 JSON：

```json
{"startTime":"2026-10-04 00:00:00",
 "endTime":"2026-12-31 23:59:59",
 "crontab":"0 0 3 * * ? *",
 "timezoneId":"Asia/Shanghai"}
```

**★ 第 1 步：创建（创建出来是「下线」状态，所以这一步绝对安全）**

```python
# 在 lrh-1 上跑（这是最小可用版；也可以照这个逻辑给 ds_evt_pipeline.py 加一个 subcommand）
import json, urllib.request, urllib.parse, http.cookiejar
BASE = "http://192.168.87.131:12345/dolphinscheduler"
PROJ, WF_CODE = 23110291751840, 23240142767136      # gmall / gmall_evt_pipeline

cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.open(urllib.request.Request(BASE + "/login",
    data=urllib.parse.urlencode({"userName":"admin","userPassword":"dolphinscheduler123"}).encode(),
    headers={"Content-Type":"application/x-www-form-urlencoded"})).read()

param = {"startTime":"2026-10-04 00:00:00", "endTime":"2026-12-31 23:59:59",
         "crontab":"0 0 3 * * ? *", "timezoneId":"Asia/Shanghai"}
req = urllib.request.Request(BASE + "/projects/%d/schedules" % PROJ,
    data=urllib.parse.urlencode({
        "processDefinitionCode": WF_CODE,
        "schedule": json.dumps(param, ensure_ascii=False),
        "failureStrategy": "END", "warningType": "NONE",
        "processInstancePriority": "MEDIUM", "workerGroup": "default"}).encode(),
    headers={"Content-Type":"application/x-www-form-urlencoded"})
print(json.loads(op.open(req).read().decode())["code"])       # 0 = 成功
```

**★ 第 2 步：立刻确认它确实是「下线」（关键安全检查）**

```bash
sudo -n mysql dolphinscheduler -t -e \
 "select id, process_definition_code, crontab, release_state, timezone_id from t_ds_schedules;"
# 期望 release_state = 0  ← 0 表示 OFFLINE，绝不会触发
```

实测输出：

```
1  23240142767136  0 0 3 * * ? *  0  Asia/Shanghai
```

**★ 第 3 步：让它生效（⚠️ 这一步之后它就会真的按点自动跑了）**

```
POST /projects/{projectCode}/schedules/{id}/online
```

**★ 第 4 步：想暂停 / 想删掉**

```
POST   /projects/{projectCode}/schedules/{id}/offline     # 暂停（保留配置）
DELETE /projects/{projectCode}/schedules/{id}             # 删除
```

删完再查一次，确认恢复原状：

```bash
sudo -n mysql dolphinscheduler -N -B -e "select count(*) from t_ds_schedules;"   # → 0
```

### 6.4 关于 cron 表达式（初学者一定搞错的地方）

**DS 用的是 Quartz 风格，字段数比 Linux 的 crontab 多一个「秒」，而且秒在最前面：**

```
秒  分  时  日  月  周  [年]
0   0   3   *   *   ?   *      = 每天 03:00:00
0   */30 *  *   *   ?          = 每 30 分钟
0   0   2   ?   *   MON        = 每周一 02:00
```

| 容易错 | 说明 |
|---|---|
| 以为是 5 段 | **是 6 段（秒在最前）**，写 5 段会解析失败 |
| `日` 和 `周` 同时写 `*` | Quartz 要求这两者**必须有一个是 `?`**（"不指定"），不能都写 `*` |
| 时区 | 一定要显式写 `timezoneId`（本项目用 `Asia/Shanghai`）。不写会跟服务器默认时区走，可能差 8 小时 |

> 💡 **初学者福利**：**网页上有 cron 生成器**（工作流定义 → 那一行的时钟图标 → 创建定时），点选"每天 3 点"它自动帮你生成，不用手写。

---

## 七、最可能踩的 9 个坑（附一眼判据）

| # | 现象 | **一眼判据** | 解法 |
|---|---|---|---|
| 1 | 工作流"运行"按钮是灰的，点不动 | 列表页状态列显示"下线" | 先点该行的**上线** |
| 2 | 节点一直"运行中"，**但日志里没有任何报错、也永远不提交作业** | `ps` 里 beeline 进程状态是 **`Tl+`** | 任务脚本结尾漏了 **`</dev/null`**（beeline 被 SIGTTOU 冻住） |
| 3 | 停了任务，但它好像还在跑 | `ps -ef \| grep jdbc:hive2` 还有输出；`yarn application -list` 还有作业 | **DS 停不掉远端 beeline**（只杀 worker 上的进程树）→ `pkill -f 'jdbc:hive2'` |
| 4 | `systemctl` 说 active，但连不上 / 查询失败 | `ss -lntp \| grep :10000` 没监听，或发查询报错 | active 之后还要等几十秒 → **一律用外部判据**（端口 + 真实查询） |
| 5 | 新建工作流，明明建成功了却查不到 | `query-by-name` 回 `50003 does not exist` | 这个接口**是坏的**，改用分页列表接口自己按名字找 |
| 6 | 配定时回 `10076 create schedule error`，看不出问题 | api-server 日志里有 `Parse object exception ... ScheduleParam` | `schedule` 要传 **JSON 对象**，不是 cron 字符串 |
| 7 | 检查"有没有残留 beeline"时，明明没有却总是报 1 个 | 换个不自我匹配的写法，数字就变成 0 | **监控命令自己被自己匹配到了**（见下方说明）→ 用 `pgrep -f 'jdbc[:]hive2'` |
| **8** | ★ **打开工作流，16 个节点只剩 1 个**（且没有连线） | 查 task code 量级：`select max(code) from t_ds_task_definition;` **> 2^53** 就是它 | 接口把 code 当 **JSON 数字**返回，浏览器双精度浮点**只能精确到 2^53** → 多个 code 塌缩成同一个 → 只剩 1 个节点。**执行完全正常**（数据库/master 是 64 位整数）→ 只能人眼发现。**解法：在网页上拖节点让 DS 自己生成 code**（见下方说明） |
| **9** | **实例一直 RUNNING，一个任务都不下发；master CPU 打满、日志每秒涨几千行** | master 日志刷 `DagHelper.parsePostNodes ... NullPointerException` + `will resend` | **`startNodeList`（从指定节点重跑）触发的**。**重启 master 无效**，必须停 master + 删掉那条实例（见 §4.5） |

### ★ 第 8 坑细说：画布上 16 个节点只剩 1 个 → 查 task code 是不是超过 2^53

> ⚠️ **我当天先写错过一次**：一开始我把根因写成"节点坐标间距只有 45px，太挤"。
> **那是错的** —— 改成 220px 之后画布**依然**只有 1 个节点。真根因是**整数精度**。

**现象**：工作流**执行完全正常**（16 个节点全跑成功、12 张表行数全对），
但打开 DAG 画布，**只有一个节点，而且没有任何连线**。

**为什么**：DS 接口把 task code 当 **JSON 数字**返回，而浏览器用 JavaScript 的 `Number`
（IEEE754 双精度浮点）解析 —— **它只能精确表示 ≤ 2^53 = 9007199254740992（约 9.0e15）的整数**。
超过之后，**多个不同的 code 在浏览器里会变成同一个数** → 16 个节点塌缩成 1 个。

**判据（一条 SQL 定案）**：

```bash
sudo -n mysql dolphinscheduler -N -B -e "
select concat('最大 task code = ', max(code),
              '   JS 安全上限 2^53 = ', 9007199254740992,
              '   安全？ ', if(max(code) <= 9007199254740992, '✅ 是', '❌ 否 → 画布一定只剩 1 个'))
from t_ds_task_definition;"
```

实测对照（本项目真实数据）：

| code 从哪来 | 量级 | 浏览器能分清吗 | 画布上 |
|---|---|---|---|
| **网页上拖节点，DS 自己生成** | `23242403233824`（2.3e13） | ✅ 能 | ✅ 16 个正常 |
| 我程序化手写的 19 位 | `3300000000000000001`（3.3e18） | ❌ 塌缩 | ❌ 只剩 1 个 |
| 旧工作流 `gmall_dw_pipeline` | `1100000000000000001`（1.1e18） | ❌ 塌缩 | ❌ 一直只剩 1 个 |

> **最后一行请你记住**：本项目那两个旧工作流的 code 也是 19 位手写值，
> **所以它们从一开始就只显示 1 个节点**，只是没人打开看过。**不是这次新弄坏的。**

**解法**：

- **最省事**：**在网页上拖节点建，让 DS 自己生成 code** —— 这是最安全的做法。
  完整步骤见 `从零在网页上创建gmall_evt_pipeline工作流.md`
- **程序化建**：自己指定的 code **必须 ≤ 2^53**（如 14 位 `33000000000001`）

> ★ **教训**：**"跑得对"和"显示得对"是两条独立的技术栈。**
> 执行链路是 **Java/Python（64 位整数）**，显示链路是 **浏览器 JavaScript（双精度浮点）** ——
> 同一个 ID 在两边能表示的范围**不一样**。跨这条边界传 ID 时：**要么用字符串，要么确认范围。**
> 而且因为"数据全对、测试全绿"，它**只能靠人眼看出来**。

**顺便说一个次要问题**（同一天也踩了，但不是根因）：
节点坐标间距也要 **≥ 220px**（DS 节点图形宽约 180~200px），否则**即使 code 修好了也会挤成一团**。
两个条件都要满足：**code ≤ 2^53 且 间距 ≥ 220px**。

### ★ 第 9 坑细说：`startNodeList` 会把 master 打成 NPE 死循环

完整现象、证据表、以及**救火步骤**见 **§4.5**。这里只留一句：

> **要重跑就整条重跑。** 带 `startNodeList` 的启动在本项目的 DS 3.1.8 上三次复现、三次必挂，而且**重启 master 修不好**。

### ★ 第 7 坑细说：`pgrep -f` 会匹配到"正在执行这条检查命令的进程自己"

这坑我自己在写本教程时也踩了一次，值得单独讲，因为你写监控脚本时**一定会遇到**。

```bash
# ✗ 错的写法：永远至少返回 1
pgrep -cf 'jdbc:hive2'
#    为什么：-f 是"匹配整条命令行"。而这条检查命令本身（bash -c "... pgrep -cf 'jdbc:hive2' ..."）
#    的命令行里就含有 jdbc:hive2 这个字符串 → 它把自己算进去了

# ✓ 对的写法：用中括号拆开关键字，让正则匹配"jdbc:hive2"、但这行命令的字面内容是"jdbc[:]hive2"
pgrep -cf 'jdbc[:]hive2'      # → 0，真实结果
```

**同一次检查里的实测对照**（同一时刻、同一台机器）：

```
pgrep -cf 'jdbc:hive2'     → 1      ← 假阳性（匹配到自己）
pgrep -cf 'jdbc[:]hive2'   → 0      ← 真实值
jps -l | grep -i beeline   → (无)   ← 交叉验证，确认真的没有
```

> 💡 **通用教训**：**监控脚本的"模式字符串"本身也是一种数据。**
> 任何用 `grep` / `pgrep -f` 去"找有没有 X"的检查，都要想一下"我这句话本身会不会被算成 X"。
> 交叉验证（`jps`、`yarn application -list`）是好习惯。
>
> 📌 顺带说明：本项目 `ds_preflight.sh` / `ds_monitor.sh` 里的同类检查**没有这个问题** —— 因为它们的命令行是 `bash /home/lrh/workspace/new_chain/ds_preflight.sh`，**里面不含**这个关键字。只有**内联写在一行命令里**的检查才会自我匹配。

**通用套路：接口只回一句笼统的错误码时，别猜 —— 去 `api-server` 日志里找真正的异常。**

```bash
A=/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/api-server/logs
tail -50 $A/dolphinscheduler-api.log
# 找 [ERROR] 那一行往下 20 行，通常有 Java 异常栈，里面直接写着参数/类型问题
```

---

## 八、验收清单：怎么证明"我这轮真的跑通了"

**别只看"服务是 active"、也别只看"节点是绿的"。** 要**过程证据 + 结果证据**两条都要。

### ① 过程证据：16 个节点全绿

```bash
sudo -n mysql dolphinscheduler -N -B -e \
 "select concat('实例状态=', state) from t_ds_process_instance where id=<你的实例id>;"
# 期望：实例状态=7

sudo -n mysql dolphinscheduler -N -B -e \
 "select concat('节点总数=', count(*), ' 成功=', sum(state=7), ' 非成功=', sum(state<>7))
  from t_ds_task_instance where process_instance_id=<你的实例id>;"
# 期望：节点总数=16 成功=16 非成功=0
```

### ② 结果证据：12 张表的行数要对得上

```bash
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
$BEELINE -u jdbc:hive2://lrh-1:10000 --silent=true --outputformat=csv2 -e "
select 'ods_event_log_di' t, count(*) c from gmall.ods_event_log_di
union all select 'dwd_event_log_di', count(*) from gmall.dwd_event_log_di
union all select 'dwd_purchase_fact_di', count(*) from gmall.dwd_purchase_fact_di
union all select 'dim_product', count(*) from gmall.dim_product
union all select 'dws_user_event_day', count(*) from gmall.dws_user_event_day
union all select 'dws_product_event_day', count(*) from gmall.dws_product_event_day
union all select 'dws_category_event_day', count(*) from gmall.dws_category_event_day
union all select 'dws_trade_day', count(*) from gmall.dws_trade_day
union all select 'ads_active_user_day', count(*) from gmall.ads_active_user_day
union all select 'ads_funnel_day', count(*) from gmall.ads_funnel_day
union all select 'ads_repurchase_day', count(*) from gmall.ads_repurchase_day
union all select 'ads_top_product_day', count(*) from gmall.ads_top_product_day
;" 2>/dev/null | grep -E "^(ods_|dwd_|dws_|ads_|dim_)"
```

**期望值**（2026-10-03 实测基线，对不上就说明这轮有问题）：

| 表 | 行数 |
|---|---|
| `ods_event_log_di` | 10,630,245 |
| `dwd_event_log_di` | 10,623,154 |
| `dwd_purchase_fact_di` | 186,059 |
| `dim_product` | 140,665 |
| `dws_user_event_day` | 1,622,820 |
| `dws_product_event_day` | 1,265,817 |
| `dws_category_event_day` | 1,853 |
| `dws_trade_day` | 31 |
| `ads_active_user_day` / `ads_funnel_day` / `ads_repurchase_day` | 各 31 |
| `ads_top_product_day` | 1,550 |

### ③ 业务证据：GMV 四路必须收敛到同一个数

去 `verify` 节点的日志里找这一段（**这是整套校验里最有价值的一段**）：

```
| 商品维度 GMV    | 5.75759965E7 |
| 类目维度 GMV    | 5.75759965E7 |
| 交易维度 GMV    | 5.75759965E7 |
| DWD事实表 GMV   | 5.75759965E7 |     ← 四条互相独立的计算路径，结果必须一样
```

**为什么这条最有价值**：商品/类目/交易三条路径**维度不同、算法不同**，但算的是同一批 purchase 事件。**只要任何一条写错，这里立刻不相等。**

### ④ 收工检查：数据面要干净

```bash
# ⚠️ 注意用中括号写法，否则这条命令会匹配到自己（见 §7 第 7 坑）
echo "beeline=$(pgrep -cf 'jdbc[:]hive2')"                     # 期望 0
/opt/hadoop/hadoop-3.3.6/bin/yarn application -list | tail -3  # 期望没有 RUNNING 的
```

**为什么这一步不能省**（这是本项目真实踩过的坑）：
上一次（09-30）有人用 DS 停了工作流，界面显示"已停止"，**但真正干活的 beeline 在 lrh-1 上又跑了 3 天**，把 5 天数据全写完了。
如果那时直接启动新一轮，就会变成**两个 beeline 抢 HiveServer2、同时 `INSERT OVERWRITE` 同一张表** —— 结果不可预测。

> **一句话**：**"控制面显示停了" ≠ "数据面真的停了"。** 两边都要看。

---

## 九、术语速查表

| 名词 | 一句话解释 |
|---|---|
| **DolphinScheduler (DS)** | 工厂的自动排班系统：决定几点跑、按什么顺序跑、挂了怎么办 |
| **工作流 / 工作流定义** | 排班表（图纸）。一份图纸可以跑很多趟 |
| **工作流实例** | 今天这一趟实际执行（"填过的那张表"） |
| **节点 / 任务定义** | 图纸上的一个格子 |
| **任务实例** | 这一趟里某个格子的实际执行 |
| **依赖边（relation）** | 格子之间的箭头。本项目 16 个格子串成一条直线 |
| **SHELL 节点** | 格子内容就是一段 shell 命令，本项目每格跑一个 SQL |
| **上线 / 下线（release）** | 把图纸贴到墙上 / 取下来。**只有上线的能运行** |
| **租户（tenantCode）** | 以哪个 Linux 用户身份跑任务。本项目是 `lrh` |
| **worker 组（workerGroup）** | 指定在哪群机器上跑。本项目是 `default`（= lrh-2/lrh-3 两台） |
| **crontab（Quartz 风格）** | 定时表达式。**6 段，秒在最前**：`秒 分 时 日 月 周` |
| **幂等（INSERT OVERWRITE）** | 同一段写入跑一次和跑十次，结果一样 → 失败了可以放心重跑 |
| **外部判据** | 判"服务好了没"不看它自己报的状态，而是从外面真试一下（端口 + 真实查询） |
| **孤儿进程** | 父进程没了但自己还活着的进程。本项目 `ssh` 断开后的 beeline 就是 |

---

## 附录 A：2026-10-03 那次实测的完整时间线（供你对照）

```
10:22:02   工作流实例 #6 开始
10:22:03   preflight            → 10:22:27 ✅ 24s      （体检 + 生成分段 SQL）
10:22:28   ods_ready            → 10:24:27 ✅ 1m59s
10:24:27   dwd_ddl              → 10:24:46 ✅ 19s
10:24:47   dwd_event_clean      → 10:33:24 ✅ 8m37s    （清洗 1063 万行）
10:33:24   dwd_purchase_fact    → 10:34:39 ✅ 1m15s
10:34:40   dwd_dim_product      → 10:42:16 ✅ 7m36s
10:42:17   evt_ddl              → 10:42:34 ✅ 17s
10:42:35   dws_user             → 10:45:23 ✅ 2m48s
10:45:25   dws_product          → 10:48:01 ✅ 2m36s
10:48:02   dws_category         → 10:50:02 ✅ 2m00s
10:50:03   dws_trade            → 10:51:12 ✅ 1m09s
10:51:13   ads_active_user      → 10:54:42 ✅ 3m29s
10:54:43   ads_funnel           → 10:56:30 ✅ 1m47s
10:56:31   ads_repurchase       → 10:58:58 ✅ 2m27s
10:58:59   ads_topn             → 11:00:47 ✅ 1m48s
11:00:48   verify               → 11:16:11 ✅ 15m23s   （三层校验，6 个 MR 作业）
11:16:12   ★ 实例 #6 = SUCCESS，16/16 节点全绿，总计 54 分 10 秒
11:16:12   收工检查：beeline=0、YARN=0、DS 排队命令=0（干净收工）
```

**对照要点**：如果某次跑出来**明显慢于**上面这些数（比如 `dwd_event_clean` 超过 15 分钟），先去看 YARN 是不是在排队，而不是怀疑 SQL。

---

## 附录 B：常用命令一张纸（复制粘贴版）

```bash
# ── 连集群 ──────────────────────────────────────────────
ssh -i "D:\数据开发环境\VMware\sshkeys\cluster_ed25519" lrh@192.168.87.131

# ── 拉起 DS（每次虚拟机重启后）──────────────────────────
DS=/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin
$DS/bin/dolphinscheduler-daemon.sh start master-server   # lrh-1
$DS/bin/dolphinscheduler-daemon.sh start api-server      # lrh-1（最后启）
ssh lrh-2 "$DS/bin/dolphinscheduler-daemon.sh start worker-server"
ssh lrh-3 "$DS/bin/dolphinscheduler-daemon.sh start worker-server"

# ── 看 DS 状态 ──────────────────────────────────────────
sudo -n mysql dolphinscheduler -t -e "select code,name,release_state from t_ds_process_definition;"
sudo -n mysql dolphinscheduler -t -e "select id,name,state,start_time,end_time from t_ds_process_instance order by id desc limit 5;"
sudo -n mysql dolphinscheduler -N -B -e "select count(*) from t_ds_schedules;"

# ── 跑一次真实链路 ──────────────────────────────────────
cd /home/lrh/workspace/new_chain
python3 ds_evt_pipeline.py info      # 先确认节点数/上线状态
python3 ds_evt_pipeline.py online
python3 ds_evt_pipeline.py run
setsid nohup bash ds_monitor.sh <实例id> >/dev/null 2>&1 </dev/null &
tail -f /home/lrh/workspace/new_chain/ds_steps/monitor.log

# ── 出问题先看这三处 ────────────────────────────────────
tail -50 /home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/api-server/logs/dolphinscheduler-api.log
tail -30 /home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/master-server/logs/dolphinscheduler-master.log
ps -ef | grep jdbc:hive2 | grep -v grep        # 有没有孤儿 beeline
/opt/hadoop/hadoop-3.3.6/bin/yarn application -list

# ── 应急：停掉一切并清理 ────────────────────────────────
pkill -f 'jdbc:hive2'                          # 确认没别人在跑再执行
cd /home/lrh/workspace/new_chain && python3 ds_evt_pipeline.py offline
```

---

*本教程所有命令与数字均来自 2026-10-03 的实测运行（DS 实例 #6，16/16 节点 SUCCESS，54 分 10 秒）。
凡是未实测的路径（网页按钮的具体位置、`startNodeList` 局部重跑）都已就地标注。
定时调度一节：创建/查询/删除实测通过，`/online` 那一步**故意未执行**。*
