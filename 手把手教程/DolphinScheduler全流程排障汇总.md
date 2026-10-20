# DolphinScheduler gmall 数仓全流程排障汇总

> 日期：2026-09-22
> 场景：在 VMware 三节点集群（lrh-1/2/3）上用 DolphinScheduler 3.1.8 编排 gmall 数仓全流程
> 链路：建库 → DDL → ODS → DWD → DWS → ADS → 验证
> 结果：尾段工作流实例 4 于 22:01:47 全节点 SUCCESS，各层数据落库校验通过

---

## 一、问题速查表

| # | 问题 | 现象 | 根因 | 解决 |
|---|---|---|---|---|
| 1 | **master 事件死循环（NPE）** | master CPU 100%，日志 ~96MB/分钟暴涨 | 旧定义实例触发 START_WORKFLOW 事件抛 NPE，`WorkflowStartEventHandler` 无限重发 | 停 master → 从 DB 清除失控实例 → 截断日志 → 重启 master |
| 2 | REST 无法停止/删除实例 | `stop` 返回 50006，实例卡在 `READY_STOP`；`delete` 返回 50006/10163 | DS 状态机约束：非终态实例不允许删；定义被实例引用不允许删 | 绕过 REST，直接 `DELETE FROM t_ds_*` 清库 |
| 3 | api-server 被 OOM 杀掉 | 接口全是 `Connection refused` | lrh-1 仅 3868MB 内存，master+api+alert+HS2+RM 挤爆 | 停 alert-server 腾内存，重启 api-server |
| 4 | stale beeline 进程并发 | 两个 beeline 同跑 `etl_dws_user.sql` 抢 HiveServer2 | 实例被删但进程未回收，残留 beeline 继续跑 | 定位 PID → `kill -9` |
| 5 | YARN 作业长时间 ACCEPTED | 作业进度 0%，长时间不推进 | 单节点可用内存不足，容器排队等资源 | 属正常排队；降 SQL 端内存参数（见 §3.5） |
| 6 | 实例观察脚本超时退出 | 输出 `=== timeout, still not terminal ===` | 脚本 deadline 设 2400s，而 ads_opt 单节点耗时 40 分钟 | 加大 deadline 至 5400s |
| 7 | **PowerShell 转义陷阱** | `CLIXML` 报错、`unexpected EOF`、命令被截断 | PS 会吞掉反引号/`$((...))`/双引号 | 复杂脚本写成 `.sh` 文件 scp 上传后执行 |
| 8 | PATH 被覆盖导致命令找不到 | `jps: command not found`、`/bin/yarn: No such file` | `export PATH=$JAVA_HOME/bin:$PATH` 在非交互 shell 里 `$PATH` 为空 | 显式写全 PATH |
| 9 | 本机 12345 隧道失效 | Python 报 `WinError 10061 目标计算机积极拒绝` | 隧道进程随观察脚本一起退出 | 改为直连 `192.168.87.131:12345` |
| 10 | SSH 免密登录失败 | `Permission denied (publickey,password)` | 未指定密钥文件 | `ssh -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519` |
| 11 | VM 里没有 curl | 探活脚本报 `command not found` | 最小化安装未带 curl | 用 Python `urllib` / `ss -ltn` 替代 |
| 12 | beeline SQL 引号被吞 | `syntax error near unexpected token '('` | 经 PS → ssh 两层传递后 SQL 里的引号丢失 | SQL 写进 `.sql` 文件，用 `beeline -f` 执行 |
| 13 | PowerShell 调 DS 接口 401 | 登录返回 `code=0`，后续请求仍 401 | `WebRequestSession` 未正确携带 `sessionId` Cookie | 改用 Python `http.cookiejar` 保持会话 |
| 14 | 新建工作流传 `code` 被忽略 | 请求里指定 code，落库变成另一个 code | DS 自动生成真实 code | 创建后从响应/DB 回读真实 code |
| 15 | lrh-3 短暂不可达 | `ping` 100% 丢包，`No route to host` | 节点在重启/内存压力窗口内 | 等待恢复；当前 3 节点均 RUNNING |
| **16** | **停任务杀不掉远端 beeline（孤儿进程）** | DS 里实例已 STOP、任务已 KILL，但 lrh-1 上 beeline 还在跑、YARN 还在提交作业 | 任务脚本是 `ssh lrh-1 'beeline …'`；非交互会话**没有终端**，ssh 断开时 sshd 不发 SIGHUP，远端进程变孤儿 | 见 §1.1 第 1 条：`pkill -f 'jdbc:hive2'`；并把它做成工作流的开工前检查 |
| **17** | **`releaseState` 传 0/1 报 10108** | `POST …/release?releaseState=1` → `10108 release process definition error`（没有任何细节） | 参数类型是枚举 `ReleaseState`，Spring 要**枚举名** | 传 `ONLINE` / `OFFLINE` |
| **18** | **`timeout` 传 0 报 10002** | `POST …/start-process-instance` → `10002 task timeout parameter is not valid` | DS 内部校验是 `timeout > 0 && timeout <= 86400`，0 非法 | 传 `86400`（24h）；判据见 §1.1 第 3 条 |
| **19** | **`query-by-name` 接口是坏的** | 对**确实存在**的工作流也回 `50003 … does not exist` | 该接口在本 build 下不可用（Swagger 里有、但不能用） | 改用分页列表 `GET /process-definition?pageNo=1&pageSize=200` 自己按名字找 |
| **20** | ★★ **工作流有 16 个节点，画布上看着只有 1 个**（而且没有任何连线） | 查 task code 的量级：`select max(code) from t_ds_task_definition;` 若 **> 9007199254740992（2^53）** 就是它 | 接口把 code 当 **JSON 数字**返回，浏览器用 JavaScript 双精度浮点解析（只能精确到 2^53）→ **多个不同 code 塌缩成同一个数** → 画布只剩 1 个节点。**数据库/master 是 64 位整数，所以执行完全正常**（跑得对、显示全错） | **在网页上拖节点，让 DS 自己生成 code**（实测 2.3e13，安全）；自己指定 code 时**必须 ≤ 2^53**；见 §1.5 |
| **21** | **★ 实例一直 RUNNING、一个任务都不下发；master CPU 打满、日志每秒涨几千行** | master 日志刷 `DagHelper.parsePostNodes(DagHelper.java:306) NullPointerException` + `will resend` | **`startNodeList`（"从指定节点重跑"）触发**。实测 3/3 必挂，**重启 master 修不好** | 停 master → 删掉卡死实例 → 截断日志 → 重启；**今后一律整条重跑**；见 §1.6 |
| **22** | **节点数翻倍（16 个变 32 个）** | `select count(*) from t_ds_task_definition_log where code between …` 有残留 | `PUT` 更新定义会往 **`*_log` 版本历史表**里再写一份，同一 code 出现两个版本 | 清库时必须**连 `t_ds_task_definition_log` / `t_ds_process_definition_log` 一起清**；见 §1.7 |
| **23** | **某一轮跑批突然变成两倍耗时**（任务列表里同一个节点有两条记录） | 实例里某节点有 **2 条 task_instance**，第 1 条 `state=8`（need fault tolerance）且 `end_time` 为空 | **worker 掉注册 → 容错重跑**。本项目的 worker 只有 **3868MB 内存**，跑重活时 CPU/内存吃紧会让 worker 与 ZooKeeper 的心跳断掉，master 就判它"死了"并把任务换到另一台重跑；而且因为连不上那台 worker，**旧 YARN 作业杀不掉**，两个作业抢资源 → 重试那一遍慢好几倍 | 跑批前确认 **2 个 worker 都在**（`ls /dolphinscheduler/nodes/worker`）；掉了一个就 `stop`+`start worker-server` 重新注册；见 §1.8 |

---

## 一.1、2026-10-03 补充：新增 7 个坑（第 16~22 坑，详述）

> 背景：这一轮是把**真实数据链路（1063 万行）**接进 DS、建成 16 节点工作流 `gmall_evt_pipeline`。
> 完整过程见 `../实验分析/实验08_真实链路接入DolphinScheduler.md`。

### 1.1 第 16 坑：DS 停任务**杀不掉**远端 beeline（这是第 4 坑的再次发生，而且更隐蔽）

**现象**：09-30 的 `dws_order` 节点被停掉后，`t_ds_task_instance` 里是 `state=9 (KILL)`、`exitStatusCode:143`，
但 **lrh-1 上的 beeline 一直活到 10-03**，把 `etl_dws_order.sql` 的 5 天数据全部跑完了。

**证据链（怎么证明"它真的还在跑"）**：

```bash
# ① 进程还在，且父进程是"不分配终端的 sshd"
ps -eo pid,ppid,etimes,lstart,cmd | grep -E "jdbc:hive2" | grep -v grep
#    → bash -c … beeline … ，其父是 "sshd: lrh@notty"

# ② YARN 应用的时间戳是铁证（Start 在 09-30，Finish 在 10-03）
yarn application -status application_<ts>_0023 | grep -E "Start-Time|Finish-Time"

# ③ 结果表居然是全的 —— 这轮"被停掉"的任务其实跑完了
#    （这意味着：如果它在写到一半被杀，表就是半截数据，而没人会知道）
```

**根因**：任务脚本本体是 `ssh lrh-1 'beeline …'`。命令跑在 **lrh-1** 上，由 lrh-1 的 sshd 拉起。
因为是非交互会话（`-n`，**没有分配终端**），ssh 连接断开时 sshd **不会**给这个子进程发 SIGHUP → 它变成孤儿继续跑。
DS 只能杀掉 **worker 机器（lrh-2/3）上的进程树**，管不到 lrh-1 上这个孤儿。

**判据（停任务后必须做）**：

```bash
# DS 侧状态已是终态，但数据面未必：
sudo -n mysql dolphinscheduler -t -e \
  "select id,name,state from t_ds_task_instance where process_instance_id=<id>;"
ps -ef | grep jdbc:hive2 | grep -v grep     # ← 这一条才是"真的停了吗"
yarn application -list                       # ← 作业还在提交就说明没停干净
```

**处置**：

```bash
pkill -f 'jdbc:hive2'        # 确认没有别人在跑再执行
```

**预防**：本次新建的工作流把这项检查做成了**第 1 个节点 `preflight`**：检测到有 beeline 或 YARN 作业就**直接拒绝开工**，
并打印清理命令。24 秒换掉一整类"两个 beeline 抢 HiveServer2 / 同时 INSERT OVERWRITE 同一张表"的不可预测故障。

### 1.2 第 17 坑：`releaseState` 必须传枚举名，不能传 0/1

```bash
# ✗ 错：回 10108「release process definition error」，没有任何细节
curl -X POST ".../process-definition/<code>/release?releaseState=1&name=<wf>"
# ✓ 对：
curl -X POST ".../process-definition/<code>/release?releaseState=ONLINE&name=<wf>"
```

真实异常在 api-server 日志里（`api-server/logs/dolphinscheduler-api.log`）：
`No enum constant org.apache.dolphinscheduler.common.enums.ReleaseState.1`。
**接口返回的 `msg` 是笼统的，真正的异常一定要去 api-server 日志里找。**

### 1.3 第 18 坑：`timeout` 必须落在 `(0, 86400]`

`timeout=0`（"不超时"的直觉写法）会被判非法，返回 `10002 task timeout parameter is not valid`。
**这是反编译看出来的**，Swagger 文档完全没写取值范围：

```bash
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
J=/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/api-server/libs/dolphinscheduler-api-3.1.8.jar
cd /tmp && unzip -o -q "$J" 'org/apache/dolphinscheduler/api/service/impl/ExecutorServiceImpl.class'
javap -p -c org/apache/dolphinscheduler/api/service/impl/ExecutorServiceImpl.class | grep -B12 TASK_TIMEOUT
#  47: aload 17 / 49: Integer.intValue / 52: ifle 65        ← timeout <= 0 → 报错
#  60: ldc #14  // int 86400 / 62: if_icmple 81             ← timeout > 86400 → 报错
```

> **方法论**：接口只回一句笼统错误码、日志里也没有栈时，**别猜，去读它的字节码**。`javap -c | grep -B12 <错误常量>` 通常 1 分钟就能定位。

### 1.4 第 19 坑：`query-by-name` 接口是坏的（Swagger 里有，但不能用）

```bash
# ✗ 对已存在的 gmall_evt_pipeline 也会回 50003 does not exist
GET /projects/<proj>/process-definition/query-by-name?name=gmall_evt_pipeline
# ✓ 改用分页列表，自己按 name 匹配
GET /projects/<proj>/process-definition?pageNo=1&pageSize=200   → data.totalList[].name
```

**所以：不要用 `query-by-name` 判断"工作流是否已存在"。** 否则会误判成"没建成"，从而重复创建或误删。

### 1.5 ★★ 第 20 坑：task code 超过 2^53 → 16 个节点在画布上塌缩成 1 个

> ⚠️ **本节曾在 2026-10-03 当天写错过一次**：当时我把根因写成"节点坐标间距只有 45px，太挤"。
> **那是错的** —— 我把间距改成 220px 之后，画布**依然**只有 1 个节点，说明那不是根因。
> 真正的原因是**整数精度**（见下）。按本项目"错误的原因比错误的现象更危险"的纪律，这里保留更正说明。

**现象**：工作流**执行完全正常**（16 个节点全部 SUCCESS、12 张表行数全对），
但打开 DAG 画布**只有一个节点，而且没有任何连线**。
（这坑是用户先发现的 —— 所有自动化测试都通过，只有人眼看画布才暴露。）

**根因**：**DS 的接口把 task code / process definition code 当 JSON *数字* 返回**
（不是字符串），而浏览器用 JavaScript 的 `Number`（IEEE754 双精度浮点）解析它。
双精度**只能精确表示 ≤ 2^53 = 9007199254740992（约 9.0e15）的整数**；超过之后，
**两个不同的 64 位整数在浏览器里会变成同一个数**。

我用的是 19 位 code（`3300000000000000001` ≈ 3.3e18）。在这个量级，双精度的最小间隔（ULP）约 **512** ——
也就是说**最后两三位根本存不下**。于是 16 个 code 在浏览器里**全部塌缩成同一个数** `3300000000000000000`，
DAG 按 code 建 map → 只剩 1 个节点；`locations` 里的 `taskCode` 同样塌缩 → 只剩 1 个坐标。

**判据（一条 SQL 定案）**：

```bash
sudo -n mysql dolphinscheduler -N -B -e "
select concat('最大 task code = ', max(code),
              '   JS 安全上限 2^53 = ', 9007199254740992,
              '   安全？ ', if(max(code) <= 9007199254740992, '✅ 是', '❌ 否（画布一定塌缩成 1 个）'))
from t_ds_task_definition where code in
  (select post_task_code from t_ds_process_task_relation
   where process_definition_code = <你的工作流code>);"
```

**实测对照**：

| code 来源 | 量级 | 浏览器能分清吗 | 画布 |
|---|---|---|---|
| **网页上拖节点，DS 自动生成**（`gen-task-codes` 实测） | `23242403233824`（2.3e13） | ✅ 能 | ✅ 正常 |
| 手写 19 位（我程序化建的） | `3300000000000000001`（3.3e18） | ❌ 塌缩成 1 个 | ❌ 只有 1 个 |
| **旧工作流 `gmall_dw_pipeline`** | `1100000000000000001`（1.1e18） | ❌ 同样塌缩 | ❌ 一直只有 1 个 |
| **旧工作流 `gmall_dw_pipeline_tail`** | `2200000000000000001`（2.2e18） | ❌ 同样塌缩 | ❌ 一直只有 1 个 |

> **注意最后两行**：本项目那两个旧工作流的 code 也是 19 位手写值（估计是之前某次为了"好记"编的），
> **所以它们从一开始就是"画布上 1 个节点"**，只是没人打开看而已。这是个**历史遗留缺陷**，
> 不是本次新引入的。

**解法**：

1. **最省事**：**在网页上拖节点建**，让 DS 自己生成 code（就是安全量级）。
   教程见 `从零在网页上创建gmall_evt_pipeline工作流.md`
2. **程序化建**：自己指定的 code **必须 ≤ 2^53**（例如 14 位 `33000000000001`），
   并在代码里加断言。本项目 `ds_evt_pipeline.py` 已加：
   ```python
   assert TASK_CODE_BASE + len(NODES) <= 2 ** 53, "task code 超过 JS 安全整数，画布会塌缩成 1 个节点"
   ```
   且 `info` 子命令会自检并打印 `task code 最大 = … → JS 安全？ ✅ 是`

**副产物说明（不是根因，但确实值得改）**：我第一版还把节点坐标间距设成了 45px
（DS 节点图形宽约 180~200px），所以**即使 code 修好了，45px 也仍然会挤成一团**。
现在 `ds_evt_pipeline.py` 用的是 `NODE_DX = 220`（照抄网页拖出来的间距）。
**两个问题都要满足：code ≤ 2^53 且 间距 ≥ 220px。**

> ★ **教训**：**"跑得对"和"显示得对"是两条独立的技术栈。**
> 执行链路是 **Python/Java（64 位整数）**，显示链路是 **浏览器 JavaScript（双精度浮点）** ——
> 同一个 ID 在两边能表示的范围**不一样**。跨这一层边界传 ID 时，**要么用字符串，要么确认范围**。
> 而且因为"数据全对、测试全绿"，它**只能靠人眼看出来**。

### 1.6 ★★ 第 21 坑：`startNodeList`（从指定节点重跑）会把 master 打成 NPE 死循环

**这是本次踩得最重的一个，而且它推翻了本文件 §3.1 原来的结论。**

**现象（三个一起出现才算）**：

1. master **CPU 打满**，日志**每秒涨几千行**（实测 2 分钟涨到 **179 MB**，能把磁盘吃干）
2. 工作流实例永远停在 **RUNNING**，**一个任务都不下发**
3. 界面上**看不到任何任务级报错**（因为任务压根没被创建）

**日志特征**：

```
[ERROR] WorkflowExecuteRunnable:[709] - Start workflow error
java.lang.NullPointerException: null
    at org.apache.dolphinscheduler.service.utils.DagHelper.parsePostNodes(DagHelper.java:306)
    at ...WorkflowExecuteRunnable.submitPostNode(WorkflowExecuteRunnable.java:1319)
[ERROR] WorkflowStartEventHandler:[72] - Failed to submit the workflow instance, will resend...
```

**根因（反编译定位到具体那一行）**：`DagHelper.java:306` 是

```java
if (preTaskCode == null) {
    beginNodes = dag.getBeginNode();     // ← 第 306 行：dag 为 null，NPE
```

即 **DAG 对象是 null**（带 `startNodeList` 启动时没能构建出 DAG），于是 `WorkflowStartEventHandler` 无限重发 START 事件。

**怎么证明是 `startNodeList` 而不是别的**（对照实验，2026-10-03）：

| 实例 | 启动方式 | 结果 |
|---|---|---|
| 6 | **普通全量** | ✅ 16/16 SUCCESS |
| 9 | **普通全量**（2 节点测试流） | ✅ SUCCESS |
| **7、8、11** | **带 `startNodeList` / `TASK_ONLY`** | ❌ **NPE 死循环** |
| 12 | **普通全量** | ✅ 正常 |

三次带 `startNodeList` 全部必挂、零例外；两次普通全量全部成功。**结论明确。**

**★ 最关键的一条：重启 master 修不好。**
我试过 —— 重启后 master 会重新捡起那条实例继续循环。（§3.1 原来写"重启即可清除"，**是错的**。）

**救火步骤（顺序不能颠倒）**：

```bash
DS=/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin
export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH

# ① 先停 master（必须先停！否则清了库，master 内存态还是脏的）
$DS/bin/dolphinscheduler-daemon.sh stop master-server
for i in $(seq 1 12); do sleep 5; jps -l | grep -q MasterServer || break; done

# ② 找出那条卡死实例（特征：state=1 且它名下的 task_instance 数为 0）
sudo -n mysql dolphinscheduler -t -e "
 select pi.id, pi.name, pi.state, count(ti.id) as tasks
 from t_ds_process_instance pi
 left join t_ds_task_instance ti on ti.process_instance_id = pi.id
 group by pi.id, pi.name, pi.state order by pi.id desc limit 5;"

# ③ 删掉它
sudo -n mysql dolphinscheduler -e "
 delete from t_ds_task_instance    where process_instance_id = <id>;
 delete from t_ds_process_instance where id = <id>;
 delete from t_ds_command          where process_instance_id = <id>;
 delete from t_ds_error_command    where process_instance_id = <id>;"

# ④ 截断暴涨的日志，回收磁盘
: > $DS/master-server/logs/dolphinscheduler-master.log

# ⑤ 重启 master，用【普通全量】方式重跑
$DS/bin/dolphinscheduler-daemon.sh start master-server
```

**预防**：本项目 `ds_evt_pipeline.py` **已经删掉了这个功能**（连代码都不留，防止有人顺手用）。
**要重跑就整条重跑** —— ETL 幂等，约 54 分钟。

### 1.7 第 22 坑：`PUT` 更新定义会在 `*_log` 表留下重复行 → 节点数翻倍

**现象**：`python3 ds_evt_pipeline.py info` 显示 **"节点 32 个"**（本该 16 个），而且工作流启动即 NPE。

**根因**：DS 把任务定义的**版本历史**放在另一张表里：

| 表 | 作用 |
|---|---|
| `t_ds_task_definition` / `t_ds_process_definition` | **当前**版本 |
| `t_ds_task_definition_log` / `t_ds_process_definition_log` | **历史**版本（每次更新追加） |

`PUT /process-definition/{code}` 更新定义时，会往 `*_log` 里**再写一份**（同一 code 出现 v1、v2 两行）。
API 查 `taskDefinitionList` 时把两边合起来 → **同一个 code 出现两次 → 16 变 32**。

实测数据：

```
t_ds_task_definition      = 16      ← 当前版本
t_ds_task_definition_log  = 66      ← 历史残留（16×4 次更新 + 2 个测试任务）
```

**解法（清库时要连 `*_log` 一起清）**：

```sql
delete from t_ds_task_definition      where code between ...;
delete from t_ds_task_definition_log  where code between ...;   -- ← 少了这句就会翻倍
delete from t_ds_process_definition      where code in (...);
delete from t_ds_process_definition_log  where code in (...);   -- ← 同理
```

> **推论（对项目有影响）**：这也解释了为什么 `gmall_dw_pipeline`（PUT 更新过一次、现为 version=2）
> **一直跑不起来、库里没有成功实例** —— 它的 `*_log` 里同样有重复行。
> **在本 build 下，改工作流的可靠做法是"删掉重建"，而不是"更新"。**
> （注：用网页 DAG 编辑器保存，走的也是这个 PUT 路径。）

---

### 1.8 第 23 坑：worker 掉注册 → 容错重跑 → 耗时翻倍（实测 54 分变 97 分）

**现象**：某一轮跑批的耗时突然变成将近两倍；**在「任务实例」列表里，某个节点有两条记录**
（一条 `state=8`、`end_time` 为空；一条 `state=7` 成功）。

**实测案例（2026-10-03，实例 13）**：

| 事实 | 数据 |
|---|---|
| 整链耗时 | **96 分 58 秒**（正常基线 **54 分 10 秒**） |
| `dwd_event_clean` 第 1 次（id=45，跑在 **lrh-3**） | 17:46:14 起，`state=8`，**没有结束时间** |
| `dwd_event_clean` 第 2 次（id=46，跑在 **lrh-2**） | 18:04:56 → 18:39:18，**2062 秒（34 分）** ← 正常只要 **517 秒（8.5 分）** |
| 多出来的时间 | 第 1 次白跑 18 分半 + 第 2 次慢出 25 分半 ≈ **+44 分钟** |

**master 日志里的铁证**：

```
18:04:51.959  FailoverService: Worker failover staring, workerServer: 192.168.87.132:1234
18:04:52.003  Worker[192.168.87.132:1234] failover: begin to failover taskInstance,
                will set the status to NEED_FAULT_TOLERANCE
18:04:56.064  [WARN] connect to Host{address='192.168.87.132:1234'} error      ← 连不上 lrh-3 了
18:04:56.066  [ERROR] Kill yarn job failure, taskInstanceId: 45                ← 旧作业杀不掉
```

**根因**：**lrh-3 的 DS worker 与 ZooKeeper 的心跳断了**（master 于是判它"死了"）。
lrh-3 那台机器**并没有重启**（`uptime` 连续、HDFS/YARN 都还在），掉的是 **DS worker 的注册**。
大概率是**资源吃紧**：worker 节点只有 **3868MB 内存**，跑重活时 CPU 被抢，
journalctl 里在 17:52~18:06 期间持续报 `workqueue: psi_avgs_work hogged CPU ...`。

**为什么重试那一遍慢了 4 倍**：因为 lrh-3 连不上，**master 杀不掉第 1 次那个 YARN 作业**，
于是"第 1 次的残留作业"和"第 2 次的新作业"**同时往同一张表写** → 互相抢资源。

**判据（一眼看出来）**：

```bash
# ① 某个节点是不是有两条任务实例记录
sudo -n mysql dolphinscheduler -t -e "
select id,name,state,host,start_time,end_time from t_ds_task_instance
where process_instance_id=<实例id> and name='<节点名>' order by id;"
#    第 1 条 state=8（need fault tolerance）且 end_time 为空 = 容错重跑

# ② master 日志里的 worker failover
grep -aE "Worker failover|NEED_FAULT_TOLERANCE|Kill yarn job failure" \
  <安装目录>/master-server/logs/dolphinscheduler-master*.log | tail
```

**处置**：让掉线的 worker 重新注册即可（它会重新向 ZooKeeper 报到）：

```bash
DS=/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin
ssh lrh-3 "$DS/bin/dolphinscheduler-daemon.sh stop worker-server"
sleep 8
ssh lrh-3 "$DS/bin/dolphinscheduler-daemon.sh start worker-server"
sleep 35

# 确认 ZK 里是「2 个 worker + 1 个 master」
printf 'ls /dolphinscheduler/nodes/worker\nls /dolphinscheduler/nodes/master\nquit\n' \
  | timeout 25 /opt/zookeeper/apache-zookeeper-3.8.7-bin/bin/zkCli.sh -server lrh-1:2181 2>/dev/null | grep '^\['
# 期望：[192.168.87.130:1234, 192.168.87.132:1234]  /  [192.168.87.131:5678]
```

**预防（跑批前 10 秒就能做）**：

```bash
# DS 认到 2 个 worker 才开跑
printf 'ls /dolphinscheduler/nodes/worker\nquit\n' \
  | timeout 25 /opt/zookeeper/apache-zookeeper-3.8.7-bin/bin/zkCli.sh -server lrh-1:2181 2>/dev/null | grep '^\['
```

> ★ **这次要给它记一功**：**DS 的容错机制真的生效了** —— worker 掉了，它自动把任务换到另一台重跑，
> **全程没要人干预，最后仍然 16/16 成功、数据完全正确**。代价只是多花 43 分钟。
> 这说明"容错"不是纸面功能，是能救命的；而"旧作业杀不掉"则是它没做干净的地方。

> ⚠️ **遇到这种情况不要慌着"重跑一遍"**：先看任务是不是已经成功了（本例第 2 次是 `state=7`）。
> 如果已经成功、数据也对，那就**只是慢，不是错**，没必要再来一遍。

---

## 一.2、★ 更正：§3.1 的根因结论是错的

本文件 §3.1 记录过一次"master 事件死循环（NPE）"，当时的结论是：

> "NPE 来自 **master 进程内存态**（旧定义更新后的版本缓存/实例上下文不一致），**重启即可清除**，不需要改定义"

**2026-10-03 的对照实验证明这个结论不成立**：

- **重启 master 修不好** —— 重启后它会重新捡起那条实例继续循环（实测复现 2 次）
- **真正的触发条件是"带 `startNodeList` 启动"**，与"定义是否被更新过"无关
  （我用**从没被更新过**的新定义 + `startNodeList` 启动，一样必挂）
- 当时之所以"重启看起来有效"，是因为**同时把脏实例从库里删掉了** —— 是"删实例"起了作用，不是"重启"

**修正后的正确认知**：看到这个 NPE，**先怀疑"是不是有人用了从指定节点重跑"**，
然后按 §1.6 的步骤停 master + 删实例。**光重启没有用。**

---

## 二、环境基线

### 2.1 节点分布

| 主机 | IP | 角色 | DS 进程 |
|---|---|---|---|
| lrh-1 | 192.168.87.131 | Master + API + RM + HiveServer2 | MasterServer(22699)、ApiApplicationServer(24842) |
| lrh-2 | 192.168.87.130 | Worker + NodeManager | WorkerServer(2963)、NodeManager(1847) |
| lrh-3 | 192.168.87.132 | Worker + NodeManager | WorkerServer(3367)、NodeManager(1394) |

> alert-server 已主动停止（lrh-1 内存仅 3868MB，需要让位给 master/api/HiveServer2）。
> YARN 三节点均 `RUNNING`，各节点 `availMemoryMB=2048`。

### 2.2 关键路径与端口

| 项 | 值 |
|---|---|
| DS 安装目录 | `/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin` |
| DS master 日志 | `<安装目录>/master-server/logs/dolphinscheduler-master.log` |
| DS worker 日志 | `<安装目录>/worker-server/logs/` |
| DS Web/API 端口 | 12345 |
| DS master 注册端口 | 5678 |
| YARN RM Web | 192.168.87.131:8088 |
| HiveServer2 | 10000 |
| Web 账号 | `admin` / `dolphinscheduler123` |

### 2.3 本机（Windows）连接方式

```powershell
# 远程执行命令（必须带 -i 密钥，否则 Permission denied）
ssh -n -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 -o BatchMode=yes `
    -o StrictHostKeyChecking=no lrh@192.168.87.131 "<远程命令>"

# 上传脚本
scp -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 tmp_x.sh lrh@192.168.87.131:/home/lrh/workspace/
```

---

## 三、问题详述

### 3.1 ★ master 事件死循环（NPE 无限重发）

> ⚠️ **2026-10-03 更正**：本节下面的"根因"和"重启即可清除"**都不准确**，已由对照实验证伪。
> **真正的触发条件是"带 `startNodeList` 启动"（从指定节点重跑），而且重启 master 修不好。**
> 请以 **§1.6** 为准；本节保留原文，因为它的**现象描述和处置步骤**仍然有效。

**现象**

- master 进程 CPU 100%，单实例日志以约 1.7MB/s（≈96MB/分钟）速度暴涨，磁盘可用空间被快速吃掉
- 工作流实例停在 `SUBMITTED`/`RUNNING` 不动，任务一个都不下发

**关键证据**

```
[ERROR] WorkflowExecuteRunnable:[709] - Start workflow error
        java.lang.NullPointerException
[ERROR] WorkflowStartEventHandler:[72] - Failed to submit the workflow instance,
        will resend...
```

同一行 `will resend` 在日志里重复出现成千上万次 —— 这是死循环的决定性特征。

**根因定位过程**

1. 先怀疑定义数据损坏 → 查库比对：
   ```sql
   select code, version, release_state from t_ds_process_definition;
   -- 23110365900704  version=2  OFFLINE   ← 旧定义（PUT 更新过一次）
   -- 23123283707168  version=1  ONLINE    ← 新定义
   select count(*) from t_ds_process_task_relation where process_definition_code = 23123283707168;
   -- 9 条串行边，完整
   ```
2. 定义数据（工序、依赖边、version）**完全一致且正确** → 排除「定义损坏」
3. 结论：NPE 来自 **master 进程内存态**（旧定义更新后的版本缓存/实例上下文不一致），重启即可清除，不需要改定义

**解决方案（`tmp_fix.sh`）**

```bash
#!/bin/bash
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH=$JAVA_HOME/bin:$PATH
DS=/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin
OLD=23110365900704

# 1) 停 master（等 jps 里没有 MasterServer）
$DS/bin/dolphinscheduler-daemon.sh stop master-server
for i in 1 2 3 4 5 6; do
  sleep 5
  if ! jps -l | grep -q MasterServer; then echo "master stopped after $((i*5))s"; break; fi
done

# 2) 从 DB 清除失控实例（REST 删不掉，见 3.2）
sudo -n mysql dolphinscheduler -e "
delete from t_ds_task_instance    where process_instance_id in (1,2,3);
delete from t_ds_process_instance where id in (1,2,3);
delete from t_ds_command          where process_instance_id in (1,2,3) or process_definition_code=$OLD;
delete from t_ds_error_command    where process_instance_id in (1,2,3);"

# 3) 截断暴涨日志，回收磁盘
rm -f $DS/master-server/logs/dolphinscheduler-master.2026-09-22_2*.log
: > $DS/master-server/logs/dolphinscheduler-master.log

# 4) 重启 master
$DS/bin/dolphinscheduler-daemon.sh start master-server
for i in 1 2 3 4 5 6 7 8; do
  sleep 5
  if jps -l | grep -q MasterServer; then echo "master up after $((i*5))s"; break; fi
done
```

**验证**

- master 启动日志：`MasterServer started in 49.156 seconds`、注册成功、识别到 2 个 worker 节点
- 日志文件行数稳定在 492 行（不再暴涨），CPU 从 100% 降到 0.57
- 磁盘可用空间回收：9.4G → 11G

**注意**

- 必须先停 master 再清库，否则清理完 master 内存态里仍有脏数据
- MySQL 走 root socket 认证：`sudo -n mysql dolphinscheduler -e "..."` 可用；
  `mysql -uroot -proot` 会被拒（`ERROR 1698`）

---

### 3.2 REST 接口无法停止/删除实例

**现象**

```
stop   实例 2 → 200 {"code":50006}    # 实例停在 READY_STOP，没真正停
delete 实例 2 → 200 {"code":50006}
delete 旧定义  → 200 {"code":10163}   # 仍被实例占用
```

**根因**

- `50006`：DS 状态机要求实例先进入终态才允许删除；`READY_STOP` 只是「准备停」，不算终态
- `10163`：`t_ds_process_instance` 里还有引用该定义的实例记录，不允许删定义

**解决**

放弃 REST，直接清库（脚本见 §3.1 第 2 步）。清完即可删除定义或重新触发。

**相关表**

`t_ds_process_instance`、`t_ds_task_instance`、`t_ds_command`、`t_ds_error_command`

**实例状态码参考**

| 值 | 含义 | 值 | 含义 |
|---|---|---|---|
| 0 | SUBMITTED | 6 | FAILURE |
| 1 | RUNNING_EXECUTION | 7 | SUCCESS |
| 4 | READY_STOP | 9 | KILL |
| 5 | STOP | 17 | BLOCK |

---

### 3.3 api-server 被 OOM 杀掉

**现象**

观察脚本突然报：

```
URLError: [Errno 111] Connection refused
```

**证据**

```bash
dmesg | grep -i "out of memory"
# Out of memory: Killed process 19247 (java)   ← ApiApplicationServer
free -m
# total 3868  used 3031  free 532   ← 内存极度紧张
```

**根因**

lrh-1 总共只有 3868MB 内存，同时跑 master + api + alert + HDFS + YARN RM + HiveServer2，触发系统 OOM killer 随机杀进程。

**解决（`tmp_api_fix.sh`）**

```bash
$DS/bin/dolphinscheduler-daemon.sh stop alert-server   # 调度链路不需要 alert
$DS/bin/dolphinscheduler-daemon.sh start api-server
# 探活：VM 没有 curl，用 ss 看端口
for i in $(seq 1 12); do
  sleep 5
  if ss -ltn | grep -q ':12345'; then echo "api port 12345 listening after $((i*5))s"; break; fi
done
```

**结果**：`api port 12345 listening after 45s`，端口 5678/12345 均 LISTEN。

**长期建议**：单机 3.8G 不要再启用 alert-server；SQL 端内存参数也要收紧（见 §3.5）。

---

### 3.4 stale beeline 进程并发争抢 HiveServer2

**现象**

同一份 `etl_dws_user.sql` 有两个 beeline 在跑，互相抢 HiveServer2 连接与 YARN 资源，任务耗时异常、偶发超时。

**证据**

```bash
ps -eo pid,lstart,rss,args | grep hive-beeline | grep -v grep
# 21147  20:44:29  ... etl_dws_user.sql   ← 来自已被删除的实例 3
# 23470  20:50:02  ... etl_dws_user.sql   ← 当前实例 4
```

**根因**

用 DB 直删实例时，DS 只是丢了记录，**worker 上已经拉起的子进程不会被回收**，继续跑完为止。

**解决**

```bash
kill -9 21147
```

**教训**：直删实例后，一定要顺手检查并清理 worker 上的残留子进程（`hive-beeline` / `MRAppMaster` / `YarnChild`）。正确做法是优先用 DS 的 KILL 接口，直删只作为兜底。

---

### 3.5 YARN 作业长时间 ACCEPTED（资源排队）

**现象**

节点进度长时间显示 0%，实例整体卡住：

```
application_1789976424581_0093  INSERT INTO ads_dau (Stage-3)  MAPREDUCE  ACCEPTED  0%
```

**根因**

三节点各 `availMemoryMB=2048`，单 SQL 默认容器内存申请偏大，作业提交后在队列里排队等容器。

**排查手段**

```bash
yarn application -list            # 看 State / Progress
# 或直接打 YARN RM REST（无需登录）
# http://192.168.87.131:8088/ws/v1/cluster/apps?states=ACCEPTED,RUNNING
```

**解决**

- 属于**正常排队**，ADS 阶段 21 条语句逐条提交，"每条 ACCEPTED 一会儿再 RUNNING"是预期行为，不需要干预
- 若确实需要加速，在数仓 SQL 里收紧内存参数：
  ```sql
  set mapreduce.map.memory.mb=1024;
  set mapreduce.reduce.memory.mb=1024;
  set mapreduce.map.java.opts=-Xmx768m;
  set mapreduce.reduce.java.opts=-Xmx768m;
  set hive.exec.reducers.bytes.per.reducer=128000000;
  ```
  本次 `etl_dws_*.sql` / `etl_ads_*.sql` 已加这套参数

**判断技巧**：应用名后缀 `(Stage-3)`、`(Stage-5)` 会随执行推进变化，**后缀在变就说明在正常推进**，不是死锁。

---

### 3.6 实例观察脚本超时退出

**现象**

```
=== timeout, still not terminal ===
```

**根因**

脚本 `deadline = time.time() + 2400`（40 分钟）。而实例 4 实际跑了 1h11m45s，其中 ads_opt 单节点就占 39m54s。

**解决**

把 deadline 改为 5400s（90 分钟），并保留「内容变化才打印」的去重逻辑，避免刷屏：

```python
deadline = time.time() + 5400
last = ""
while time.time() < deadline:
    ...
    if line != last:
        print(time.strftime("%H:%M:%S"), line, flush=True)
        last = line
    if st in TERMINAL:      # TERMINAL = {5, 6, 7, 9}
        break
    time.sleep(20)
```

---

### 3.7 ★ PowerShell 转义陷阱（最容易被坑）

**现象**

远程命令报 `#< CLIXML ...` 一大段 XML 错误、`unexpected EOF while looking for matching `''`、或命令后半段被截断。

**根因**

PowerShell 会主动解释并吞掉这些字符：

- 反引号 `` ` `` 是 PS 的转义符
- `$(...)` 是 PS 的子表达式
- `$((...))` 会被拆成 `$( (...)` 导致语法错误
- 单引号字符串里嵌双引号也可能被剥离（实测 `grep -E "a|b"` 到远端变成 `grep -E a|b`，管道被当成了 shell 管道）

**解决**

| 场景 | 做法 |
|---|---|
| 简单命令 | PS 外层用双引号，内层用单引号：`ssh ... "jps -l \| grep -E 'MasterServer\|WorkerServer'"` |
| 复杂脚本（含反引号/算术/多行） | **写成 `.sh` 文件 → scp 上传 → 远端 `bash` 执行** |
| SQL | 写进 `.sql` 文件 → `beeline -f` |

> 结论：**不要在 PS 命令行里内嵌 bash 脚本**。这一条能省掉绝大多数排障时间。

---

### 3.8 PATH 被覆盖导致常规命令找不到

**现象**

```
jps: command not found
/bin/yarn: No such file or directory
tail/ps/grep/cut: command not found
```

**根因**

脚本里写了 `export PATH=$JAVA_HOME/bin:$PATH`，但在非交互式 ssh 执行环境下 `$PATH` 可能为空，于是整个 PATH 只剩 java 目录，`tail`/`ps`/`grep` 全丢。

**解决**

脚本开头显式写全 PATH：

```bash
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH="$JAVA_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export HADOOP_HOME=/opt/hadoop/hadoop-3.3.6
export PATH="$PATH:$HADOOP_HOME/bin"
```

---

### 3.9 本机 12345 隧道失效 → 改直连

**现象**

```python
ConnectionRefusedError: [WinError 10061] 由于目标计算机积极拒绝，无法连接。
```

**根因**

本地 `ssh -L 12345:localhost:12345` 隧道进程与观察脚本同生命周期，脚本一退出隧道就断了；脚本里写死 `localhost:12345` 就再也连不上。

**解决**

确认 DS api-server 监听在 `0.0.0.0`，直接改直连：

```python
BASE = "http://192.168.87.131:12345/dolphinscheduler"
```

> 直连返回 `401 未经授权` 说明网络是通的，只是需要登录 —— 这本身就是一个有效的连通性探针。

---

### 3.10 SSH 免密登录必须指定密钥

**现象**

```
lrh@192.168.87.131: Permission denied (publickey,password).
```

**解决**

```powershell
ssh -n -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 -o BatchMode=yes `
    -o StrictHostKeyChecking=no lrh@192.168.87.131 "<命令>"
```

- 密钥位置：`D:\数据开发环境\VMware\sshkeys\cluster_ed25519`（生成脚本 `VMware/scripts/gen_key.ps1`）
- 加 `-n` 可避免 ssh 占用 stdin 导致管道脚本挂住
- `BatchMode=yes` 让认证失败立刻返回错误，而不是卡在密码提示

---

### 3.11 VM 里没有 curl

**现象**

探活脚本报 `curl: command not found`。

**解决**

| 需求 | 替代方案 |
|---|---|
| 看端口是否监听 | `ss -ltn \| grep ':12345'` |
| 调 HTTP 接口 | `python3 -c "import urllib.request; ..."` |
| 调 YARN RM | Windows 侧 `Invoke-RestMethod http://192.168.87.131:8088/ws/v1/...` |

---

### 3.12 beeline SQL 引号被吞 → 用 `.sql` 文件

**现象**

```
bash: -c: line 1: syntax error near unexpected token `('
```

远端收到的命令里 SQL 的双引号没了，`count(*)` 的括号被 bash 当成语法。

**根因**

SQL 经过 PowerShell → ssh → bash 三层传递，引号被逐层剥离。

**解决**

```powershell
# 1) 本地写好 SQL 文件
# 2) 上传
scp -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 tmp_final_check.sql lrh@192.168.87.131:/home/lrh/workspace/
# 3) 远端执行
ssh -n -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 lrh@192.168.87.131 "export PATH=...; beeline -u jdbc:hive2://localhost:10000 --silent=true -f /home/lrh/workspace/tmp_final_check.sql"
```

---

### 3.13 PowerShell 调 DS 接口登录后仍 401

**现象**

```powershell
$s = New-Object Microsoft.PowerShell.Commands.WebRequestSession
Invoke-RestMethod .../login -Body @{userName='admin';userPassword='dolphinscheduler123'} -WebSession $s
# login code=0    ← 登录成功
Invoke-RestMethod .../process-instances -WebSession $s
# 401 未经授权    ← 仍然未授权
```

**根因**

DS 的会话靠响应里的 `sessionId` Cookie，`WebRequestSession` 在当前 PS 版本下未正确回传。

**解决**

用 Python 的 `http.cookiejar` 维持会话（稳定可靠）：

```python
import http.cookiejar, urllib.request, urllib.parse, json

BASE = "http://192.168.87.131:12345/dolphinscheduler"
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

data = urllib.parse.urlencode(
    {"userName": "admin", "userPassword": "dolphinscheduler123"}).encode()
r = urllib.request.Request(BASE + "/login", data=data, method="POST")
r.add_header("Content-Type", "application/x-www-form-urlencoded")
opener.open(r, timeout=60)          # 之后所有请求复用 opener 即可
```

---

### 3.14 新建工作流时传入的 `code` 被忽略

**现象**

创建请求里指定了自定义 `code`，但查询时返回的是另一个值。

**根因**

DS 创建接口会**自动生成**真实 `processDefinitionCode`，请求里的 `code` 不生效（PUT 更新时另当别论，version 会 +1）。

**解决**

创建后从接口响应或 DB 回读真实 code，再拿它去触发/查实例：

```sql
select code, name, version, release_state from t_ds_process_definition;
```

---

### 3.15 lrh-3 短暂不可达

**现象**

```
ping lrh-3 → 100% packet loss (No route to host)
```

**根因**

节点处于重启 / 内存高压窗口内，短暂失联。

**解决**

无需处理，等待节点恢复。恢复后需确认 WorkerServer 与 NodeManager 已重新注册：

```bash
jps -l | grep -E 'WorkerServer|NodeManager'
# lrh-3: WorkerServer(3367) + NodeManager(1394)  ✅
```

---

## 四、排障顺序（推荐）

遇到「工作流卡住/下发不下去」时，按这个顺序看，能最快定位：

1. **看 master 日志是否在暴涨**
   ```bash
   ls -l .../master-server/logs/dolphinscheduler-master.log   # 几秒内涨几十 MB → 死循环
   tail -n 200 .../dolphinscheduler-master.log | grep -v "will resend" | tail -30
   ```
2. **查实例与任务实例状态**
   ```sql
   select id, name, state from t_ds_task_instance where process_instance_id = <id>;
   ```
3. **查 YARN 是否有作业在跑**
   ```bash
   yarn application -list
   ```
4. **查 worker 上是否真有子进程**
   ```bash
   ps -eo pid,etime,args | grep -E 'hive-beeline|MRAppMaster|YarnChild' | grep -v grep
   ```
5. **查资源**
   ```bash
   free -m; df -h /; dmesg | tail -20
   ```
6. **查 api-server 是否活着**
   ```bash
   ss -ltn | grep ':12345'
   ```

---

## 五、本次最终运行结果

### 5.1 实例 4 = `gmall_dw_pipeline_tail`

| 节点 | 状态 | 开始 | 结束 | 耗时 |
|---|---|---|---|---|
| dws_user | ✅ SUCCESS | 20:50:02 | 21:00:05 | 10m03s |
| dws_sku | ✅ SUCCESS | 21:00:06 | 21:09:24 | 9m18s |
| dws_order | ✅ SUCCESS | 21:09:25 | 21:17:20 | 7m55s |
| ads_opt | ✅ SUCCESS | 21:17:21 | 21:57:15 | 39m54s |
| verify | ✅ SUCCESS | 21:57:16 | 22:01:46 | 4m30s |
| **工作流总计** | **✅ SUCCESS** | 20:50:02 | **22:01:47** | **1h11m45s** |

### 5.2 数据落库校验（beeline 直查）

| 表 | 行数 | 说明 |
|---|---|---|
| dws_user_day | 5000 | 5 天 × 1000 用户 |
| dws_sku_day | 2500 | 5 天 × 500 sku |
| dws_order_day | 5 | 每天 1 行日汇总 |
| ads_dau | 5 | 每天 1 行 |
| ads_conversion_funnel | 5 | 每天 1 行 |
| ads_repurchase_rate | 5 | 每天 1 行 |
| ads_top_sku | 2500 | 5 天 × 500 排行 |

### 5.3 Web UI 观察入口

- 地址：`http://192.168.87.131:12345/dolphinscheduler`
- 账号：`admin` / `dolphinscheduler123`
- 路径：**项目管理 → gmall → 工作流实例** → 最新一条 `gmall_dw_pipeline_tail-1-…`（id=4）
- 甘特图看串行耗时分布，节点右侧「日志」看 beeline 输出

---

## 六、一句话总结

本次卡点 **90% 集中在两类问题**：一是 **master 侧状态机异常**（NPE 死循环、REST 删不掉实例）—— 处理套路是「停进程 → 清 DB → 重启」；二是 **操作侧 PowerShell/ssh 传递导致的命令失真** —— 处理套路是「复杂逻辑落地成文件，再上传执行」。把这两条固化成习惯，排障效率会明显提升。
