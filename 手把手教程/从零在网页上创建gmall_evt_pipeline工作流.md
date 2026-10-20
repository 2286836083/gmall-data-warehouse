# 从零在网页上建出 16 节点工作流（gmall_evt_pipeline）

> **这份教程教你什么**：**不用脚本、不用接口**，纯在 DolphinScheduler 网页上把真实数据链路的
> 16 个节点一个个建出来，连成一条线，上线、运行。
>
> **为什么值得自己建一遍**：
> 1. 用网页建时，**DS 会自己生成节点 code**（实测 `23242403233824` 这种 14 位量级，**浏览器能精确表示**）
>    → **显示和执行都不会出问题**；而程序化手写 19 位 code 会让画布上 16 个节点塌缩成 1 个（见 §7 第 1 坑）
> 2. 建一遍你就彻底明白"工作流定义 / 节点 / 依赖边 / 上线 / 实例"是怎么回事
>
> **前置**：集群已开机、DS 已拉起（`master` / `api` / 两个 `worker`）、能登录网页。
> 建完约需 **30~45 分钟**（主要是 16 次复制粘贴）。

---

## 〇、先看清你要建什么

**一条从左到右的直线**，16 个 SHELL 节点，串行：

```
①preflight → ②ods_ready → ③dwd_ddl → ④dwd_event_clean → ⑤dwd_purchase_fact
→ ⑥dwd_dim_product → ⑦evt_ddl → ⑧dws_user → ⑨dws_product → ⑩dws_category
→ ⑪dws_trade → ⑫ads_active_user → ⑬ads_funnel → ⑭ads_repurchase → ⑮ads_topn → ⑯verify
```

| # | 节点名 | 干什么 | 读哪个文件 | 实测耗时 |
|---|---|---|---|---|
| 1 | `preflight` | 开工前体检 + 生成分段 SQL | `ds_preflight.sh` | 24s |
| 2 | `ods_ready` | ODS 就绪检查 | `evt_ods_ready.sh` | 2m |
| 3 | `dwd_ddl` | 建 DWD/DIM 三表 | `dwd_ddl.sql` | 19s |
| 4 | `dwd_event_clean` | 清洗 1063 万行 | `ds_steps/dwd_step2.sql` | 8m37s |
| 5 | `dwd_purchase_fact` | 派生购买事实 | `ds_steps/dwd_step3.sql` | 1m15s |
| 6 | `dwd_dim_product` | 派生商品维度 | `ds_steps/dwd_step4.sql` | 7m36s |
| 7 | `evt_ddl` | 建 4 DWS + 4 ADS | `evt_ddl.sql` | 17s |
| 8 | `dws_user` | DWS·用户主题 | `ds_steps/evt_step1.sql` | 2m48s |
| 9 | `dws_product` | DWS·商品主题 | `ds_steps/evt_step2.sql` | 2m36s |
| 10 | `dws_category` | DWS·类目主题 | `ds_steps/evt_step3.sql` | 2m |
| 11 | `dws_trade` | DWS·交易主题 | `ds_steps/evt_step4.sql` | 1m9s |
| 12 | `ads_active_user` | ADS·活跃用户 | `ds_steps/evt_step5.sql` | 3m29s |
| 13 | `ads_funnel` | ADS·转化漏斗 | `ds_steps/evt_step6.sql` | 1m47s |
| 14 | `ads_repurchase` | ADS·复购率 | `ds_steps/evt_step7.sql` | 2m27s |
| 15 | `ads_topn` | ADS·TopN 商品 | `ds_steps/evt_step8.sql` | 1m48s |
| 16 | `verify` | 三层校验 | `evt_verify.sql` | 15m23s |

**整链实测 54 分 10 秒。**

---

## 一、前置检查（30 秒，在 lrh-1 上敲）

```bash
# ① DS 三件套在不在
export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH
jps -l | grep -E "MasterServer|ApiApplicationServer"

# ② 节点要读的文件齐不齐（应该 6 个 ✅ + ds_steps 里 14 个 sql）
for f in ds_preflight.sh evt_ods_ready.sh dwd_ddl.sql evt_ddl.sql evt_verify.sql; do
  [ -f /home/lrh/workspace/new_chain/$f ] && echo "✅ $f" || echo "❌ $f 缺失"
done
ls -1 /home/lrh/workspace/new_chain/ds_steps/*.sql | wc -l     # 期望 14

# ③ 集群空闲吗（有 beeline/YARN 作业就先别建，说明有别的任务在跑）
pgrep -cf 'jdbc[:]hive2'
/opt/hadoop/hadoop-3.3.6/bin/yarn application -list | grep -cE '^[[:space:]]*application_'
```

> 💡 `pgrep` 的写法用了中括号 `jdbc[:]hive2`：直接写 `'jdbc:hive2'` 会**匹配到这条命令自己**，永远返回 1。

---

## 二、第 1 步：创建工作流（填一个对话框）

```
浏览器打开 http://192.168.87.131:12345/dolphinscheduler    账号 admin / dolphinscheduler123
→ 顶部「项目管理」→ 点项目 gmall
→ 左侧「工作流定义」→ 右上角「创建工作流」
```

弹出的对话框里填：

| 字段 | 填什么 | 说明 |
|---|---|---|
| **工作流名称** | `gmall_evt_pipeline` | ⚠️ 名字不能和已有的重复（现在这个名字是空的，可以直接用） |
| **租户** | `lrh` | 必填。这是"以哪个 Linux 用户身份跑任务" |
| **描述** | 真实数据链路：ODS→DWD→DWS→ADS→校验，16 节点 | 随便写 |
| **执行策略** | 并行 | 保持默认 |
| **超时** | 0 / 留空 | 保持默认 |

点确定 → 进入 **DAG 画布**（左边一列是任务类型工具栏，中间是空白网格画布）。

---

## 三、第 2 步：把 16 个节点建出来（本教程的核心）

### 3.1 先把三个脚本模板记下来

**所有 16 个节点只有三种写法**，你只要改文件名：

**模板 A · SQL 节点用（14 个：③~⑮ 加上最后单独说的 verify）**

```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH; /opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -f 这里换成文件名 </dev/null 2>&1'
```

**模板 B · 脚本节点用（只有第 1、2 个节点）**

```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'bash 这里换成文件名 2>&1'
```

**模板 C · verify 节点用**：和模板 A 一样，只是**去掉 ` --silent=true `**。
（去掉之后三层校验的表格会完整打进日志，方便你截图当证据。）

> **这三段里每个字符都有用，尤其是三处**：
>
> | 片段 | 为什么不能省 |
> |---|---|
> | `ssh ... lrh-1 '...'` | DS 的 worker 在 lrh-2/lrh-3 上，**那两台没装 Hive 客户端**，任务必须跳回 lrh-1 执行 |
> | `</dev/null` | ★ beeline 是交互式程序，**没有终端又没重定向 stdin 时会被 SIGTTOU 冻住**。现象是：DS 里一直"运行中"，**但永远不提交作业、日志里也没有任何报错** —— 极难查 |
> | `2>&1` | 让 beeline 的报错也进 DS 任务日志，不然失败了看不到原因 |

### 3.2 逐个建：拖节点 → 双击 → 填名字和脚本

**操作循环（重复 16 次）**：

1. 从左侧工具栏把 **`SHELL`** 拖到画布上空白处
2. **双击**这个节点，弹出编辑框
3. **节点名称**：填下表里的名字（**必须一模一样**，后面自检要用）
4. **脚本**：把模板里的文件名替换掉，整段粘进"脚本"框
5. 其余字段（运行标志 / 优先级 / Worker分组 / 失败重试 / 超时）**全部保持默认**
6. 点确定

| # | 节点名称（填这个） | 脚本用哪个模板 | 文件名（替换模板里的"文件名"） |
|---|---|---|---|
| 1 | `preflight` | **B** | `/home/lrh/workspace/new_chain/ds_preflight.sh` |
| 2 | `ods_ready` | **B** | `/home/lrh/workspace/new_chain/evt_ods_ready.sh` |
| 3 | `dwd_ddl` | A | `/home/lrh/workspace/new_chain/dwd_ddl.sql` |
| 4 | `dwd_event_clean` | A | `/home/lrh/workspace/new_chain/ds_steps/dwd_step2.sql` |
| 5 | `dwd_purchase_fact` | A | `/home/lrh/workspace/new_chain/ds_steps/dwd_step3.sql` |
| 6 | `dwd_dim_product` | A | `/home/lrh/workspace/new_chain/ds_steps/dwd_step4.sql` |
| 7 | `evt_ddl` | A | `/home/lrh/workspace/new_chain/evt_ddl.sql` |
| 8 | `dws_user` | A | `/home/lrh/workspace/new_chain/ds_steps/evt_step1.sql` |
| 9 | `dws_product` | A | `/home/lrh/workspace/new_chain/ds_steps/evt_step2.sql` |
| 10 | `dws_category` | A | `/home/lrh/workspace/new_chain/ds_steps/evt_step3.sql` |
| 11 | `dws_trade` | A | `/home/lrh/workspace/new_chain/ds_steps/evt_step4.sql` |
| 12 | `ads_active_user` | A | `/home/lrh/workspace/new_chain/ds_steps/evt_step5.sql` |
| 13 | `ads_funnel` | A | `/home/lrh/workspace/new_chain/ds_steps/evt_step6.sql` |
| 14 | `ads_repurchase` | A | `/home/lrh/workspace/new_chain/ds_steps/evt_step7.sql` |
| 15 | `ads_topn` | A | `/home/lrh/workspace/new_chain/ds_steps/evt_step8.sql` |
| 16 | `verify` | **C**（去掉 --silent） | `/home/lrh/workspace/new_chain/evt_verify.sql` |

> 💡 **省力小技巧**：`dws_*` 和 `ads_*` 这 8 个节点的脚本几乎一样，**只差最后的文件名**。
> 所以别每次从零粘 —— 建完一个之后，**试试在画布上选中节点按 `Ctrl+C` / `Ctrl+V` 复制**，
> 再双击改名字和文件名。能省一半时间（能不能用取决于版本，试一下不亏）。

### 3.3 建完之后先别急着连线，自查一下

在画布上应该是 **16 个散落的节点**。如果少于 16 个，说明有节点被建在**重叠的位置**上 —— 拖开找找。

---

## 四、第 3 步：连线（16 个节点连成一条直线）

**每个节点的右侧有一个小圆点**，把它拖到**下一个节点**上，就连成一条边。

```
preflight → ods_ready → dwd_ddl → dwd_event_clean → dwd_purchase_fact → dwd_dim_product
→ evt_ddl → dws_user → dws_product → dws_category → dws_trade
→ ads_active_user → ads_funnel → ads_repurchase → ads_topn → verify
```

**共 15 条边**（16 个节点，首尾相连）。箭头方向必须是**从左往右**（前一个 → 后一个）。

> ⚠️ **方向千万别连反**。如果连成 `ods_ready → preflight`，那 `preflight` 就会等 `ods_ready`，
> 而 `ods_ready` 又等…… 整个流程跑不起来或顺序错乱。
>
> **连完检查一遍**：`preflight` 应该**没有**任何箭头指向它（它是起点）；
> `verify` 应该**没有**任何箭头从它出发（它是终点）。

---

## 五、第 4 步：保存 → 上线

```
右上角「保存」   → 回到工作流定义列表
找到 gmall_evt_pipeline 那一行 → 点「上线」
```

> ★ **为什么要"上线"**：DS 里只有**已上线**的工作流才能运行。
> 类比：**保存 = 把排班表画好收进抽屉；上线 = 把它贴到车间墙上**。没贴上，工人看不到，不会干活。
> 列表页状态列会显示 **已上线**（和你截图里那个蓝色标签一样）。

---

## 六、第 5 步：怎么确认"建对了"（3 个自检）

### 自检 1 · 网页上看

刷新页面，打开 `gmall_evt_pipeline`，画布上应该是 **16 个节点排成一条线**，每个节点右边有一条箭头连向下一个。

**❌ 如果只看到 1 个节点** → 直接看 §7 第 1 坑。

### 自检 2 · 数据库核对（在 lrh-1 上敲）

```bash
sudo -n mysql dolphinscheduler --default-character-set=utf8mb4 -t -e "
select (select count(*) from t_ds_task_definition where name in
        ('preflight','ods_ready','dwd_ddl','dwd_event_clean','dwd_purchase_fact','dwd_dim_product',
         'evt_ddl','dws_user','dws_product','dws_category','dws_trade',
         'ads_active_user','ads_funnel','ads_repurchase','ads_topn','verify')) as 同名节点数;"

# 看你这套工作流的 16 个节点 + 15 条边（把 <code> 换成列表页里的 code）
sudo -n mysql dolphinscheduler --default-character-set=utf8mb4 -t -e "
select r.id, pre.name as 前, post.name as 后
from t_ds_process_task_relation r
left join t_ds_task_definition pre  on r.pre_task_code  = pre.code
left join t_ds_task_definition post on r.post_task_code = post.code
where r.process_definition_code = <code> order by r.id;"
```

**期望**：16 条关系行（第 1 行 `前` 为 NULL，表示起点），一条不多一条不少。

### 自检 3 · 顺手确认 code 是"浏览器安全"的（这是最关键的一条）

```bash
sudo -n mysql dolphinscheduler -N -B -e "
select concat('  最大 task code = ', max(code),
              '   JS安全上限 2^53 = ', 9007199254740992,
              '   安全？ ', if(max(code) <= 9007199254740992, '✅ 是', '❌ 否'))
from t_ds_task_definition where name in ('preflight','verify');"
```

**必须打印 ✅ 是**。如果是 ❌，画布上一定会塌缩成 1 个节点（原因见 §7 第 1 坑）。

---

## 七、5 个坑（按踩到概率排序）

### ★ 第 1 坑：节点 code 超过 2^53 → 画布上 16 个节点只剩 1 个

**这是本项目真实踩过的坑**（2026-10-03）。DS 的接口把节点 code 当 **JSON 数字**返回，
而浏览器用 JavaScript 的 Number（IEEE754 双精度）解析 —— **它只能精确表示 ≤ 2^53 = 9007199254740992 的整数**。
超过之后，**多个不同的 code 会变成同一个数**，画布上就只剩一个节点。

| code 来源 | 量级 | 浏览器能分清吗 |
|---|---|---|
| **网页上拖节点，DS 自动生成** | `23242403233824`（2.3e13） | ✅ 能 |
| 手写 19 位（如 `3300000000000000001`） | 3.3e18 | ❌ **塌缩成 1 个** |

**所以：在网页上建，让 DS 自己生成 code，就是最安全的做法。** 千万不要自己指定长 code。

> **这坑为什么阴**：数据库和 master 用的是 64 位整数，**执行完全正常**（16 个节点全跑成功），
> **只有浏览器分不清** → "跑得对、显示全错"，所有自动化测试都发现不了。

### ★ 第 2 坑：别用「从当前节点开始重跑」

网页上实例页里那个「**从当前节点开始重跑**」，以及接口的 `startNodeList` 参数 ——
**在本项目这套 DS 3.1.8 上会把 master 打成 NPE 死循环**（实测 3 次复现、3 次必挂）：

- master CPU 打满，日志**每秒涨几千行**（实测 2 分钟涨到 179 MB，能把磁盘吃干）
- 实例永远卡在"运行中"，**一个任务都不下发**
- **而且重启 master 修不好**，必须停 master + 从库里删掉那条实例

**要重跑就整条重跑**（ETL 是 `INSERT OVERWRITE`，幂等，约 54 分钟）。详见
`DolphinScheduler全流程排障汇总.md` 第 21 坑。

### 第 3 坑：改完节点一定要**重新上线**

保存只是改了图纸；**改了必须重新上线才生效**（有些版本改了会自动下线，注意看列表页的状态列）。

### 第 4 坑：忘记 `</dev/null` → 节点永远"运行中"但什么都不干

见 §3.1 的说明。判据：`ps` 里 beeline 进程状态是 **`Tl+`**（被冻住）。

### 第 5 坑：`preflight` 节点会拒绝开工 —— 那是设计如此，不是 bug

`preflight` 会检查：5 个服务 active、3 个 DataNode、3 个 NodeManager、HiveServer2 真能用（端口 + 真实查询）、
**以及集群上有没有残留的 beeline / YARN 作业**。任何一项不过就**非 0 退出**，工作流停在第一个节点。

**看到 `preflight` 失败时**：先读它的日志（它会把原因和建议命令都打印出来）。
最常见的是"上一轮被停掉后留下了孤儿 beeline"：

```bash
ps -ef | grep jdbc:hive2 | grep -v grep    # 还有输出 = 没停干净
pkill -f 'jdbc:hive2'                      # 确认没别人在跑再执行
```

> 背景：DS 的"停止"**杀不掉真正干活的进程** —— 任务是 `ssh lrh-1 'beeline …'`，
> 命令跑在 lrh-1 上，而 DS 只能杀掉 worker（lrh-2/3）上的进程树。
> 实测过一次：被停掉的任务，它的 beeline 在 lrh-1 上**又跑了 3 天**，把 5 天数据写完了。

---

## 八、第 6 步：运行它

```
工作流定义 → gmall_evt_pipeline 那一行 → 点「运行」（闪电图标）
```

跑起来后：

```
左侧「工作流实例」→ 最上面那条 → 看整体状态
左侧「任务实例」  → 看每个节点的状态和耗时，点「日志」看 beeline 输出
```

**怎么判断"卡住了"还是"正常推进"**：去 <http://192.168.87.131:8088> 看 YARN，
**作业的 `Stage-N` 编号在变就是在正常推进**。两个阶段之间 YARN 里可能一个作业都没有，那是正常的空档。

**期望结果**：16/16 节点绿色，整链约 54 分钟。

**跑完怎么验证数据对不对**（3 层证据，详见 `DolphinScheduler零基础操作教程.md` §8）：

| 层 | 期望 |
|---|---|
| 节点状态 | 16 个全 SUCCESS |
| 12 张表行数 | `ods_event_log_di` 10,630,245 / `dwd_event_log_di` 10,623,154 / `dwd_purchase_fact_di` 186,059 / `dim_product` 140,665 / `dws_user_event_day` 1,622,820 / `dws_product_event_day` 1,265,817 / `dws_category_event_day` 1,853 / `dws_trade_day` 31 / 4 张 ADS 各 31 / `ads_top_product_day` 1,550 |
| GMV 四路对账 | 商品/类目/交易/DWD事实表 **四个数必须完全相等**（历史值 `5.75759965E7`） |

---

## 附录：16 条脚本（一次性列出，方便对照）

**① preflight**
```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'bash /home/lrh/workspace/new_chain/ds_preflight.sh 2>&1'
```
**② ods_ready**
```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'bash /home/lrh/workspace/new_chain/evt_ods_ready.sh 2>&1'
```
**③ dwd_ddl**
```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH; /opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -f /home/lrh/workspace/new_chain/dwd_ddl.sql </dev/null 2>&1'
```
**④ dwd_event_clean**
```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH; /opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -f /home/lrh/workspace/new_chain/ds_steps/dwd_step2.sql </dev/null 2>&1'
```
**⑤ dwd_purchase_fact** → 把上面 `dwd_step2.sql` 换成 `dwd_step3.sql`
**⑥ dwd_dim_product** → 换成 `dwd_step4.sql`
**⑦ evt_ddl** → 换成 `/home/lrh/workspace/new_chain/evt_ddl.sql`
**⑧ dws_user** → 换成 `ds_steps/evt_step1.sql`
**⑨ dws_product** → 换成 `ds_steps/evt_step2.sql`
**⑩ dws_category** → 换成 `ds_steps/evt_step3.sql`
**⑪ dws_trade** → 换成 `ds_steps/evt_step4.sql`
**⑫ ads_active_user** → 换成 `ds_steps/evt_step5.sql`
**⑬ ads_funnel** → 换成 `ds_steps/evt_step6.sql`
**⑭ ads_repurchase** → 换成 `ds_steps/evt_step7.sql`
**⑮ ads_topn** → 换成 `ds_steps/evt_step8.sql`
**⑯ verify**（★ 去掉 `--silent=true`）
```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH; /opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f /home/lrh/workspace/new_chain/evt_verify.sql </dev/null 2>&1'
```

---

*本教程的节点清单、脚本模板与耗时均来自 2026-10-03 的实测运行（16/16 SUCCESS，54 分 10 秒）。
"code 超过 2^53 导致画布塌缩"这条坑是同日的真实事故，已实测复现并定位到 `DagHelper.parsePostNodes` 之外的
前端精度问题。*
