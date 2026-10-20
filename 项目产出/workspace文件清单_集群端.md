# 集群端 workspace 文件清单

> **查证方式**：2026-10-20 在 `lrh@192.168.87.131` 上逐个读取文件头注释与 `stat`，非凭文件名推测
> **当前状态**：已按链路分成 `new_chain/` 与 `old_chain/` 两个目录（2026-10-20 变更，见第六节）

---

## 〇、一句话结构

```
/home/lrh/workspace/
├── new_chain/     ← 新链路：真实数据 REES46，2019-10-01~31，1063 万行 —— 当前在用
├── old_chain/     ← 旧链路：mock 生成，2026-08-20~24，5 天 —— 2026-09-23 后停用
└── (根目录只留通用工具)
```

| | 旧链路 `old_chain/` | 新链路 `new_chain/` |
|---|---|---|
| **数据** | mock 生成，2026-08-20~24（5 天） | REES46 真实数据，2019-10（31 天） |
| **ODS 表** | `ods_user_log_di` / `ods_sku_info_di` / `ods_order_info_di` | `ods_event_log_di`（单表事件流） |
| **表数** | 14 张 | ODS 1 + DWD/DIM 3 + DWS/ADS 15 |
| **入口** | `run_pipeline.sh`（9 步） | `dwd_run.sh` → `evt_run.sh`，或 DS 工作流 |
| **状态** | 最后跑完 2026-09-23 22:44 | 当前在用 |

⚠️ **两套表在同一个 `gmall` 库里共存**，换链路不会删旧表。

---

## 一、`new_chain/` —— 新链路（当前在用）

### 1.1 核心 SQL（按执行顺序）

| 文件 | 干什么 |
|---|---|
| `ods_event_log_ddl.sql` | ODS 建表，**1 张**事件流表（9 字段 + dt 分区） |
| `load_ods.sh` | 把 REES46 原始日志（1.3GB）装进 ODS，31 个分区 |
| `profile_ods.sql` | ODS 数据画像 —— **DWD 清洗规则的设计依据**（先摸清脏在哪） |
| `dwd_ddl.sql` | DWD 明细 + 购买事实 + 商品维表，**3 张** |
| `etl_ods_to_dwd.sql` | ODS→DWD，**3 条 INSERT**：清洗去重 / 派生购买事实 / 派生商品维度 |
| `evt_ddl.sql` | DWS + ADS 建表，**15 张** |
| `evt_etl.sql` | DWS 四主题 + ADS 四指标，**8 段** |
| `evt_verify.sql` | 三层校验：分区数 / GMV 四路交叉对账 / 方向性 |
| `evt_fix_dws_trade_day.sql` | 一次性修复：重建 `dws_trade_day`，去掉源表不存在的 `session_cnt` |
| `smoke_dwd_1day.sql` | 冒烟验证：只跑 1 天，先确认算子不炸 |

**记忆锚**：`ods_event_log_ddl` → `load_ods` → `dwd_ddl` → `etl_ods_to_dwd` → `evt_ddl` → `evt_etl` → `evt_verify`

### 1.2 执行器

| 文件 | 干什么 |
|---|---|
| `dwd_run.sh` | ODS→DWD **5 步**，带状态文件 `dwd_run/status.tsv`，支持断点续跑 |
| `evt_run.sh` | DWS/ADS **10 步**（8 段计算 + 建表 + 校验），含启动前自检 |
| `dwd_progress.sh` | 进度探针，读状态文件 + MR 百分比，供看板轮询 |

### 1.3 DolphinScheduler 编排

| 文件 | 干什么 |
|---|---|
| `ds_evt_pipeline.py` | 建/上线 **`gmall_evt_pipeline`**（**16 节点**，Hive 版，写 `gmall` 正式库） |
| `ds_spark_pipeline.py` | 建/上线 **`gmall_evt_pipeline_spark`**（**11 节点**，Spark 旁路，写 `gmall_bench`） |
| `ds_preflight.sh` | 第 1 节点：开工体检，并调用 `ds_prepare_steps.sh` 生成分段 |
| `evt_ods_ready.sh` | 第 2 节点：ODS 就绪 gate，必要时重新装载 |
| `ds_prepare_steps.sh` | 把大 SQL 切成「每节点一份」，输出到 `ds_steps/` |
| `ds_monitor.sh` | 实时进度监视器，写 `ds_steps/monitor.log` |
| `spark_preflight.sh` | Spark 旁路开工检查：**有残留 Spark 应用就拒绝开工** |

> **工作流 code（2026-10-20 重建后）**：`gmall_evt_pipeline` = `23427787174432`，
> `gmall_evt_pipeline_spark` = `23427795259296`。实际 code 以 `ds_steps/wf_code*.txt` 为准。

### 1.4 大屏 + 实验 + 运行产物

| 项 | 是什么 |
|---|---|
| `export_dashboard_real.sh` + `csv2json_real.py` | 从 Hive 导出 6 张表 → `dashboard_data_real.js` |
| `dashboard_real/` | 大屏数据产物（`dashboard_data_real.js` + `raw/`） |
| `bench/hive_bench/`（25 项） | 双引擎对比 · Hive 侧基准 |
| `bench/spark_bench/`（27 项） | 双引擎对比 · Spark 侧基准 |
| `ds_steps/`（15 项） | DS 分段 SQL + `wf_code*.txt` + `monitor.log` |
| `evt_run/`（27 项） | `evt_run.sh` 的临时分段 + `status.tsv`（含 `run_run_run` 重复拼接垃圾） |
| `dwd_run/`（11 项） | `dwd_run.sh` 的临时分段 + `status.tsv` |
| `run.log` / `run_evt*.log` | 三次执行器运行日志 |
| `smoke.log` / `smoke.done` | 冒烟验证日志与完成标记 |
| `*.bak-before-ds-work` | DS 改造前的备份（2 个） |

## 二、`old_chain/` —— 旧链路（已停用）

| 文件 | 干什么 |
|---|---|
| `generate_mock_data.py` | 造三类 mock 数据：用户行为 / 订单 / 商品 |
| `split_by_date.py` | 把行为日志、订单**按日期拆成多文件**（对应按天分区） |
| `gmall_ddl_day8.sql` | 旧库建表，**14 张** |
| `batch_load.sql` | 把 mock 数据批量灌进 ODS 三张表 |
| `clean_dwd.sql` | ODS → DWD/DIM 清洗与维度补全 |
| `etl_dws_user.sql` | → `dws_user_day` |
| `etl_dws_sku.sql` | → `dws_sku_day` |
| `etl_dws_order.sql` | → `dws_order_day` |
| `etl_ads_opt.sql` | DWS/DWD → 4 张 ADS 指标表 |
| `verify_ads.sql` | 打印 ADS 结果做人工核对 |
| `run_pipeline.sh` | **一键 9 步**：建库→DDL→ODS→DWD→DWS→ADS→verify |
| `export_dashboard_data.sh` + `csv2json.py` | 旧链路大屏导出与转换 |
| `dashboard/` | 旧大屏数据产物 |
| `log/` | `run_pipeline.sh` 每步日志（`pipeline.out` 末次 2026-09-23 22:44 `ALL_DONE`） |
| `mock_data/`（24MB） | mock CSV：订单 / 商品 / 用户日志 + 按日拆分目录 |

> 这些 SQL 头部都有 `-- ds-tuned (small cluster)` —— 当年为小集群压内存时加的，
> **那批"统一压到 512MB"的配置后来被证明是丢信息的**（DWS 大基数聚合必须放大到 1024MB）。

## 三、根目录留下的通用工具

| 文件 | 为什么留在这里 |
|---|---|
| `verify.sh` + `verify_report{,2}.txt` | 「执行原理专题」的验证脚本与报告，**与具体链路无关** |
| `fix_metastore_charset.sh` | 中文注释乱码**根治**脚本（元数据库 latin1→utf8mb4），一次性任务 |
| `fix_comments.sql` | 由 `gen_comment_fix.py` 自动生成的批量修复 SQL（**勿手改**） |
| `reapply_comments.sql` | 字符集修好后重刷注释，一次性任务 |

前三个是**考古材料**，问题已解决，不用再跑。

---

## 四、这次分家改了什么代码（必须知道）

移动文件只是第一步。**真正决定它能不能跑的是路径**：

| 文件 | 改了什么 |
|---|---|
| `dwd_run.sh` / `evt_run.sh` / `ds_prepare_steps.sh` | `WS=/home/lrh/workspace` → `.../new_chain`（原来是**写死绝对路径**，不是 `dirname $0`） |
| `ds_preflight.sh` / `ds_monitor.sh` / `evt_ods_ready.sh` | 内部调用的绝对路径 → `new_chain/...` |
| `load_ods.sh` / `export_dashboard_real.sh` | 用了 `HERE="$(cd "$(dirname "$0")" && pwd)"`，**自动跟着目录走**，只改了默认输出位置 |
| `ds_evt_pipeline.py` / `ds_spark_pipeline.py` | 6 处节点脚本路径 + `TASK_CODE_BASE` 换段 |
| `spark_preflight.sh` / `ds_prepare_steps.sh` | `spark_bench/` → `bench/spark_bench/` |
| `old_chain/run_pipeline.sh` | `cd` 与 `LOG` 改到 `old_chain/`，裸文件名靠 `cd` 生效 |
| `old_chain/export_dashboard_data.sh` | 默认输出 → `old_chain/dashboard` |

## 五、★ 顺手修掉的两个真 bug

**这两个都不是分家造成的，是工具本来就有的坑，分家过程把它们暴露了。**

### bug 1：`DSClient.delete` 方法名冲突

`ds_evt_pipeline.py` / `ds_spark_pipeline.py` 里有两个 `delete`：

- 第 176 行：`def delete(self, path)` —— 底层 HTTP DELETE
- 第 391 行：`def delete(self)` —— 业务动作"删除工作流"

后者**覆盖**了前者，于是业务方法里 `self.delete("/projects/...")` 会抛
`TypeError: DSClient.delete() takes 1 positional argument but 2 were given`。

**以前从没触发**，因为这条 `delete` 路径一直没人走过（工作流是建好就没删过）。

→ 已把业务方法改名为 `delete_workflow()`，main 里的映射同步更新。

### bug 2：`TASK_CODE_BASE` 固定 + DS 不清理孤儿任务

DS 删除 process definition 时，**不会级联删除 `t_ds_task_definition` 里的任务记录**。
而 `TASK_CODE_BASE` 是固定值（Spark 版原来是 `34000000000001`），重建时又用同一段 code
→ 库里同一 code 出现两条记录 → API 的 `taskDefinitionList` 返回 **双份节点**。

**实测症状**：`info` 显示「节点 22 个；画布坐标 11 个」，每个节点名打印两遍。

→ 已把 Spark 版 `TASK_CODE_BASE` 换到 `35000000000001`，并在代码注释里写明原因。
→ Hive 版没这个问题，因为它的旧工作流是**网页手工建的**（code 是雪花 ID，跟 `33000000000001` 段不冲突）。

## 六、变更记录

### 2026-10-20 · 按链路分家

- 备份：`/home/lrh/workspace_backup_before_split_20261020.tar.gz`（**4.8MB**，移动前完整打包）
- 新建 `new_chain/`（含 `bench/`）与 `old_chain/`，按上表归位
- 改掉全部硬编码路径，**实测验证**：
  - `bash -n` 全部 14 个 .sh **0 语法错误**
  - `python3 -m py_compile` 两个 .py **通过**
  - **实跑 `ds_prepare_steps.sh`** → 输出 `PREPARE_STEPS_OK`，12 个分段全部生成到 `new_chain/ds_steps/`
- DS 工作流**重建并上线**：
  - `gmall_evt_pipeline`：旧 code `23242698601760` → 新 `23427787174432`，**16 节点**
  - `gmall_evt_pipeline_spark`：旧 code `23376974369696` → 新 `23427795259296`，**11 节点**
  - 两个工作流节点路径**全部指向 `new_chain/`**，无残留
- DS 服务已拉起：api/master/alert（lrh-1）+ worker×2（lrh-2/3，**实测 15 秒注册完**）

## 七、回滚

```bash
# 完整回滚到分家前（会覆盖当前 workspace）
cd /home/lrh
tar xzf workspace_backup_before_split_20261020.tar.gz
```

DS 侧回滚：需用备份里的旧 `ds_*.py` 重新 `delete` + `create`，
旧 code 会在库里重建（但 `TASK_CODE_BASE` 冲突问题会再次出现，此时应换新段）。

## 八、⚠️ 一个尚未同步的地方

**本地 `D:\数据开发\项目产出\数据接入\` 与 `可视化\` 里的脚本副本，仍是分家前的版本**：
路径写的是 `/home/lrh/workspace/xxx`，也没有 bug 1 / bug 2 的修复。

它们现在的身份是**历史快照**，不是可部署源。
如果以后要从本地上传覆盖集群，会**把路径改回去、把两个 bug 带回来**。

→ 要么使用前先核对，要么做一次正式同步（尚未做）。
