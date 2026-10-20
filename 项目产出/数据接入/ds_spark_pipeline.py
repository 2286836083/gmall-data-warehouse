#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DolphinScheduler 工作流 gmall_evt_pipeline_spark 的「建 / 上线 / 触发 / 查看 / 删除」工具。

【它与 gmall_evt_pipeline（Hive 版）的关系】
  · Hive 版：16 节点，ODS→DWD→DWS→ADS→校验，用 beeline 跑 Hive SQL，写 gmall 库（正式数据）
  · 本工作流：11 节点，**只覆盖 DWS/ADS 八段**，用 spark-sql 跑**同一份 SQL**，写 gmall_bench 库
    （旁路实验库，不碰 gmall 一个字节）

  为什么只覆盖 DWS/ADS：与《实验10》的双引擎对比范围保持一致（那次的结论就是基于这八段），
  这样"命令行跑"与"DS 里跑"两次结果可以互相印证。DWD 段未纳入，属已知待办。

【★ 本工作流要回答的真问题】
  在 DS 里，Spark 天然被切成"一个节点一个 spark-sql 调用"= 每个节点各起一个会话。
  而《实验10》已经证明：Spark 的收益**几乎全部来自会话复用**（单会话 75s vs 分段 381s）。
  → 所以本工作流跑出来的耗时，正好量化"接进调度后，Spark 还剩多少优势"。

【为什么节点数是 11 而不是 8】
  preflight(残留检查) + bench_ddl(建 gmall_bench 表) + 8 段业务 + reconcile(对账) = 11

【为什么走 REST 接口而不是直接写 MySQL】
  DS 的 master 会把工作流定义缓存在内存里，直接 INSERT 数据库 master 看不到；
  而 code / version / 依赖边的一致性由服务端保证。走官方接口最稳。

【为什么节点脚本里要 ssh 回 lrh-1】
  DS 的 worker 跑在 lrh-2 / lrh-3 上，Spark 的 driver 在 client 模式下要在提交机上，
  而本项目的 Spark 配置、HDFS jar 分发、Hive 元数据访问都在 lrh-1 上验证过。
  所以任务跳回 lrh-1 执行 spark-sql —— 与《实验10》的跑法完全一致。

【用法】（在 lrh-1 上执行）
  python3 ds_spark_pipeline.py create     # 新建定义（已存在则报错）
  python3 ds_spark_pipeline.py info       # 打印定义与节点清单（含 code / 间距自检）
  python3 ds_spark_pipeline.py online     # 上线
  python3 ds_spark_pipeline.py run        # 触发一次（全量）
  python3 ds_spark_pipeline.py delete     # 删除定义
  python3 ds_spark_pipeline.py relayout   # 只重排画布坐标

⚠️ 与 Hive 版同样的限制：**故意不提供** startNodeList（从指定节点重跑）——
   实测会触发 DS 3.1.8 master 的 NPE 死循环。要重跑就整条重跑（幂等）。
"""

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import http.cookiejar

BASE = "http://192.168.87.131:12345/dolphinscheduler"
USER = "admin"
PASSWORD = "dolphinscheduler123"
PROJECT_CODE = 23110291751840          # 项目 gmall
WF_NAME = "gmall_evt_pipeline_spark"
TENANT = "lrh"
TASK_CODE_BASE = 34000000000001        # ⚠️ 必须 ≤ 2^53；且与 Hive 版的 33000000000001 段不冲突
CODE_FILE = "/home/lrh/workspace/ds_steps/wf_code_spark.txt"

# 【★ 与 Hive 版共享的一条约束】task code / process definition code ≤ 2^53 = 9007199254740992
#   超过会被浏览器的双精度浮点塌缩 → 画布上只显示 1 个节点（而执行完全正常）。
#   详见 ds_evt_pipeline.py 开头的长注释，以及《排障汇总》第 22 坑。


# ---------------------------------------------------------------- 节点命令模板
SPARK = "/opt/spark-3.5.9-bin-hadoop3"

# spark-sql 的参数：与 spark_bench_run.sh 的 SC 数组逐项一致（保持"命令行跑"与"DS 跑"同口径）
SPARK_CONF = (
    "--master yarn --deploy-mode client "
    "--conf spark.ui.enabled=false "
    "--conf spark.sql.sources.partitionOverwriteMode=dynamic "
    "--conf spark.sql.shuffle.partitions=6 "
    "--conf spark.executor.memory=1408m "
    "--conf spark.executor.cores=2 "
    "--conf spark.executor.instances=3 "
    "--conf spark.executor.memoryOverhead=384 "
    "--conf spark.driver.memory=1g"
)


def _spark(sql_path):
    """生成一个节点的 rawScript：ssh 回 lrh-1 跑 spark-sql。

    两个细节都不能省（与 Hive 版同理）：
    · `</dev/null` —— spark-sql 是交互式 shell（jline），没有控制终端又不重定向 stdin
      时会被 SIGTTOU 冻住（状态 Tl+），DS 里表现为"永远运行中、日志无报错、不提交作业"。
    · `2>&1` —— 让 Spark 的报错也进 DS 任务日志，否则日志里只有 stdout，排错时看不到异常。
    """
    return ("ssh -o StrictHostKeyChecking=no -n lrh-1 "
            "'export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH; "
            + SPARK + "/bin/spark-sql " + SPARK_CONF + " -f " + sql_path + " </dev/null 2>&1'")


def _shell(script_path):
    return ("ssh -o StrictHostKeyChecking=no -n lrh-1 "
            "'bash " + script_path + " 2>&1'")


B = "/home/lrh/workspace/spark_bench"

NODES = [
    ("spark_preflight",   _shell("/home/lrh/workspace/spark_preflight.sh")),
    ("spark_bench_ddl",   _spark(B + "/bench_ddl.sql")),
    ("spark_dws_user",    _spark(B + "/step_1.sql")),
    ("spark_dws_product", _spark(B + "/step_2.sql")),
    ("spark_dws_category", _spark(B + "/step_3.sql")),
    ("spark_dws_trade",   _spark(B + "/step_4.sql")),
    ("spark_ads_active",  _spark(B + "/step_5.sql")),
    ("spark_ads_funnel",  _spark(B + "/step_6.sql")),
    ("spark_ads_repurchase", _spark(B + "/step_7.sql")),
    ("spark_ads_topn",    _spark(B + "/step_8.sql")),
    ("spark_reconcile",   _spark(B + "/reconcile.sql")),
]

# ---------------------------------------------------------------- DAG 画布坐标
# 与 Hive 版同样的经验值：节点宽 180~200px，间距必须 ≥ 220px，否则挤成一团"看起来只有 1 个节点"。
NODE_X0 = 80
NODE_DX = 220
NODE_Y = 160

assert TASK_CODE_BASE + len(NODES) <= 2 ** 53, (
    "task code 超过 JS 安全整数 2^53 —— 浏览器会把不同节点塌缩成同一个数！")


def build_locations():
    return [{"taskCode": TASK_CODE_BASE + i + 1,
             "x": NODE_X0 + i * NODE_DX,
             "y": NODE_Y}
            for i in range(len(NODES))]


# ---------------------------------------------------------------- HTTP 会话
class DSClient(object):
    def __init__(self):
        self.cj = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cj))

    def _request(self, path, data=None, method=None):
        url = BASE + path
        body = None
        headers = {"Accept": "application/json", "User-Agent": "ds-spark-pipeline/1.0"}
        if data is not None:
            body = urllib.parse.urlencode(data).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            raw = self.opener.open(req, timeout=120).read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", "replace")
        try:
            return json.loads(raw)
        except ValueError:
            return {"code": -1, "msg": raw[:400], "success": False}

    def login(self):
        r = self._request("/login", {"userName": USER, "userPassword": PASSWORD})
        if not r.get("success"):
            raise SystemExit("登录失败: %s" % json.dumps(r, ensure_ascii=False))
        return r

    def get(self, path, params=None):
        if params:
            path = path + "?" + urllib.parse.urlencode(params)
        return self._request(path)

    def post(self, path, params=None):
        return self._request(path, params or {}, method="POST")

    def delete(self, path):
        return self._request(path, method="DELETE")

    # ------------------------------------------------------------ 业务动作
    def _payload(self, locations):
        tasks = []
        for i, (name, script) in enumerate(NODES):
            tasks.append({
                "code": TASK_CODE_BASE + i + 1,
                "name": name,
                "version": 1,
                "description": "",
                "delayTime": 0,
                "taskType": "SHELL",
                "taskParams": {
                    "resourceList": [],
                    "localParams": [],
                    "rawScript": script,
                    "conditionResult": {"successNode": [""], "failedNode": [""]},
                    "dependence": {},
                },
                "flag": "YES",
                "taskPriority": "MEDIUM",
                "workerGroup": "default",
                "environmentCode": -1,
                # 重活节点不自动重试：INSERT OVERWRITE 幂等，但上一轮被停时若留下孤儿
                # spark 应用，重试会变成两个应用抢 YARN 资源（本项目有过 43 分钟的教训）。
                "failRetryTimes": 0,
                "failRetryInterval": 1,
                "timeoutFlag": "CLOSE",
                "timeoutNotifyStrategy": "",
                "timeout": 0,
                "taskExecuteType": "BATCH",
            })

        relations = []
        for i, (name, _) in enumerate(NODES):
            code = TASK_CODE_BASE + i + 1
            relations.append({
                "name": "",
                "preTaskCode": 0 if i == 0 else TASK_CODE_BASE + i,
                "preTaskVersion": 0 if i == 0 else 1,
                "postTaskCode": code,
                "postTaskVersion": 1,
                "conditionType": "NONE",
                "conditionParams": {},
            })

        return {
            "name": WF_NAME,
            "description": "Spark 版旁路实验：DWS/ADS 八段（同 SQL），写 gmall_bench，用于与 Hive 版对比",
            "globalParams": "[]",
            "locations": json.dumps(locations, ensure_ascii=False),
            "timeout": 0,
            "tenantCode": TENANT,
            "taskRelationJson": json.dumps(relations, ensure_ascii=False),
            "taskDefinitionJson": json.dumps(tasks, ensure_ascii=False),
            "executionType": "PARALLEL",
        }

    def list_definitions(self):
        """注：本 DS 3.1.8 的 query-by-name 接口是坏的（对存在的定义也回 50003），
        只能用分页列表接口自己按名字找。"""
        r = self.get("/projects/%d/process-definition" % PROJECT_CODE,
                     {"pageNo": 1, "pageSize": 200})
        if not r.get("success"):
            raise SystemExit("列出工作流定义失败: %s" % json.dumps(r, ensure_ascii=False)[:300])
        d = r.get("data") or {}
        return d.get("totalList") or d.get("records") or []

    def definition_code(self, required=True):
        for d in self.list_definitions():
            if d.get("name") == WF_NAME:
                return d["code"]
        if required:
            raise SystemExit("工作流 %s 不存在" % WF_NAME)
        return None

    def create(self):
        if self.definition_code(required=False):
            raise SystemExit("工作流 %s 已存在，先 delete 或改名" % WF_NAME)
        body = self._payload(build_locations())
        r = self.post("/projects/%d/process-definition" % PROJECT_CODE, body)
        if not r.get("success"):
            raise SystemExit("创建工作流失败: %s" % json.dumps(r, ensure_ascii=False)[:600])
        # 坑：请求里指定的 code 会被 DS 忽略，必须回读真实 code
        code = self.definition_code()
        os.makedirs(os.path.dirname(CODE_FILE), exist_ok=True)
        with open(CODE_FILE, "w") as f:
            f.write(str(code))
        print("创建成功  name=%s  code=%s（已写入 %s）" % (WF_NAME, code, CODE_FILE))
        return code

    def relayout(self):
        code = self.definition_code()
        print("① 下线（DS 不允许编辑在线的工作流）")
        self.release(state=0)
        print("② 写入新坐标：起点 x=%d，间距 %dpx，共 %d 个节点"
              % (NODE_X0, NODE_DX, len(NODES)))
        r = self._request("/projects/%d/process-definition/%d" % (PROJECT_CODE, code),
                          self._payload(build_locations()), method="PUT")
        if not r.get("success"):
            raise SystemExit("更新失败: %s" % json.dumps(r, ensure_ascii=False)[:600])
        print("③ 重新上线")
        self.release(state=1)
        return code

    def release(self, state=1):
        """坑：releaseState 要传**枚举名**（ONLINE / OFFLINE），传 1/0 只会回笼统的 10108。"""
        code = self.definition_code()
        r = self.post("/projects/%d/process-definition/%d/release" % (PROJECT_CODE, code),
                      {"releaseState": "ONLINE" if state == 1 else "OFFLINE",
                       "name": WF_NAME})
        if not r.get("success"):
            raise SystemExit("上下线失败: %s" % json.dumps(r, ensure_ascii=False))
        d = self.get("/projects/%d/process-definition/%d" % (PROJECT_CODE, code))["data"]
        print("已%s  code=%s  releaseState=%s"
              % ("上线" if state == 1 else "下线", code, d["processDefinition"]["releaseState"]))
        return code

    def run(self):
        """触发一次**全量**运行。

        坑：timeout 必须落在 (0, 86400]；传 0（"不超时"的直觉写法）会被判非法（笼统的 10002）。
        """
        code = self.definition_code()
        r = self.post("/projects/%d/executors/start-process-instance" % PROJECT_CODE, {
            "processDefinitionCode": code,
            "scheduleTime": "",
            "failureStrategy": "END",
            "warningType": "NONE",
            "processInstancePriority": "MEDIUM",
            "workerGroup": "default",
            "taskDependType": "TASK_POST",
            "timeout": 86400,
        })
        if not r.get("success"):
            raise SystemExit("触发失败: %s" % json.dumps(r, ensure_ascii=False)[:600])
        print("已触发（全量）: %s" % json.dumps(r.get("data"), ensure_ascii=False))
        return r.get("data")

    def info(self):
        code = self.definition_code()
        d = self.get("/projects/%d/process-definition/%d" % (PROJECT_CODE, code))["data"]
        pd = d["processDefinition"]
        locs = pd.get("locations")
        if isinstance(locs, str):
            try:
                locs = json.loads(locs)
            except ValueError:
                locs = []
        xs = sorted(p["x"] for p in (locs or []))
        gaps = sorted({b - a for a, b in zip(xs, xs[1:])}) if len(xs) > 1 else []
        print("name=%s code=%s version=%s releaseState=%s"
              % (pd["name"], pd["code"], pd["version"], pd["releaseState"]))
        print("节点 %d 个；画布坐标 %d 个，水平间距=%s（<220px 会挤成一团）"
              % (len(d.get("taskDefinitionList") or []), len(locs or []), gaps))
        codes = [t["code"] for t in (d.get("taskDefinitionList") or [])]
        if codes:
            mx = max(codes)
            print("task code 最大 = %s（2^53 = %s）→ JS 安全？ %s"
                  % (mx, 2 ** 53, "✅ 是" if mx <= 2 ** 53 else "❌ 否！"))
        for t in sorted(d.get("taskDefinitionList") or [], key=lambda x: x["code"]):
            script = (t.get("taskParams") or {}).get("rawScript", "")
            if " -f " in script:
                target = "-f " + script.split(" -f ")[-1].split(" ")[0].strip("'")
            elif "bash " in script:
                target = "bash " + script.split("bash ")[-1].split(" ")[0].strip("'")
            else:
                target = script[:60]
            print("  %-22s %s" % (t["name"], target))
        return code

    def delete(self):
        code = self.definition_code()
        self.release(state=0)
        r = self.delete("/projects/%d/process-definition/%d" % (PROJECT_CODE, code))
        if not r.get("success"):
            raise SystemExit("删除失败（可能被实例引用）: %s"
                             % json.dumps(r, ensure_ascii=False)[:400])
        print("已删除 code=%s" % code)


def main():
    args = sys.argv[1:]
    if not args or args[0] not in ("create", "relayout", "online", "offline",
                                   "run", "info", "delete"):
        print(__doc__)
        raise SystemExit(2)
    action = args[0]
    c = DSClient()
    c.login()
    if action == "create":
        c.create()
    elif action == "relayout":
        c.relayout()
    elif action == "online":
        c.release(state=1)
    elif action == "offline":
        c.release(state=0)
    elif action == "run":
        c.run()
    elif action == "info":
        c.info()
    elif action == "delete":
        c.delete()


if __name__ == "__main__":
    main()
