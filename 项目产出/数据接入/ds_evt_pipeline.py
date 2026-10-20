#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DolphinScheduler 工作流 gmall_evt_pipeline 的「建 / 上线 / 触发 / 查看 / 重排」工具。

它对应的链路是**真实数据链路**（REES46 1063 万行）：
    ODS 就绪 → DWD(建表 + 清洗 + 购买事实 + 商品维度) → DWS(用户/商品/类目/交易)
    → ADS(活跃/漏斗/复购/TopN) → 三层校验
一共 16 个 SHELL 节点，串行依赖，一个节点一个 SQL。

【为什么走 REST 接口而不是直接写 MySQL】
  DS 的 master 会把工作流定义缓存在内存里，直接 INSERT 数据库 master 看不到；
  而 code / version / 依赖边的一致性由服务端保证。走官方接口最稳。

【为什么节点脚本里要 ssh 回 lrh-1】
  DS 的 worker 跑在 lrh-2 / lrh-3 上，那两台**没装 Hive 客户端**，
  所以任务必须跳回 lrh-1 执行 beeline。这是本项目既有做法，不要改。

【用法】（在 lrh-1 上执行）
  python3 ds_evt_pipeline.py create     # 新建定义（已存在则报错）
  python3 ds_evt_pipeline.py relayout   # 只重排画布坐标（节点挤成一团时用）
  python3 ds_evt_pipeline.py online     # 上线（上线后才能运行）
  python3 ds_evt_pipeline.py run        # 触发一次（全量；本工具不提供"局部重跑"，原因见 run() 注释）
  python3 ds_evt_pipeline.py info       # 打印定义与节点清单（含画布间距自检）
  python3 ds_evt_pipeline.py delete     # 删除定义（无实例引用时才行）

⚠️ 本工具**故意不提供** startNodeList（从指定节点重跑）：实测它会触发 DS 3.1.8 的
   master NPE 死循环。要重跑就整条重跑（ETL 幂等，约 54 分钟）。
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
WF_NAME = "gmall_evt_pipeline"
TENANT = "lrh"
TASK_CODE_BASE = 33000000000001        # ⚠️ 必须 ≤ 2^53，理由见下面的长注释
CODE_FILE = "/home/lrh/workspace/ds_steps/wf_code.txt"

# ---------------------------------------------------------------- ★ code 的取值范围
# 【★ 最重要的一条约束】task code / process definition code 必须 ≤ 2^53 = 9007199254740992（≈9.0e15）
#
# 为什么：DS 的接口把这些 code 当 **JSON 数字**返回（不是字符串），而浏览器用 JavaScript 的
# Number（IEEE754 双精度）解析。双精度只能精确表示 ≤2^53 的整数；超过之后，**两个不同的
# 64 位整数在浏览器里会变成同一个数**。
#
# 实测（2026-10-03，就是"工作流只有 1 个节点"事故的根因）：
#   我把 code 定成 3300000000000000001（19 位，≈3.3e18）
#   → 模拟浏览器解析后：16 个 code **全部塌缩成 1 个**（都等于 3300000000000000000）
#   → 后果：DAG 画布上只画得出 **1 个节点**，而且没有任何连线
#   → 更阴的是：**数据库和 master 全是 64 位整数，16 个节点跑得好好的**（所以 16/16 SUCCESS），
#     只有浏览器分不清 → "执行正常、显示全错"
#
# 对照（同一套环境实测）：
#   DS 自己生成的 code：gen-task-codes → 23242403233824（≈2.3e13）  ✅ JS 安全
#   旧工作流 gmall_dw_pipeline：1100000000000000001（1.1e18）      ❌ 会塌缩（一直显示成 1 个节点）
#
# 所以：**要么用 DS 的 gen-task-codes 生成，要么自己定一个 ≤ 9.0e15 的值**（这里用 14 位）。
# 文件下面（NODES 定义之后）有一行 assert 专门防止以后有人又把它改大。


# ---------------------------------------------------------------- 节点命令模板
def _beeline(sql_path, silent=True):
    """生成一个节点的 rawScript：ssh 回 lrh-1 跑 beeline。

    结尾的 </dev/null 不能省：beeline 是 jline 交互程序，没有控制终端又没重定向
    stdin 时会被 SIGTTOU 冻住（进程状态 Tl+），DS 里表现为"一直运行中、日志无报错、
    也永远不提交作业"，极难排查。2>&1 是为了让 beeline 的报错也进 DS 任务日志。
    """
    opt = " --silent=true" if silent else ""
    return (
        "ssh -o StrictHostKeyChecking=no -n lrh-1 "
        "'export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH; "
        "/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000"
        + opt + " -f " + sql_path + " </dev/null 2>&1'"
    )


def _shell(script_path):
    return ("ssh -o StrictHostKeyChecking=no -n lrh-1 "
            "'bash " + script_path + " 2>&1'")


DS = "/home/lrh/workspace/ds_steps"

NODES = [
    ("preflight",          _shell("/home/lrh/workspace/ds_preflight.sh")),
    ("ods_ready",          _shell("/home/lrh/workspace/evt_ods_ready.sh")),
    ("dwd_ddl",            _beeline("/home/lrh/workspace/dwd_ddl.sql")),
    ("dwd_event_clean",    _beeline(DS + "/dwd_step2.sql")),
    ("dwd_purchase_fact",  _beeline(DS + "/dwd_step3.sql")),
    ("dwd_dim_product",    _beeline(DS + "/dwd_step4.sql")),
    ("evt_ddl",            _beeline("/home/lrh/workspace/evt_ddl.sql")),
    ("dws_user",           _beeline(DS + "/evt_step1.sql")),
    ("dws_product",        _beeline(DS + "/evt_step2.sql")),
    ("dws_category",       _beeline(DS + "/evt_step3.sql")),
    ("dws_trade",          _beeline(DS + "/evt_step4.sql")),
    ("ads_active_user",    _beeline(DS + "/evt_step5.sql")),
    ("ads_funnel",         _beeline(DS + "/evt_step6.sql")),
    ("ads_repurchase",     _beeline(DS + "/evt_step7.sql")),
    ("ads_topn",           _beeline(DS + "/evt_step8.sql")),
    # 校验节点不用 --silent，要把三层校验的表格完整打进 DS 任务日志，作为人可读证据
    ("verify",             _beeline("/home/lrh/workspace/evt_verify.sql", silent=False)),
]

# ---------------------------------------------------------------- DAG 画布坐标
# ⚠️ 这两个值是踩坑换来的，别随手改小：
#   DS 的 DAG 节点本身宽约 180~200px，**坐标间距必须 >= 220px**，
#   否则节点在画布上互相盖住，看起来就像"整个工作流只有一个节点"。
#   （对照：本项目网页手工拖出来的 gmall_dw_pipeline，实测间距正好是 220px。）
#   注意：坐标只影响"画得好不好看"，**完全不影响执行** —— 执行靠的是依赖边。
NODE_X0 = 80     # 第一个节点的 x
NODE_DX = 220    # 相邻节点的水平间距
NODE_Y = 160     # 统一 y（16 个节点一条直线，箭头最短最清楚）

# ★ 启动即自检：task code 必须落在 JS 安全整数范围内（见文件开头 TASK_CODE_BASE 处的长注释）
assert TASK_CODE_BASE + len(NODES) <= 2 ** 53, (
    "task code 超过 JS 安全整数 2^53 —— 浏览器会把不同节点塌缩成同一个数，"
    "画布上只显示 1 个节点（数据库和实际执行都正常，所以极难发现）！")


def build_locations():
    """生成 16 个节点的画布坐标。"""
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
        headers = {"Accept": "application/json", "User-Agent": "ds-evt-pipeline/1.0"}
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
        """组装创建/更新工作流所需的完整请求体（create 与 relayout 共用）。"""
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
                # 重活节点不自动重试：INSERT OVERWRITE 虽然幂等，但如果上一轮
                # 被停掉时留下了孤儿 beeline，重试就会变成两个 beeline 抢 HS2。
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
            "description": "真实数据链路（REES46 1063万行）：ODS→DWD→DWS→ADS→校验，16 节点串行",
            "globalParams": "[]",
            "locations": json.dumps(locations, ensure_ascii=False),
            "timeout": 0,
            "tenantCode": TENANT,
            "taskRelationJson": json.dumps(relations, ensure_ascii=False),
            "taskDefinitionJson": json.dumps(tasks, ensure_ascii=False),
            "executionType": "PARALLEL",
        }

    def list_definitions(self):
        """列出项目下所有工作流定义。

        注：本项目这套 DS 3.1.8 里 /process-definition/query-by-name 是坏的——
        对**确实存在**的定义也返回 50003「does not exist」，所以不能拿它做存在性判断，
        只能用分页列表接口自己按名字找。
        """
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
        """只重排画布坐标（节点位置太挤时用），流程逻辑一个字都不动。

        为什么要"下线 → 改 → 上线"：DS 不允许编辑已上线的工作流定义。
        改完 DS 会把定义版本号 +1，历史实例仍指向旧版本，不受影响。
        """
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

        # 回读校验：间距对不对，一眼就能看出来
        d = self.get("/projects/%d/process-definition/%d" % (PROJECT_CODE, code))["data"]
        pd = d["processDefinition"]
        locs = pd.get("locations")
        if isinstance(locs, str):
            locs = json.loads(locs)
        xs = sorted(p["x"] for p in (locs or []))
        gaps = sorted({b - a for a, b in zip(xs, xs[1:])})
        print("④ 回读：version=%s releaseState=%s locations=%d 个，水平间距=%s"
              % (pd["version"], pd["releaseState"], len(locs or []), gaps))
        return code

    def release(self, state=1):
        """上线/下线。

        坑：releaseState 这个参数要的是**枚举名**（ONLINE / OFFLINE），
        传 1 / 0 会直接抛 MethodArgumentTypeMismatchException，
        接口只回一句笼统的 10108「release process definition error」，很难猜。
        """
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

        ★ 这里刻意**没有**提供"从指定节点开始重跑"（startNodeList）功能 ——
        实测在本项目的 DS 3.1.8 上，带 startNodeList 的启动会让 master 进入
        `DagHelper.parsePostNodes` NPE 死循环：日志每秒涨几千行、CPU 打满、
        实例永远卡在 RUNNING 且**一个任务都不下发**；而且**重启 master 也修不好**，
        必须停下来 + 从库里删掉那条实例。详见《排障汇总》第 21 坑。

        所以：**要重跑就整条重跑**。本链路 ETL 是 INSERT OVERWRITE / DROP+CREATE，
        幂等，重跑一遍约 54 分钟，不会污染数据。

        坑：timeout 参数 DS 内部校验的是 `timeout > 0 且 timeout <= 86400`，
        传 0（也就是"不超时"的直觉写法）会被判为非法，接口只回一句笼统的
        10002「task timeout parameter is not valid」。所以给上限 86400 秒（24 小时）。
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
                  % (mx, 2 ** 53,
                     "✅ 是" if mx <= 2 ** 53 else "❌ 否！浏览器会把节点塌缩成 1 个"))
        for t in sorted(d.get("taskDefinitionList") or [], key=lambda x: x["code"]):
            script = (t.get("taskParams") or {}).get("rawScript", "")
            if " -f " in script:
                target = "-f " + script.split(" -f ")[-1].split(" ")[0].strip("'")
            elif "bash " in script:
                target = "bash " + script.split("bash ")[-1].split(" ")[0].strip("'")
            else:
                target = script[:60]
            print("  %-20s %s" % (t["name"], target))
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
    {"create": c.create,
     "relayout": c.relayout,
     "online": lambda: c.release(1),
     "offline": lambda: c.release(0),
     "run": c.run,
     "info": c.info,
     "delete": c.delete}[action]()


if __name__ == "__main__":
    main()
