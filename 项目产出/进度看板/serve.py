#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
DWD 执行进度看板 —— 本地轮询集群 + 网页实时展示

【为什么这么做】
    Hive 的 INSERT 一跑就是十几二十分钟，`beeline -f` 前台跑既看不到进度、
    SSH 单次调用还容易被超时打断。集群侧的 run_dwd.sh 会把每一步的状态写进
    status.tsv，这个脚本定期把它拉回来，算成百分比，写进 progress.json，
    浏览器每 3 秒取一次，于是就有了一个真正跟得上的进度条。

【用法】
     python serve.py            # 默认端口 8765
     python serve.py 9000       # 换端口
   然后浏览器打开 http://localhost:8765/
   停止：Ctrl+C

【依赖】只用标准库；需要本机有 ssh（Windows 自带 OpenSSH 即可）。
"""
import json
import os
import re
import subprocess
import sys
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
KEY = r"D:\数据开发环境\VMware\sshkeys\cluster_ed25519"
HOST = "lrh@192.168.87.131"
POLL_SECONDS = 5

# 支持两条链路切换：python serve.py [端口] [运行目录名]
#   dwd_run = DWD 清洗落表（1063 万行 → 三张表）
#   evt_run = DWS/ADS 汇总（DWD → 主题宽表 → 指标表）
RUN = sys.argv[2] if len(sys.argv) > 2 else "dwd_run"
REMOTE_CMD = "bash /home/lrh/workspace/dwd_progress.sh %s" % RUN

PAGE_TITLE = {
    "dwd_run": "gmall 数仓 · DWD 清洗落表进度",
    "evt_run": "gmall 数仓 · DWS/ADS 主题汇总与指标计算",
}
PAGE_SUB = {
    "dwd_run": "真实数据集 REES46 · ODS 1063 万行 → DWD/DIM 三张表（ORC + SNAPPY）",
    "evt_run": "真实数据集 REES46 · DWD → DWS 四张主题宽表 → ADS 四张指标表（ORC + SNAPPY）",
}

# 与集群侧运行器的步骤清单保持一致，用于「还没开始」的步骤也能显示名字
STEP_NAMES_DWD = {
    1: ("建表 DWD/DIM 三张", "dwd_ddl.sql"),
    2: ("清洗事件明细 1063 万行", "去重 + 解析时间 + 拆类目 + 归一空值"),
    3: ("派生购买事实", "从 purchase 事件派生订单事实"),
    4: ("派生商品维度", "窗口函数取每商品主属性（最重的一步）"),
    5: ("校验三张表行数", "分区数 + 行数核对"),
}

STEP_NAMES_EVT = {
    1: ("建表 DWS/ADS 八张", "evt_ddl.sql"),
    2: ("DWS·用户主题（用户×天）", "浏览/加购/购买数 + 消费额 + 会话数"),
    3: ("DWS·商品主题（商品×天）", "先聚合再关维表（避免大表 shuffle）"),
    4: ("DWS·类目主题（类目×天）", "买家数回明细重算（COUNT DISTINCT 不可加）"),
    5: ("DWS·交易主题（交易×天）", "订单数 / 买家数 / GMV / 客单价"),
    6: ("ADS·活跃用户 DAU/新增/回流", "窗口函数一次算出四个口径"),
    7: ("ADS·转化漏斗（三环节）", "浏览 → 加购 → 购买"),
    8: ("ADS·累计复购率", "窗口函数累计替代中间表"),
    9: ("ADS·TopN 商品", "每天只留 GMV 前 50"),
    10: ("校验（行数 + 交叉对账 + 方向性）", "三路 GMV 必须相等"),
}

STEP_NAMES = STEP_NAMES_EVT if RUN == "evt_run" else STEP_NAMES_DWD

STATE = {"payload": None, "error": None, "updated": 0}


def ssh_probe():
    r = subprocess.run(
        ["ssh", "-i", KEY, "-o", "BatchMode=yes",
         "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=8",
         HOST, REMOTE_CMD],
        capture_output=True, text=True, timeout=45,
        encoding="utf-8", errors="replace",
    )
    if r.returncode != 0 and not r.stdout:
        raise RuntimeError((r.stderr or "ssh 无输出").strip()[:200])
    return r.stdout


def parse(out):
    d = {"runner": "UNKNOWN", "start_epoch": 0, "status": {},
         "current": None, "stage": None, "job_index": None,
         "job_total": None, "mem": "", "disk": "", "done": "", "now": int(time.time())}
    section = None
    for raw in out.splitlines():
        line = raw.rstrip("\n")
        if line.startswith("### "):
            section = line[4:].strip()
            continue
        if not line.strip():
            continue
        if section == "进程":
            m = re.match(r"runner=(\w+)", line)
            if m:
                d["runner"] = m.group(1)
            m = re.match(r"start_epoch=(\d+)", line)
            if m:
                d["start_epoch"] = int(m.group(1))
            m = re.match(r"done=(.+)", line)
            if m:
                d["done"] = m.group(1).strip()
        elif section == "步骤状态":
            f = line.split("\t")
            if len(f) >= 3:
                no = f[0].strip()
                if no.isdigit():
                    d["status"][int(no)] = {
                        "state": f[2].strip(),
                        "time": f[3].strip() if len(f) > 3 else "",
                        "elapsed": f[4].strip() if len(f) > 4 else "",
                    }
        elif section == "当前步骤":
            f = line.split("|")
            if len(f) >= 4 and f[0].strip().isdigit():
                d["current"] = {"no": int(f[0]), "name": f[1], "since": int(f[3])}
        elif section == "Hive 当前阶段百分比（最后一行）":
            m = re.search(r"Stage-(\d+) map = (\d+)%,\s+reduce = (\d+)%", line)
            if m:
                d["stage"] = {"stage": int(m.group(1)),
                              "map": int(m.group(2)), "reduce": int(m.group(3))}
        elif section == "Hive 子作业编排":
            m = re.search(r"Total jobs = (\d+)", line)
            if m:
                d["job_total"] = int(m.group(1))
            m = re.search(r"Launching Job (\d+) out of (\d+)", line)
            if m:
                d["job_index"] = int(m.group(1))
                d["job_total"] = int(m.group(2))
        elif section == "内存磁盘":
            if not d["mem"]:
                d["mem"] = line.strip()
            elif not d["disk"]:
                d["disk"] = line.strip()
    return d


def build_payload(d):
    steps = []
    done_count = 0
    cur_no = d["current"]["no"] if d["current"] else None

    # 当前步骤内部的完成比例：按「第几个子作业 + 该作业 map/reduce 均值」估算
    frac = 0.0
    stage_txt = ""
    if d["stage"]:
        stage_txt = "map %d%% / reduce %d%%" % (d["stage"]["map"], d["stage"]["reduce"])
        avg = (d["stage"]["map"] + d["stage"]["reduce"]) / 2.0
        ji, jt = d["job_index"] or 1, d["job_total"] or 1
        frac = ((ji - 1) + avg / 100.0) / jt
    elif cur_no:
        frac = 0.15

    for no in sorted(STEP_NAMES):
        name, sub = STEP_NAMES[no]
        st = d["status"].get(no, {})
        state = st.get("state", "PENDING")
        # status.tsv 里「开始」那一行的状态写的是 START，要归一到 RUNNING，
        # 否则正在跑的步骤会被误显示成「等待」
        if state == "START":
            state = "RUNNING"
        if state == "OK":
            done_count += 1
        item = {"no": no, "name": name, "sub": sub, "state": state,
                "elapsed": st.get("elapsed", ""), "time": st.get("time", "")}
        if no == cur_no and state == "RUNNING":
            item["frac"] = round(frac, 4)
            item["stage"] = stage_txt
            item["job"] = ("子作业 %s/%s" % (d["job_index"], d["job_total"])
                           if d["job_index"] else "")
        steps.append(item)

    total_steps = len(STEP_NAMES)
    if d["done"]:
        overall = 1.0
    else:
        overall = (done_count + (frac if cur_no else 0)) / total_steps
    elapsed = (d["now"] - d["start_epoch"]) if d["start_epoch"] else 0
    eta = int(elapsed * (1 - overall) / overall) if 0.02 < overall < 1 else None

    return {
        "runner": d["runner"],
        "done": d["done"],
        "updated": time.strftime("%H:%M:%S"),
        "elapsed": elapsed,
        "eta": eta,
        "overall": round(overall, 4),
        "done_steps": done_count,
        "total_steps": total_steps,
        "steps": steps,
        "mem": d["mem"],
        "disk": d["disk"],
        "recent": d["stage"],
        "title": PAGE_TITLE.get(RUN, PAGE_TITLE["dwd_run"]),
        "sub": PAGE_SUB.get(RUN, PAGE_SUB["dwd_run"]),
        "run": RUN,
    }


def poll_loop():
    while True:
        try:
            payload = build_payload(parse(ssh_probe()))
            STATE["payload"] = payload
            STATE["error"] = None
        except Exception as exc:  # noqa: BLE001
            STATE["error"] = str(exc)
        STATE["updated"] = time.time()
        try:
            with open(os.path.join(HERE, "progress.json"), "w", encoding="utf-8") as fh:
                json.dump({"payload": STATE["payload"], "error": STATE["error"]},
                          fh, ensure_ascii=False)
        except OSError:
            pass
        time.sleep(POLL_SECONDS)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=HERE, **kw)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, max-age=0")
        super().end_headers()

    def log_message(self, *a):
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    threading.Thread(target=poll_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print("进度看板已启动： http://localhost:%d/" % port)
    print("数据每 %d 秒刷新一次；Ctrl+C 停止。" % POLL_SECONDS)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")


if __name__ == "__main__":
    main()
