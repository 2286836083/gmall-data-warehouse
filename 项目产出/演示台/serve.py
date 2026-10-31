#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
gmall 数仓 · 演示台（本地单文件服务）

【这个程序解决什么问题】
    面试时要在 3 分钟内让人相信「这不是一个 PPT 项目」。
    敲命令行太慢、太杂，而且一次输错就中断了讲解节奏。
    所以这里把「看的东西」和「能证明的东西」放进同一个页面：
      - 离线页签：架构、表、对账、双引擎、大屏 —— 集群关着也能演示
      - 实时页签：点一下就跑一条秒级查询，把真实返回贴在屏幕上
      - 启动页签：三台 VM 一键拉起 + 体检，带真实进度

【设计原则：绝不假装成功】
    集群不在线时，实时区显示「未连接 + 原因」，不返回任何编造的数字。
    每个查询旁边显示的是「上次实测耗时」，不是拍脑袋估的。

【用法】
    python serve.py            # 默认 127.0.0.1:8766
    python serve.py 9000       # 换端口
    然后浏览器打开 http://127.0.0.1:8766/
    停止：Ctrl+C

【依赖】只用标准库。需要本机有 ssh（Windows 自带 OpenSSH）与 vmrun（VMware Workstation）。
"""

import base64
import json
import os
import re
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "数据")
FACTS = os.path.join(DATA, "证据.json")
CACHE = os.path.join(DATA, "上次耗时.json")

# ---- 集群事实（与 CONTEXT.md §二/§三 一致；别凭记忆改）----
NODE = "lrh@192.168.87.131"
KEY = r"D:\数据开发环境\VMware\sshkeys\cluster_ed25519"
JAVA_HOME = "/opt/jdk/jdk1.8.0_202"
BEELINE = "/opt/hive/apache-hive-3.1.3-bin/bin/beeline"
HIVE_URL = "jdbc:hive2://lrh-1:10000"
HADOOP = "/opt/hadoop/hadoop-3.3.6/bin"

# ---- 三台虚拟机 ----
VMRUN = r"D:\VMware\vmrun.exe"
VMS = [
    {"name": "lrh-1", "vmx": r"D:\虚拟机\Ubuntu 64-bit.vmx", "mem": 7168},
    {"name": "lrh-2", "vmx": r"D:\虚拟机2\Ubuntu 64-bit.vmx", "mem": 3392},
    {"name": "lrh-3", "vmx": r"D:\虚拟机3\Ubuntu 64-bit.vmx", "mem": 4096},
]

PORTS = [
    (8020, "HDFS RPC"),
    (9870, "HDFS UI"),
    (8088, "YARN UI"),
    (9083, "Metastore"),
    (10000, "HiveServer2"),
    (2181, "ZooKeeper"),
    (12345, "DS API"),
]

# 演示可选件：DolphinScheduler 不随开机自启，没启动是常态而非故障。
# 这类端口不作为"集群健康"的判据，避免把"没点开 DS"误读成"连接坏了"。
OPTIONAL_PORTS = {12345}

# known_hosts 固定到本目录：默认位置（~/.ssh）可能不存在或不可写，
# OpenSSH 每次会往 stderr 打一行 "Failed to add the host to the list of known hosts"，
# 而 stderr 会被合并进页面输出，看起来像"连接出错"。
KNOWN_HOSTS = os.path.join(DATA, "_known_hosts")
try:
    os.makedirs(DATA, exist_ok=True)
    open(KNOWN_HOSTS, "a").close()
except Exception:  # noqa: BLE001
    pass

SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=6",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=" + KNOWN_HOSTS,
    "-o", "LogLevel=ERROR",
    "-o", "ServerAliveInterval=15",
]


def ssh_script(script, timeout=30):
    """把脚本经 stdin 送进远端 bash -s，避免引号地狱。返回 (rc, out, elapsed)。"""
    t0 = time.time()
    try:
        p = subprocess.run(
            ["ssh", "-i", KEY] + SSH_OPTS + [NODE, "bash -s"],
            input=script.encode("utf-8"),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=timeout,
        )
        out = p.stdout.decode("utf-8", "replace")
        err = p.stderr.decode("utf-8", "replace")
        return p.returncode, (out + ("\n" + err if err.strip() else "")), time.time() - t0
    except subprocess.TimeoutExpired:
        return 124, "TIMEOUT: 超过 %ss 未返回" % timeout, time.time() - t0
    except FileNotFoundError:
        return 127, "本机找不到 ssh 命令", time.time() - t0
    except Exception as e:  # noqa: BLE001
        return 1, "SSH 调用失败: %s" % e, time.time() - t0


# ══════════════════════════════════════════════════════════════
#  健康检查
# ══════════════════════════════════════════════════════════════
HEALTH_CACHE = {"ts": 0, "payload": None}
HEALTH_LOCK = threading.Lock()

HEALTH_SCRIPT = """
export JAVA_HOME=%(java)s
export PATH=$JAVA_HOME/bin:%(hadoop)s:$PATH
echo "##HOST"
hostname 2>/dev/null || echo unknown
echo "##UPTIME"
uptime -p 2>/dev/null || uptime 2>/dev/null || echo unknown
echo "##PORTS"
for p in %(ports)s; do
  if ss -lnt 2>/dev/null | grep -qE "[:.]$p[[:space:]]"; then echo "$p UP"; else echo "$p DOWN"; fi
done
echo "##JPS"
jps 2>/dev/null | awk '$2 != "Jps" {print $2}' | sort | uniq -c | awk '{printf "%%s x%%s, ", $2, $1}'
echo ""
echo "##MEM"
free -m 2>/dev/null | awk '/^Mem:/{print $2" "$7}'
echo "##END"
""" % {
    "java": JAVA_HOME,
    "hadoop": HADOOP,
    "ports": " ".join(str(p) for p, _ in PORTS),
}


def parse_health(text):
    sec, cur = {}, None
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("##"):
            cur = s[2:]
            sec[cur] = []
        elif cur:
            sec[cur].append(s)

    ports = {}
    for ln in sec.get("PORTS", []):
        m = re.match(r"(\d+)\s+(UP|DOWN)", ln)
        if m:
            ports[int(m.group(1))] = m.group(2) == "UP"

    up = sum(1 for v in ports.values() if v)
    core_up = sum(1 for p, _ in PORTS if p not in OPTIONAL_PORTS and ports.get(p, False))
    return {
        "ok": True,
        "hostname": (sec.get("HOST") or ["?"])[0],
        "uptime": (sec.get("UPTIME") or ["?"])[0],
        "ports": [{"port": p, "name": n, "up": ports.get(p, False),
                   "optional": p in OPTIONAL_PORTS} for p, n in PORTS],
        "ports_up": up,
        "ports_total": len(PORTS),
        # 核心端口 = 非可选。分开统计，避免「DS 没启」被读成「集群坏了」：
        # Hadoop 全家桶起不来才是真故障，DS 是演示可选件，本来就不随开机自启。
        "core_up": core_up,
        "core_total": len([p for p, _ in PORTS if p not in OPTIONAL_PORTS]),
        "optional_down": [n for p, n in PORTS if p in OPTIONAL_PORTS and not ports.get(p, False)],
        "jps": "".join(sec.get("JPS", [])).strip().strip(","),
        "mem": (sec.get("MEM") or [""])[0],
    }


def health(force=False):
    with HEALTH_LOCK:
        if not force and HEALTH_CACHE["payload"] and time.time() - HEALTH_CACHE["ts"] < 5:
            return HEALTH_CACHE["payload"]

    rc, out, el = ssh_script(HEALTH_SCRIPT, timeout=20)
    if rc != 0 or "##END" not in out:
        low = out.lower()
        if "timeout" in low or "timed out" in low or "unreachable" in low:
            reason = "集群未启动（SSH 连接超时）"
        elif "permission denied" in low:
            reason = "SSH 密钥被拒绝（检查 key 路径与权限）"
        elif "no route" in low:
            reason = "网络不可达（VMware 虚拟网络未启用？）"
        else:
            reason = "SSH 无有效输出：" + out.strip()[:160]
        payload = {"ok": False, "reason": reason, "elapsed": round(el, 1)}
    else:
        payload = parse_health(out)
        payload["elapsed"] = round(el, 1)
        payload["ok"] = payload["ports_up"] > 0

    with HEALTH_LOCK:
        HEALTH_CACHE["ts"] = time.time()
        HEALTH_CACHE["payload"] = payload
    return payload


# ══════════════════════════════════════════════════════════════
#  实时查询
# ══════════════════════════════════════════════════════════════
NOISE = re.compile(
    r"^(SLF4J|log4j|WARN|INFO|Beeline version|Connecting to|Connected to|"
    r"Transaction isolation|Closing:|Failed to add the host|"
    r"No rows selected|0: jdbc)",
    re.I,
)
# ★ 曾经这里还过滤了 "Error: CompileError"，已删除：
#   把错误行当噪声滤掉，SQL 编译失败时页面只会显示"无输出"，
#   真实原因被自己屏蔽 —— 宁可显示难看的报错，也不能显示空白。

LIVE = {
    "hdfs_count": {
        "label": "ODS 目录与体积",
        "kind": "shell",
        "cmd": "%s/hdfs dfs -count -h /user/hive/warehouse/gmall.db/ods_event_log_di" % HADOOP,
        "expect": "秒级 · 目录数 / 文件数 / 体积",
        "timeout": 30,
        "proof": "数据真在 HDFS 上，不是内存里编的",
    },
    "hdfs_parts": {
        "label": "分区目录数",
        "kind": "shell",
        "cmd": "%s/hdfs dfs -ls /user/hive/warehouse/gmall.db/ods_event_log_di 2>/dev/null | grep -c '^d'" % HADOOP,
        "expect": "秒级 · 应为 31",
        "timeout": 30,
        "proof": "按天分区的物理证据",
    },
    "gmv": {
        "label": "全月 GMV 与成交笔数",
        "kind": "hive",
        # 必须用 format_number 而不是 ROUND：gmv 是 DOUBLE，
        # ROUND(SUM(gmv),2) 经 tsv2 输出仍是科学计数法（实测 5.75759965E7），
        # 屏幕上和简历的 57,575,996.50 对不上。format_number 返回字符串，输出即最终形态。
        "sql": "SELECT format_number(SUM(gmv), 2) AS gmv, SUM(order_cnt) AS orders "
               "FROM gmall.ads_trade_day",
        "expect": "约 34 秒 · 应为 57,575,996.50 / 186059",
        "timeout": 120,
        "proof": "指标能当场复算，且和简历上的数字一致",
    },
    "desc_purchase": {
        "label": "购买事实表结构",
        "kind": "hive",
        "sql": "DESC gmall.dwd_purchase_fact_di",
        "expect": "约 10 秒（beeline 冷启动）",
        "timeout": 120,
        "proof": "表是真建的，注释也在",
    },
    "partitions": {
        "label": "ODS 分区清单",
        "kind": "hive",
        "sql": "SHOW PARTITIONS gmall.ods_event_log_di",
        "expect": "约 10 秒 · 应为 31 行",
        "timeout": 120,
        "proof": "分区边界与数据源时间跨度一致",
    },
    "count_ods": {
        "label": "ODS 总行数",
        "kind": "hive",
        "sql": "SELECT COUNT(*) AS cnt FROM gmall.ods_event_log_di",
        "expect": "20~60 秒 · 应为 10630245",
        "timeout": 240,
        "proof": "最硬的一条：数据量能不能对上",
    },
}


def load_cache():
    try:
        with open(CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return {}


def save_cache(d):
    try:
        os.makedirs(DATA, exist_ok=True)
        with open(CACHE, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=1)
    except Exception:  # noqa: BLE001
        pass


def run_live(key):
    spec = LIVE.get(key)
    if not spec:
        return {"ok": False, "reason": "未知查询：%s" % key}

    h = health()
    if not h.get("ok"):
        return {"ok": False, "reason": h.get("reason", "集群未连接"), "key": key}

    if spec["kind"] == "hive":
        b64 = base64.b64encode(spec["sql"].encode("utf-8")).decode("ascii")
        script = (
            "export JAVA_HOME=%s\n"
            "export PATH=$JAVA_HOME/bin:%s:$PATH\n"
            "echo '%s' | base64 -d > /tmp/_demo_q.sql\n"
            "%s -u %s --silent=true --outputformat=tsv2 -f /tmp/_demo_q.sql 2>&1\n"
        ) % (JAVA_HOME, HADOOP, b64, BEELINE, HIVE_URL)
    else:
        script = "export JAVA_HOME=%s\nexport PATH=$JAVA_HOME/bin:%s:$PATH\n%s\n" % (
            JAVA_HOME, HADOOP, spec["cmd"])

    rc, out, el = ssh_script(script, timeout=spec["timeout"])

    lines = [ln.rstrip() for ln in out.splitlines() if ln.strip()]
    body = [ln for ln in lines if not NOISE.match(ln.strip())]
    text = "\n".join(body[-60:]).strip()

    ok = rc == 0 and bool(text) and "TIMEOUT" not in text
    reason = ""
    if not ok:
        if "TIMEOUT" in text:
            reason = "执行超时（%ss）—— 集群可能负载过高，或 HiveServer2 未就绪" % spec["timeout"]
        elif "Could not connect" in text or "Connection refused" in text:
            reason = "HiveServer2 未就绪（10000 端口未监听）"
        else:
            reason = text[-200:] or "无输出（rc=%s）" % rc

    c = load_cache()
    c[key] = {"elapsed": round(el, 1), "at": time.strftime("%Y-%m-%d %H:%M"), "ok": ok}
    save_cache(c)

    return {
        "ok": ok,
        "key": key,
        "label": spec["label"],
        "sql": spec.get("sql") or spec.get("cmd"),
        "proof": spec["proof"],
        "elapsed": round(el, 1),
        "out": text,
        "reason": reason,
    }


# ══════════════════════════════════════════════════════════════
#  虚拟机启动
# ══════════════════════════════════════════════════════════════
BOOT = {"running": False, "done": False, "steps": [], "log": [], "started": 0, "error": ""}
BOOT_LOCK = threading.Lock()


def vmrun_list():
    try:
        p = subprocess.run([VMRUN, "-T", "ws", "list"],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=25)
        out = p.stdout.decode("utf-8", "replace")
        running = [ln.strip() for ln in out.splitlines()[1:] if ln.strip()]
        return running
    except FileNotFoundError:
        return []
    except Exception:  # noqa: BLE001
        return []


def boot_worker():
    def log(msg):
        BOOT["log"].append("%s  %s" % (time.strftime("%H:%M:%S"), msg))
        BOOT["log"] = BOOT["log"][-40:]

    try:
        running = vmrun_list()
        log("vmrun 报告运行中的虚拟机：%d 台" % len(running))

        for i, vm in enumerate(VMS):
            BOOT["steps"][i]["state"] = "doing"
            if any(vm["vmx"].lower() in r.lower().replace("/", "\\") for r in running):
                BOOT["steps"][i]["state"] = "skip"
                log("%s 已在运行，跳过" % vm["name"])
                continue
            log("启动 %s（%d MB）…" % (vm["name"], vm["mem"]))
            t0 = time.time()
            try:
                p = subprocess.run([VMRUN, "-T", "ws", "start", vm["vmx"], "nogui"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
                err = p.stderr.decode("utf-8", "replace").strip()
                if p.returncode != 0:
                    BOOT["steps"][i]["state"] = "fail"
                    BOOT["steps"][i]["msg"] = err[:120]
                    log("%s 启动失败：%s" % (vm["name"], err[:120]))
                    continue
            except subprocess.TimeoutExpired:
                BOOT["steps"][i]["state"] = "fail"
                BOOT["steps"][i]["msg"] = "vmrun 超时"
                log("%s vmarun 超时" % vm["name"])
                continue
            BOOT["steps"][i]["state"] = "done"
            BOOT["steps"][i]["msg"] = "%.0fs" % (time.time() - t0)
            log("%s 已启动，耗时 %.0fs" % (vm["name"], time.time() - t0))

        log("等待 SSH 就绪（最多 180 秒）…")
        t0 = time.time()
        while time.time() - t0 < 180:
            h = health(force=True)
            if h.get("ok"):
                log("SSH 已就绪，端口 UP %d/%d" % (h["ports_up"], h["ports_total"]))
                break
            time.sleep(5)
        else:
            log("等待超时：SSH 仍未就绪")

        BOOT["done"] = True
    except Exception as e:  # noqa: BLE001
        BOOT["error"] = str(e)
        BOOT["done"] = True
        log("异常：%s" % e)
    finally:
        BOOT["running"] = False


def start_boot():
    with BOOT_LOCK:
        if BOOT["running"]:
            return False
        BOOT.update({
            "running": True, "done": False, "error": "", "started": time.time(),
            "log": [],
            "steps": [{"name": v["name"], "state": "wait", "msg": ""} for v in VMS],
        })
    threading.Thread(target=boot_worker, daemon=True).start()
    return True


# ══════════════════════════════════════════════════════════════
#  HTTP
# ══════════════════════════════════════════════════════════════
MIME = {".html": "text/html; charset=utf-8", ".json": "application/json; charset=utf-8",
        ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8",
        ".png": "image/png", ".jpg": "image/jpeg", ".svg": "image/svg+xml",
        ".ico": "image/x-icon", ".woff2": "font/woff2"}


def read_file(path):
    with open(path, "rb") as f:
        return f.read()


class Handler(BaseHTTPRequestHandler):
    server_version = "gmall-demo/1.0"

    def send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_bytes(self, body, ctype):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        path = u.path
        qs = parse_qs(u.query)

        if path == "/api/facts":
            try:
                return self.send_json(json.loads(read_file(FACTS).decode("utf-8")))
            except Exception as e:  # noqa: BLE001
                return self.send_json({"error": "读取证据.json 失败: %s" % e}, 500)

        if path == "/api/health":
            return self.send_json(health(force=qs.get("force", ["0"])[0] == "1"))

        if path == "/api/live/spec":
            return self.send_json({"queries": {
                k: {"label": v["label"], "sql": v.get("sql") or v["cmd"],
                    "expect": v["expect"], "proof": v["proof"]}
                for k, v in LIVE.items()
            }})

        if path == "/api/live":
            key = (qs.get("k") or [""])[0]
            try:
                return self.send_json(run_live(key))
            except Exception as e:  # noqa: BLE001
                return self.send_json({"ok": False, "reason": "服务端异常: %s" % e}, 500)

        if path == "/api/live/cache":
            return self.send_json(load_cache())

        if path == "/api/vm":
            return self.send_json({"running": vmrun_list(), "total": len(VMS)})

        if path == "/api/boot":
            started = start_boot()
            return self.send_json({"started": started, "state": BOOT})

        if path == "/api/boot/status":
            return self.send_json(BOOT)

        if path == "/viz" or path.startswith("/viz/"):
            rel = path[5:].lstrip("/") or "dashboard_real.html"
            target = os.path.join(os.path.dirname(HERE), "可视化", rel)
            if os.path.isfile(target):
                ext = os.path.splitext(target)[1].lower()
                return self.send_bytes(read_file(target), MIME.get(ext, "application/octet-stream"))
            return self.send_json({"error": "未找到大屏文件: %s" % rel}, 404)

        if path == "/":
            path = "/index.html"

        target = os.path.join(HERE, path.lstrip("/"))
        if os.path.isfile(target):
            ext = os.path.splitext(target)[1].lower()
            return self.send_bytes(read_file(target), MIME.get(ext, "application/octet-stream"))

        self.send_json({"error": "404 %s" % path}, 404)

    def log_message(self, *a):
        pass


def main():
    args = [a for a in sys.argv[1:]]
    open_browser = "--open" in args
    ports = [int(a) for a in args if a.isdigit()]
    port = ports[0] if ports else 8766

    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print("=" * 58)
    print("  gmall 数仓 · 演示台")
    print("  http://127.0.0.1:%d/" % port)
    print("  集群: %s（实时区依赖它；离线区不需要）" % NODE)
    print("  停止: Ctrl+C")
    print("=" * 58)

    if open_browser:
        import webbrowser
        threading.Timer(1.2, lambda: webbrowser.open("http://127.0.0.1:%d/" % port)).start()

    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
