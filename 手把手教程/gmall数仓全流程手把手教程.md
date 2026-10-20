# gmall 电商数仓全流程手把手教程（从零到跑通）

> 本教程面向「拿到这套环境、想完整跑一遍」的人。
> 从登录集群 → 启动服务 → 建库建表 → 逐层跑批 → DolphinScheduler 编排 → 验证结果，一步一条命令，可直接照抄。
>
> 配套文档：
>
> - 《虚拟机集群使用指南.md》——日常运维、组件启停、端口速查（**本文不重复讲原理，只讲"怎么跑通"**）
> - 《DolphinScheduler全流程排障汇总.md》——跑的途中踩过的 15 个坑及解法（**遇到报错先翻它**）

***

## 目录

- [〇、开跑前必读](#〇开跑前必读)
- [一、环境与节点地图](#一环境与节点地图)
- [二、第 0 步：登录 lrh-1（全程唯一一次 ssh）](#二第-0-步登录-lrh-1全程唯一一次-ssh)
- [三、第 1 步：启动基础服务](#三第-1-步启动基础服务)
- [四、第 2 步：启动 DolphinScheduler](#四第-2-步启动-dolphinscheduler)
- [五、第 3 步：准备数据与脚本](#五第-3-步准备数据与脚本)
- [六、第 4 步：逐层跑数（路径 A：原生 Shell 一键跑批）](#六第-4-步逐层跑数路径-a原生-shell-一键跑批)
- [七、第 5 步：DolphinScheduler 可视化编排（路径 B）](#七第-5-步dolphinscheduler-可视化编排路径-b)
- [八、第 6 步：结果验证](#八第-6-步结果验证)
- [九、常见坑速查](#九常见坑速查)
- [十、收尾：停服务与省内存](#十收尾停服务与省内存)
- [附录：从 Windows 批量操作（自动化用）](#附录从-windows-批量操作自动化用)

***

## 〇、开跑前必读

### 0.1 操作视角：登录一次，之后都在虚拟机上敲

本教程的叙事流是：

```
Windows 终端  ──ssh 登录──▶  lrh-1（主节点）
                                │
                                ├─ 绝大多数操作：直接在 lrh-1 上敲命令
                                └─ 需要操作 lrh-2 / lrh-3 时：从 lrh-1 上 `ssh lrh-2` 跳过去
```

**只有第二章那一条命令是在 Windows 上执行的**，其余章节的命令全部是在 lrh-1 的 shell 里直接敲。
（附录里另给了一套"从 Windows 一键批量"的写法，那是给自动化/脚本用的，手工操作不必看。）

### 0.2 两条跑数路径，选一条

| 路径                        | 怎么做                                   | 适用场景             | 耗时          |
| ------------------------- | ------------------------------------- | ---------------- | ----------- |
| **路径 A：原生 Shell**         | 在 lrh-1 上执行 `run_pipeline.sh`，9 步串行跑完 | 只想快速验证数仓逻辑对不对    | 约 40 分钟     |
| **路径 B：DolphinScheduler** | Web UI 上建工作流、点"运行"，可视化看每个节点状态         | 要演示调度、看节点级进度、做编排 | 约 1 小时 10 分 |

两条路径**跑的是同一批 SQL 脚本**，区别只是"谁来驱动"。先跑通 A 再玩 B 最省心。

### 0.3 3 个必须记住的结论（能省你 80% 的排错时间）

1. **所有 Hive SQL 都跑在 lrh-1 上**，通过 `beeline -u jdbc:hive2://localhost:10000` 连本机 HiveServer2。lrh-2/lrh-3 上没有 Hive 客户端。
2. **多语句 SQL 一律写成文件再执行**（`beeline -f xxx.sql`），别在命令行里拼长 SQL —— 引号很容易被剥掉（见排障文档问题 12）。
3. **lrh-1 只有 3868MB 内存**（不是 4GB，实测 `free -m` 就是这个数）。全套服务起来后 `available` 只剩 300MB 左右，swap 会被用到 1.7GB。DolphinScheduler 堆必须锁死 512MB，别同时开 alert-server，内存不够会被 OOM 杀。

***

## 一、环境与节点地图

### 1.1 三台机器

| 主机名       | IP             | 角色                                                                        |
| --------- | -------------- | ------------------------------------------------------------------------- |
| **lrh-1** | 192.168.87.131 | NameNode + ResourceManager + HiveServer2 + ZK + MySQL + **DS Master/API** |
| **lrh-2** | 192.168.87.130 | DataNode + NodeManager + ZK + **DS Worker**                               |
| **lrh-3** | 192.168.87.132 | DataNode + NodeManager + ZK + **DS Worker**                               |

三台机器的 hosts 已互相解析，所以集群内部可以直接用主机名（`ssh lrh-2`、`ping lrh-3`）。

### 1.2 关键路径（写脚本时要用）

| 组件               | 路径                                                             |
| ---------------- | -------------------------------------------------------------- |
| JDK              | `/opt/jdk/jdk1.8.0_202`                                        |
| Hadoop           | `/opt/hadoop/hadoop-3.3.6`                                     |
| Hive             | `/opt/hive/apache-hive-3.1.3-bin`                              |
| 工作脚本目录           | `/home/lrh/workspace/old_chain/`                                         |
| DolphinScheduler | `/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin` |

### 1.3 端口 / 网页速查

| 服务                       | 端口    | 地址                                             |
| ------------------------ | ----- | ---------------------------------------------- |
| HDFS NameNode UI         | 9870  | <http://192.168.87.131:9870>                   |
| YARN ResourceManager UI  | 8088  | <http://192.168.87.131:8088>                   |
| HiveServer2 (JDBC)       | 10000 | `jdbc:hive2://192.168.87.131:10000`            |
| Hive Metastore           | 9083  | —                                              |
| ZooKeeper                | 2181  | —                                              |
| MySQL                    | 3306  | —                                              |
| **DolphinScheduler Web** | 12345 | <http://192.168.87.131:12345/dolphinscheduler> |

DS 登录账号：`admin` / `dolphinscheduler123`

### 1.4 登录后的现成环境（不用你配）

lrh-1 已经通过 `/etc/profile.d/` 配好了登录环境，**登录 shell 里直接可用**：

| 变量 / 命令 | 状态                                                                       |
| ------- | ------------------------------------------------------------------------ |
| `JAVA_HOME` | `/opt/jdk/jdk1.8.0_202`（已设）                                              |
| `HADOOP_HOME` | `/opt/hadoop/hadoop-3.3.6`（已设）                                          |
| `jps` / `hdfs` / `yarn` / `mysql` | 已加入 `PATH`，直接敲即可                                        |
| `sudo`  | 三台机器都免密                                                          |
| **`beeline`** | ⚠️ **不在 PATH**，要用全路径 `/opt/hive/apache-hive-3.1.3-bin/bin/beeline` |
| systemd 服务 | `zookeeper` 三台都有；`hadoop` / `mysql` / `hive-metastore` / `hive-server2` **只有 lrh-1 有**（详见 §3） |

***

## 二、第 0 步：登录 lrh-1（全程唯一一次 ssh）

### 2.1 在 Windows 上：先切到项目根目录

SSH 私钥在 `D:\数据开发环境\VMware\sshkeys\cluster_ed25519`（**在项目目录之外**——2026-10-20 已把私钥移出项目，避免误入 git 仓库），**必须带** **`-i`** **指定，否则报** **`Permission denied`**（问题 10）。

命令里请用**绝对路径**。若在别的目录用相对路径，会报下面这种错（示例里的路径是迁移前的旧位置，现已不存在）：

```
Warning: Identity file <旧目录>/db_ed25519 not accessible: No such file or directory.
lrh@192.168.87.131: Permission denied (publickey,password).
```

**这不是密钥坏了，是路径没找对。** 所以先切目录：

```powershell
cd "D:\数据开发"
```

> 不想切目录的话，就把 `-i` 换成绝对路径：
> `-i "D:\数据开发\VMware\sshkeys\db_ed25519"`

### 2.2 登录 lrh-1

```powershell
ssh -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 -o StrictHostKeyChecking=no lrh@192.168.87.131
```

看到这样的提示符就说明登录成功了：

```
Welcome to Ubuntu ...
Last login: ...
lrh@lrh-1:~$
```

> 这里**故意不加** `-n` 和 `-o BatchMode=yes`：那是给"执行完一条命令就退出"的自动化场景用的；手工操作要的是一个**能持续交互的会话**。

### 2.3 记住两个绝对路径（本文全部用绝对路径，不用变量）

本文所有命令都**直接写绝对路径**，不依赖任何 shell 变量，所以随便新开终端、重新登录都能照抄即用。你只需要记住这两个：

| 用途 | 绝对路径 |
| --- | --- |
| 连 Hive | `/opt/hive/apache-hive-3.1.3-bin/bin/beeline` |
| 启停 DS | `/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh` |

> ⚠️ **为什么不用 `export BEELINE=...` 这种变量**：一旦你在新终端里忘了 export，`$BEELINE` 会**静默展开成空字符串**，命令就变成 `-u jdbc:hive2://...`，报出极具误导性的：
>
> ```
> -u: command not found
> ```
>
> 看起来像 beeline 坏了，其实只是变量为空。所以本文一律用绝对路径，从根上避免这个问题。

<details>
<summary>可选：如果你实在嫌路径长，想让 <code>beeline</code> 全局可用（点开看，两个方案都已实测）</summary>

**⚠️ 先说一个错误做法：不要建软链接。**

```bash
# ❌ 这样是错的
sudo ln -s /opt/hive/apache-hive-3.1.3-bin/bin/beeline /usr/local/bin/beeline
```

`beeline` 脚本内部用 `bin=$(dirname "$0")` 推导自己的目录，软链接会让它算出 `/usr/local/bin`，然后报：

```
/usr/local/bin/beeline: line 21: /usr/local/bin/hive: No such file or directory
```

**正确方案 A：写个包装脚本**（推荐）

```bash
sudo tee /usr/local/bin/beeline >/dev/null <<'EOF'
#!/bin/bash
exec /opt/hive/apache-hive-3.1.3-bin/bin/beeline "$@"
EOF
sudo chmod +x /usr/local/bin/beeline
```

之后 `beeline -u jdbc:hive2://localhost:10000` 直接可用（实测通过）。

**正确方案 B：把 Hive 的 bin 加进 PATH**

```bash
echo 'export PATH=$PATH:/opt/hive/apache-hive-3.1.3-bin/bin' >> ~/.bashrc
source ~/.bashrc
```

（实测通过。通过 PATH 找到时 `$0` 是完整路径，所以不会踩上面软链接那个坑。）

本文正文仍按绝对路径写，这样即使不做这一步也能照抄。

</details>

### 2.4 提示符约定

下文所有命令都写成**在 lrh-1 上直接敲**的形式，不再套 ssh 外壳。例如：

```bash
jps -l                                # 在 lrh-1 上执行
ssh lrh-2 "bash -l -c 'jps -l'"       # 从 lrh-1 跳到 lrh-2 执行（集群内免密已通）
```

**第二章之后，除非特别说明，所有命令都在 lrh-1 上敲。**

### 2.5 ⚠️ 跳节点时为什么要套 `bash -l -c`

这是本教程最容易踩的一个坑，先说清楚：

| 写法                      | 是否加载 `/etc/profile.d/*.sh` | 结果                     |
| ----------------------- | ------------------------- | ---------------------- |
| `ssh lrh-2`（交互登录）       | ✅ 加载                      | `jps`、`hdfs`、`yarn` 都能用 |
| `ssh lrh-2 "jps -l"`（非交互） | ❌ **不加载**                 | 报 `jps: command not found` |
| `ssh lrh-2 "bash -l -c 'jps -l'"` | ✅ 加载                      | 正常输出                   |

原因：`JAVA_HOME`、`hadoop/bin` 这些都是在 `/etc/profile.d/java.sh`、`hadoop.sh` 里配的，**只有登录 shell 才会加载**。
`ssh host "命令"` 走的是非交互 shell，`PATH` 只有：

```
/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin
```

里面既没有 `jdk/bin` 也没有 `hadoop/bin`。

**所以规则是**：

- 跳到别的节点执行**任何依赖 PATH 的命令**（`jps`、`hdfs`、`yarn`…）→ 套一层 `bash -l -c '...'`
- 跳到别的节点执行**用绝对路径的命令**（`sudo ...`、`/opt/.../xxx.sh`）→ 不用套，直接写即可

> 另一种等效写法是直接写全路径，如 `ssh lrh-2 "/opt/jdk/jdk1.8.0_202/bin/jps -l"`，效果一样，但命令一多就啰嗦，本文统一用 `bash -l -c`。

***

## 三、第 1 步：启动基础服务

以下基础服务都是 **systemd 托管**，但**单元文件并不是三台都装了**，这一点很关键：

| systemd 服务名      | 装在哪台机器     | 说明                                                  |
| ---------------- | ---------- | --------------------------------------------------- |
| `zookeeper`      | **三台都有**   | ZooKeeper，每台各自独立启动                                   |
| `hadoop`         | **只有 lrh-1** | 执行 `start-all.sh`，**会 ssh 到 lrh-2/lrh-3 把 DataNode/NodeManager 一起拉起来** |
| `mysql`          | **只有 lrh-1** | MySQL（DS 元数据库），发行版自带                                 |
| `hive-metastore` | **只有 lrh-1** | Hive 元数据服务，`Requires=mysql.service`                  |
| `hive-server2`   | **只有 lrh-1** | HiveServer2（注意是 `server2`，不是 `server2-server`），`Requires=hive-metastore.service` |

> ⚠️ **高频踩坑点**：`hadoop.service` **只在 lrh-1 上存在**。
> 如果你在 lrh-2 或 lrh-3 上执行 `sudo systemctl start hadoop`，会报：
>
> ```
> Failed to start hadoop.service: Unit hadoop.service not found.
> ```
>
> **这不是环境坏了**，是设计如此——从节点的 DataNode/NodeManager 由 lrh-1 统一拉起。
> 因为 lrh-1 的 hadoop.service 内容是 `ExecStart=/opt/hadoop/hadoop-3.3.6/sbin/start-all.sh`，而 `start-all.sh` 会读 `etc/hadoop/workers` 文件（内容为 `lrh-1`/`lrh-2`/`lrh-3`），逐个 ssh 过去启动守护进程。

### 3.1 启动 zookeeper（三台各自）

```bash
# lrh-1 本机
sudo systemctl start zookeeper

# lrh-2、lrh-3
ssh lrh-2 "sudo systemctl start zookeeper"
ssh lrh-3 "sudo systemctl start zookeeper"
```

### 3.2 启动 hadoop（只在 lrh-1 执行这一条）

```bash
sudo systemctl start hadoop
```

这一条命令会把**整个集群**拉起来：lrh-1 的 NameNode/SecondaryNameNode/DataNode/ResourceManager/NodeManager，以及 lrh-2、lrh-3 的 DataNode/NodeManager。

> **ZooKeeper 要先起**。hadoop.service 里写了 `After=zookeeper.service`，所以上面两条命令的顺序不能反。
> 如果 lrh-2/lrh-3 上有 DataNode/NodeManager 没起来（开机时网络未就绪的竞态问题），再执行一次 `sudo systemctl start hadoop` 即可补齐。

### 3.3 启动 mysql / hive（只在 lrh-1）

```bash
sudo systemctl start mysql hive-metastore hive-server2
```

> Hive 启动较慢（约 30\~60 秒），`hive-server2` 起来后才能连 10000 端口。
> `hive-metastore` 依赖 mysql（`Requires=mysql.service`），`hive-server2` 依赖 metastore，所以这三条写成一条命令让 systemd 自己排序即可。

### 3.4 体检：确认都起来了

```bash
jps -l
ss -ltn | grep -E '9870|8088|10000|9083|2181|3306'
```

**lrh-1 正常应有**：`NameNode`、`SecondaryNameNode`、`DataNode`、`ResourceManager`、`NodeManager`、`QuorumPeerMain`、`RunJar`×2（metastore + hiveserver2）。

**端口应全部 LISTEN**：9870 / 8088 / 10000 / 9083 / 2181 / 3306。

顺手确认另外两台：

```bash
ssh lrh-2 "bash -l -c 'jps -l'"
ssh lrh-3 "bash -l -c 'jps -l'"
```

应各看到 `DataNode`、`NodeManager`、`QuorumPeerMain`。

> ⚠️ **别漏了 `bash -l -c`**。写成 `ssh lrh-2 "jps -l"` 会报 `bash: line 1: jps: command not found`，因为非交互 ssh 不加载 `/etc/profile.d/`（原因见 §2.5）。

### 3.5 验证 Hive 连通（关键一步）

```bash
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -e "show databases;"
```

看到 `default` 和 `gmall` 两个库即 OK（首次跑还没有 `gmall`，只有 `default` 也正常）。

> 💡 这里正是把命令写在 lrh-1 上的好处：`beeline -e "show databases;"` 的引号在本地 shell 里是完整的，不会像"从 Windows 经 ssh 传进去"那样被层层剥离。

### 3.6 ⚠️ 等 30 秒再启 DS（很重要）

`sudo systemctl start hadoop` 会 ssh 到三台机器拉起 DataNode/NodeManager，这个过程 CPU/内存/IO 负载很高，大约持续 20\~30 秒。

**如果这时候立刻启 DolphinScheduler 的 api-server，它的 ZooKeeper 连接会超时，api-server 直接启动失败**（实测踩到过，日志里是 `zookeeper connect timeout: lrh-1:2181,lrh-2:2181,lrh-3:2181`，DS Web 打不开）。

所以这里先等一下，顺便确认 ZK 真的就绪了：

```bash
sleep 30
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
timeout 25 /opt/zookeeper/apache-zookeeper-3.8.7-bin/bin/zkCli.sh -server lrh-1:2181 ls /dolphinscheduler
```

输出里能列出 `nodes` 就说明 ZK 正常，可以往下走了。实测输出（原样照抄）是：

```
WATCHER::

WatchedEvent state:SyncConnected type:None path:null
[nodes]
2026-09-23 21:58:32,744 [myid:] - INFO  [main:o.a.z.u.ServiceUtils@45] - Exiting JVM with code 0
```

> **只看方括号那行 `[nodes]` 即可**，其余全是 zkCli 自己的日志，不是报错：
>
> - `WATCHER::` / `WatchedEvent ...` —— 连接 ZK 成功的通知
> - `Exiting JVM with code 0` —— `ls` 执行完主动退出，`code 0` 就是成功
>
> （如果此刻 master 正在跑任务，这个列表里可能还会多出别的节点，不影响判断。）

***

## 四、第 2 步：启动 DolphinScheduler

DS **不在 systemd 里**，用自己的守护脚本启动。因为登录 shell 已带 `JAVA_HOME`，直接敲即可。

> 下文统一用全路径 `/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh`，**不定义变量**（理由见 §2.3）。

### 4.1 lrh-1 启动 api-server + master-server

```bash
/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh start api-server
sleep 20
/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh start master-server
```

> **不要启 alert-server**，省内存。
> **启动顺序**：先 api，后 master（api 起来会占 12345 端口）。
> **`sleep 20` 不能省**：api-server 是 Spring Boot 应用，要 15\~25 秒才把 12345 端口拉起来。

### 4.2 lrh-2 / lrh-3 启动 worker-server

```bash
ssh lrh-2 "/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh start worker-server"
ssh lrh-3 "/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh start worker-server"
```

> 💡 这两条**不需要**套 `bash -l -c`：守护脚本用的是绝对路径，且它内部会读 `bin/env/dolphinscheduler_env.sh`（其中已硬编码 `export JAVA_HOME=/opt/jdk/jdk1.8.0_202`），不依赖 ssh 传进去的环境变量。

### 4.3 验证 DS 起来了

先等 30 秒让服务注册到 ZooKeeper：

```bash
sleep 30
jps -l | grep -E 'MasterServer|ApiApplicationServer|AlertServer'
ss -ltn | grep 12345
```

**期望输出**（实测）：

```
8929 org.apache.dolphinscheduler.api.ApiApplicationServer
8505 org.apache.dolphinscheduler.server.master.MasterServer
LISTEN 0      50                           *:12345            *:*
```

再看两个 worker：

```bash
ssh lrh-2 "bash -l -c 'jps -l | grep WorkerServer'"
ssh lrh-3 "bash -l -c 'jps -l | grep WorkerServer'"
```

应各看到一行 `WorkerServer`。

**最后浏览器打开** <http://192.168.87.131:12345/dolphinscheduler> ，用 `admin` / `dolphinscheduler123` 登录。能进就说明 DS 全链路 OK。

在 lrh-1 上也可以用命令行确认 Web 真的活着（不需要浏览器）：

```bash
python3 -c "import urllib.request; print(urllib.request.urlopen('http://192.168.87.131:12345/dolphinscheduler', timeout=15).status)"
```

返回 `200` 即 OK。

### 4.4 如果只看到 MasterServer、没有 ApiApplicationServer

这是**最常见的一个失败**（实测踩到过）。症状：`jps` 里只有 `MasterServer`，`ss -ltn | grep 12345` 是空的，网页打不开。

**先看日志确认原因**：

```bash
tail -40 /home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/api-server/logs/api-server-lrh-1.out
```

如果看到这行：

```
RegistryException: zookeeper connect timeout: lrh-1:2181,lrh-2:2181,lrh-3:2181
```

说明是**启动太早**（ZK 还没稳，或 `start-all.sh` 正在拉满负载）→ 回 §3.6 等 ZK 就绪，然后**直接重启 api-server** 即可：

```bash
/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh start api-server
sleep 30
/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh status api-server
```

> 实测：第一次启动失败，**原样重试一次就成功了**，不需要改任何配置。

如果是被 OOM 杀掉（`free -m` 的 available 接近 0、swap 用满），那就先停掉 alert-server、或先别跑别的吃内存任务，再重启 api-server。

### 4.5 确认服务注册进 ZooKeeper（可选）

```bash
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
timeout 25 /opt/zookeeper/apache-zookeeper-3.8.7-bin/bin/zkCli.sh -server lrh-1:2181 ls /dolphinscheduler/nodes/worker
timeout 25 /opt/zookeeper/apache-zookeeper-3.8.7-bin/bin/zkCli.sh -server lrh-1:2181 ls /dolphinscheduler/nodes/master
```

**实测期望输出**（只看方括号那行；`WATCHER::` / `WatchedEvent ...` / `Exiting JVM` 都是 zkCli 的日志，不是报错）：

```
[192.168.87.130:1234, 192.168.87.132:1234]     ← 两个 worker
[192.168.87.131:5678]                          ← 一个 master
```

两个 worker 都在，说明 DS 集群编排就绪，可以进第 5 步。

***

## 五、第 3 步：准备数据与脚本

所有脚本都在 **lrh-1 的** **`/home/lrh/workspace/old_chain/`**，已经准备好，**不需要你重新造**。清单如下：

| 文件                      | 作用                                      |
| ----------------------- | --------------------------------------- |
| `generate_mock_data.py` | 生成模拟数据（用户行为/订单/商品）                      |
| `split_by_date.py`      | 把大 CSV 按天拆成 `mock_data/*_by_date/*.csv` |
| `gmall_ddl_day8.sql`    | 建 14 张表（ODS/DWD/DIM/DWS/ADS）            |
| `batch_load.sql`        | 把 `mock_data/` 的 CSV 灌进 ODS 层           |
| `clean_dwd.sql`         | ODS → DWD/DIM 清洗与维度补全                   |
| `etl_dws_user.sql`      | DWD → `dws_user_day`                    |
| `etl_dws_sku.sql`       | DWD → `dws_sku_day`                     |
| `etl_dws_order.sql`     | DWD → `dws_order_day`                   |
| `etl_ads_opt.sql`       | DWS/DWD → 4 张 ADS 指标表（优化版）              |
| `verify_ads.sql`        | 打印 ADS 结果做人工核对                          |
| `run_pipeline.sh`       | 路径 A 的一键跑批入口（9 步串行）                     |
| `log/`                  | 每步的日志输出                                 |
| `mock_data/`            | 造好的 CSV 数据                              |

先看一眼目录，确认文件都在：

```bash
cd /home/lrh/workspace/old_chain
ls -1
```

### 5.1 数仓分层与表清单（14 张）

| 层       | 表                                                                           | 说明                |
| ------- | --------------------------------------------------------------------------- | ----------------- |
| **ODS** | `ods_user_log_di` / `ods_order_info_di` / `ods_sku_info_di`                 | 原始数据，按天分区，`\t` 分隔 |
| **DWD** | `dwd_user_behavior_di` / `dwd_order_fact_di`                                | 明细事实，按天分区         |
| **DIM** | `dim_user` / `dim_sku`                                                      | 用户/商品维度           |
| **DWS** | `dws_user_day` / `dws_sku_day` / `dws_order_day`                            | 主题日汇总，按天分区        |
| **ADS** | `ads_dau` / `ads_conversion_funnel` / `ads_repurchase_rate` / `ads_top_sku` | 应用层指标             |

数据日期范围：**2026-08-20 \~ 2026-08-24（5 天）**。

> 如需重新造数据（一般不用）：
>
> ```bash
> cd /home/lrh/workspace/old_chain
> python3 generate_mock_data.py
> python3 split_by_date.py
> ```

***

## 六、第 4 步：逐层跑数（路径 A：原生 Shell 一键跑批）

### 6.1 先看一眼跑批脚本

`run_pipeline.sh` 的逻辑就是**按顺序把 9 个 SQL 文件丢给 beeline**，任何一步日志里出现 `Error:` 或 `FAILED:` 就中断退出。

```bash
cat /home/lrh/workspace/old_chain/run_pipeline.sh
```

核心骨架（节选，与真实文件一致）：

```bash
#!/bin/bash
export JAVA_HOME=/opt/jdk/jdk1.8.0_202
export PATH=$JAVA_HOME/bin:/opt/hive/apache-hive-3.1.3-bin/bin:/opt/hadoop/hadoop-3.3.6/bin:/opt/hadoop/hadoop-3.3.6/sbin:/usr/bin:/bin
export HADOOP_HOME=/opt/hadoop/hadoop-3.3.6
export HADOOP_CONF_DIR=/opt/hadoop/hadoop-3.3.6/etc/hadoop
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline
URL=jdbc:hive2://localhost:10000
cd /home/lrh/workspace/old_chain
LOG=/home/lrh/workspace/old_chain/log

step() { echo "===== $(date '+%F %T') $1 =====" >> $LOG/pipeline.out; }
chk()  { if grep -qE 'Error:|FAILED:' "$1"; then echo "FAILED at $2 -> $1" >> $LOG/pipeline.out; exit 1; fi; }

step create_db
$BEELINE -u $URL -e "CREATE DATABASE IF NOT EXISTS gmall;" > $LOG/create_db.log 2>&1
chk $LOG/create_db.log create_db

step ddl
$BEELINE -u $URL -f gmall_ddl_day8.sql > $LOG/ddl.log 2>&1
chk $LOG/ddl.log ddl

# ……中间各步（batch_load / clean_dwd / dws_user / dws_sku / dws_order / ads_opt）结构完全相同……

step verify
$BEELINE -u $URL -f verify_ads.sql > $LOG/verify_ads.log 2>&1
chk $LOG/verify_ads.log verify

step ALL_DONE
```

**9 个步骤**：`create_db → ddl → batch_load → clean_dwd → dws_user → dws_sku → dws_order → ads_opt → verify`

> 注意 `step()` 是往 `log/pipeline.out` **追加**写一行 `===== 时间 步骤名 =====`，最后一步 `step ALL_DONE` 写的也是这种带时间戳的格式（不是裸的 `ALL_DONE`），所以 §6.3 里要认「时间戳 + ALL_DONE」。

### 6.2 后台启动跑批

```bash
cd /home/lrh/workspace/old_chain
mv -f log/pipeline.out log/pipeline.out.bak 2>/dev/null    # 先归档旧日志，否则会看到上次的 ALL_DONE
setsid nohup bash run_pipeline.sh </dev/null >/dev/null 2>&1 &
echo "started pid=$!"
```

> **为什么要 `setsid nohup ... &`**：整条链路要跑 40 分钟，如果直接前台跑，你的 ssh 会话一断（网络抖动、关窗口）作业就被 SIGHUP 杀掉了。`setsid` 让它脱离终端独立运行，断线也不受影响。

> ⚠️ **第一行 `mv` 不能省**。`run_pipeline.sh` 是用 `>>` **追加**写 `log/pipeline.out` 的，环境里已经存在上次跑完留下的 `ALL_DONE`。不归档的话，你一执行 `tail` 就会立刻看到 `ALL_DONE`，误以为"这么快就跑完了"。

### 6.3 观察进度

看总进度文件（每完成一步追加一行 `===== 时间 步骤名 =====`）：

```bash
tail -20 /home/lrh/workspace/old_chain/log/pipeline.out
```

跑的过程中可以反复执行这条命令查看。看到 **本次运行时间戳** 的 `ALL_DONE` 就全跑完了（约 40 分钟）。

**实测一次完整跑批的进度长这样**：

```
===== 2026-09-17 13:13:57 dws_user =====
===== 2026-09-17 13:17:44 dws_sku =====
===== 2026-09-17 13:21:27 dws_order =====
===== 2026-09-17 13:25:12 ads_opt =====
===== 2026-09-17 13:42:59 verify =====
===== 2026-09-17 13:45:05 ALL_DONE =====
```

> 注意 `ads_opt` 那一行之后要等约 18 分钟才出现 `verify`——它内部要逐条提交约 21 条 MapReduce 语句，**这是最慢的一步，不是卡住了**。

想实时盯着看：

```bash
tail -f /home/lrh/workspace/old_chain/log/pipeline.out     # Ctrl+C 退出
```

中途某步失败会看到 `FAILED at xxx`，这时去看对应 log：

```bash
tail -50 /home/lrh/workspace/old_chain/log/<失败步骤>.log
```

### 6.4 也可以自己单步跑（推荐新手先这样走一遍）

想手工逐层验证，就在 lrh-1 上一条条来，每跑完一步都可以查一下结果：

```bash
cd /home/lrh/workspace/old_chain

/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -e "CREATE DATABASE IF NOT EXISTS gmall;"
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f gmall_ddl_day8.sql      # 建 14 张表
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f batch_load.sql          # ODS 灌数
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f clean_dwd.sql           # ODS→DWD/DIM
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f etl_dws_user.sql        # DWS 用户
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f etl_dws_sku.sql         # DWS 商品
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f etl_dws_order.sql       # DWS 订单
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f etl_ads_opt.sql         # ADS 指标（最慢，约 18 分钟）
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f verify_ads.sql          # 结果打印
```

单步跑时也可以另开一个终端，用 YARN 页面 <http://192.168.87.131:8088> 看作业提交情况。

> **性能提示**：所有 ETL 脚本开头都有一组小集群内存 `SET`（map/reduce 各 1024MB、`-Xmx768m`、`bytes.per.reducer=128000000`）。
> **不要去掉**，也**不要开本地模式**（`hive.exec.mode.local.auto=false`）——本地模式对 JOIN+聚合会偶发 `return code 2`（见 `etl_ads_opt.sql` 顶部注释）。

***

## 七、第 5 步：DolphinScheduler 可视化编排（路径 B）

路径 B 是**把上面 9 条 beeline 命令，变成 DS 上的 9 个 SHELL 节点串行工作流**。
这一章主要在**浏览器**里操作，只有少量查状态命令需要在 lrh-1 上敲。

### 7.1 已有工作流定义（当前环境实际状态）

| 项目      | 工作流名                     | code             | 任务数 | 状态           |
| ------- | ------------------------ | ---------------- | --- | ------------ |
| `gmall` | `gmall_dw_pipeline`      | `23110365900704` | 9   | 已下线（offline） |
| `gmall` | `gmall_dw_pipeline_tail` | `23123283707168` | 5   | 已上线（online）  |

- **全流程** `gmall_dw_pipeline`（9 节点）：`create_db → ddl → ods_load → dwd_clean → dws_user → dws_sku → dws_order → ads_opt → verify`
- **尾段** `gmall_dw_pipeline_tail`（5 节点，用于重跑后半段）：`dws_user → dws_sku → dws_order → ads_opt → verify`

> 为什么要拆一个"尾段"工作流？整条链路里 `ads_opt` 最慢（约 40 分钟），如果前面都跑完了、只想重跑后半段，用尾段工作流能省掉前面的时间。

> ⚠️ **只有「已上线」的工作流才能运行**。列表页里状态是「下线」的那行，**运行按钮是灰的**，点不动——先点该行的 **上线**，再点 **运行**。
> 当前环境里 `gmall_dw_pipeline` 正好是**下线**状态，想跑全流程要先点它那一行的 **上线**。

### 7.2 新建一个工作流（从零操作一遍）

1. 浏览器打开 <http://192.168.87.131:12345/dolphinscheduler> ，登录 `admin` / `dolphinscheduler123`
2. 顶部 **项目管理** → 找到（或新建）项目 `gmall`
3. 左侧 **工作流定义** → **创建工作流**
4. 从左侧工具栏拖入 **SHELL** 节点，连成一条链（每个节点右侧的"+"拖到下一个节点）
5. 双击每个 SHELL 节点，填写节点名与脚本内容（见 7.3）
6. 右上角 **保存** → 回到列表 **上线** → 点 **运行**

### 7.3 每个 SHELL 节点的脚本模板

所有节点都是同一套模板，只换 `-f` 后面的 SQL 文件名：

```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH; /opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -f /home/lrh/workspace/old_chain/gmall_ddl_day8.sql </dev/null'
```

> ⚠️ **结尾的 `</dev/null` 不能省**。beeline 是基于 jline 的交互式程序，当它没有控制终端又没重定向 stdin 时，会被 `SIGTTOU` 信号冻结——`ps` 里进程状态是 `Tl+`，DS 任务会一直显示「运行中」但**永远不提交任何作业**，日志里也看不到报错，非常难查。重定向 `</dev/null` 就避开了这个问题。
> （这正是当前环境里 9 个节点实际使用的写法，已实测跑通。）

各节点对应关系：

| 节点名（建议）     | `-f` 的 SQL 文件                                         |
| ----------- | ----------------------------------------------------- |
| `create_db` | 不用 `-f`，改成 `-e`（注意 DS 里引号要转义，见下方示例） |
| `ddl`       | `gmall_ddl_day8.sql`                                  |
| `ods_load`  | `batch_load.sql`                                      |
| `dwd_clean` | `clean_dwd.sql`                                       |
| `dws_user`  | `etl_dws_user.sql`                                    |
| `dws_sku`   | `etl_dws_sku.sql`                                     |
| `dws_order` | `etl_dws_order.sql`                                   |
| `ads_opt`   | `etl_ads_opt.sql`                                     |
| `verify`    | `verify_ads.sql`                                      |

`create_db` 节点因为只有一条语句，用 `-e` 内联（**DS 的 rawScript 里内层双引号要写成 `\"`**，这是环境里实际在用的写法）：

```bash
ssh -o StrictHostKeyChecking=no -n lrh-1 'export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=$JAVA_HOME/bin:$PATH; /opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 -e \"CREATE DATABASE IF NOT EXISTS gmall;\" </dev/null'
```

> **⚠️ 注意区分两种"ssh"**：
>
> - **本章这个 `ssh ... lrh-1` 是 DS 任务脚本内部的行为**，不是让你手工敲的。因为 DS 的 worker 跑在 lrh-2/lrh-3 上，而那两台机器**没有 Hive 客户端**，所以任务要先跳回 lrh-1 再执行 beeline。这是设计如此，不要改。
> - 手工操作时（第二～六章、第八～十章）**不需要**在命令外面套 `ssh ... lrh-1`，因为你本来就在 lrh-1 上。
>
> 另外两条经验：
>
> - `export PATH=$JAVA_HOME/bin:$PATH` 里的 `$PATH` 在 DS 任务环境里是能拿到的；但为稳妥也可以像 `run_pipeline.sh` 一样写全绝对路径。
> - **不要**在 rawScript 里写多层嵌套引号或 `$(...)`，DS 会原样透传，容易出问题（问题 7）。

### 7.4 触发与观察

- **列表页**：工作流定义 → 点某行的 **运行**（闪电图标）
- **实例页**：**工作流实例** / **任务实例** 里能看到每个节点的状态、耗时、日志
- **状态色**：绿色 = 成功，红色 = 失败，蓝色 = 运行中
- 点进"任务实例"可以 **查看日志**，等价于看 `/home/lrh/workspace/old_chain/log/*.log`

**也可以在 lrh-1 上用命令行看进度**（不依赖网页）。DS 的元数据都在 MySQL 里：

```bash
sudo mysql dolphinscheduler -t -e "select id,name,state,start_time,end_time from t_ds_process_instance order by id desc limit 5;"
```

`state` 含义：`1`=运行中、`6`=失败、`7`=成功。

> 💡 这条 SQL 带引号还有 `select ... from`，如果是从 Windows 经 ssh 传过去，PowerShell 会把它当成自己的语法去解析从而报错；**在 lrh-1 本地敲就完全没问题**——这也是本教程改成"登录后在本地操作"的收益之一。

YARN 侧看作业排队/执行情况：浏览器打开 <http://192.168.87.131:8088> ，或从 lrh-1 上看：

```bash
yarn application -list
```

> 输出第一行是 `INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at lrh-1/192.168.87.131:8032` 之类的日志，**不是报错**。真正看的是 `Total number of applications (...):0` 和下面的表格。

### 7.5 实测运行结果（供对照）

一次完整的尾段工作流（`gmall_dw_pipeline_tail`）实测耗时：

| 节点          | 耗时              |
| ----------- | --------------- |
| `dws_user`  | 10 分 03 秒       |
| `dws_sku`   | 9 分 18 秒        |
| `dws_order` | 7 分 55 秒        |
| `ads_opt`   | **39 分 54 秒**   |
| `verify`    | 4 分 30 秒        |
| **合计**      | **约 1 小时 11 分** |

> `ads_opt` 慢是正常的：它内部会向 YARN 提交约 21 条 MapReduce 语句，逐条跑。
> 判断"卡住"还是"正常推进"：去 YARN 8088 看是否有新 app 在提交，只要 app 后缀（`Stage-N`）在变，就是在正常推进。

> ⚠️ **同一批 SQL，走 DS 比走 Shell 慢不少**，这是实测现象，不是故障：`ads_opt` 走 Shell（§6.4）约 18 分钟，走 DS（上表）要约 40 分钟；整条链路也是同理。
> 原因是 DS 的任务在 lrh-2/lrh-3 上执行、再 ssh 回 lrh-1 提交，多了一层跳转，且节点调度、日志落盘都有额外开销。**急着验证逻辑就用路径 A，要演示编排就用路径 B。**

***

## 八、第 6 步：结果验证

### 8.1 先看行数（最快判断成功与否）

在 lrh-1 上，把 SQL 写进文件再执行（避免引号问题）：

```bash
cat > /tmp/check_rows.sql <<'EOF'
select 'dws_user' t, count(*) c from gmall.dws_user_day
union all select 'dws_sku', count(*) from gmall.dws_sku_day
union all select 'dws_order', count(*) from gmall.dws_order_day
union all select 'ads_dau', count(*) from gmall.ads_dau
union all select 'ads_funnel', count(*) from gmall.ads_conversion_funnel
union all select 'ads_repurchase', count(*) from gmall.ads_repurchase_rate
union all select 'ads_top_sku', count(*) from gmall.ads_top_sku;
EOF

/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -f /tmp/check_rows.sql
```

**参考答案（必须一致）**：

> 说明：这条 SQL 用了 `union all` 但没写 `order by`，所以**行的先后顺序每次都可能不同**，只看「表名 → 行数」的对应关系即可，不要纠结顺序。

| 表                       | 行数       | 推算              |
| ----------------------- | -------- | --------------- |
| `dws_user_day`          | **5000** | 5 天 × 1000 用户/天 |
| `dws_sku_day`           | **2500** | 5 天 × 500 SKU/天 |
| `dws_order_day`         | **5**    | 每天 1 行          |
| `ads_dau`               | **5**    | 每天 1 行          |
| `ads_conversion_funnel` | **5**    | 每天 1 行          |
| `ads_repurchase_rate`   | **5**    | 每天 1 行          |
| `ads_top_sku`           | **2500** | 5 天 × 500 排名    |

### 8.2 再看指标值（对照真实数据）

同样写文件执行：

```bash
cat > /tmp/check_ads.sql <<'EOF'
select 'dau' as tbl, dt, cast(dau as string) as v1, cast(new_dau as string) as v2 from gmall.ads_dau
union all select 'funnel', dt, concat(cast(view_uv as string),'/',cast(cart_uv as string),'/',cast(order_uv as string),'/',cast(pay_uv as string)), '' from gmall.ads_conversion_funnel
union all select 'repurchase', dt, cast(repurchase_rate as string), '' from gmall.ads_repurchase_rate
union all select 'top_sku', dt, concat(sku_name,' gmv=',cast(gmv as string)), cast(rank_num as string) from gmall.ads_top_sku where rank_num <= 3;
EOF

/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -f /tmp/check_ads.sql
```

**实测参考答案（本环境真实产出）**：

**DAU**（`ads_dau`）—— 每天 1000，`new_dau` 均为 0：

| dt         | dau  | new\_dau |
| ---------- | ---- | -------- |
| 2026-08-20 | 1000 | 0        |
| 2026-08-21 | 1000 | 0        |
| 2026-08-22 | 1000 | 0        |
| 2026-08-23 | 1000 | 0        |
| 2026-08-24 | 1000 | 0        |

**转化漏斗**（`ads_conversion_funnel`，格式 `浏览/加购/下单/支付`）：

| dt         | view/cart/order/pay    |
| ---------- | ---------------------- |
| 2026-08-20 | 1000 / 963 / 851 / 650 |
| 2026-08-21 | 1000 / 945 / 872 / 632 |
| 2026-08-22 | 1000 / 956 / 865 / 624 |
| 2026-08-23 | 1000 / 946 / 866 / 628 |
| 2026-08-24 | 1000 / 957 / 864 / 631 |

**复购率**（`ads_repurchase_rate`）：

| dt         | repurchase\_rate |
| ---------- | ---------------- |
| 2026-08-20 | 0.999            |
| 2026-08-21 | 1.0              |
| 2026-08-22 | 1.0              |
| 2026-08-23 | 1.0              |
| 2026-08-24 | 1.0              |

**热销 TopN**（`ads_top_sku` 每天 TOP3 示例）：

| dt         | rank | sku       | gmv          |
| ---------- | ---- | --------- | ------------ |
| 2026-08-20 | 1    | 一加 手机 3代  | 4,522,616.79 |
| 2026-08-20 | 2    | 荣耀 配件 15代 | 4,388,184.84 |
| 2026-08-24 | 1    | 三星 电脑 15代 | 4,655,706.29 |

> 表中数值与上表一致即说明链路跑通、数据正确。

**或者直接进 beeline 交互模式，想看什么查什么**（推荐日常使用）：

```bash
/opt/hive/apache-hive-3.1.3-bin/bin/beeline -u jdbc:hive2://localhost:10000
```

进去之后：

```sql
use gmall;
select * from ads_dau order by dt;
select * from ads_conversion_funnel order by dt;
!q
```

> ⚠️ 用 `DESCRIBE` 看表结构时会看到中文注释是 `??`，这是 metastore 字符集的老问题，**不影响数据**，不用管。
> ⚠️ 看到 `Live datanodes(2)`、`mappers:0` 之类的信息**不是报错**，是正常日志。

### 8.3 想看底层文件（可选）

```bash
hdfs dfs -ls -R /user/hive/warehouse/gmall.db | head -50
```

***

## 九、常见坑速查

按"现象 → 一句话解法"列出，详细分析见《DolphinScheduler全流程排障汇总.md》。

| #  | 现象                              | 一句话解法                                                    |
| -- | ------------------------------- | -------------------------------------------------------- |
| 1  | DS master 日志暴涨 / 事件死循环 NPE      | 停 master → 清 `t_ds_process_instance` 等实例表 → 截断日志 → 重启    |
| 2  | REST 停不掉/删不掉工作流实例（50006/10163）  | 绕过 REST，直接删 `t_ds_*` 表里的实例记录                             |
| 3  | 网页打不开 / DS 服务全 STOP             | lrh-1 内存被 OOM 杀，停 alert-server + 重启 api-server           |
| 4  | beeline 进程残留、作业并发异常             | `kill -9` 清掉 stale beeline                               |
| 5  | YARN 作业长时间 ACCEPTED             | 内存小在排队，属正常；别急着 kill                                      |
| 6  | 观察脚本自动退出说 timeout               | 走 DS 整条链路要 70 分钟以上（`ads_opt` 一步就 40 分钟），deadline 要设 ≥5400 秒 |
| 7  | 脚本里的 SQL 报语法错误                  | SQL 写文件 → `beeline -f`，别在命令行里拼                            |
| 8  | `beeline: command not found`    | beeline 不在 PATH（连登录 shell 也没有），用 `/opt/hive/apache-hive-3.1.3-bin/bin/beeline` |
| 9  | 本机 12345 隧道打不开                  | 别开隧道，直连 `192.168.87.131:12345`                           |
| 10 | `Permission denied (publickey)` | Windows 侧 ssh 要带 `-i D:\数据开发环境\VMware\sshkeys\cluster_ed25519`            |
| 11 | 虚拟机里 `curl: command not found`  | 最小化安装没 curl，改用 `ss -ltn` / `yarn application -list`       |
| 12 | beeline 内联 SQL 引号被吞             | 同 #7，改成 `-f` 文件方式                                        |
| 13 | 用 PowerShell 调 DS 接口登录成功仍 401    | `WebRequestSession` 不传 Cookie，改用 Python `http.cookiejar` |
| 14 | 新建工作流指定 code 不生效                | DS 会自己生成 code，创建后**回读**真实 code                           |
| 15 | lrh-3 短暂连不上                     | 重启/内存窗口期，等一会儿就恢复                                         |
| 16 | 在 lrh-2/lrh-3 上 `systemctl start hadoop` 报 `Unit hadoop.service not found` | **正常现象**：hadoop.service 只在 lrh-1 有；在 lrh-1 上执行即可拉起全集群（见 §3.2） |
| 17 | `ssh lrh-2 "jps -l"` 报 `jps: command not found` | 非交互 ssh 不加载 `/etc/profile.d/`；改成 `ssh lrh-2 "bash -l -c 'jps -l'"`（见 §2.5） |
| 18 | `$BEELINE -u ...` 报 `-u: command not found` | 变量为空（新终端忘了 export）。**本文已改用绝对路径**；若你自定义过变量，先 `echo $BEELINE` 确认非空 |
| 19 | DS 只有 MasterServer，没有 ApiApplicationServer，12345 不通 | api-server 启动过早，ZK 连接超时；等 ZK 就绪后**原样重启一次**即可（见 §4.4） |
| 20 | 刚启动 `run_pipeline.sh` 就看到 `ALL_DONE` | `log/pipeline.out` 是追加写入，那是**上次**的旧记录；启动前先 `mv log/pipeline.out log/pipeline.out.bak`（见 §6.2） |
| 21 | DS 任务一直「运行中」，但 YARN 8088 里没有任何 app 提交 | rawScript 里 beeline 少了 `</dev/null`，被 `SIGTTOU` 冻结（`ps` 状态 `Tl+`）；补上 stdin 重定向（见 §7.3） |
| 22 | 工作流的「运行」按钮是灰的、点不动 | 该工作流处于「下线」状态，先点「上线」再运行（见 §7.1） |

> 说明：#7 / #8 / #10 / #12 / #13 都是**从 Windows 侧用脚本/ssh 批量操作时**才会遇到的坑。按本教程"登录 lrh-1 后本地操作"的方式走，这些基本不会碰到。

**排障顺序（照这个走）**：

1. 端口在不在：`ss -ltn | grep -E '9870|8088|10000|2181|12345'`
2. 进程在不在：`jps -l`
3. DS 网页能不能开
4. 看 `log/pipeline.out` 卡在哪一步
5. 看那一步的 `log/*.log` 有没有 `Error:`
6. 看 YARN 8088 有没有 app 在跑

***

## 十、收尾：停服务与省内存

### 10.1 停 DolphinScheduler

在 lrh-1 上：

```bash
/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh stop master-server
/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh stop api-server
```

停两个 worker：

```bash
ssh lrh-2 "/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh stop worker-server"
ssh lrh-3 "/home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh stop worker-server"
```

### 10.2 停基础服务

```bash
# 在 lrh-1：按依赖倒序停
sudo systemctl stop hive-server2 hive-metastore hadoop mysql zookeeper

# lrh-2、lrh-3：只有 zookeeper 可停（它们没有 hadoop.service）
ssh lrh-2 "sudo systemctl stop zookeeper"
ssh lrh-3 "sudo systemctl stop zookeeper"
```

> `sudo systemctl stop hadoop` 在 lrh-1 上执行会调 `stop-all.sh`，它同样读 `workers` 文件，**把 lrh-2/lrh-3 的 DataNode/NodeManager 一并停掉**，不用你手工去从节点停。
> 从节点上**不要**执行 `systemctl stop hadoop`，会报 `Unit hadoop.service not found`。

### 10.3 退出登录

```bash
exit
```

### 10.4 内存要点（重要）

- lrh-1 只有 **\~3.8GB** 内存，同时跑 HDFS+YARN+Hive+MySQL+ZK+DS，**必须抠着用**。
- **alert-server 建议长期停着**。
- DS 各服务的 JVM 堆已在配置文件里锁死 **512MB**，别去调大。
- 跑批时如果 lrh-1 明显卡，优先检查是不是有 stale 的 beeline 进程：

```bash
jps -l
ps -ef | grep -v grep | grep beeline
```

***

## 附录：从 Windows 批量操作（自动化用）

> **手工操作请忽略本附录**，按第二～十章走即可。
> 这里保留"从 Windows 一条命令打过去"的写法，适合做批处理、CI、或懒得分步操作时用。
>
> 前提：先 `cd "D:\数据开发"`（否则 `-i` 的相对路径找不到密钥）。

```powershell
# 0) 切到项目根目录
cd "D:\数据开发"

# 1) 基础服务
#    zookeeper：三台各自启
foreach ($ip in "192.168.87.131","192.168.87.130","192.168.87.132") {
  ssh -n -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 -o BatchMode=yes -o StrictHostKeyChecking=no lrh@$ip "sudo systemctl start zookeeper"
}
#    hadoop：只在 lrh-1 启（它会 ssh 拉起 lrh-2/lrh-3 的 DataNode/NodeManager）
ssh -n -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 -o BatchMode=yes -o StrictHostKeyChecking=no lrh@192.168.87.131 "sudo systemctl start hadoop mysql hive-metastore hive-server2"

# 2) DolphinScheduler
ssh -n -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 -o BatchMode=yes -o StrictHostKeyChecking=no lrh@192.168.87.131 "export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=/opt/jdk/jdk1.8.0_202/bin:/usr/bin:/bin; /home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh start api-server; sleep 20; /home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh start master-server"
foreach ($ip in "192.168.87.130","192.168.87.132") {
  ssh -n -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 -o BatchMode=yes -o StrictHostKeyChecking=no lrh@$ip "export JAVA_HOME=/opt/jdk/jdk1.8.0_202; export PATH=/opt/jdk/jdk1.8.0_202/bin:/usr/bin:/bin; /home/lrh/dolphinscheduler/apache-dolphinscheduler-3.1.8-bin/bin/dolphinscheduler-daemon.sh start worker-server"
}

# 3) 路径 A：一键跑批（约 40 分钟）
#    先归档旧日志，否则 tail 会立刻看到上次的 ALL_DONE
ssh -n -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 -o BatchMode=yes -o StrictHostKeyChecking=no lrh@192.168.87.131 "cd /home/lrh/workspace/old_chain && mv -f log/pipeline.out log/pipeline.out.bak 2>/dev/null; setsid nohup bash run_pipeline.sh </dev/null >/dev/null 2>&1 & echo started"

# 4) 看进度
ssh -n -i D:\数据开发环境\VMware\sshkeys\cluster_ed25519 -o BatchMode=yes -o StrictHostKeyChecking=no lrh@192.168.87.131 "tail -20 /home/lrh/workspace/old_chain/log/pipeline.out"
```

> ⚠️ 这种写法下有**三个**必须注意的点（否则会报错或静默出错）：
>
> - **非交互 ssh 的 `$PATH` 可能为空**，所以每条命令都要显式 `export PATH=...`（问题 8）。
> - **引号会被逐层剥离**，所以 SQL 一律写文件再用 `beeline -f`（问题 7 / 12）。
> - **别在 PowerShell 的双引号字符串里用 `$变量`**：PowerShell 会先把它当自己的变量展开（未定义 → 空串），命令就残废了。本附录里的路径全部写成绝对路径，就是为了避开这一点。
