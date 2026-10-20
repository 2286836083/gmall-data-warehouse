# 02 · Linux 与 Shell 生存手册

> **这章解决什么**：集群上的服务器**没有图形界面**（案例集群甚至把桌面关了，每台省 600MB 内存）。
> 你所有的工作——装软件、改配置、看日志、跑任务、查进程——全在一个黑框里完成。
> 这章让你在这个黑框里不慌。
>
> **不需要背**。你只需要知道"有这个东西、大概能干什么"，剩下的靠翻这一章的速查表。

---

## 一、为什么必须先学这个（三个真实场景）

1. **装 Hadoop** = 下载压缩包 → 解压 → 改 XML 配置文件 → 设环境变量。**全程命令行。**
2. **任务跑挂了** = 去服务器上看日志 → 找到报错行 → 看进程还在不在。**全程命令行。**
3. **数据要上传** = 从本地把 1.3GB 的 CSV 传到服务器 → 再放进 HDFS。**全程命令行。**

> 📌 **本项目对照**：案例集群三台都是 Ubuntu，桌面环境被刻意关掉
> （`bash /home/lrh/desktop_switch.sh off|on|status`），就为了省出内存给计算用。

---

## 二、先建立地图：Linux 的目录是干什么的

Windows 是 `C:\ D:\` 分盘，Linux 是一棵树，从 `/` 开始：

```
/                        ← 根
├── home/
│   └── lrh/             ← 你的家目录（≈ 我的文档）。登录进来默认就在这
│       ├── workspace/   ← 案例项目的工作脚本都放这
│       └── .ssh/        ← SSH 密钥（权限必须是 600）
├── opt/                 ← 【重点】第三方软件装这里
│   ├── jdk/jdk1.8.0_202/
│   ├── hadoop/hadoop-3.3.6/
│   ├── hive/apache-hive-3.1.3-bin/
│   └── spark-3.5.9-bin-hadoop3/
├── etc/                 ← 配置文件集中营
│   ├── hosts            ← 主机名 ↔ IP 映射（集群必备）
│   └── profile.d/       ← 登录时自动加载的环境变量脚本
├── var/log/             ← 系统日志
├── tmp/                 ← 临时目录（重启可能清空，别放重要东西）
└── usr/local/           ← 另一处常见软件安装位置
```

**必须记住的三个符号**：

| 符号 | 含义 |
|---|---|
| `~` | 家目录（`/home/lrh`） |
| `.` | 当前目录 |
| `..` | 上一级目录 |
| `/` 开头 | **绝对路径**（从根开始，任何地方都能用） |
| 其他 | 相对路径（从当前目录出发） |

> **写脚本一律用绝对路径。** 相对路径会在"脚本被谁调用、从哪调用"上反复翻车。

---

## 三、20 个命令（按用途分组，够你活下来）

### 3.1 走位与查看

```bash
pwd                      # 我在哪（print working directory）
ls                       # 看当前目录有什么
ls -lh                   # -l 详细，-h 人类可读的大小（KB/MB/GB）
ls -la                   # -a 包含隐藏文件（以 . 开头的）
cd /opt/hadoop           # 去某个目录（绝对路径）
cd ..                    # 回上一级
cd ~                     # 回家
```

### 3.2 文件操作

```bash
mkdir -p a/b/c           # -p：父目录不存在就一起建（建多层目录必须加）
touch x.txt              # 建空文件
cat x.txt                # 把整个文件打印出来（适合小文件）
less x.txt               # 分页看（空格翻页，q 退出）—— 看日志用这个，别用 cat
head -20 x.txt           # 看前 20 行
tail -20 x.txt           # 看后 20 行
tail -f app.log          # ★ 实时追踪日志末尾（任务在跑时用，Ctrl+C 退出）
cp a.txt b.txt           # 复制
cp -r dir1 dir2          # 复制目录必须 -r
mv a.txt b.txt           # 移动 / 重命名
rm a.txt                 # 删除（Linux 没有回收站！）
rm -rf dir               # 强制递归删除目录 —— ⚠️ 最危险的命令之一，敲之前先 pwd
```

> ⚠️ **`rm -rf` 是这条路上最贵的教训**。三条保命习惯：
> ① 删之前先 `pwd` 和 `ls` 确认位置；
> ② 路径尽量用绝对路径；
> ③ 批量删除前先用 `ls` 把同样的通配模式跑一遍，看看会命中什么。

### 3.3 查找与统计（这几个在数据开发里天天用）

```bash
grep "ERROR" app.log              # 在文件里找包含 ERROR 的行
grep -n "ERROR" app.log           # -n 显示行号
grep -i "error" app.log           # -i 忽略大小写
grep -v "INFO" app.log            # -v 反向：排除包含 INFO 的
grep -r "dfs.replication" /opt/hadoop/etc/hadoop/   # -r 递归查整个目录

wc -l events.csv                  # 数行数（统计数据量第一件事就是它）
sort x.txt | uniq -c              # 排序后去重计数（低频版 GROUP BY）
du -sh /opt/hadoop                # 这个目录多大（-s 汇总 -h 可读）
df -h                             # 磁盘还剩多少
```

### 3.4 文本处理三剑客（awk / sed 必学一个）

```bash
# awk：按列取数据。默认以空格分列，-F 指定分隔符
awk -F',' '{print $1, $3}' events.csv        # 打印第1列和第3列
awk -F',' 'NR>1 {sum += $7} END {print sum}' events.csv   # 求第7列总和（跳过表头）
awk -F',' '{print $NF}' events.csv           # NF = 最后一列

# sed：替换 / 按行取
sed -n '10,20p' app.log                      # 只看第 10~20 行（排错时截日志很有用）
sed -i 's/old/new/g' file.sql                # -i 直接改文件，把 old 全换成 new
sed -i "s/FROM gmall\.dws_trade_day/FROM gmall.ads_trade_day/" x.sql   # 案例项目真实用过
```

> 📌 **本项目对照**：案例项目切大屏数据源时，就是用两条 `sed -i` 改的导出脚本。
> 改完还做了**新旧导出文件逐字节比对**，确认只差时间戳——**改脚本一定要有验证动作**。

### 3.5 管道与重定向（Shell 的精髓）

```bash
# 管道 | ：把前一个命令的输出，当成后一个命令的输入
cat app.log | grep ERROR | wc -l              # 数一下有几条 ERROR
ps -ef | grep java                            # 在进程列表里找 java

# 重定向
hdfs dfs -ls / > out.txt          # > 写入文件（覆盖）
hdfs dfs -ls / >> out.txt         # >> 追加
command 2> err.log                # 2> 只把错误输出存下来
command > /dev/null 2>&1          # 丢弃所有输出（后台任务的常见写法）
```

**`2>&1` 是什么意思**：Linux 里 `1` = 标准输出（正常结果），`2` = 标准错误。
`2>&1` 就是"把错误也并到标准输出去"。

> ⚠️ **一个真实的坑**：非交互环境下有些命令不在 PATH 里（比如 `yarn`），
> 如果你习惯性加 `2>/dev/null` 把错误丢掉，就会得到"看起来合法的空答案"——
> 然后你会去查一个根本不存在的故障。**看不到报错比报错更危险。**

### 3.6 进程与资源

```bash
ps -ef                            # 看所有进程
ps -ef | grep "[j]ava"            # ★ 用方括号打断字面量，否则会把 grep 自己数进去
top                               # 实时看 CPU/内存占用（q 退出）
free -m                           # 内存还剩多少（-m 以 MB 显示）
jps -l                            # ★ 只看 Java 进程（Hadoop 生态查进程第一命令）
kill <pid>                        # 温柔地请进程退出（SIGTERM，退出码 143）
kill -9 <pid>                     # 强制杀（SIGKILL，退出码 137，不给它收尾的机会）
```

> ⚠️ **两个真实的诊断陷阱**：
> 1. **`jps` 返回空 ≠ 进程不存在。** root 用户启动的 JVM 不在你的 `hsperfdata` 目录里，`jps` 就看不见。
>    真判据是 `ps -ef | grep "[d]olphinscheduler"` + `ss -lnt` 看端口。
> 2. **用 `grep` 数 `ps` 的结果会虚高**，因为你这条命令自身的命令行也被算进去了。
>    案例项目里就出过"数出 3 个 MasterServer，实际一个都没有"的假阳性。

### 3.7 权限

```bash
ls -l                             # 看权限：rwxr-xr-x 这种
chmod +x script.sh                # 加可执行权限（脚本跑之前忘了这步会报 Permission denied）
chmod 600 ~/.ssh/id_ed25519       # ★ 私钥必须是 600，否则 SSH 拒绝使用
chown lrh:lrh file.txt            # 改属主
sudo <命令>                        # 以管理员身份执行
```

权限三元组快速理解：`rwx` = 读(4)/写(2)/执行(1)，三组分别是 属主/属组/其他人。
`chmod 755` = 属主全权限，其他人可读可执行。

---

## 四、vim：只要学会 5 个动作

服务器上改配置文件，99% 的时候没有记事本。最小可用集：

```bash
vim /opt/hadoop/etc/hadoop/core-site.xml
```

| 动作 | 按键 | 说明 |
|---|---|---|
| 进入编辑模式 | `i` | 左下角出现 `-- INSERT --` |
| 退出编辑模式 | `Esc` | 必须退出编辑模式才能执行命令 |
| 保存并退出 | `:wq` + 回车 | write + quit |
| 不保存退出 | `:q!` + 回车 | ⚠️ 改错了想重来就用这个 |
| 搜索 | `/关键词` + 回车，`n` 下一个 | 在几百行的 XML 里找配置项 |

> 卡住了怎么办：`Esc` 多按几下，然后 `:q!` 退出。**所有人第一次用 vim 都卡过。**

如果你实在受不了，可以用 `nano`（`nano file.txt`，底部有快捷键提示），但**考试/面试/别人机器上不一定有**。

---

## 五、SSH：你怎么进入集群，以及三台机器怎么互相进入

### 5.1 从你的电脑连进去

```bash
ssh lrh@192.168.87.131                          # 密码登录
ssh -i "D:/数据开发环境/VMware/sshkeys/cluster_ed25519" lrh@192.168.87.131   # 密钥登录
```

> 📌 **本项目对照**：案例集群用 ed25519 密钥，且**三台机器之间做了互信免密**
> （在 lrh-1 上直接 `ssh lrh-2` 就能过去，不需要密码）。
> 这一步不是可选优化——**Hadoop 的主节点要靠 SSH 去拉起其他节点的进程**，没有免密就跑不起来。

### 5.2 配免密（集群必做）

```bash
ssh-keygen -t ed25519                 # 一路回车（不要设密码，否则自动化会卡住）
ssh-copy-id lrh@lrh-2                 # 把公钥发到目标机器
ssh lrh@lrh-2                         # 测试：这次应该不用输密码了
```

**物理上发生了什么**：`ssh-copy-id` 把你的公钥追加到对方 `~/.ssh/authorized_keys` 里。
之后你连过去时，对方用你的公钥发一段加密挑战，你用本地私钥解开——证明"你就是你"，全程不传密码。

**配完还是让输密码？按顺序查这三条**（这是最高频的故障）：

```bash
chmod 700 ~/.ssh                      # ① 目录权限
chmod 600 ~/.ssh/authorized_keys      # ② 文件权限（不对会直接被忽略）
# ③ 看服务端日志找真正原因
sudo tail -50 /var/log/auth.log       # Ubuntu
```

### 5.3 传文件

```bash
scp file.csv lrh@lrh-1:/home/lrh/workspace/        # 上传单个文件
scp -r ./dir lrh@lrh-1:/home/lrh/                  # 上传目录
scp lrh@lrh-1:/home/lrh/out.csv ./                 # 下载
```

> ⚠️ **Windows 上的坑**：从 Git Bash 传中文路径/中文参数会被本地代码页编码破坏，
> **静默返回空**，很容易误判成远端故障。传文件用 `scp`，别把中文塞进 `ssh` 命令行参数里。

---

## 六、环境变量：为什么装完软件还要"配环境"

**问题**：你装了 Hadoop，敲 `hadoop` 却提示 `command not found`。

**原因**：Linux 找命令时，只在 `PATH` 变量列出的那几个目录里找。你的软件不在里面。

```bash
echo $PATH                            # 看当前 PATH（一堆用冒号分隔的目录）
which java                            # 这个命令实际在哪
export PATH=$PATH:/opt/hadoop/hadoop-3.3.6/bin     # 临时加（关掉终端就失效）
```

**永久生效**：写进 `~/.bashrc`（只对当前用户）或 `/etc/profile.d/xxx.sh`（所有用户，案例集群用这个）：

```bash
# /etc/profile.d/hadoop.sh  ← 案例集群的真实内容【模板可抄】
export HADOOP_HOME=/opt/hadoop/hadoop-3.3.6
export HADOOP_CONF_DIR=/opt/hadoop/hadoop-3.3.6/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

```bash
source /etc/profile                   # 让改动立刻生效（不用重新登录）
```

> **为什么 Hadoop 一定要设 `JAVA_HOME`**：Hadoop 本身是 Java 写的，它启动时会去找 Java。
> 即使你在命令行能敲 `java`，Hadoop 的启动脚本是另一个进程，可能拿不到你的 PATH。
> 所以要在 `hadoop-env.sh` 里**显式写死** `JAVA_HOME`。

---

## 七、后台运行：为什么你必须会这一招

**场景**：你要跑一个 20 分钟的 ETL。如果你直接在终端里 `bash etl.sh`：

- 网络一断 → SSH 断开 → 进程收到挂断信号 → **任务死在半路**
- 你不敢关电脑，只能干等

```bash
# 基础版：& 放后台（SSH 一断还是会被杀）
bash etl.sh > etl.log 2>&1 &

# 进阶版：nohup 忽略挂断信号
nohup bash etl.sh > etl.log 2>&1 &

# ★ 推荐版：setsid 让进程脱离当前会话，彻底不受 SSH 断开影响
setsid nohup bash etl.sh > etl.log 2>&1 &
```

| 写法 | SSH 断开后还活着吗 |
|---|---|
| `cmd &` | ❌ 通常会死 |
| `nohup cmd &` | ✅ 一般能活 |
| `setsid nohup cmd &` | ✅✅ 最稳（案例项目的长任务约定） |

```bash
tail -f etl.log           # 盯进度
jobs                      # 看当前 shell 的后台任务
ps -ef | grep etl         # 确认它还在
```

> 📌 **本项目对照**：这是案例项目的**硬约定**——跑耗时程序必须 `setsid nohup ... &`，
> 并且要写**状态文件**让人能看到真实进度。原因很实在：
> 一次 DWD 全量跑批 20 分 53 秒，DWS+ADS 28 分 01 秒，整链走调度 54 分 10 秒。
> **这么长的任务，不允许"跑了但不知道跑到哪了"。**

---

## 八、Shell 脚本：把一串命令存起来重复执行

你会很快发现：ETL 不是一条命令，是"建表 → 装载 → 清洗 → 汇总 → 校验"一串。
所以必须会写脚本。

```bash
#!/usr/bin/env bash
set -eu            # ★ -e：任何一条命令失败就退出；-u：用了未定义变量就报错
                   #   没有这两行，脚本会在错误发生后继续跑，把问题搞得更乱

DATADIR="${1:-/home/lrh/workspace/new_chain/real_data}"   # ${1:-默认值}：第一个参数，没给就用默认
TABLE=gmall.ods_event_log_di
BEELINE=/opt/hive/apache-hive-3.1.3-bin/bin/beeline   # ★ 用全路径，别依赖 PATH

echo "数据目录：$DATADIR"
if [ ! -d "$DATADIR" ]; then
  echo "目录不存在，退出"; exit 1
fi

for f in "$DATADIR"/*.csv; do
  d=$(basename "$f" .csv)                 # 从路径里取出文件名（去掉 .csv）
  echo "装载 $d ..."
done
```

**只要掌握这 8 个语法就能写 90% 的运维脚本**：

| 语法 | 作用 |
|---|---|
| `VAR="值"` / `$VAR` | 定义与使用变量（**等号两边不能有空格**） |
| `"$VAR"` | 加引号——路径里有空格时才不会裂开（**永远加引号**） |
| `$(命令)` | 把命令的执行结果当值用 |
| `${1:-默认}` | 取第 1 个参数，没有就用默认 |
| `if [ 条件 ]; then ... fi` | 判断（**方括号两边必须空格**） |
| `for x in ...; do ... done` | 循环 |
| `exit 1` | 非 0 退出表示失败（调度系统靠这个判断成功与否） |
| `set -eu` | 失败即停 |

---

## 九、动手实验室

### 实验 2-1：命令行体检（在任意 Linux/虚拟机上做）

```bash
pwd && whoami && hostname          # 我是谁、在哪台机器、在哪个目录
free -m                            # 内存
df -h                              # 磁盘
ls -la ~ | head -20                # 家目录里有什么
echo $PATH | tr ':' '\n'           # 把 PATH 竖着打印，看清有哪些目录
```

### 实验 2-2：数一份数据（模拟数据开发的第一步）

```bash
# 造一份迷你日志
printf '%s\n' \
  "2019-10-01 00:00:11 UTC,view,101,9,electronics.phone,apple,999.0,u1,s1" \
  "2019-10-01 00:00:15 UTC,cart,101,9,electronics.phone,apple,999.0,u1,s1" \
  "2019-10-01 00:01:02 UTC,purchase,101,9,electronics.phone,apple,999.0,u1,s1" \
  "2019-10-01 00:02:00 UTC,view,102,9,,samsung,499.0,u2,s2" \
  > mini.csv

wc -l mini.csv                                        # 总行数
awk -F',' '{print $2}' mini.csv | sort | uniq -c      # 各事件类型有多少条
awk -F',' '{print $8}' mini.csv | sort -u | wc -l     # 去重用户数（手写版 COUNT DISTINCT）
awk -F',' '$2=="purchase" {sum+=$7} END {print sum}' mini.csv   # GMV
```

**想一想**：上面这三条 awk，是不是分别对应了后面要写的
`COUNT`、`COUNT(DISTINCT)`、`SUM(CASE WHEN ...)`？
**HQL 只不过是把这些操作交给一群机器去做。**

### 实验 2-3：感受后台与日志

```bash
setsid nohup bash -c 'for i in $(seq 1 30); do echo "第 $i 秒"; sleep 1; done' > /tmp/tick.log 2>&1 &
tail -f /tmp/tick.log       # 看它一行行冒出来，Ctrl+C 退出
ps -ef | grep "[s]eq"       # 确认进程还在
```

---

## 十、常见坑

| 现象 | 原因 | 解法 |
|---|---|---|
| `command not found` | 不在 PATH，或非交互 shell 没加载 profile | 用全路径；或写进 `/etc/profile.d/` |
| `Permission denied`（执行脚本） | 没有可执行权限 | `chmod +x script.sh` |
| `Permission denied`（SSH 登录） | 私钥权限太松 | `chmod 600 私钥` |
| SSH 还是让输密码 | `authorized_keys` 权限不对 | `chmod 600 ~/.ssh/authorized_keys` |
| 脚本里中文路径报错 | Windows 侧编码问题 | 别把中文放进命令行参数，用 `scp`/脚本内部构造 |
| 后台任务 SSH 断开后死了 | 没脱离会话 | `setsid nohup ... &` |
| `grep ps` 数出来的进程数不对 | 把 grep 自己数进去了 | `grep "[j]ava"` |
| 脚本报错了还在往下跑 | 缺 `set -e` | 头部加 `set -eu` |
| `rm -rf` 删错地方 | 用了相对路径 | 删前 `pwd`；用绝对路径 |

---

## 十一、自测

**每题标了对应的正文小节——答不出来就回去看那节，不要硬想。**

1. `~`、`.`、`..`、`/opt` 分别指哪？—— 对应 **§二**
2. 想看一个 500MB 的日志文件里第 3000~3050 行，用什么命令？—— 对应 **§3.4**
3. `command > out.log 2>&1` 里的 `2>&1` 是什么？—— 对应 **§3.5**
4. 为什么后台跑长任务要用 `setsid nohup`？—— 对应 **§七**
5. 让你写个脚本：接收一个目录参数，数出里面每个 `.csv` 文件的行数。能写出来吗？—— 对应 **§八**

<details>
<summary><b>参考答案要点</b>（先自己答一遍，再点开核对）</summary>

**1. 四个符号**
- `~` = 家目录（`/home/lrh`）；`.` = 当前目录；`..` = 上一级
- `/opt` = 第三方软件安装目录（案例集群的 JDK / Hadoop / Hive / Spark 全在这）
- 加分项：写脚本**一律用绝对路径**，相对路径会在"谁调用、从哪调用"上反复翻车

**2. 取日志的第 3000~3050 行**
```bash
sed -n '3000,3050p' app.log
```
- 关键：`-n` 必须带，否则 sed 会把**整个文件**打印一遍
- 500MB 的文件别用 `cat`；要持续看新的用 `tail -f`

**3. `2>&1` 是什么**
- Linux 里 `1` = 标准输出（正常结果），`2` = 标准错误
- `2>&1` = 把错误也并到标准输出去，这样两者一起进文件或管道
- 常配套：`> /dev/null 2>&1` 丢弃全部输出
- ⚠️ 反例警示：**看不到报错比报错更危险**——把 `2>/dev/null` 当习惯加，会得到"看起来合法的空答案"

**4. 为什么 `setsid nohup`**
| 写法 | SSH 断开后还活着吗 |
|---|---|
| `cmd &` | ❌ 通常会死（收到挂断信号） |
| `nohup cmd &` | ✅ 一般能活 |
| `setsid nohup cmd &` | ✅✅ 脱离会话，最稳 |
- 案例项目的硬约定：跑耗时程序必须 `setsid nohup ... &`，并写**状态文件**让别人看得到真实进度

**5. 脚本（要点：默认值 / 引号 / set -eu）**
```bash
#!/usr/bin/env bash
set -eu                      # 失败即停；未定义变量报错
DIR="${1:-.}"                # 取第 1 个参数，没给就用当前目录
for f in "$DIR"/*.csv; do
  echo "$(wc -l < "$f")  $f" # ★ 用 < 重定向，否则会连文件名一起打印
done
```
</details>

---

## 下一章

[03 · 从零搭一套三节点集群](./03_环境_从零搭一套三节点集群.md) —— 亲手把 HDFS 和 YARN 跑起来，这是第一个里程碑。
