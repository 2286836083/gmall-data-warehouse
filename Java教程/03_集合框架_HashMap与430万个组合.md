# 03 · 集合框架 —— HashMap 与那 430 万个组合

> 本份解决一件事：**430 万个聚合键到底要吃多少内存？为什么 410m 必然不够？**
> 读完你应该能：说出 HashMap 存一条数据要占多少字节，并据此判断"这个作业该给多少内存"。

---

## 一、先说为什么是 HashMap

大数据里这几个东西，本质**都是同一件事**：

| 你熟悉的东西 | 本质 |
|---|---|
| SQL 的 `GROUP BY` | 在内存里维护「键 → 累计值」的映射 |
| Spark 的 hash 聚合 | 同一个东西 |
| Shuffle 时 map 端预聚合 | 还是它 |
| 你那 430 万个 `(product_id, dt)` 组合 | **430 万个映射条目** |

而这个"键 → 值"的映射，在 Java 里就是 `HashMap`。

---

## 二、机制：HashMap 长什么样

### 2.1 结构：数组 + 链表 + 红黑树

```
table[]  （一个数组，下标就是"桶"）
   │
   ├─[0] → null
   ├─[1] → Node → Node → null          ← 哈希冲突的元素挂成链表
   ├─[2] → null
   ├─[3] → TreeNode（红黑树）           ← 链表太长时升级
   └─...
```

**为什么需要链表**：两个不同的 key 可能算出**同一个桶下标**（哈希冲突），
总得有个办法把它们都放下——挂成链就是最简单的办法。

**为什么又需要红黑树**：如果很多人挤在同一个桶里（比如有人恶意构造相同 hashCode 的 key），
链表查询是 O(n)，会被拖成 O(n²)。所以 JDK 8 加了：

```
链表长度 > 8  且  table 长度 ≥ 64  →  链表转成红黑树（查询变 O(log n)）
红黑树节点 < 6                     →  退化回链表
```

### 2.2 `put(key, value)` 的完整顺序

```
① 算 hash：h = hash(key)
   注意不是直接用 key.hashCode()，而是把高 16 位和低 16 位异或
   （让高位也参与运算，减少冲突）
        ↓
② 定位桶：index = h & (table.length - 1)
   因为 table.length 是 2 的幂，所以这个 & 等价于取模，但更快
        ↓
③ 看这个桶是不是空的
   ├─ 空 → 直接放进去，结束
   └─ 非空 → 继续
        ↓
④ 桶里第一个节点的 key 和要放的 key 相等吗？（先比 hash，再比 equals）
   ├─ 相等 → 覆盖旧值，结束
   └─ 不等 → 继续
        ↓
⑤ 顺着链表/红黑树往下找
   ├─ 找到相同的 key → 覆盖
   └─ 走到尾都没找到 → 挂到末尾
        ↓
⑥ 挂完之后检查：元素总数 > 阈值（容量 × 0.75）？
   ├─ 是 → 【扩容 resize】，见下节
   └─ 否 → 结束
```

### 2.3 扩容（resize）—— 一个很贵的操作

```
旧 table 长度 16  →  新 table 长度 32（翻倍）
然后【把每一个元素重新算一遍位置，搬过去】
```

JDK 8 有个巧妙优化：因为容量是 2 的幂翻倍，
每个元素的新位置**要么是原位置，要么是「原位置 + 旧容量」**，
只需要看 hash 的那一位是 0 还是 1——不用真的重新取模。
**但搬元素本身的成本还在**，所以：

> **能预估容量时，用 `new HashMap<>(预期大小)` 一次性给够，避免多次扩容。**
> 这是最容易拿到的性能优化。

注意：`new HashMap<>(n)` 里的 n 是期望元素个数，HashMap 会自己算成合适的容量。

---

## 三、★ 内存账本：430 万个条目要吃多少

这是本份最重要的部分。**我们把你那次故障算一遍。**

### 3.1 单个条目占多少（JDK 8，开启压缩指针，默认情况）

HashMap 里每个条目是一个 `Node` 对象：

```
Node 对象本身:
  对象头        12 字节
  int hash       4 字节
  K key   引用   4 字节（压缩指针）
  V value 引用   4 字节
  Node next 引用 4 字节
  ─────────────────────
  合计 28 → 按 8 字节对齐 = 32 字节
```

再加上 key 和 value 这两个对象本身。假设 key 是 `"12345|2019-10-01"` 这样的字符串：

```
String 对象:     对象头 12 + hash 4 + byte[]引用 4 + coder 1  → 对齐 24 字节
byte[16] 数组:   数组头 16 + 16 字节内容                    =  32 字节
                                                    key 小计 ≈ 56 字节

value（聚合累计值，假设两个 long）:
                 对象头 12 + 16 字节字段                    → 对齐 32 字节
```

**单条目合计 ≈ 32 + 56 + 32 = 120 字节**

### 3.2 还要加上哈希桶数组本身

HashMap 不会把数组塞满（负载因子 0.75），所以：

```
需要容量 ≥ 430万 / 0.75 = 573 万
容量必须是 2 的幂 → 取 2^23 = 8,388,608

数组本身 = 8,388,608 × 4 字节 = 33.6 MB   ← 光这个空数组就 33 MB
```

### 3.3 总账

| 项 | 计算 | 大小 |
|---|---|---|
| 桶数组 | 8,388,608 × 4 B | **33.6 MB** |
| 430 万个 Node | 4.3M × 32 B | **137.6 MB** |
| 430 万个 key（字符串） | 4.3M × 56 B | **240.8 MB** |
| 430 万个 value（累加器） | 4.3M × 32 B | **137.6 MB** |
| **合计** | | **≈ 550 MB** |

### 3.4 结论

> **你的堆上限是 410 MB，而光是存这 430 万个键就需要约 550 MB。**
> **这不是"差一点"，是从一开始就必然失败。**

而扩容到 `-Xmx820m` 之后，550 MB 能放下了 —— 所以 63 秒成功。

### 3.5 ⚠️ 必须诚实说明的两点

**第一，这是按「朴素 Java HashMap」算的。**
真实的 Hive / Spark **不会**用这么朴素的结构：
- Spark 用 Tungsten 的 `BytesToBytesMap`，把 key 以**紧凑二进制**存在一个连续数组里，
  每个条目只有几十字节，而且可以用**堆外内存**
- Spark 的聚合算子还能**溢写磁盘**（spill），内存不够时先落盘
- Hive 的 reduce 端聚合默认走**排序聚合**，不一定建哈希表

所以**真实占用会明显低于 550 MB**。

**第二，但"量级思维"是成立的。**
不管引擎怎么优化，**430 万个聚合键必须同时驻留**这个矛盾不变，
在 512 MB 这种小容器下必然吃紧 —— 这和你实测的结果一致。

> 面试时讲这个，正确说法是：
> "我按朴素 HashMap 估算约 550 MB，**量级上**说明 410 MB 不够；
> 真实引擎结构更紧凑还会溢写，但矛盾一样。
> **重点是我会先算量级，而不是等它炸了才加内存。**"

---

## 四、`hashCode` 和 `equals`：一条必须同时遵守的契约

```java
// 错：只重写 equals 不重写 hashCode
class Key {
  String productId, dt;
  @Override public boolean equals(Object o) { /* 比两个字段 */ }
  // 没重写 hashCode → 用的是 Object 默认的（按内存地址算）
}
```

后果：**两个内容相同的 Key 对象，equals 相等，hashCode 却不相等** →
HashMap 认为它们是不同的键 → 查不到、去重失效。

**契约**（背下来）：

```
① equals 相等  →  hashCode 必须相等
② hashCode 相等 → equals 不一定相等（哈希冲突是正常的）
```

> 这条在大数据里直接对应一个真实问题：
> **用自定义对象做 join / group by 的 key 时，如果没正确实现这两个方法，结果会静默出错。**

---

## 五、其他必会的集合

| 类 | 底层 | 关键特性 | 什么时候用 |
|---|---|---|---|
| `ArrayList` | 数组 | 随机访问 O(1)；扩容 1.5 倍；中间插入慢 | 按序号访问、追加 |
| `LinkedList` | 双向链表 | 插入删除 O(1)；随机访问 O(n) | **实际很少用**，ArrayList 通常更好 |
| `HashSet` | 就是 HashMap（只用了 key） | 去重 | 去重、判断存在 |
| `ConcurrentHashMap` | 见下 | 线程安全 | 多线程共享的映射 |

### 5.1 `ConcurrentHashMap` 的版本差异（常考）

| 版本 | 实现方式 |
|---|---|
| **JDK 7** | **分段锁 Segment**：把整个 map 切成 16 段，每段一把锁，不同段可并发 |
| **JDK 8+** | **CAS + `synchronized`（只锁单个桶的头节点）**：粒度更细，并发度更高 |

> 常见追问："JDK 8 的 ConcurrentHashMap 还用分段锁吗？"
> 答：**不用了**，改成 CAS + 对桶头节点加 synchronized。

---

## 六、可跑的验证

### 6.1 亲眼看 HashMap 吃掉多少内存

```java
// MapMemory.java —— 算一算 HashMap 到底占多少
import java.util.HashMap;

public class MapMemory {
  public static void main(String[] a) {
    Runtime rt = Runtime.getRuntime();
    System.gc();
    long before = rt.totalMemory() - rt.freeMemory();

    int n = 1_000_000;
    HashMap<String, Long> map = new HashMap<>(n * 2);
    for (int i = 0; i < n; i++) {
      map.put("product_" + i + "|2019-10-01", (long) i);
    }

    System.gc();
    long after = rt.totalMemory() - rt.freeMemory();
    long usedMB = (after - before) >> 20;

    System.out.println("插入 " + n + " 条");
    System.out.println("占用 ≈ " + usedMB + " MB");
    System.out.println("每条 ≈ " + ((after - before) / n) + " 字节");
    System.out.println("→ 按此估算 430 万条约需 " + (usedMB * 4.3) + " MB");
  }
}
```

```bash
javac MapMemory.java && java -Xmx2g MapMemory
```

**看什么**：打印出的"每条 ≈ ? 字节"应该在 100~200 之间，
和上面 3.1 的理论估算对得上。**对得上，说明你的内存账本是准的。**

> 注意：这个测法有误差（`Runtime` 的读数不精确，且 JVM 有内部开销）。
> 但**量级是对的**，这就够了。要精确可以用 JOL（Java Object Layout）工具。

### 6.2 看扩容有多贵

```java
// ResizeCost.java —— 对比预分配容量 vs 不预分配
import java.util.HashMap;

public class ResizeCost {
  public static void main(String[] a) {
    int n = 2_000_000;

    long t0 = System.currentTimeMillis();
    HashMap<Integer,Integer> m1 = new HashMap<>();          // 不预分配 → 反复扩容
    for (int i = 0; i < n; i++) m1.put(i, i);
    long t1 = System.currentTimeMillis();

    HashMap<Integer,Integer> m2 = new HashMap<>(n * 2);      // 一次性给够
    for (int i = 0; i < n; i++) m2.put(i, i);
    long t2 = System.currentTimeMillis();

    System.out.println("不预分配: " + (t1 - t0) + " ms");
    System.out.println("预分配  : " + (t2 - t1) + " ms");
  }
}
```

```bash
javac ResizeCost.java && java -Xmx2g ResizeCost
```

**看什么**：预分配通常会明显更快 —— 这就是"能预估就预分配"的直接证据。

---

## 七、面试官会问（这一节的）

| 问题 | 答法要点 |
|---|---|
| HashMap 底层结构？ | 数组 + 链表 + 红黑树（JDK 8+） |
| 为什么链表要转红黑树？ | 防止哈希冲突被恶意利用，把查询从 O(n) 降到 O(log n) |
| 什么时候转？ | 链表长度 > 8 **且** table ≥ 64；节点 < 6 时退化回链表 |
| 为什么容量是 2 的幂？ | 可以用 `h & (len-1)` 代替取模，更快；扩容时还能用位判断新位置 |
| 负载因子为什么是 0.75？ | 空间和时间成本的折中（0.5 太浪费，1.0 冲突太多） |
| 重写 equals 必须重写 hashCode 吗？ | **必须**，否则违反契约，HashMap 会查不到 |
| ConcurrentHashMap 怎么做线程安全？ | JDK 7 分段锁；**JDK 8 起改成 CAS + 锁桶头节点** |
| **HashMap 存 100 万条要多少内存？** | ★ 每条 100~200 字节量级 → 约 100~200 MB，**要会算** |

---

## 八、术语卡

| 术语 | 一句话 |
|---|---|
| **哈希冲突** | 不同的 key 算出了相同的桶下标 |
| **负载因子 (load factor)** | 装多少就该扩容，默认 0.75 |
| **扩容 / rehash** | 容量翻倍并把所有元素重新定位，是个贵操作 |
| **红黑树** | 一种自平衡二叉查找树，查询 O(log n) |
| **CAS** | Compare And Swap，无锁原子操作 |
| **分段锁** | JDK 7 ConcurrentHashMap 的做法，已废弃 |
| **压缩指针** | 堆 < 32 GB 时引用只用 4 字节，省内存 |
| **对齐填充** | 对象大小按 8 字节对齐，所以 28 字节会变成 32 |

---

## 九、把这一份接到你的项目上

以后再遇到"这个作业要给多少内存"，你的判断顺序应该是：

```
① 这个作业有没有"要同时装住大量数据的结构"？
   （GROUP BY / DISTINCT / JOIN 都要装）
        ↓
② 如果有，先估键的数量级
   （GROUP BY a, b → 就是 a×b 的组合数；你那次是 430 万）
        ↓
③ 按每条 100~200 字节估算总占用
   （你那次 ≈ 550 MB）
        ↓
④ 反推堆要多大，再给堆外留 20% 余量 → 得出容器大小
        ↓
⑤ 如果算出来超过硬件上限 → 改方案（溢写 / 换聚合方式 / 拆作业）
```

**这就是从"试出来的"变成"算出来的"。** 这比多背十个 API 有用得多。
