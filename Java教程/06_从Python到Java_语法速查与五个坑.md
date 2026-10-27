# 06 · 从 Python 到 Java —— 语法速查与常见坑

> 本份是**工具页**，不是教程。你会 Python，语法迁移两天就够——**别在这上面花大时间**。
> 真正要花时间的是 01~04 分册（JVM / GC / 集合 / 并发）。

---

## 一、语法对照表

| 你要做的事 | Python | Java |
|---|---|---|
| 定义变量 | `x = 1` | `int x = 1;` |
| 类型推断 | （本来就不用写类型） | `var x = 1;`（JDK 10+） |
| 常量 | 约定大写 | `final int X = 1;` |
| 列表 | `xs = [1,2,3]` | `List<Integer> xs = List.of(1,2,3);`（不可变）<br>`new ArrayList<>()`（可变） |
| 字典 | `d = {"a": 1}` | `Map<String,Integer> d = new HashMap<>();` |
| 取长度 | `len(xs)` | `xs.size()`（集合）/ `arr.length`（数组）/ `s.length()`（字符串） |
| 遍历 | `for x in xs:` | `for (int x : xs) { }` |
| 带下标遍历 | `for i, x in enumerate(xs):` | `for (int i = 0; i < xs.size(); i++) { }` |
| 函数 | `def f(x): return x+1` | `int f(int x) { return x + 1; }` |
| Lambda | `lambda x: x+1` | `x -> x + 1` |
| 字符串拼接 | `f"{a}-{b}"` | `a + "-" + b` 或 `String.format("%s-%s", a, b)` |
| 判空 | `if x is None:` | `if (x == null) { }` |
| 异常 | `try/except` | `try/catch`（且区分 checked/unchecked） |
| 类 | `class A:` | `class A { }` |
| 读文件 | `open(f).read()` | `Files.readString(Path.of(f))` |
| 按行读 | `for line in open(f):` | `Files.lines(Path.of(f))` |

---

## 二、五个一定会踩的坑

### 坑 1：`==` 和 `equals`（**最高频**）

```java
String a = new String("abc");
String b = new String("abc");

a == b        // false！比的是"是不是同一个对象"（引用地址）
a.equals(b)   // true！比的是内容
```

**规律**：

| 类型 | 用 `==` 比什么 | 比内容用什么 |
|---|---|---|
| 基本类型（`int`/`long`/`double`） | **值** ✅ | `==` 就行 |
| 引用类型（String、对象） | **引用地址**（几乎永远不是你想要的） | `.equals()` |

> Python 里 `==` 比内容、`is` 比身份；Java **正好反过来** —— `==` 是身份，`.equals()` 是内容。

### 坑 2：`Integer` 的缓存陷阱

```java
Integer a = 127, b = 127;
a == b   // true（因为 -128~127 被缓存了，是同一个对象）

Integer c = 128, d = 128;
c == d   // false（超出缓存范围，是不同对象）
```

**结论**：包装类型（`Integer`/`Long`）比大小**一律用 `.equals()` 或先拆箱**，别用 `==`。

### 坑 3：空指针（NPE）

```java
Integer x = null;
int y = x;        // 运行时 NullPointerException（自动拆箱时炸）
```

Python 里 `None + 1` 也会报错，但 Java 里 NPE 更隐蔽——
因为**自动拆装箱**会让你看不出哪里可能为空。

**防御**：不确定时用 `Optional`，或者老老实实判 null。

### 坑 4：数组 vs List

```java
int[] arr = new int[10];      // 定长，arr.length
List<Integer> list = new ArrayList<>();  // 变长，list.size()
```

大数据里**绝大多数情况用 List/Map**，数组只在追求性能时用（因为省掉对象开销）。

### 坑 5：字符串拼接在循环里很慢

```java
// 慢：每次 + 都新建一个 String 对象（String 不可变）
String s = "";
for (int i = 0; i < 100000; i++) s += i;

// 快：用 StringBuilder
StringBuilder sb = new StringBuilder();
for (int i = 0; i < 100000; i++) sb.append(i);
String s = sb.toString();
```

**原因**：`String` 不可变，每次拼接都要新建对象 → 产生海量垃圾 → GC 压力（回看 02 分册）。

---

## 三、一个完整例子：读 CSV 并做聚合

把 03（HashMap）和 05（IO）串起来。这就是你 DWS 层做的事的简化版。

```java
// CsvAgg.java —— 读 CSV，按"类目"聚合金额
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class CsvAgg {
  public static void main(String[] args) throws Exception {
    // key = 类目，value = [总金额, 订单数]
    Map<String, double[]> agg = new HashMap<>();

    try (BufferedReader br = Files.newBufferedReader(Path.of(args[0]))) {
      String line;
      br.readLine();                      // 跳过表头
      while ((line = br.readLine()) != null) {
        String[] f = line.split(",");     // 简单切分（真实 CSV 要用库处理引号）
        if (f.length < 3) continue;

        String category = f[0];
        double price;
        try {
          price = Double.parseDouble(f[2]);
        } catch (NumberFormatException e) {
          continue;                       // 脏数据跳过（你项目里 price<=0 有 465 条）
        }

        agg.computeIfAbsent(category, k -> new double[2]);
        double[] v = agg.get(category);
        v[0] += price;                    // 累计金额
        v[1] += 1;                        // 计数
      }
    }

    // 按金额倒序输出 Top 10
    agg.entrySet().stream()
       .sorted((a, b) -> Double.compare(b.getValue()[0], a.getValue()[0]))
       .limit(10)
       .forEach(e -> System.out.printf("%-20s 金额=%.2f  订单数=%.0f%n",
           e.getKey(), e.getValue()[0], e.getValue()[1]));

    System.out.println("\n类目总数: " + agg.size());
  }
}
```

### 跑起来

```bash
# 造一点测试数据
cat > data.csv <<'EOF'
category,product_id,price
electronics,p1,1999.00
electronics,p2,899.50
unknown,p3,29.90
electronics,p1,1999.00
books,p4,45.00
unknown,p5,abc
books,p6,78.00
EOF

javac CsvAgg.java && java CsvAgg data.csv
```

**你会看到**：`electronics` 排第一，`unknown` 也在（你项目里 `unknown` 占 9.7%），
脏数据 `abc` 被跳过。

### 这段代码里有哪些前面学过的东西

| 行 | 对应分册 |
|---|---|
| `BufferedReader` | 05 —— 缓冲，避免逐字节系统调用 |
| `HashMap` | 03 —— 聚合就是一张哈希表 |
| `computeIfAbsent` | 03 —— 没有就新建 |
| 跳过脏数据 | 你项目里 `price <= 0` 有 465 条，同理 |
| `agg.size()` | 03 —— 这就是"组合数"，430 万那种 |

---

## 四、环境速查

| 你要做什么 | 命令 |
|---|---|
| 编译 | `javac Xxx.java` |
| 运行 | `java Xxx` |
| 打成 jar | `jar cf a.jar *.class` |
| 你本机的版本 | JDK **13.0.1**（`java -version` 已确认） |
| 集群版本 | **JDK 8** |

> ⚠️ **版本差异提醒**：本机是 13，集群是 8。
> 写普通语法练习没事；**如果要提交到集群跑，编译时要用 `-source 8 -target 8`**，
> 或者本机另装一个 JDK 8。

---

## 五、接下来学什么

语法不要继续深入了。按这个顺序回到主线：

```
06（本份，两天够）
   ↓
01 JVM 内存   ← 你已有实战素材，转化率最高
   ↓
02 GC        ← 讲清楚你那个报错
   ↓
03 HashMap   ← 会把"内存够不够"算出来
   ↓
04 并发      ← 线程池 → 对应 YARN 容器
   ↓
05 IO/序列化 ← shuffle 为什么慢
   ↓
→ 进入 Scala → 用 DataFrame API 重写你那条 DWS
```
