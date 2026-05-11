# logspout-dsl

**Producer YAML** 的解析、校验与合并：声明式 **`template`** + **`fields`** + 嵌套 **`sink:`**（`kafka` \| `file` \| `stdout`）。**`logspout start`** 可将多个 YAML **按顺序合并**为一份后再序列化交给 daemon。

实现要点：内置字段类型（`FieldSpec`）、Handlebars 渲染、sink 摘要 `format_sink_summary` 等。

## 顶层结构

| 键 | 含义 |
|----|------|
| `min-interval` | 可选；两次输出之间的最小间隔（毫秒），用于限速 |
| `template` | Handlebars 模板，`{{field_name}}` 占位 |
| `fields` | 字段名 → **`type:`** 内置描述（见下表） |
| `sink` | **嵌套块**，必须含 **`type`** |

避免字段名 **`len`**、**`if`** 等与 Handlebars 内置 helper 冲突（见仓库 **`etc/apache.schema.yaml`** 注释）。

## `sink`

| 键 | 含义 |
|----|------|
| `type` | **`file`** \| **`stdout`** \| **`kafka`** |
| `output` | **仅 `file` 需要**：相对 **`worker_output_dir`**（[`logspout-config`](../logspout-config/README.md)） |
| `max-size` | 整数（字节）或字符串（如 **`64KiB`**、**`10MiB`**，1024 进制）；超限则清空文件再继续；**`0` 或不写 = 不限制** |

### `type: file`

写文件；示例：**[`etc/apache.sink.file.yaml`](../etc/apache.sink.file.yaml)**。

### `type: stdout`

标准输出。

### `type: kafka`

行发到 Kafka；**不需要** `output`。参数在 **`sink.kafka`**（broker、topic、SSL 等）。示例：**[`etc/apache.sink.kafka.yaml`](../etc/apache.sink.kafka.yaml)**（勿提交真实口令）。

## `fields`：内置 `type`

YAML 中为 **`type: <kebab-case>`**。以下为常用类型（完整枚举见 **`src/builtins.rs`** 的 `FieldSpec`）。

| type | 说明 |
|------|------|
| `uuid-v4` | 随机 UUID |
| `name-en` | 英文人名风格 |
| `ipv4` | 随机 IPv4 |
| `timestamp` | 当前时间；**`format`** 为 strftime |
| `pick` | **`values`** 列表均匀随机 |
| `integer` | **`min`**–**`max`** 闭区间随机整数 |
| `sentence` | 随机英文词组；**`min`**/**`max`** 控制词数 |
| `url` | 随机绝对 URL |
| `url-path` | 随机 path/query，适合 HTTP 请求行 |
| `hostname` | FQDN 风格主机名 |
| `domain-suffix` | TLD |
| `lorem-word` | 单随机小写词 |
| `company-name` | 公司名 |
| `user-agent` | UA 字符串 |
| `username` | 登录名风格 |
| `counter` | 从 0 递增，`u64` 环绕 |
| `template` | 子模板：嵌套 **`template`** + **`fields`** |
| `one-of` | 多分支随机选一；分支可为**字面量**或内嵌 **`template`/`fields`**；**仅选中分支求值**（lazy） |

## 合并多个 YAML

```bash
logspout start schema.yaml sink.yaml
```

后者覆盖前者同名键；仓库示例：**`etc/apache.schema.yaml`** + **`etc/apache.sink.file.yaml`**。

更多示例：**[`etc/`](../etc/)**（Apache Combined、RFC 5424、LEEF 等）。
