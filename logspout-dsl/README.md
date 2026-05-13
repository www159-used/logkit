# logspout-dsl

**Worker 模板配置 YAML** 的解析与校验：声明式 **`template`** + **`fields`** + 嵌套 **`sink:`**（`kafka` \| `file` \| `stdout`）。**`logspout start`** 读取单份 `.yaml` / `.yml`，经 [`parse_template_config`](src/runner.rs) 校验后序列化交给 daemon；Serde 映射见 [`worker_config.rs`](src/worker_config.rs)。

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

行发到 Kafka；**不需要** `output`。参数在 **`sink.kafka:`**（`brokers`、`topic`、可选 **`headers:`**，以及 worker 识别的 `acks`、`timeout-ms`、`compression`、`security.protocol`、`ssl.*`、`sasl.*` 等字段）。**`ssl.truststore.location` / `ssl.keystore.location`** 支持 **`.pem` / `.crt` / `.jks` / `.p12`**：**`.jks`** 由 **`jks`**（纯 Rust）解析，**无需 `keytool`/JDK**；**`.p12`/`.pfx`** 仍调用 **`openssl pkcs12`**（须在 `PATH`）。并配置 **`ssl.truststore.password`** / **`ssl.keystore.password`**；客户端 **JKS** 含多个私钥时默认取**别名升序第一条**（可选 **`ssl.keystore.alias`** 显式指定）。示例：**[`etc/apache.sink.kafka.yaml`](../etc/apache.sink.kafka.yaml)**（勿提交真实口令）。

**`sink.kafka.headers`**（可选）：YAML 可解析并参与展示；**`logspout-worker`** 经 **rdkafka（librdkafka）** 会作为 **Kafka record headers** 逐条发送。值类型：**字符串** / **整数、浮点** / **布尔** / **`null`**（空值 header）；**不支持**嵌套 mapping / array。

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

## 单文件 producer

**`logspout start`** 仅接受一份 YAML，须同时含 `template` / `fields` / **`sink:`**（及 `sink.type`）等必填项。若习惯拆分 schema 与 sink，请先用编辑器或 **`yq`** 等工具合成一份后再启动。仓库单文件示例：**[`etc/apache.combined.file.yaml`](../etc/apache.combined.file.yaml)**。

更多片段与组合参考：**[`etc/`](../etc/)**（Apache Combined、RFC 5424、LEEF 等）。
