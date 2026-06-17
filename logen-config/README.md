# logen-config

**共用 TOML**：内嵌一份参考默认配置，与用户 **`--defaults-file`**（或环境变量 **`LOGEN_DEFAULTS_FILE`**）**深度合并**，后者覆盖前者。

配置仅 **2 段**：**`[client]`**（logen）与 **`[logend]`**（logend）。

公开入口：**`load_merged`**、**`LogenConfig`**、**`resolve_client_connect`**、**`LogenError`** 等。

## 指定配置文件

| 方式 | 说明 |
|------|------|
| `--defaults-file PATH` | **`logen`** / **`logend`** 命令行 |
| `LOGEN_DEFAULTS_FILE` | 非空路径时，等价于在 argv 最前插入 `--defaults-file` |

## `[client]`（logen）

| 键 | 含义 |
|----|------|
| `transport` | **`unix`**（默认）或 **`tcp`** |
| `host` / `port` | TCP 模式必填；也可用 CLI **`-H` / `-P`** 或 **`LOGEN_HOST` / `LOGEN_PORT`** |
| `socket` | 可选；覆盖 Unix 套接字路径（默认与 `[logend]` UDS 相同） |

CLI **`-S` / `--sock`** 优先于 TCP 设置。

## `[logend]`（logend）

| 键 | 含义 |
|----|------|
| `tmp_dir` | 单实例根目录；默认其下有 **`logend.sock`**、**`logend.pid`**、**`logend.log`** |
| `pid_record_suffix` | pid 文件末尾额外字节 |
| `log_level` | 未设置 **`RUST_LOG`** 时的 tracing 默认规格 |
| `socket` | 可选；覆盖 UDS 路径 |
| `listen` | 可选；TCP 监听（如 `0.0.0.0:19407`） |
| `worker_output_dir` | **必填**；`sink.type: file` 时 `output` 相对此目录 |
| `heartbeat_timeout_secs` / `heartbeat_interval_secs` | 心跳与健康判定 |
| `runtime_threads` | worker tokio runtime 线程数（可选） |
| `max_decoding_message_size_bytes` / `max_encoding_message_size_bytes` | gRPC 消息大小上限 |
| `ping_reply_text` | `logen ping` 返回值 |

## 示例

**logend 机器**（`/etc/logkit/logend.toml`）：

```toml
[logend]
tmp_dir = "/var/lib/logkit"
listen = "0.0.0.0:19407"
worker_output_dir = "/data/logkit/output"
heartbeat_timeout_secs = 30
heartbeat_interval_secs = 1
max_decoding_message_size_bytes = 4194304
max_encoding_message_size_bytes = 4194304
ping_reply_text = "PONG"
```

**远端 logen**（`~/.logen/remote.toml`，可仅含 `[client]`，其余走内嵌默认合并）：

```toml
[client]
transport = "tcp"
host = "10.0.0.5"
port = 19407
```

```bash
export LOGEN_DEFAULTS_FILE=~/.logen/remote.toml
logen list
```

## 相关文档

- **[`logen`](../logen/README.md)**、**[`logend`](../logend/README.md)**
-实例 YAML：**[`logen-dsl` 规范](../guide/src/logen-dsl/intro.md)**
