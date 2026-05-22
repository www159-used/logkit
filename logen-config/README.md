# logen-config

**共用 TOML**：内嵌一份参考默认配置，与用户 **`--defaults-file`**（或环境变量 **`LOGEN_DEFAULTS_FILE`**）**深度合并**，后者覆盖前者。

公开入口：**`load_merged`**、**`LogenConfig`**、**`LogenError`** 等。

## 指定配置文件

| 方式 | 说明 |
|------|------|
| `--defaults-file PATH` | **`logen`** / **`logend`** 命令行 |
| `LOGEN_DEFAULTS_FILE` | 非空路径时，等价于在 argv 最前插入 `--defaults-file`（见 crate 内 `inject_defaults_file_from_env`） |

## 参考模板位置

仓库内全文：**[`assets/conf.ref.toml`](assets/conf.ref.toml)**（构建时嵌入；注释即文档）。

## 常用段摘要

### `[common]`

| 键 | 含义 |
|----|------|
| `tmp_dir` | 单实例根目录；其下有 **`logend.sock`**、**`logend.pid`**、日志等。**多实例须不同 `tmp_dir`**。 |

### `[daemon]`（`logend`）

| 键 | 含义 |
|----|------|
| `pid_record_suffix` | 写入 pid 文件末尾的额外字节（如换行）。 |
| `log_level` | 默认 **`info`**。传给 **logend** 的 **`tracing_subscriber::EnvFilter`**：仅当未设置 **`RUST_LOG`** 时作为默认规格。 |

### `[protocol.grpc]`

| 键 | 含义 |
|----|------|
| `max_decoding_message_size_bytes` / `max_encoding_message_size_bytes` | gRPC 消息大小上限；`start` 会传送整份实例 YAML。 |
| `ping_reply_text` | `logen ping` 返回值。 |
| `client_connect_uri` | tonic 所需的**形式合法** HTTP URI；**实际仍为 Unix 套接字传输**，不对该主机建 TCP。 |

### `[worker]`

| 键 | 含义 |
|----|------|
| `worker_output_dir` | **必填**。**`sink.type: file`** 时，`output` 相对此目录；daemon 托管的配置副本也在此目录树约定位置。 |
| `heartbeat_timeout_secs` / `heartbeat_interval_secs` | 心跳与健康判定（见 `logen list` / `stat`）。 |

若实例 YAML 使用 **Kafka**（`sink.type: kafka`），行日志发往 Kafka，**不再使用**基于 `worker_output_dir` 的文件 `output`（与 `conf.ref.toml` 注释一致）。

### 兼容

合并后 **`[log_server]`**、**`[log_worker]`** 会并入 **`[worker]`**，便于旧配置迁移。

## 相关文档

- 客户端与 daemon：**[`logen`](../logen/README.md)**、**[`logend`](../logend/README.md)**  
-实例 YAML 语法：**[`logen-dsl` 规范](../guide/src/logen-dsl/intro.md)**（`cd guide && mdbook build`）  
