# logspout-worker

造日志的**核心库**；同时提供独立二进制，便于本地调试 producer YAML，无需先起 **`logspout-daemon`**。

## 库

daemon 通过 **`EmbeddedProducerWorker`** / **`run_producer_at_path`** 在 Tokio 上下文中驱动与 CLI 二进制相同的管线。

## 独立二进制

```bash
logspout-worker -f CONFIG.yaml
```

- **`-f`**：producer YAML 路径（[`logspout-dsl`](../logspout-dsl/README.md)）。
- 输出路径解析：以**进程当前工作目录**为主推导基准（失败时退化为配置路径父目录），与 daemon 场景下统一落在 **`worker_output_dir`** 的行为不同；调试相对路径时注意 **`cwd`**。

## 可选：向 daemon 上报心跳（环境变量）

若下列变量**全部**设置，独立进程也会按间隔向控制套接字发 **`Heartbeat`**（与嵌入模式对齐统计）：

| 变量 | 含义 |
|------|------|
| `LOGSPOUT_CONTROL_SOCKET` | 控制面 Unix 套接字路径 |
| `LOGSPOUT_WORKER_ID` | 实例 id（兼容旧名 **`LOGSPOUT_SERVER_ID`**） |
| `LOGSPOUT_HEARTBEAT_INTERVAL_SECS` | 间隔秒数（解析失败默认 5，且最小为 1） |
| `LOGSPOUT_CLIENT_CONNECT_URI` | 与 TOML **`[protocol.grpc].client_connect_uri`** 相同形式的 URI |

日常通过 **`logspout start`** 时由 daemon 注入这些变量；手工跑 **`logspout-worker`** 一般无需设置。

## Kafka

Producer 含 **`sink.type: kafka`** 时使用 **`kafka-rs`** 发送；详见 [`logspout-dsl`](../logspout-dsl/README.md)。
