# logend

**控制面守护进程**：在 **Unix 套接字**上提供 **gRPC**（[`logen-proto`](../logen-proto/README.md)），在进程内通过 **`logen-worker`** 库启动造日志任务。

## 启动

```bash
logend [--defaults-file PATH]
```

环境变量 **`LOGEN_DEFAULTS_FILE`** 可代替 `--defaults-file`（与 [`logen-config`](../logen-config/README.md) 约定一致）。

前台常驻；生产环境可用 systemd、supervisor 等托管。

## 运行目录与文件

由 TOML **`[common].tmp_dir`** 决定单实例根目录，通常包含：

| 路径 | 说明 |
|------|------|
| `{tmp_dir}/logend.sock` | gRPC 监听 |
| `{tmp_dir}/logend.pid` | 进程 pid |
| `{tmp_dir}/logend.log` | 运行日志（**tracing-subscriber**，**追加**写入，compact 格式含时间戳与 span 字段） |

**多实例并行**：必须为每个 daemon 配置**不同的 `tmp_dir`**，否则套接字冲突。

## 诊断日志

- TOML **`[daemon].log_level`**（默认 **`info`**）：在未设置环境变量 **`RUST_LOG`** 时作为 **`EnvFilter`** 默认规格。
- 若设置了 **`RUST_LOG`**，则**优先环境变量**（`tracing_subscriber::EnvFilter` 语义，与常见 `RUST_LOG=debug` 用法一致）。
- 示例：`RUST_LOG=debug`、`RUST_LOG=logend=trace`（**Ping**、**Heartbeat** 等为 `trace`，避免默认级别刷盘过快）。
- 日志**仅写入** `{tmp_dir}/logend.log`，不复制到 stderr/stdout；前台排查请 `tail -f` 该文件。

## 与 worker 的关系

- **`logen start`**（CLI）把实例 YAML 发给 daemon；daemon 分配 id、落盘副本并 **`TokioEmbeddedWorker`** 驱动循环。

心跳间隔、超时、`worker_output_dir` 等见 **[`logen-config`](../logen-config/README.md)** 的 **`[worker]`** 段。

## 客户端连接

使用仓库 **`logen`** CLI（见 [`logen/README.md`](../logen/README.md)）；确保双方共用同一 **`tmp_dir`** 或对 CLI 使用 **`-S`**。
