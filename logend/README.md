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
| `{tmp_dir}/logend.log` | 运行日志（`log` + **flexi_logger**，**追加**写入） |

**多实例并行**：必须为每个 daemon 配置**不同的 `tmp_dir`**，否则套接字冲突。

## 诊断日志

- TOML **`[daemon].log_level`**（默认 **`info`**）：在未设置环境变量 **`RUST_LOG`** 时作为 flexi_logger 的默认规格。
- 若设置了 **`RUST_LOG`**，则**优先环境变量**（flexi_logger `try_with_env_or_str` 语义）。
- 示例：`RUST_LOG=debug`、`RUST_LOG=logend=trace`（**Ping**、**Heartbeat** 等为 `trace`，避免默认级别刷盘过快）。
- **`warn`** 及以上会**同时复制到 stderr**（`duplicate_to_stderr(Duplicate::Warn)`），便于前台或 systemd 收集。

## 与 worker 的关系

- **`logen start`**（CLI）把 producer YAML 发给 daemon；daemon 分配 id、落盘副本并 **`TokioEmbeddedProducerWorker`** 驱动循环。

心跳间隔、超时、`worker_output_dir` 等见 **[`logen-config`](../logen-config/README.md)** 的 **`[worker]`** 段。

## 客户端连接

使用仓库 **`logen`** CLI（见 [`logen/README.md`](../logen/README.md)）；确保双方共用同一 **`tmp_dir`** 或对 CLI 使用 **`-S`**。
