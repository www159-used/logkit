# logend

**控制面守护进程**：在 **Unix 套接字**（及可选 **TCP**）上提供 **gRPC**（[`logen-proto`](../logen-proto/README.md)），在进程内通过 **`logen-worker`** 库启动造日志任务。

## 启动

```bash
logend [--defaults-file PATH]
```

环境变量 **`LOGEN_DEFAULTS_FILE`** 可代替 `--defaults-file`（与 [`logen-config`](../logen-config/README.md) 约定一致）。

前台常驻；生产环境可用 systemd、supervisor 等托管。

## 运行目录与文件

由 TOML **`[logend].tmp_dir`** 决定单实例根目录，通常包含：

| 路径 | 说明 |
|------|------|
| `{tmp_dir}/logend.sock` | gRPC UDS 监听（可用 `[logend].socket` 覆盖） |
| `{tmp_dir}/logend.pid` | 进程 pid |
| `{tmp_dir}/logend.log` | 运行日志 |

**多实例并行**：必须为每个 daemon 配置**不同的 `[logend].tmp_dir`**。

可选 **`[logend].listen`** 开启 TCP（如 `0.0.0.0:19407`），供远端 **logen** 连接。

## 诊断日志

- TOML **`[logend].log_level`**（默认 **`info`**）：未设置 **`RUST_LOG`** 时作为 tracing 默认规格。
- 若设置了 **`RUST_LOG`**，则**优先环境变量**。
- 日志**仅写入** `{tmp_dir}/logend.log`；前台排查请 `tail -f` 该文件。

## 与 worker 的关系

- **`logen start`** 把实例 YAML 发给 daemon；daemon 分配 id 并 **`TokioEmbeddedWorker`** 驱动循环。

心跳、`worker_output_dir`、gRPC 消息大小等均在 **[`logen-config`](../logen-config/README.md)** 的 **`[logend]`** 段。

## 客户端连接

使用 **`logen`** CLI（见 [`logen/README.md`](../logen/README.md)）；本地用 **`-S`**，远端用 **`-H`/`-P`** 或 **`[client]`** 配置。
