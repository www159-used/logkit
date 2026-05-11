# logspout-daemon

**控制面守护进程**：在 **Unix 套接字**上提供 **gRPC**（[`logspout-proto`](../logspout-proto/README.md)），在进程内通过 **`logspout-worker`** 库启动造日志任务（与独立二进制 `logspout-worker` 共用实现）。

## 启动

```bash
logspout-daemon [--defaults-file PATH]
```

环境变量 **`LOGSPOUT_DEFAULTS_FILE`** 可代替 `--defaults-file`（与 [`logspout-config`](../logspout-config/README.md) 约定一致）。

前台常驻；生产环境可用 systemd、supervisor 等托管。

## 运行目录与文件

由 TOML **`[common].tmp_dir`** 决定单实例根目录，通常包含：

| 路径 | 说明 |
|------|------|
| `{tmp_dir}/logspout-daemon.sock` | gRPC 监听 |
| `{tmp_dir}/logspout-daemon.pid` | 进程 pid |
| `{tmp_dir}/logspout-daemon.log` | 日志（若实现写入） |

**多实例并行**：必须为每个 daemon 配置**不同的 `tmp_dir`**，否则套接字冲突。

## 与 worker 的关系

- **`logspout start`**（CLI）把合并后的 producer YAML 发给 daemon；daemon 分配 id、落盘副本并 **`TokioEmbeddedProducerWorker`** 驱动循环。
- 调试时可单独运行 **`logspout-worker -f`**（见 [`logspout-worker`](../logspout-worker/README.md)），不必经过 daemon。

心跳间隔、超时、`worker_output_dir` 等见 **[`logspout-config`](../logspout-config/README.md)** 的 **`[worker]`** 段。

## 客户端连接

使用仓库 **`logspout`** CLI（见 [`logspout/README.md`](../logspout/README.md)）；确保双方共用同一 **`tmp_dir`** 或对 CLI 使用 **`-S`**。
