# logkit 介绍

Rust 写的日志调试 / 灌流工具链：**`logend`** 在 Unix 套接字上提供 gRPC 控制面，**`logen`** 是 CLI；造日志在 daemon 进程内由 **`logen-worker`** 完成，实例配置用 **`logen-dsl`** 描述的 YAML（`template` / `fields` / `sink`）。

本项目旨在解放 heka 的奴役，以及对旧工具的改造。

**thanks to: [logspout](https://github.com/jiwen624/logspout)**（本仓库已更名为 logen，与上游项目无隶属关系）

许可：**GPL-3.0**（见 [`LICENSE`](../../LICENSE)）。

## 组件

| Crate | 作用 |
|-------|------|
| **logend** | 守护进程，监听 UDS，调度 worker 实例 |
| **logen** | 客户端 CLI |
| **logen-worker** | 渲染模板并写入 stdout / 文件 / Kafka |
| **logen-dsl** | 实例 YAML 解析与校验 |
| **logen-config** | 共用 TOML（`tmp_dir`、心跳、输出目录等） |
| **logen-proto** | gRPC 定义与生成代码 |

各 crate 细节见仓库内对应 `README.md`。

## 参考

- [logen-dsl](../../logen-dsl/guide/book/index.html)