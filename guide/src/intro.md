# logkit 介绍

本项目旨在解放 heka 的奴役，以及对旧工具的改造。

**thanks to: [logspout](https://github.com/jiwen624/logspout)**（本仓库已更名为 logen，与上游项目无隶属关系）

许可：**AGPL-3.0**（见 [`LICENSE`](../../LICENSE)）。

## 组件

| Crate | 作用 |
|-------|------|
| **logend** | 守护进程，监听 UDS，调度 worker 实例 |
| **logen** | 客户端 CLI |
| **logen-worker** | 渲染模板并写入 stdout / 文件 / Kafka |
| **logen-dsl** | YAML 文件解析与校验 |
| **logen-config** | 项目内约定，类似mysql的client段。共用 TOML（`tmp_dir`、心跳、输出目录等） |
| **logen-proto** | 进程间通信协议 |

各 crate 细节见仓库内对应 `README.md`。

## 文档

```bash
cd guide && mdbook build
# 本地预览：mdbook serve --open
```

- [logen-dsl 配置规范](logen-dsl/intro.md)（源码目录 `guide/src/logen-dsl/`）