# 简介

**logen** 是面向 **`logend`** 的 **gRPC 客户端**：在 **Unix 域套接字**上连接守护进程，启动 / 查询 / 停止造日志 **worker 实例**。**仅支持 Unix**（无 Windows 套接字路径）。

CLI 本身不渲染日志、不写 Kafka；它把**实例 YAML**交给 daemon，由进程内的 **`logen-worker`** 执行。YAML 语法见 **[logen-dsl 规范](../logen-dsl/intro.md)**。

## 与 logend 的关系

```modern-dot
digraph {
  rankdir=LR

  user [label="你"]
  logen [label="logen CLI"]
  sock [label="Unix socket\nlogend.sock"]
  logend [label="logend"]
  worker [label="logen-worker\nTokio 任务"]
  out [label="stdout / file / Kafka"]

  user -> logen
  logen -> sock [label="gRPC over UDS"]
  sock -> logend
  logend -> worker
  worker -> out
}
```

| 组件 | 角色 |
|------|------|
| **logend** | 监听套接字、管理实例生命周期、可选心跳与健康状态 |
| **logen** | 发 `start` / `stop`、查 `list` / `stat`、调试 `ping` / `echo` |
| **logen-config** | CLI 与 daemon **共用**的 TOML（`tmp_dir`、gRPC 消息大小、输出目录等） |

实例 YAML 见 [logen-dsl](../logen-dsl/intro.md)；上手步骤见全书 [快速上手](../quick-start.md)。
