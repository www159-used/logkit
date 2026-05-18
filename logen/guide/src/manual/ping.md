# ping

探测 **`logend`** 是否在监听，并打印配置中的应答文案。

## 用法

```bash
logen ping
```

## 行为

- 经 Unix 套接字建立 gRPC 连接，调用 **`Ping`** RPC。
- 标准输出打印 TOML **`[protocol.grpc].ping_reply_text`**（默认一般为 `pong`，以你的 `--defaults-file` 为准）。

## 示例

```bash
$ logen ping
pong
```

## 何时使用

- 确认 daemon 已启动、套接字路径正确。
- 在 `start` 之前做最小链路检查（亦可配合 [echo](echo.md)）。

## 相关

- 套接字与 TOML：[配置与套接字](../reference/config.md)
- 报错「socket 不存在」：[常见问题](../reference/troubleshooting.md)
