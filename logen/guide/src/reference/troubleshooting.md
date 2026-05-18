# 常见问题

## unix socket 不存在

**现象**

```text
unix socket "/tmp/.../logend.sock" does not exist. Start logend first, ...
```

**处理**

1. 先启动 **`logend`**。
2. 确认 CLI 与 daemon 使用同一 **`tmp_dir`**（同一 `--defaults-file` 或 `LOGEN_DEFAULTS_FILE`）。
3. 或 **`logen -S`** 指向 daemon 实际监听的 `.sock`。

## transport error on unix socket

**现象**

连接阶段失败，提示 transport error。

**处理**

- 检查套接字路径、权限（当前用户能否读写）。
- 确认没有旧 daemon 残留 pid / 套接字文件冲突；必要时清理 `tmp_dir` 后重启 logend。

## start 失败：解析 / 校验错误

**现象**

CLI 在本地报错（read / parse / validate），未得到 id。

**处理**

- 对照 [logen-dsl](../../logen-dsl/guide/book/index.html) 检查 `template`、`fields`、`sink`。
- `sink.type: file` 须非空 **`output`**；`kafka` 须 brokers、topic（或 agent 模式）等。

## start 失败：gRPC / message size

**现象**

daemon 侧拒绝或客户端报 message too large。

**处理**

- 增大 TOML **`[protocol.grpc].max_encoding_message_size_bytes`**（及 decoding 上限）。
- 精简 YAML 或拆分调试（避免在单文件内塞入巨大内联 PEM）。

## stop / cat：id 歧义

**现象**

前缀匹配到多个实例。

**处理**

- 使用 [list](manual/list.md) 中的完整 UUID，或加长前缀直至唯一。

## list 中 healthy 为 false

**现象**

`alive` 为 true 但 `healthy` 为 false。

**处理**

- 用 [stat](manual/stat.md) 看 **sec_since_hb** 与 **hb_timeout_s**。
- 检查 worker 是否阻塞（如同步 Kafka 初始化过慢）；调大超时或修复 sink 连通性。

## 非 Unix 平台

**logen** 不支持 Windows 等无 Unix domain socket 的环境；请在 Linux / macOS 上使用。

## 进一步阅读

- [logend README](../../../logend/README.md)
- [logen-config README](../../../logen-config/README.md)
- [logen-dsl 规范](../../logen-dsl/guide/book/index.html)
