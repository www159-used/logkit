# echo

将参数原文经 daemon **回显**，用于验证 gRPC 与消息大小配置是否正常。

## 用法

```bash
logen echo [TEXT...]
```

`TEXT` 为**必填**（至少一段文字）；多段参数以空格拼接为一条消息。

## 行为

- 调用 **`Echo`** RPC，daemon 将 `message` 原样写回响应。
- 标准输出打印回显内容，末尾**无**额外换行（与 `ping` 不同，取决于 daemon 返回内容）。

## 示例

```bash
$ logen echo hello logkit
hello logkit
```

## 何时使用

- `ping` 通过后，确认读写路径与编码无问题。
- 粗测 `max_encoding_message_size_bytes` / `max_decoding_message_size_bytes` 是否过小（极大 payload 可能失败）。

## 相关

- gRPC 消息上限：[配置与套接字](../reference/config.md) · `[protocol.grpc]`
