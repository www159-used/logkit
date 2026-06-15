# start

读取**单份**实例 YAML，校验后交给 **`logend`** 启动一个 worker 任务。

## 用法

```bash
logen start CONFIG.yaml
```

| 参数 | 说明 |
|------|------|
| **CONFIG.yaml** | 实例配置文件路径；须含 `template`、`fields`、`sink` 等（见 [logen-dsl](../../logen-dsl/intro.md)） |

## 行为

1. 在 CLI 侧读取文件，用 **`logen-dsl`** 解析并 **`validate_sink`**。
2. 将规范化后的 YAML 作为 **`instance_yaml`**，经 gRPC **`StartWorker`** 发给 daemon（**内存传递**，不在 CLI 侧落盘副本）。
3. **`config_label`** 为你在命令行传入的路径字符串（用于 `stat` 展示等）。
4. 标准输出：`{id}\t{status}`（制表符分隔）。

## 示例

```bash
$ logen start etc/apache.combined.file.yaml
f47ac10b-58cc-4372-a567-0e02b2c3d479    started
```

仓库根目录 [`etc/`](../../../../etc/) 提供多种格式示例。

## 配置要求

- **TOML** 中 **`[worker].worker_output_dir`** 必须已配置（`logend` 启动时会校验）。
- **`sink.type: file`** 时，`output` 为相对 **`worker_output_dir`** 的路径。
- **`sink.type: kafka`** 时，需 broker、TLS 等满足 [logen-dsl · Kafka](../../logen-dsl/sink/kafka.md)。

## 消息大小

整份 YAML 作为单次 RPC 载荷。若文件很大，需调大 TOML **`[protocol.grpc].max_encoding_message_size_bytes`**（及 daemon 侧 decoding 上限），见 [配置与套接字](../reference/config.md)。
