# logen-proto

**gRPC 契约**：**`protobuf`** 定义 + **`tonic`** 生成 Rust 服务端 / 客户端代码。

## 定义文件

- **`proto/logen/v1/logen.proto`** — `package logen.v1`，服务 **`Logen`**（`Ping`、`Echo`、`ListWorkers`、`RunControlScript`、`StopWorker`、`CatWorker`、`Heartbeat`、`StatWorker`）。
- **`proto/agent/v1/event.proto`** — `package logen.agent.v1`，消息 **`EventInfo`**（Kafka agent 模式 `format: pb` 的 value 编码；字段与上游 log_parser 兼容）。

传输：**Unix 套接字**或 **TCP**（由 `[client]` / `[logend].bind`+`port` 配置）；控制面仍为 gRPC，见 [`logen-config`](../logen-config/README.md)。

## 生成代码

由 **`build.rs`** 在构建时执行 **`tonic_build::compile_protos`**；修改 `.proto` 后 **`cargo build`** 即可重新生成，无需手抄生成结果。

## 使用者

- **`logen`**：客户端（[`logen/README.md`](../logen/README.md)）
- **`logend`**：服务端（[`logend/README.md`](../logend/README.md)）
- **`logen-worker`**：嵌入 daemon 的任务内可选 **`Heartbeat`** 客户端；agent Kafka **`format: pb`** 使用 **`logen_proto::EventInfo`**（[`logen-worker/README.md`](../logen-worker/README.md)）
