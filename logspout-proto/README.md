# logspout-proto

**gRPC 契约**：**`protobuf`** 定义 + **`tonic`** 生成 Rust 服务端 / 客户端代码。

## 定义文件

- **`proto/logspout/v1/logspout.proto`** — `package logspout.v1`，服务 **`Logspout`**（`Ping`、`Echo`、`ListWorkers`、`StartWorker`、`StopWorker`、`CatWorker`、`Heartbeat`、`StatWorker`）。

传输：**Unix 套接字**（进程间仍为 gRPC 帧）；URI 仅用于 tonic Endpoint 构造，见 [`logspout-config`](../logspout-config/README.md) 的 **`client_connect_uri`**。

## 生成代码

由 **`build.rs`** 在构建时执行 **`tonic_build::compile_protos`**；修改 `.proto` 后 **`cargo build`** 即可重新生成，无需手抄生成结果。

## 使用者

- **`logspout`**：客户端（[`logspout/README.md`](../logspout/README.md)）
- **`logspout-daemon`**：服务端（[`logspout-daemon/README.md`](../logspout-daemon/README.md)）
- **`logspout-worker`**：可选 **`Heartbeat`** 客户端（[`logspout-worker/README.md`](../logspout-worker/README.md)）
