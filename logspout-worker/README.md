# logspout-worker

造日志的**库**：由 **`logspout-daemon`** 在进程内通过 **`EmbeddedProducerWorker`** / **`run_producer_at_path`** 驱动；**不提供独立命令行入口**。

## 与 daemon 的关系

- 控制面：**`logspout`** → **`logspout-daemon`**（gRPC over Unix socket）。
- 造数执行：daemon 内 **`TokioEmbeddedProducerWorker`** 启动 Tokio 任务，调用本库的 **`run_producer_with_events`**（经 **`run_producer_at_path`**，可选 **`ProducerHeartbeatEnv`** 向 daemon 上报心跳）。

## Kafka

**`sink.type: kafka`** 走 **[rdkafka](https://github.com/fede1024/rust-rdkafka)**（**CMake**；TLS 为 **openssl vendored**；依赖里含 **curl-sys**，交叉 **musl** 时注意与仓库 **`scripts/logkit-pack.sh`** 一致）。**`.jks`** 用 **`jks`** crate 转 PEM；**`.p12`/`.pfx`** 仍要本机 **`openssl pkcs12`**。字段与 YAML 约定见 **[`logspout-dsl`](../logspout-dsl/README.md)**。

**musl 交叉**：推荐 **`cargo-zigbuild`**；打发行包可直接 **`./scripts/logkit-pack.sh musl`**。
