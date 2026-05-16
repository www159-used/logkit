# logen-worker

造日志的**库**：由 **`logend`** 在进程内通过 **`EmbeddedProducerWorker`** 驱动；**不提供独立命令行入口**。

## 与 daemon 的关系

- 控制面：**`logen`** → **`logend`**（gRPC over Unix socket）。
- 造数执行：daemon 内 **`TokioEmbeddedProducerWorker`** 启动 Tokio 任务，直接消费内存中的配置并运行（可选 **`ProducerHeartbeatEnv`** 向 daemon 上报心跳）。

## Kafka

**`sink.type: kafka`** 走 **[rdkafka](https://github.com/fede1024/rust-rdkafka)**（**CMake**；TLS 为 **openssl vendored**；依赖里含 **curl-sys**，交叉 **musl** 时注意与仓库 **`scripts/logkit-pack.sh`** 一致）。**`.jks`** 用 **`jks`** crate 转 PEM；**`.p12`/`.pfx`** 仍要本机 **`openssl pkcs12`**。字段与 YAML 约定见 **[`logen-dsl` 规范](../logen-dsl/guide/src/intro.md)**（mdBook：`cd ../logen-dsl/guide && mdbook build`）。

**musl 交叉**：推荐 **`cargo-zigbuild`**；打发行包可直接 **`./scripts/logkit-pack.sh musl`**。
