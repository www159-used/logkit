//! `logen-worker`：造日志库；由 **`logend`** 进程内嵌入（[`daemon_api::EmbeddedWorker`]、`runtime` 内存配置运行入口）。

pub mod daemon_api;
pub mod runtime;
pub mod sink;

pub use daemon_api::{EmbeddedWorker, SpawnedWorkerTasks, TokioEmbeddedWorker};
pub use runtime::WorkerHeartbeatEnv;
pub use sink::{
    build_line_sink, FileLineSink, KafkaConfigError, KafkaLineSink, LogLineSink, SinkError,
    StdoutLineSink,
};

/// **仅供集成测试** [`tests/kafka_probe`]：对集群发 metadata 请求并返回 `(broker 数, topic 元数据条目数)`。
#[doc(hidden)]
pub fn probe_kafka_ssl_cluster(
    k: &logen_dsl::KafkaConfig,
) -> Result<(usize, usize), SinkError> {
    sink::kafka::probe_kafka_ssl_cluster(k)
}

/// **仅供集成测试** [`tests/kafka_probe`]：按当前 TLS 配置向配置中的 topic **发送一条** UTF-8 文本。
#[doc(hidden)]
pub fn produce_one_kafka_ssl_line(
    k: &logen_dsl::KafkaConfig,
    payload: &str,
) -> Result<(), SinkError> {
    sink::kafka::produce_one_kafka_ssl_line(k, payload)
}
