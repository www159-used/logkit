//! `logspout-worker`：模板日志行输出（[`LogLineSink`]）与可嵌入 daemon 的 API（[`runtime::run_producer_at_path`]、[`daemon_api::EmbeddedProducerWorker`]）。

pub mod daemon_api;
mod jks_fixture;
pub mod runtime;
pub mod sink;
/// Kafka TLS 探针、一次性 produce、fixture JKS 配置（与主 sink API 分离）。
pub mod kafka_smoke;

pub use daemon_api::{EmbeddedProducerWorker, SpawnedProducerTasks, TokioEmbeddedProducerWorker};
pub use runtime::{run_producer_at_path, ProducerHeartbeatEnv};
pub use sink::{
    build_line_sink, validate_kafka_config, FileLineSink, KafkaLineSink, KafkaLineSinkError,
    LogLineSink, StdoutLineSink,
};
