//! `logspout-worker`：模板日志行输出（[`LogLineSink`]）与可嵌入 daemon 的 API（[`runtime::run_producer_at_path`]、[`daemon_api::EmbeddedProducerWorker`]）。

pub mod daemon_api;
pub mod runtime;
pub mod sink;

pub use daemon_api::{EmbeddedProducerWorker, TokioEmbeddedProducerWorker};
pub use runtime::{run_producer_at_path, ProducerHeartbeatEnv};
pub use sink::{
    build_line_sink, validate_kafka_config, FileLineSink, KafkaLineSink, LogLineSink,
    StdoutLineSink,
};
