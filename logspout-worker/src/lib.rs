//! `logspout-worker`：模板日志行输出（[`LogLineSink`]）与可嵌入 daemon 的 API（[`runtime::run_producer_at_path`]、[`daemon_api::EmbeddedProducerWorker`]）。

pub mod daemon_api;
pub mod runtime;
pub mod sink;

pub use daemon_api::{EmbeddedProducerWorker, SpawnedProducerTasks, TokioEmbeddedProducerWorker};
pub use runtime::{run_producer_at_path, ProducerHeartbeatEnv};
pub use sink::{
    build_line_sink, EmitLineError, FileLineSink, KafkaLineSink, KafkaLineSinkError, LogLineSink,
    StdoutLineSink,
};

/// **仅供集成测试** [`tests/kafka_probe`]：对集群发 metadata 请求并返回 `(broker 数, topic 元数据条目数)`。
///
/// 实现位于 [`sink::kafka::probe_kafka_ssl_cluster`]，为 `pub(crate)`；集成测试 crate 无法直接引用，故在此做薄转发。
/// 与 [`KafkaLineSink`] 使用相同的 librdkafka 客户端配置路径。**非稳定对外 API**，故 `#[doc(hidden)]`。
#[doc(hidden)]
pub fn probe_kafka_ssl_cluster(
    k: &logspout_dsl::KafkaConfig,
) -> Result<(usize, usize), KafkaLineSinkError> {
    sink::kafka::probe_kafka_ssl_cluster(k)
}

/// **仅供集成测试** [`tests/kafka_probe`]：按当前 TLS 配置向配置中的 topic **发送一条** UTF-8 文本（用于可选冒烟 produce）。
///
/// 实现位于 [`sink::kafka::produce_one_kafka_ssl_line`]，为 `pub(crate)`；集成测试 crate 无法直接引用，故在此做薄转发。
/// 与 [`KafkaLineSink`] 使用相同的 librdkafka / TLS 材料路径。**非稳定对外 API**，故 `#[doc(hidden)]`。
#[doc(hidden)]
pub fn produce_one_kafka_ssl_line(
    k: &logspout_dsl::KafkaConfig,
    payload: &str,
) -> Result<(), KafkaLineSinkError> {
    sink::kafka::produce_one_kafka_ssl_line(k, payload)
}
