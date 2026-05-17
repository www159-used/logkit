//! 行日志 sink 库侧统一错误（供 [`super::LogLineSink`]、`build_line_sink` 等使用）。

use thiserror::Error;

/// Kafka sink 启动期配置/字段校验错误（与运行期 broker 交互失败区分）。
#[derive(Debug, Error)]
#[error("{0}")]
pub struct KafkaConfigError(String);

impl KafkaConfigError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    KafkaConfig(#[from] KafkaConfigError),

    #[error("kafka: {0}")]
    Kafka(String),

    #[error(transparent)]
    Ssl(#[from] java_ssl_pem::JavaSslPemError),

    /// `validate_sink` 之后不应出现的内部一致性问题。
    #[error("sink: {0}")]
    Internal(String),
}
