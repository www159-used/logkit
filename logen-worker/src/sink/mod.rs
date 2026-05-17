//! 一行日志的输出目标：统一由 [`LogLineSink`] 约束。

mod context_id;
mod error;
mod file;
pub(crate) mod kafka;
mod kafka_agent;
mod kafka_jks;
mod log_id;
mod stdout;

pub use error::{KafkaConfigError, SinkError};
pub use file::FileLineSink;
pub use kafka::KafkaLineSink;
pub use stdout::StdoutLineSink;

use std::path::Path;

use async_trait::async_trait;
use logen_dsl::SinkConfig;

#[async_trait]
pub trait LogLineSink: Send {
    async fn emit_line(&mut self, line: &str) -> Result<(), SinkError>;
}

/// 按 [`SinkConfig`] 构造行日志 sink（须已通过 [`validate_sink`](logen_dsl::validate_sink)）。
pub fn build_line_sink(
    sink: &SinkConfig,
    output_base: &Path,
) -> Result<Box<dyn LogLineSink>, SinkError> {
    match sink {
        SinkConfig::Kafka { kafka: Some(k), .. } => {
            let s = KafkaLineSink::new(k)?;
            Ok(Box::new(s))
        }
        SinkConfig::Kafka { kafka: None, .. } => Err(SinkError::Internal(
            "sink.type kafka but sink.kafka missing after validation".into(),
        )),
        SinkConfig::File {
            output,
            max_size_bytes,
            ..
        } => {
            let rel = output
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    SinkError::Internal(
                        "sink.type file but output missing after validation".into(),
                    )
                })?;
            Ok(Box::new(FileLineSink::open(
                output_base,
                rel,
                *max_size_bytes,
            )?))
        }
        SinkConfig::Stdout => Ok(Box::new(StdoutLineSink)),
    }
}
