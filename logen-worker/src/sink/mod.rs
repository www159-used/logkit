//! 一行日志的输出目标：统一由 [`LogLineSink`] 约束。

mod context_id;
mod error;
mod file;
pub(crate) mod kafka;
pub(crate) mod kafka_agent;
mod kafka_jks;
mod log_id;
mod stdout;

pub use error::{KafkaConfigError, SinkError};
pub use file::FileLineSink;
pub use kafka::KafkaLineSink;
pub use stdout::StdoutLineSink;

use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use async_trait::async_trait;
use logen_model::SinkConfig;
use tokio::sync::mpsc;

#[async_trait]
pub trait LogLineSink: Send {
    /// 消费 render→sink 通道中的行直至发送端关闭。
    async fn drain_lines(&mut self, line_rx: mpsc::Receiver<String>) -> Result<(), SinkError>;
}

/// 按 [`SinkConfig`] 构造行日志 sink（Kafka/stdout 等约束在 script / `finalize_worker_config` 侧保证；
/// File 的缺省 `output` 由 [`SinkConfig::fill_default_output`](logen_model::SinkConfig::fill_default_output) 补全）。
pub fn build_line_sink(
    sink: &SinkConfig,
    worker_id: &str,
    retry_total: Arc<AtomicU64>,
) -> Result<Box<dyn LogLineSink>, SinkError> {
    match sink {
        SinkConfig::Kafka { kafka: Some(k), .. } => {
            let s = KafkaLineSink::new(k, worker_id, retry_total)?;
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
            let path = output
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    SinkError::Internal("sink.type file but output missing after validation".into())
                })?;
            Ok(Box::new(FileLineSink::open(
                Path::new(path),
                *max_size_bytes,
            )?))
        }
        SinkConfig::Stdout => Ok(Box::new(StdoutLineSink)),
    }
}
