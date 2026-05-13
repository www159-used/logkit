//! 一行日志的输出目标：统一由 [`LogLineSink`] 约束，便于新增 syslog、gRPC 等实现。

mod file;
pub(crate) mod kafka;
mod kafka_jks;
mod stdout;

pub use file::FileLineSink;
pub use kafka::{KafkaLineSink, KafkaLineSinkError};
pub use stdout::StdoutLineSink;

use std::path::Path;

use async_trait::async_trait;
use logspout_dsl::{SinkConfig, TemplateConfig};

/// 写入单条渲染后的日志行（UTF-8 文本）。实现可为 stdout、文件、消息队列等。
#[async_trait]
pub trait LogLineSink: Send {
    async fn emit_line(&mut self, line: &str) -> Result<(), String>;
}

/// 按 [`TemplateConfig::sink`] 构造行日志 sink（须已通过 [`validate_template_sink`]）。
pub fn build_line_sink(
    cfg: &TemplateConfig,
    output_base: &Path,
) -> Result<Box<dyn LogLineSink>, String> {
    match &cfg.sink {
        SinkConfig::Kafka { kafka: Some(k), .. } => Ok(Box::new(
            KafkaLineSink::try_new(k).map_err(|e| e.to_string())?,
        )),
        SinkConfig::Kafka { kafka: None, .. } => Err(
            "internal: sink.type kafka but sink.kafka missing after validation".into(),
        ),
        SinkConfig::File {
            output,
            max_size_bytes,
            ..
        } => {
            let rel = output
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .expect("validate_template_sink ensures output");
            Ok(Box::new(FileLineSink::open(
                output_base,
                rel,
                *max_size_bytes,
            )?))
        }
        SinkConfig::Stdout { .. } => Ok(Box::new(StdoutLineSink)),
    }
}
