//! 一行日志的输出目标：统一由 [`LogLineSink`] 约束，便于新增 syslog、gRPC 等实现。

mod file;
pub(crate) mod kafka;
mod kafka_jks;
mod stdout;

pub use file::FileLineSink;
pub use kafka::{validate_kafka_config, KafkaLineSink, KafkaLineSinkError};
pub use stdout::StdoutLineSink;

use std::path::Path;

use async_trait::async_trait;
use logspout_dsl::{LineSinkType, TemplateConfig};

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
    match cfg.sink.sink_type {
        LineSinkType::Kafka => {
            let k = cfg
                .sink
                .kafka
                .as_ref()
                .expect("validate_template_sink ensures kafka");
            Ok(Box::new(
                KafkaLineSink::try_new(k).map_err(|e| e.to_string())?,
            ))
        }
        LineSinkType::File => {
            let rel = cfg
                .sink
                .output
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .expect("validate_template_sink ensures output");
            Ok(Box::new(FileLineSink::open(
                output_base,
                rel,
                cfg.sink.max_size_bytes,
            )?))
        }
        LineSinkType::Stdout => Ok(Box::new(StdoutLineSink)),
    }
}
