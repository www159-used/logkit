use async_trait::async_trait;

use super::{LogLineSink, SinkError};

/// 标准输出，每条一行。
pub struct StdoutLineSink;

#[async_trait]
impl LogLineSink for StdoutLineSink {
    async fn emit_line(&mut self, line: &str) -> Result<(), SinkError> {
        println!("{line}");
        Ok(())
    }
}
