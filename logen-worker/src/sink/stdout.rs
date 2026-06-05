use async_trait::async_trait;
use tokio::sync::mpsc;

use super::{LogLineSink, SinkError};

/// 标准输出，每条一行。
pub struct StdoutLineSink;

#[async_trait]
impl LogLineSink for StdoutLineSink {
    async fn drain_lines(&mut self, mut line_rx: mpsc::Receiver<String>) -> Result<(), SinkError> {
        while let Some(line) = line_rx.recv().await {
            println!("{line}");
        }
        Ok(())
    }
}
