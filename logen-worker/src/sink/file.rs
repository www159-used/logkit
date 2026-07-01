use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::{LogLineSink, SinkError};

/// 追加写本地文件；`max_size > 0` 时超过则截断为空再继续写（与历史 worker 行为一致）。
pub struct FileLineSink {
    writer: BufWriter<File>,
    max_size: u64,
}

impl FileLineSink {
    /// `path` 为 daemon 归一化后的最终绝对路径。
    pub fn open(path: &Path, max_size: u64) -> Result<Self, SinkError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(SinkError::from)?;
        }
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(SinkError::from)?;
        Ok(Self {
            writer: BufWriter::new(f),
            max_size,
        })
    }

    fn write_line(&mut self, line: &str) -> Result<(), SinkError> {
        writeln!(&mut self.writer, "{line}")
            .and_then(|_| self.writer.flush())
            .map_err(SinkError::from)?;
        if self.max_size == 0 {
            return Ok(());
        }
        let f = self.writer.get_mut();
        let meta = f.metadata().map_err(SinkError::from)?;
        if meta.len() <= self.max_size {
            return Ok(());
        }
        f.set_len(0)
            .and_then(|_| f.seek(SeekFrom::Start(0)))
            .map_err(SinkError::from)?;
        Ok(())
    }
}

#[async_trait]
impl LogLineSink for FileLineSink {
    async fn drain_lines(&mut self, mut line_rx: mpsc::Receiver<String>) -> Result<(), SinkError> {
        while let Some(line) = line_rx.recv().await {
            self.write_line(&line)?;
        }
        Ok(())
    }
}
