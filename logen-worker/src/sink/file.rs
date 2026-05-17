use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use async_trait::async_trait;

use super::{LogLineSink, SinkError};

/// 追加写本地文件；`max_size > 0` 时超过则截断为空再继续写（与历史 worker 行为一致）。
pub struct FileLineSink {
    writer: BufWriter<File>,
    max_size: u64,
}

impl FileLineSink {
    /// `rel` 相对 **`output_base`**（嵌入 daemon 时为 `[worker].worker_output_dir`）。
    pub fn open(output_base: &Path, rel: &str, max_size: u64) -> Result<Self, SinkError> {
        let path = output_base.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(SinkError::from)?;
        }
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(SinkError::from)?;
        Ok(Self {
            writer: BufWriter::new(f),
            max_size,
        })
    }
}

#[async_trait]
impl LogLineSink for FileLineSink {
    async fn emit_line(&mut self, line: &str) -> Result<(), SinkError> {
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
