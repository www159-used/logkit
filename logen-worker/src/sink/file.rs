use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use async_trait::async_trait;

use super::{EmitLineError, LogLineSink};

/// 追加写本地文件；`max_size > 0` 时超过则截断为空再继续写（与历史 worker 行为一致）。
pub struct FileLineSink {
    writer: BufWriter<File>,
    max_size: u64,
}

impl FileLineSink {
    /// `rel` 相对 **`output_base`**（独立进程时常为当前工作目录；嵌入 daemon 时为 `[worker].worker_output_dir`）。
    pub fn open(output_base: &Path, rel: &str, max_size: u64) -> Result<Self, String> {
        let path = output_base.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("create_dir_all {}: {e}", parent.display()))?;
        }
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("open output {}: {e}", path.display()))?;
        Ok(Self {
            writer: BufWriter::new(f),
            max_size,
        })
    }
}

#[async_trait]
impl LogLineSink for FileLineSink {
    async fn emit_line(&mut self, line: &str) -> Result<(), EmitLineError> {
        writeln!(&mut self.writer, "{line}")
            .and_then(|_| self.writer.flush())?;
        if self.max_size == 0 {
            return Ok(());
        }
        let f = self.writer.get_mut();
        let meta = f.metadata()?;
        if meta.len() <= self.max_size {
            return Ok(());
        }
        f.set_len(0).and_then(|_| f.seek(SeekFrom::Start(0)))?;
        Ok(())
    }
}
