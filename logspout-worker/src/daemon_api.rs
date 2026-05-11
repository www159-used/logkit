//! 供 **`logspout-daemon`** 使用的嵌入 worker 约定：用 trait 约束「如何 spawn 造日志任务」，便于测试替换或日后其它实现。

use std::path::PathBuf;

use tokio::task::JoinHandle;

use crate::runtime::{run_producer_at_path, ProducerHeartbeatEnv};

/// Daemon 唯一需要依赖的 worker 能力：在当前 Tokio runtime 上启动任务，并得到可 `abort` / `is_finished` 的句柄。
pub trait EmbeddedProducerWorker: Send + Sync {
    /// `server_id` 仅用于任务失败时的日志标识。
    fn spawn_producer_task(
        &self,
        server_id: String,
        config_path: String,
        output_base: PathBuf,
        heartbeat: Option<ProducerHeartbeatEnv>,
    ) -> JoinHandle<()>;
}

/// 默认实现：调用 [`run_producer_at_path`]（模板 + sink + 可选心跳）。
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioEmbeddedProducerWorker;

impl EmbeddedProducerWorker for TokioEmbeddedProducerWorker {
    fn spawn_producer_task(
        &self,
        server_id: String,
        config_path: String,
        output_base: PathBuf,
        heartbeat: Option<ProducerHeartbeatEnv>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = run_producer_at_path(config_path, output_base, heartbeat).await {
                eprintln!("logspout producer task {server_id}: {e}");
            }
        })
    }
}
