//! 供 **`logspout-daemon`** 使用的嵌入 worker 约定：用 trait 约束「如何 spawn 造日志任务」，便于测试替换或日后其它实现。

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::runtime::{
    run_producer_with_events, spawn_heartbeat_task, ProducerHeartbeatEnv,
};

pub struct SpawnedProducerTasks {
    pub worker_task: JoinHandle<()>,
    pub heartbeat_task: Option<JoinHandle<()>>,
}

/// Daemon 唯一需要依赖的 worker 能力：在当前 Tokio runtime 上启动任务，并得到可 `abort` / `is_finished` 的句柄。
pub trait EmbeddedProducerWorker: Send + Sync {
    /// `worker_id` 仅用于任务失败时的日志标识。
    fn spawn_producer_task(
        &self,
        worker_id: String,
        config_path: String,
        output_base: PathBuf,
        heartbeat: Option<ProducerHeartbeatEnv>,
    ) -> SpawnedProducerTasks;
}

/// 默认实现：调用 [`run_producer_at_path`]（模板 + sink + 可选心跳）。
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioEmbeddedProducerWorker;

impl EmbeddedProducerWorker for TokioEmbeddedProducerWorker {
    fn spawn_producer_task(
        &self,
        worker_id: String,
        config_path: String,
        output_base: PathBuf,
        heartbeat: Option<ProducerHeartbeatEnv>,
    ) -> SpawnedProducerTasks {
        let events = Arc::new(AtomicU64::new(0));
        let heartbeat_task = heartbeat.map(|hb| spawn_heartbeat_task(hb, events.clone()));
        let worker_task = tokio::spawn(async move {
            if let Err(e) = run_producer_with_events(config_path, output_base, events).await {
                eprintln!("logspout producer task {worker_id}: {e}");
            }
        });
        SpawnedProducerTasks {
            worker_task,
            heartbeat_task,
        }
    }
}
