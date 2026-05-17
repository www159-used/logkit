use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use logen_dsl::WorkerConfig;
use tokio::task::JoinHandle;
use tracing::{info_span, Instrument};

use crate::runtime::{run_worker_with_config, spawn_heartbeat_task, WorkerHeartbeatEnv};

pub struct SpawnedWorkerTasks {
    pub worker_task: JoinHandle<()>,
    pub heartbeat_task: Option<JoinHandle<()>>,
}

pub trait EmbeddedWorker: Send + Sync {
    /// `worker_id` 仅用于任务失败时的日志标识。
    fn spawn_worker_task(
        &self,
        worker_id: String,
        config_label: String,
        worker_config: WorkerConfig,
        output_base: PathBuf,
        heartbeat: Option<WorkerHeartbeatEnv>,
    ) -> SpawnedWorkerTasks;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TokioEmbeddedWorker;

impl EmbeddedWorker for TokioEmbeddedWorker {
    fn spawn_worker_task(
        &self,
        worker_id: String,
        config_label: String,
        worker_config: WorkerConfig,
        output_base: PathBuf,
        heartbeat: Option<WorkerHeartbeatEnv>,
    ) -> SpawnedWorkerTasks {
        let events = Arc::new(AtomicU64::new(0));
        let heartbeat_task = heartbeat.map(|hb| spawn_heartbeat_task(hb, events.clone()));
        let span = info_span!("worker", id = %worker_id);
        let worker_task = tokio::spawn(
            async move {
                if let Err(e) = run_worker_with_config(
                    config_label,
                    worker_config,
                    output_base,
                    events,
                )
                .await
                {
                    tracing::error!("logen worker task failed: {e:#}");
                }
            }
            .instrument(span),
        );
        SpawnedWorkerTasks {
            worker_task,
            heartbeat_task,
        }
    }
}
