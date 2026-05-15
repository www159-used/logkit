//! 供 **`logspout-daemon`** 使用的嵌入 worker 约定：用 trait 约束「如何 spawn 造日志任务」，便于测试替换或日后其它实现。

use std::path::PathBuf;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use futures_util::FutureExt;
use logspout_dsl::TemplateConfig;
use tokio::task::JoinHandle;
use tracing::{info_span, Instrument};

use crate::runtime::{
    run_producer_with_config, spawn_heartbeat_task, ProducerHeartbeatEnv,
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
        config_label: String,
        producer_cfg: TemplateConfig,
        output_base: PathBuf,
        heartbeat: Option<ProducerHeartbeatEnv>,
    ) -> SpawnedProducerTasks;
}

/// 默认实现：直接消费 daemon 传入的内存配置（模板 + sink + 可选心跳）。
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioEmbeddedProducerWorker;

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

impl EmbeddedProducerWorker for TokioEmbeddedProducerWorker {
    fn spawn_producer_task(
        &self,
        worker_id: String,
        config_label: String,
        producer_cfg: TemplateConfig,
        output_base: PathBuf,
        heartbeat: Option<ProducerHeartbeatEnv>,
    ) -> SpawnedProducerTasks {
        let events = Arc::new(AtomicU64::new(0));
        let heartbeat_task = heartbeat.map(|hb| spawn_heartbeat_task(hb, events.clone()));
        let span = info_span!("worker", id = %worker_id);
        let worker_task = tokio::spawn(
            async move {
                let result = AssertUnwindSafe(run_producer_with_config(
                    config_label,
                    producer_cfg,
                    output_base,
                    events,
                ))
                .catch_unwind()
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::error!("logspout producer task failed: {e}");
                    }
                    Err(payload) => {
                        let detail = panic_payload_to_string(payload);
                        tracing::error!("logspout producer task panicked: {detail}");
                    }
                }
            }
            .instrument(span),
        );
        SpawnedProducerTasks {
            worker_task,
            heartbeat_task,
        }
    }
}
