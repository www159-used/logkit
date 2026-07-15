//! `logen-worker`：造日志库；由 **`logend`** 进程内嵌入（[`EmbeddedWorker`]）。
//!
//! 启动载荷为已装配的 [`WorkerConfig`]；控制脚本只在 `logend` 内解释。

use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use logen_model::{format_sink_summary, WorkerConfig};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::{info_span, Instrument};

pub mod runtime;
pub mod sink;

#[doc(hidden)]
pub mod agent_fixtures;

pub use runtime::WorkerHeartbeatEnv;
pub use sink::kafka_agent::{
    build_agent_message, build_runtime_agent_config, KafkaAgentMessage, RuntimeAgentConfig,
};
pub use sink::{
    build_line_sink, FileLineSink, KafkaConfigError, KafkaLineSink, LogLineSink, SinkError,
    StdoutLineSink,
};

use runtime::{run_worker_with_config, spawn_heartbeat_task};

pub struct SpawnedWorkerTasks {
    pub worker_task: JoinHandle<()>,
    pub heartbeat_task: Option<JoinHandle<()>>,
}

/// 嵌入式 worker 启动参数。
pub struct SpawnWorkerArgs {
    pub worker_id: String,
    pub config_label: String,
    pub config: WorkerConfig,
    pub worker_output_dir: PathBuf,
    pub heartbeat: Option<WorkerHeartbeatEnv>,
}

pub trait EmbeddedWorker: Send + Sync {
    fn spawn_worker_task(&self, args: SpawnWorkerArgs) -> SpawnedWorkerTasks;
}

#[derive(Debug, Clone)]
pub struct TokioEmbeddedWorker {
    worker_handle: Handle,
    control_handle: Handle,
}

impl Default for TokioEmbeddedWorker {
    fn default() -> Self {
        let handle = Handle::current();
        Self::new(handle.clone(), handle)
    }
}

impl TokioEmbeddedWorker {
    pub fn new(worker_handle: Handle, control_handle: Handle) -> Self {
        Self {
            worker_handle,
            control_handle,
        }
    }
}

fn spawn_task_on_handle<F>(handle: &Handle, span: tracing::Span, fut: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    handle.spawn(fut.instrument(span))
}

impl EmbeddedWorker for TokioEmbeddedWorker {
    fn spawn_worker_task(&self, mut args: SpawnWorkerArgs) -> SpawnedWorkerTasks {
        let events = Arc::new(AtomicU64::new(0));
        let retry_total = Arc::new(AtomicU64::new(0));
        let heartbeat = args.heartbeat.take();
        let heartbeat_task = heartbeat.map(|hb| {
            spawn_heartbeat_task(
                &self.control_handle,
                hb,
                events.clone(),
                retry_total.clone(),
            )
        });
        let span = info_span!("worker", worker_id = %args.worker_id);
        let worker_id = args.worker_id.clone();
        let worker_task = spawn_task_on_handle(&self.worker_handle, span, async move {
            if let Err(e) = run_config_worker(args, events, retry_total).await {
                tracing::error!(worker_id = %worker_id, "logen worker task failed: {e:#}");
            }
        });
        SpawnedWorkerTasks {
            worker_task,
            heartbeat_task,
        }
    }
}

async fn run_config_worker(
    mut args: SpawnWorkerArgs,
    events: Arc<AtomicU64>,
    retry_total: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    args.config
        .sink
        .fill_default_output(&args.worker_output_dir, &args.worker_id);
    let _summary = format_sink_summary(&args.config.sink);
    run_worker_with_config(
        args.worker_id,
        args.config_label,
        args.config,
        events,
        retry_total,
    )
    .await
}

/// **仅供集成测试** [`tests/kafka_probe`]：对集群发 metadata 请求并返回 `(broker 数, topic 元数据条目数)`。
#[doc(hidden)]
pub fn probe_kafka_ssl_cluster(k: &logen_model::KafkaConfig) -> Result<(usize, usize), SinkError> {
    sink::kafka::probe_kafka_ssl_cluster(k)
}

/// **仅供集成测试**：向 topic 同步投递一条消息（与 [`KafkaLineSink`] 相同 TLS/配置路径）。
#[doc(hidden)]
pub fn produce_one_kafka_ssl_line(
    k: &logen_model::KafkaConfig,
    payload: &str,
) -> Result<(), SinkError> {
    sink::kafka::produce_one_kafka_ssl_line(k, payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：在目标 runtime handle 上 spawn 的任务确实执行。
    /// 输入：current handle 作为 worker/control。
    /// 预期：oneshot 收到信号。
    #[tokio::test]
    async fn spawn_task_on_handle_uses_target_runtime() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = Handle::current();
        let _jh = spawn_task_on_handle(&handle, tracing::Span::none(), async move {
            let _ = tx.send(());
        });
        rx.await.expect("task ran");
    }
}
