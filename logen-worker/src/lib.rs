//! `logen-worker`：造日志库；由 **`logend`** 进程内嵌入（[`EmbeddedWorker`]、`runtime` 内存配置运行入口）。

use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use logen_dsl::WorkerConfig;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::{info_span, Instrument};

pub mod runtime;
pub mod sink;

#[doc(hidden)]
pub mod agent_fixtures;

pub use runtime::WorkerHeartbeatEnv;
pub use sink::{
    build_line_sink, FileLineSink, KafkaConfigError, KafkaLineSink, LogLineSink, SinkError,
    StdoutLineSink,
};
pub use sink::kafka_agent::{
    build_agent_message, build_runtime_agent_config, KafkaAgentMessage, RuntimeAgentConfig,
};

use runtime::{run_worker_with_config, spawn_heartbeat_task};

pub struct SpawnedWorkerTasks {
    pub worker_task: JoinHandle<()>,
    pub heartbeat_task: Option<JoinHandle<()>>,
}

pub trait EmbeddedWorker: Send + Sync {
    /// `worker_id` 用于 tracing span、Kafka 投递日志与心跳。
    fn spawn_worker_task(
        &self,
        worker_id: String,
        config_label: String,
        worker_config: WorkerConfig,
        output_base: PathBuf,
        heartbeat: Option<WorkerHeartbeatEnv>,
    ) -> SpawnedWorkerTasks;
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
    fn spawn_worker_task(
        &self,
        worker_id: String,
        config_label: String,
        worker_config: WorkerConfig,
        output_base: PathBuf,
        heartbeat: Option<WorkerHeartbeatEnv>,
    ) -> SpawnedWorkerTasks {
        let events = Arc::new(AtomicU64::new(0));
        let retry_total = Arc::new(AtomicU64::new(0));
        let heartbeat_task = heartbeat.map(|hb| {
            spawn_heartbeat_task(
                &self.control_handle,
                hb,
                events.clone(),
                retry_total.clone(),
            )
        });
        let span = info_span!("worker", worker_id = %worker_id);
        let worker_task = spawn_task_on_handle(&self.worker_handle, span, async move {
            if let Err(e) = run_worker_with_config(
                worker_id.clone(),
                config_label,
                worker_config,
                output_base,
                events,
                retry_total,
            )
            .await
            {
                tracing::error!(worker_id = %worker_id, "logen worker task failed: {e:#}");
            }
        });
        SpawnedWorkerTasks {
            worker_task,
            heartbeat_task,
        }
    }
}

/// **仅供集成测试** [`tests/kafka_probe`]：对集群发 metadata 请求并返回 `(broker 数, topic 元数据条目数)`。
#[doc(hidden)]
pub fn probe_kafka_ssl_cluster(k: &logen_dsl::KafkaConfig) -> Result<(usize, usize), SinkError> {
    sink::kafka::probe_kafka_ssl_cluster(k)
}

/// **仅供集成测试** [`tests/kafka_probe`]：按当前 TLS 配置向配置中的 topic **发送一条** UTF-8 文本。
#[doc(hidden)]
pub fn produce_one_kafka_ssl_line(
    k: &logen_dsl::KafkaConfig,
    payload: &str,
) -> Result<(), SinkError> {
    sink::kafka::produce_one_kafka_ssl_line(k, payload)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    /// 测试内容：`spawn_task_on_handle` 会把任务投递到指定 runtime，而非调用方当前线程。
    /// 输入：命名为 `worker-rt` 的独立 `current_thread` runtime handle，与一个回传线程名的异步任务。
    /// 预期：任务在线程名包含 `worker-rt` 的目标 runtime 上执行。
    #[test]
    fn spawn_task_on_handle_uses_target_runtime() {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let worker_thread = thread::Builder::new()
            .name("worker-rt".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("worker runtime");
                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
                ready_tx
                    .send((rt.handle().clone(), shutdown_tx))
                    .expect("send handle");
                rt.block_on(async {
                    let _ = shutdown_rx.await;
                });
            })
            .expect("spawn worker thread");

        let (worker_handle, shutdown_tx) = ready_rx.recv().expect("recv worker handle");
        let (name_tx, name_rx) = mpsc::sync_channel(1);
        let _task = spawn_task_on_handle(
            &worker_handle,
            tracing::info_span!("test_worker_rt"),
            async move {
                let name = thread::current().name().unwrap_or("").to_string();
                name_tx.send(name).expect("send thread name");
            },
        );

        let task_thread = name_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("receive thread name");
        assert!(
            task_thread.contains("worker-rt"),
            "task thread = {task_thread:?}"
        );

        let _ = shutdown_tx.send(());
        worker_thread.join().expect("join worker thread");
    }
}
