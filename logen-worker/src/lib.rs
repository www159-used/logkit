//! `logen-worker`：造日志库；由 **`logend`** 进程内嵌入（[`EmbeddedWorker`]、`runtime` 内存配置运行入口）。

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use logen_dsl::WorkerConfig;
use tokio::task::JoinHandle;
use tracing::{info_span, Instrument};

pub mod runtime;
pub mod sink;

pub use runtime::WorkerHeartbeatEnv;
pub use sink::{
    build_line_sink, FileLineSink, KafkaConfigError, KafkaLineSink, LogLineSink, SinkError,
    StdoutLineSink,
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
        let span = info_span!("worker", worker_id = %worker_id);
        let worker_task = tokio::spawn(
            async move {
                if let Err(e) = run_worker_with_config(
                    worker_id.clone(),
                    config_label,
                    worker_config,
                    output_base,
                    events,
                )
                .await
                {
                    tracing::error!(worker_id = %worker_id, "logen worker task failed: {e:#}");
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

/// **仅供集成测试** [`tests/kafka_probe`]：对集群发 metadata 请求并返回 `(broker 数, topic 元数据条目数)`。
#[doc(hidden)]
pub fn probe_kafka_ssl_cluster(
    k: &logen_dsl::KafkaConfig,
) -> Result<(usize, usize), SinkError> {
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
