//! gRPC `Logen` 服务实现。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use logen_config::{ClientSection, LogendSection};
use logen_proto::logen_server::Logen;
use logen_proto::{
    CatWorkerReply, CatWorkerRequest, CloseControlSessionReply, CloseControlSessionRequest,
    EchoReply, EchoRequest, EvalControlSessionRequest, HeartbeatReply, HeartbeatRequest,
    ListWorkersReply, ListWorkersRequest, OpenControlSessionReply, OpenControlSessionRequest,
    PingReply, PingRequest, RunControlScriptReply, RunControlScriptRequest, StatWorkerReply,
    StatWorkerRequest, StopWorkerReply, StopWorkerRequest, WorkerEntry, WorkerStatDetail,
};
use logen_worker::{EmbeddedWorker, SpawnWorkerArgs, SpawnedWorkerTasks, WorkerHeartbeatEnv};
use tokio::sync::Mutex;
use tokio::task::block_in_place;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use crate::registry::{reap_exited, resolve_worker_id, RunningWorker};
use crate::session::ControlSessionStore;
use kafka_protocol::KafkaProtocolOptions;
use logen_script::{ControlHost, ControlSession, ScriptError, StatView, Value};

pub struct LogenSvcState {
    pub ping_reply: PingReply,
    pub logend: LogendSection,
    pub client: ClientSection,
    pub embedded_worker: Arc<dyn EmbeddedWorker>,
    pub workers: Mutex<HashMap<String, RunningWorker>>,
    pub sessions: ControlSessionStore,
}

#[derive(Clone)]
pub struct LogenSvc {
    pub inner: Arc<LogenSvcState>,
}

/// 一次控制脚本执行的请求上下文。
struct ControlRunContext {
    source_script: String,
    default_label: String,
    auto_kafka_protocol: bool,
    kafka_broker_host: Option<String>,
}

/// 单次控制脚本的宿主。
struct LogendControlHost {
    inner: Arc<LogenSvcState>,
    context: ControlRunContext,
}

#[derive(Debug, Clone)]
struct WorkerStat {
    id: String,
    label: String,
    alive: bool,
    healthy: bool,
    eps: f64,
    log_events_total: u64,
    seconds_since_heartbeat: f64,
    heartbeat_timeout_secs: u64,
    heartbeat_interval_secs: u64,
    eps_interval: f64,
    log_events_estimated: f64,
    sink_summary: String,
    retry_total: u64,
}

impl LogendControlHost {
    fn new(inner: Arc<LogenSvcState>, context: ControlRunContext) -> Self {
        Self { inner, context }
    }

    fn to_script_error(status: tonic::Status) -> ScriptError {
        ScriptError::eval_msg(format!("control plane: {status}"))
    }

    fn stat_from_running(
        id: &str,
        running: &RunningWorker,
        now: Instant,
        timeout: Duration,
        hb_timeout: u64,
        hb_interval: u64,
    ) -> WorkerStat {
        let healthy = running.last_heartbeat.elapsed() <= timeout;
        let seconds_since_heartbeat = now.duration_since(running.last_heartbeat).as_secs_f64();
        let uptime = now
            .duration_since(running.spawned_at)
            .as_secs_f64()
            .max(1e-9);
        let log_events_estimated = running.last_reported_log_events as f64
            + running.eps_interval * seconds_since_heartbeat;
        WorkerStat {
            id: id.to_string(),
            label: running.config_label.clone(),
            alive: true,
            healthy,
            eps: log_events_estimated / uptime,
            log_events_total: running.last_reported_log_events,
            seconds_since_heartbeat,
            heartbeat_timeout_secs: hb_timeout,
            heartbeat_interval_secs: hb_interval,
            eps_interval: running.eps_interval,
            log_events_estimated,
            sink_summary: running.sink_summary.clone(),
            retry_total: running.retry_total,
        }
    }
}

impl ControlHost for LogendControlHost {
    fn start(
        &self,
        config: logen_model::WorkerConfig,
        label: Option<String>,
    ) -> Result<String, ScriptError> {
        let mut kafka_options = KafkaProtocolOptions::default();
        if let Some(host) = self.context.kafka_broker_host.as_deref() {
            kafka_options = kafka_options.with_broker_host(host.to_string());
        }
        let config = logen_model::finalize_worker_config(
            config,
            self.context.auto_kafka_protocol,
            kafka_options,
        )
        .map_err(|e| ScriptError::eval_msg(format!("start: {e}")))?;

        let id = Uuid::new_v4().to_string();
        let config_label = label
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| self.context.default_label.clone());
        let sink_summary = logen_model::format_sink_summary(&config.sink);
        let heartbeat = WorkerHeartbeatEnv {
            connect: self.inner.logend.local_unix_connect(),
            worker_id: id.clone(),
            heartbeat_interval_secs: self.inner.logend.heartbeat_interval_secs.max(1),
        };
        let SpawnedWorkerTasks {
            worker_task,
            heartbeat_task,
        } = self
            .inner
            .embedded_worker
            .spawn_worker_task(SpawnWorkerArgs {
                worker_id: id.clone(),
                config_label: config_label.clone(),
                config,
                worker_output_dir: PathBuf::from(&self.inner.logend.worker_output_dir),
                heartbeat: Some(heartbeat),
            });

        let now = Instant::now();
        block_in_place(|| {
            let mut guard = self.inner.workers.blocking_lock();
            guard.insert(
                id.clone(),
                RunningWorker {
                    config_label,
                    control_script: self.context.source_script.clone(),
                    worker_task,
                    heartbeat_task,
                    spawned_at: now,
                    last_heartbeat: now,
                    last_reported_log_events: 0,
                    eps_interval: 0.0,
                    sink_summary,
                    retry_total: 0,
                },
            );
        });
        Ok(id)
    }

    fn stop(&self, id: &str) -> Result<(), ScriptError> {
        if id.trim().is_empty() {
            return Err(ScriptError::eval_msg("stop: id required"));
        }
        block_in_place(|| {
            let mut guard = self.inner.workers.blocking_lock();
            reap_exited(&mut guard);
            let id = resolve_worker_id(&guard, id, "stop").map_err(Self::to_script_error)?;
            let running = guard
                .remove(&id)
                .ok_or_else(|| ScriptError::eval_msg("stop: no such worker id"))?;
            if let Some(task) = running.heartbeat_task {
                task.abort();
            }
            running.worker_task.abort();
            Ok(())
        })
    }

    fn stat(&self, id_prefix: Option<&str>, view: StatView) -> Result<String, ScriptError> {
        let prefix = id_prefix.unwrap_or_default();
        let timeout = Duration::from_secs(self.inner.logend.heartbeat_timeout_secs.max(1));
        let hb_timeout = self.inner.logend.heartbeat_timeout_secs;
        let hb_interval = self.inner.logend.heartbeat_interval_secs;
        let stats = block_in_place(|| {
            let mut guard = self.inner.workers.blocking_lock();
            reap_exited(&mut guard);
            let now = Instant::now();
            let stats: Vec<_> = guard
                .iter()
                .filter(|(id, _)| prefix.is_empty() || id.starts_with(prefix))
                .map(|(id, running)| {
                    Self::stat_from_running(id, running, now, timeout, hb_timeout, hb_interval)
                })
                .collect();
            Ok::<_, ScriptError>(stats)
        })?;
        Ok(render_stats(&stats, view))
    }
}

fn render_stats(stats: &[WorkerStat], view: StatView) -> String {
    let mut out = String::new();
    for stat in stats {
        use std::fmt::Write;
        let _ = writeln!(out, "id:\t\t{}", stat.id);
        let _ = writeln!(out, "label:\t\t{}", stat.label);
        let _ = writeln!(out, "healthy:\t{}", stat.healthy);
        if view == StatView::Brief {
            out.push('\n');
            continue;
        }
        let _ = writeln!(out, "sink:\t\t{}", stat.sink_summary);
        let _ = writeln!(out, "alive:\t\t{}", stat.alive);
        let _ = writeln!(out, "eps:\t\t{:.3}", stat.eps);
        let _ = writeln!(out, "eps_interval:\t{:.3}", stat.eps_interval);
        let _ = writeln!(out, "events_total:\t{}", stat.log_events_total);
        let _ = writeln!(out, "events_est:\t{:.1}", stat.log_events_estimated);
        let _ = writeln!(out, "retry_total:\t{}", stat.retry_total);
        let _ = writeln!(out, "sec_since_hb:\t{:.3}", stat.seconds_since_heartbeat);
        let _ = writeln!(out, "hb_timeout_s:\t{}", stat.heartbeat_timeout_secs);
        let _ = writeln!(out, "hb_interval_s:\t{}", stat.heartbeat_interval_secs);
        out.push('\n');
    }
    out
}

#[tonic::async_trait]
impl Logen for LogenSvc {
    async fn ping(
        &self,
        _req: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingReply>, tonic::Status> {
        trace!("rpc Ping");
        Ok(tonic::Response::new(self.inner.ping_reply.clone()))
    }

    async fn echo(
        &self,
        req: tonic::Request<EchoRequest>,
    ) -> Result<tonic::Response<EchoReply>, tonic::Status> {
        let msg = req.into_inner().message;
        debug!("rpc Echo chars={}", msg.len());
        Ok(tonic::Response::new(EchoReply { message: msg }))
    }

    async fn list_workers(
        &self,
        _req: tonic::Request<ListWorkersRequest>,
    ) -> Result<tonic::Response<ListWorkersReply>, tonic::Status> {
        let timeout = Duration::from_secs(self.inner.logend.heartbeat_timeout_secs.max(1));
        let mut guard = self.inner.workers.lock().await;
        reap_exited(&mut guard);

        let workers: Vec<WorkerEntry> = guard
            .iter()
            .map(|(id, r)| {
                let healthy = r.last_heartbeat.elapsed() <= timeout;
                WorkerEntry {
                    id: id.clone(),
                    config_path: r.config_label.clone(),
                    alive: true,
                    healthy,
                    sink_summary: r.sink_summary.clone(),
                }
            })
            .collect();
        debug!("rpc ListWorkers -> {} entries", workers.len());
        Ok(tonic::Response::new(ListWorkersReply { workers }))
    }

    async fn stat_worker(
        &self,
        req: tonic::Request<StatWorkerRequest>,
    ) -> Result<tonic::Response<StatWorkerReply>, tonic::Status> {
        let prefix = req.into_inner().id_prefix;
        let timeout = Duration::from_secs(self.inner.logend.heartbeat_timeout_secs.max(1));
        let hb_timeout = self.inner.logend.heartbeat_timeout_secs;
        let hb_interval = self.inner.logend.heartbeat_interval_secs;

        let mut guard = self.inner.workers.lock().await;
        reap_exited(&mut guard);

        let now = Instant::now();
        let workers: Vec<WorkerStatDetail> = guard
            .iter()
            .filter(|(id, _)| prefix.is_empty() || id.starts_with(&prefix))
            .map(|(id, r)| {
                let healthy = r.last_heartbeat.elapsed() <= timeout;
                let secs_hb = now.duration_since(r.last_heartbeat).as_secs_f64();
                let uptime = now.duration_since(r.spawned_at).as_secs_f64().max(1e-9);
                let events_est = r.last_reported_log_events as f64 + r.eps_interval * secs_hb;
                let eps_rt = events_est / uptime;
                WorkerStatDetail {
                    id: id.clone(),
                    config_path: r.config_label.clone(),
                    alive: true,
                    healthy,
                    eps: eps_rt,
                    log_events_total: r.last_reported_log_events,
                    seconds_since_heartbeat: secs_hb,
                    heartbeat_timeout_secs: hb_timeout,
                    heartbeat_interval_secs: hb_interval,
                    eps_interval: r.eps_interval,
                    log_events_estimated: events_est,
                    sink_summary: r.sink_summary.clone(),
                    retry_total: r.retry_total,
                }
            })
            .collect();

        debug!(
            "rpc StatWorker id_prefix={:?} -> {} entries",
            prefix,
            workers.len()
        );
        Ok(tonic::Response::new(StatWorkerReply { workers }))
    }

    async fn run_control_script(
        &self,
        req: tonic::Request<RunControlScriptRequest>,
    ) -> Result<tonic::Response<RunControlScriptReply>, tonic::Status> {
        let msg = req.into_inner();
        if msg.script.trim().is_empty() {
            return Err(tonic::Status::invalid_argument(
                "script required (non-empty .logen control script)",
            ));
        }
        let auto_kafka_protocol = msg
            .auto_kafka_protocol
            .unwrap_or(self.inner.client.auto_kafka_protocol);
        let default_label = if msg.config_label.trim().is_empty() {
            "(no label)".to_string()
        } else {
            msg.config_label
        };
        let host = Arc::new(LogendControlHost::new(
            self.inner.clone(),
            ControlRunContext {
                source_script: msg.script.clone(),
                default_label,
                auto_kafka_protocol,
                kafka_broker_host: msg
                    .kafka_broker_host
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
            },
        ));
        let result = logen_script::run_control_script(&msg.script, host)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let (worker_id, status) = match result.value {
            Some(Value::Str(id)) => (id, "started".to_string()),
            Some(Value::Unit) | None => (String::new(), "ok".to_string()),
            Some(other) => {
                return Err(tonic::Status::invalid_argument(format!(
                    "control script must end with start(...), stat(...), or stop(...); got {}",
                    other.ty()
                )));
            }
        };
        Ok(tonic::Response::new(RunControlScriptReply {
            worker_id,
            output: result.output,
            status,
        }))
    }

    async fn open_control_session(
        &self,
        req: tonic::Request<OpenControlSessionRequest>,
    ) -> Result<tonic::Response<OpenControlSessionReply>, tonic::Status> {
        let msg = req.into_inner();
        let output = Arc::new(std::sync::Mutex::new(String::new()));
        let host = Arc::new(LogendControlHost::new(
            self.inner.clone(),
            ControlRunContext {
                source_script: "(interactive)".into(),
                default_label: if msg.config_label.is_empty() {
                    "interactive".into()
                } else {
                    msg.config_label
                },
                auto_kafka_protocol: msg
                    .auto_kafka_protocol
                    .unwrap_or(self.inner.client.auto_kafka_protocol),
                kafka_broker_host: msg.kafka_broker_host,
            },
        ));
        let id = self
            .inner
            .sessions
            .open(ControlSession::new(host, output))
            .await;
        Ok(tonic::Response::new(OpenControlSessionReply {
            session_id: id,
        }))
    }

    async fn eval_control_session(
        &self,
        req: tonic::Request<EvalControlSessionRequest>,
    ) -> Result<tonic::Response<RunControlScriptReply>, tonic::Status> {
        let msg = req.into_inner();
        let (value, output) = self
            .inner
            .sessions
            .execute(&msg.session_id, &msg.source)
            .await
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let worker_id = match value {
            Some(Value::Str(id)) => id,
            _ => String::new(),
        };
        Ok(tonic::Response::new(RunControlScriptReply {
            worker_id,
            output,
            status: "ok".into(),
        }))
    }

    async fn close_control_session(
        &self,
        req: tonic::Request<CloseControlSessionRequest>,
    ) -> Result<tonic::Response<CloseControlSessionReply>, tonic::Status> {
        self.inner
            .sessions
            .close(&req.into_inner().session_id)
            .await;
        Ok(tonic::Response::new(CloseControlSessionReply {}))
    }

    async fn stop_worker(
        &self,
        req: tonic::Request<StopWorkerRequest>,
    ) -> Result<tonic::Response<StopWorkerReply>, tonic::Status> {
        let id = req.into_inner().id;
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("id required"));
        }
        let mut guard = self.inner.workers.lock().await;
        reap_exited(&mut guard);
        let id = resolve_worker_id(&guard, &id, "StopWorker")?;
        let Some(running) = guard.remove(&id) else {
            return Err(tonic::Status::not_found("no such worker id"));
        };
        info!("rpc StopWorker id={}", id);
        if let Some(task) = running.heartbeat_task {
            task.abort();
        }
        running.worker_task.abort();
        Ok(tonic::Response::new(StopWorkerReply {
            status: "stopped".into(),
        }))
    }

    async fn cat_worker(
        &self,
        req: tonic::Request<CatWorkerRequest>,
    ) -> Result<tonic::Response<CatWorkerReply>, tonic::Status> {
        let id = req.into_inner().id;
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("id required"));
        }
        let mut guard = self.inner.workers.lock().await;
        reap_exited(&mut guard);
        let id = resolve_worker_id(&guard, &id, "CatWorker")?;
        let Some(running) = guard.get(&id) else {
            return Err(tonic::Status::not_found("no such worker id"));
        };
        debug!("rpc CatWorker id={}", id);
        let label = running.config_label.clone();
        let script = running.control_script.clone();
        drop(guard);
        Ok(tonic::Response::new(CatWorkerReply {
            config_path: label,
            script,
        }))
    }

    async fn heartbeat(
        &self,
        req: tonic::Request<HeartbeatRequest>,
    ) -> Result<tonic::Response<HeartbeatReply>, tonic::Status> {
        let msg = req.into_inner();
        let id = msg.id;
        let log_events_total = msg.log_events_total;
        let retry_total = msg.retry_total;
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("id required"));
        }
        trace!(
            "rpc Heartbeat id={} log_events_total={} retry_total={}",
            id,
            log_events_total,
            retry_total
        );
        let mut guard = self.inner.workers.lock().await;
        let Some(running) = guard.get_mut(&id) else {
            warn!("rpc Heartbeat unknown worker id={}", id);
            return Err(tonic::Status::not_found("no such worker id"));
        };
        if running.worker_task.is_finished() {
            warn!("rpc Heartbeat worker task already finished id={}", id);
            if let Some(r) = guard.remove(&id) {
                if let Some(task) = r.heartbeat_task {
                    task.abort();
                }
            }
            return Err(tonic::Status::failed_precondition("worker task has ended"));
        }
        let now = Instant::now();
        let dt = now.duration_since(running.last_heartbeat);
        let secs = dt.as_secs_f64();
        if secs > 1e-9 && log_events_total >= running.last_reported_log_events {
            let de = log_events_total - running.last_reported_log_events;
            running.eps_interval = de as f64 / secs;
        }
        running.last_reported_log_events = log_events_total;
        running.last_heartbeat = now;
        running.retry_total = retry_total;
        Ok(tonic::Response::new(HeartbeatReply {}))
    }
}
