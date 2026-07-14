//! gRPC `Logen` 服务实现。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_protocol::KafkaProtocolOptions;
use logen_config::{ClientSection, LogendSection};
use logen_proto::logen_server::Logen;
use logen_proto::{
    CatWorkerReply, CatWorkerRequest, EchoReply, EchoRequest, HeartbeatReply, HeartbeatRequest,
    ListWorkersReply, ListWorkersRequest, PingReply, PingRequest, StartWorkerReply,
    StartWorkerRequest, StatWorkerReply, StatWorkerRequest, StopWorkerReply, StopWorkerRequest,
    WorkerEntry, WorkerStatDetail,
};
use logen_worker::{EmbeddedWorker, SpawnedWorkerTasks, WorkerHeartbeatEnv};
use tokio::sync::Mutex;
use tracing::{debug, info, trace, warn};

use crate::registry::{reap_exited, resolve_worker_id, RunningWorker};
use crate::start_pipeline::prepare_worker_start;

pub struct LogenSvcState {
    pub ping_reply: PingReply,
    pub logend: LogendSection,
    pub client: ClientSection,
    pub embedded_worker: Arc<dyn EmbeddedWorker>,
    pub workers: Mutex<HashMap<String, RunningWorker>>,
}

#[derive(Clone)]
pub struct LogenSvc {
    pub inner: Arc<LogenSvcState>,
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

    async fn start_worker(
        &self,
        req: tonic::Request<StartWorkerRequest>,
    ) -> Result<tonic::Response<StartWorkerReply>, tonic::Status> {
        let msg = req.into_inner();
        let auto_kafka = msg
            .auto_kafka_protocol
            .unwrap_or(self.inner.client.auto_kafka_protocol);
        let mut kafka_opts = KafkaProtocolOptions::default();
        if let Some(host) = msg
            .kafka_broker_host
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            kafka_opts = kafka_opts.with_broker_host(host.to_string());
        }
        let prepared = prepare_worker_start(
            &msg.instance_yaml,
            msg.config_label,
            auto_kafka,
            kafka_opts,
            PathBuf::from(&self.inner.logend.worker_output_dir).as_path(),
        )?;

        let hb = WorkerHeartbeatEnv {
            connect: self.inner.logend.local_unix_connect(),
            worker_id: prepared.id.clone(),
            heartbeat_interval_secs: self.inner.logend.heartbeat_interval_secs.max(1),
        };
        let SpawnedWorkerTasks {
            worker_task,
            heartbeat_task,
        } = self.inner.embedded_worker.spawn_worker_task(
            prepared.id.clone(),
            prepared.config_label.clone(),
            prepared.worker_cfg,
            Some(hb),
        );

        info!(
            "rpc StartWorker id={} label={:?} sink={}",
            prepared.id, prepared.config_label, prepared.sink_summary
        );

        let now = Instant::now();
        let mut guard = self.inner.workers.lock().await;
        guard.insert(
            prepared.id.clone(),
            RunningWorker {
                config_label: prepared.config_label,
                instance_yaml: prepared.instance_yaml,
                worker_task,
                heartbeat_task,
                spawned_at: now,
                last_heartbeat: now,
                last_reported_log_events: 0,
                eps_interval: 0.0,
                sink_summary: prepared.sink_summary,
                retry_total: 0,
            },
        );
        Ok(tonic::Response::new(StartWorkerReply {
            id: prepared.id,
            status: "started".into(),
        }))
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
        let yaml = running.instance_yaml.clone();
        drop(guard);
        Ok(tonic::Response::new(CatWorkerReply {
            config_path: label,
            yaml,
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
