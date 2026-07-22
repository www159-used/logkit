use logen_config::ClientConnect;
use logen_proto::{RunControlScriptRequest, StatWorkerRequest, StopWorkerRequest};
use serde::{Deserialize, Serialize};

use crate::connection::LogendConnection;
use crate::error::ConnectionError;

use super::logend;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSummary {
    pub id: String,
    pub config_label: String,
    pub alive: bool,
    pub healthy: bool,
    pub sink_summary: String,
    pub eps: f64,
    pub log_events_total: u64,
    pub retry_total: u64,
    pub seconds_since_heartbeat: f64,
    pub heartbeat_timeout_secs: u64,
    pub heartbeat_interval_secs: u64,
    pub eps_interval: f64,
    pub log_events_estimated: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartWorkerResult {
    pub worker_id: String,
    pub output: String,
    pub status: String,
}

pub(super) async fn run_control_script(
    conn: &LogendConnection,
    script: &str,
    config_label: &str,
) -> Result<StartWorkerResult, ConnectionError> {
    let resolved = conn.resolve()?;
    let kafka_broker_host = match &resolved.connect {
        ClientConnect::Tcp { host, .. } => Some(host.clone()),
        ClientConnect::Unix { .. } => None,
    };
    let mut client = logend::logen_client(&resolved.connect).await?;
    let reply = client
        .run_control_script(RunControlScriptRequest {
            script: script.to_string(),
            config_label: config_label.to_string(),
            auto_kafka_protocol: conn.auto_kafka_protocol,
            kafka_broker_host,
        })
        .await
        .map_err(|s| ConnectionError::msg(format!("run control script failed: {s}")))?;
    let inner = reply.into_inner();
    Ok(StartWorkerResult {
        worker_id: inner.worker_id,
        output: inner.output,
        status: inner.status,
    })
}

pub(super) async fn stat_workers(
    conn: &LogendConnection,
    id_prefix: &str,
) -> Result<Vec<WorkerSummary>, ConnectionError> {
    let resolved = conn.resolve()?;
    let mut client = logend::logen_client(&resolved.connect).await?;
    let reply = client
        .stat_worker(StatWorkerRequest {
            id_prefix: id_prefix.to_string(),
        })
        .await
        .map_err(|s| ConnectionError::msg(format!("stat workers failed: {s}")))?;
    Ok(reply
        .into_inner()
        .workers
        .into_iter()
        .map(|w| WorkerSummary {
            id: w.id,
            config_label: w.config_path,
            alive: w.alive,
            healthy: w.healthy,
            sink_summary: w.sink_summary,
            eps: w.eps,
            log_events_total: w.log_events_total,
            retry_total: w.retry_total,
            seconds_since_heartbeat: w.seconds_since_heartbeat,
            heartbeat_timeout_secs: w.heartbeat_timeout_secs,
            heartbeat_interval_secs: w.heartbeat_interval_secs,
            eps_interval: w.eps_interval,
            log_events_estimated: w.log_events_estimated,
        })
        .collect())
}

pub(super) async fn stop_worker(
    conn: &LogendConnection,
    worker_id: &str,
) -> Result<String, ConnectionError> {
    let resolved = conn.resolve()?;
    let mut client = logend::logen_client(&resolved.connect).await?;
    let reply = client
        .stop_worker(StopWorkerRequest {
            id: worker_id.to_string(),
        })
        .await
        .map_err(|s| ConnectionError::msg(format!("stop worker failed: {s}")))?;
    Ok(reply.into_inner().status)
}
