use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartWorkerResult {
    pub worker_id: String,
    pub output: String,
    pub status: String,
}

impl WorkerSummary {
    pub fn status_label_key(&self) -> WorkerStatusKey {
        if !self.alive {
            WorkerStatusKey::Stopped
        } else if self.healthy {
            WorkerStatusKey::Healthy
        } else {
            WorkerStatusKey::Unhealthy
        }
    }

    pub fn status_class(&self) -> &'static str {
        match self.status_label_key() {
            WorkerStatusKey::Stopped => "badge badge-stopped",
            WorkerStatusKey::Healthy => "badge badge-healthy",
            WorkerStatusKey::Unhealthy => "badge badge-unhealthy",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatusKey {
    Stopped,
    Healthy,
    Unhealthy,
}

#[cfg(feature = "ssr")]
impl From<logen_connection::WorkerSummary> for WorkerSummary {
    fn from(w: logen_connection::WorkerSummary) -> Self {
        Self {
            id: w.id,
            config_label: w.config_label,
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
        }
    }
}

#[cfg(feature = "ssr")]
impl From<logen_connection::StartWorkerResult> for StartWorkerResult {
    fn from(r: logen_connection::StartWorkerResult) -> Self {
        Self {
            worker_id: r.worker_id,
            output: r.output,
            status: r.status,
        }
    }
}
