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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartWorkerResult {
    pub worker_id: String,
    pub output: String,
    pub status: String,
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
