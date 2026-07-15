//! 运行中 worker 注册表：id 解析与收尸。

use std::collections::HashMap;
use std::time::Instant;

use tracing::{debug, info};

pub struct RunningWorker {
    /// `logen start` 传入的展示标签（多为用户本地路径）
    pub config_label: String,
    /// 产生本 worker 的控制脚本全文（`cat` 直接返回）
    pub control_script: String,
    pub worker_task: tokio::task::JoinHandle<()>,
    pub heartbeat_task: Option<tokio::task::JoinHandle<()>>,
    pub spawned_at: Instant,
    pub last_heartbeat: Instant,
    pub last_reported_log_events: u64,
    /// 上一心跳间隔内的 Δevents/Δt（采样）
    pub eps_interval: f64,
    /// 启动时占位或后续刷新的 sink 摘要
    pub sink_summary: String,
    pub retry_total: u64,
}

pub enum IdPick {
    One(String),
    None,
    Many(Vec<String>),
}

/// 优先精确 key；否则按 id `starts_with` 匹配；多个时返回全部（已排序）。
pub fn pick_worker_id(guard: &HashMap<String, RunningWorker>, key: &str) -> IdPick {
    if guard.contains_key(key) {
        return IdPick::One(key.to_string());
    }
    let mut ids: Vec<String> = guard
        .keys()
        .filter(|id| id.starts_with(key))
        .cloned()
        .collect();
    ids.sort();
    match ids.len() {
        0 => IdPick::None,
        1 => IdPick::One(ids[0].clone()),
        _ => IdPick::Many(ids),
    }
}

pub fn reap_exited(guard: &mut HashMap<String, RunningWorker>) {
    let mut dead: Vec<String> = Vec::new();
    for (id, running) in guard.iter() {
        if running.worker_task.is_finished() {
            dead.push(id.clone());
        }
    }
    for id in dead {
        if let Some(r) = guard.remove(&id) {
            if let Some(task) = r.heartbeat_task {
                task.abort();
            }
            info!("worker task exited id={id}");
        }
    }
}

pub fn resolve_worker_id(
    guard: &HashMap<String, RunningWorker>,
    key: &str,
    rpc: &str,
) -> Result<String, tonic::Status> {
    match pick_worker_id(guard, key) {
        IdPick::One(s) => Ok(s),
        IdPick::None => Err(tonic::Status::not_found("no such worker id")),
        IdPick::Many(ids) => {
            debug!("rpc {rpc} ambiguous prefix {key:?} matches {}", ids.len());
            Err(tonic::Status::invalid_argument(format!(
                "id prefix {key:?} matches multiple workers:\n{}",
                ids.join("\n")
            )))
        }
    }
}
