use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use http::Uri;
use hyper_util::rt::TokioIo;
use logen_dsl::{TemplateRunner, WorkerConfig};
use logen_proto::logen_client::LogenClient;
use logen_proto::HeartbeatRequest;
use tokio::runtime::Handle;
use tokio::task::{JoinHandle, JoinSet};
use tonic::transport::Endpoint;
use tower::service_fn;

use crate::sink::build_line_sink;

/// 向守护进程上报心跳所需环境。
#[derive(Debug, Clone)]
pub struct WorkerHeartbeatEnv {
    pub control_socket: String,
    pub worker_id: String,
    pub heartbeat_interval_secs: u64,
    pub client_connect_uri: String,
}

async fn heartbeat_loop(
    sock: String,
    id: String,
    period: Duration,
    uri: String,
    events: Arc<AtomicU64>,
    retry_total: Arc<AtomicU64>,
) {
    let Ok(endpoint) = Endpoint::from_shared(uri) else {
        return;
    };
    let path_sock = sock.clone();
    let Ok(channel) = endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = path_sock.clone();
            async move {
                let s = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(s))
            }
        }))
        .await
    else {
        return;
    };
    let mut client = LogenClient::new(channel);
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let total = events.load(Ordering::Relaxed);
        if client
            .heartbeat(HeartbeatRequest {
                id: id.clone(),
                log_events_total: total,
                retry_total: retry_total.load(Ordering::Relaxed),
            })
            .await
            .is_err()
        {
            break;
        }
    }
}

pub fn spawn_heartbeat_task(
    handle: &Handle,
    hb: WorkerHeartbeatEnv,
    events: Arc<AtomicU64>,
    retry_total: Arc<AtomicU64>,
) -> JoinHandle<()> {
    let iv = hb.heartbeat_interval_secs.max(1);
    handle.spawn(heartbeat_loop(
        hb.control_socket,
        hb.worker_id,
        Duration::from_secs(iv),
        hb.client_connect_uri,
        events,
        retry_total,
    ))
}

pub(crate) async fn run_worker_with_config(
    worker_id: String,
    config_name: String,
    cfg: WorkerConfig,
    output_base: PathBuf,
    events: Arc<AtomicU64>,
    retry_total: Arc<AtomicU64>,
) -> Result<()> {
    let thread_count = cfg.threads.max(1) as usize;
    let mut set = JoinSet::new();
    for t in 0..thread_count {
        let loop_name = if thread_count == 1 {
            config_name.clone()
        } else {
            format!("{config_name}#{t}")
        };
        let cfg = cfg.clone();
        let output_base = output_base.clone();
        let events = events.clone();
        let retry_total = retry_total.clone();
        let wid = worker_id.clone();
        set.spawn(async move {
            run_worker_loop(wid, loop_name, cfg, output_base, events, retry_total).await
        });
    }

    while let Some(join_res) = set.join_next().await {
        join_res.map_err(|e| anyhow::anyhow!("{config_name}: loop join: {e}"))??;
    }
    Ok(())
}

async fn run_worker_loop(
    worker_id: String,
    config_name: String,
    cfg: WorkerConfig,
    output_base: PathBuf,
    events: Arc<AtomicU64>,
    retry_total: Arc<AtomicU64>,
) -> Result<()> {
    let WorkerConfig {
        template,
        fields,
        min_interval,
        sink,
        ..
    } = cfg;

    let mut line_sink = build_line_sink(
        &sink,
        output_base.as_path(),
        worker_id.as_str(),
        retry_total,
    )
    .with_context(|| format!("{config_name}: sink"))?;
    let mut runner = TemplateRunner::try_new(template, fields)
        .map_err(|e| anyhow::anyhow!("{config_name}: template runner: {e}"))?;
    if min_interval.is_zero() {
        loop {
            let line = runner
                .next_line()
                .map_err(|e| anyhow::anyhow!("{config_name}: render: {e}"))?;
            events.fetch_add(1, Ordering::Relaxed);
            line_sink
                .emit_line(&line)
                .await
                .with_context(|| format!("{config_name}: emit"))?;
        }
    }

    let mut tick = tokio::time::interval(min_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let line = runner
            .next_line()
            .map_err(|e| anyhow::anyhow!("{config_name}: render: {e}"))?;
        events.fetch_add(1, Ordering::Relaxed);
        line_sink
            .emit_line(&line)
            .await
            .with_context(|| format!("{config_name}: emit"))?;
    }
}
