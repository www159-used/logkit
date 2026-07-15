use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use logen_config::{connect_client_channel, ClientConnect};
use logen_model::{TemplateRunner, WorkerConfig};
use logen_proto::logen_client::LogenClient;
use logen_proto::HeartbeatRequest;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

use crate::sink::build_line_sink;

/// render → sink 有界队列深度；sink 慢时在此背压，避免无界堆积。
const LINE_PIPELINE_CAPACITY: usize = 4096;

/// 向守护进程上报心跳所需环境。
#[derive(Debug, Clone)]
pub struct WorkerHeartbeatEnv {
    pub connect: ClientConnect,
    pub worker_id: String,
    pub heartbeat_interval_secs: u64,
}

async fn heartbeat_loop(
    connect: ClientConnect,
    id: String,
    period: Duration,
    events: Arc<AtomicU64>,
    retry_total: Arc<AtomicU64>,
) {
    let Ok(channel) = connect_client_channel(&connect).await else {
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
        hb.connect,
        hb.worker_id,
        Duration::from_secs(iv),
        events,
        retry_total,
    ))
}

pub(crate) async fn run_worker_with_config(
    worker_id: String,
    config_name: String,
    cfg: WorkerConfig,
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
        let events = events.clone();
        let retry_total = retry_total.clone();
        let wid = worker_id.clone();
        set.spawn(async move { run_worker_loop(wid, loop_name, cfg, events, retry_total).await });
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

    let runner = TemplateRunner::try_new(template, fields)
        .map_err(|e| anyhow::anyhow!("{config_name}: template runner: {e}"))?;

    let mut line_sink = build_line_sink(&sink, worker_id.as_str(), retry_total)
        .map_err(|e| anyhow::anyhow!("{config_name}: sink: {e}"))?;
    let (line_tx, line_rx) = mpsc::channel(LINE_PIPELINE_CAPACITY);
    let sink_name = config_name.clone();
    let sink_task = tokio::spawn(async move {
        line_sink
            .drain_lines(line_rx)
            .await
            .map_err(|e| anyhow::anyhow!("{sink_name}: sink: {e}"))
    });

    let render_res = render_enqueue_loop(runner, &config_name, events, line_tx, min_interval).await;
    let sink_join = sink_task
        .await
        .map_err(|e| anyhow::anyhow!("{config_name}: sink task join: {e}"))?;

    render_res?;
    sink_join?;
    Ok(())
}

async fn render_enqueue_loop(
    mut runner: TemplateRunner,
    config_name: &str,
    events: Arc<AtomicU64>,
    line_tx: mpsc::Sender<String>,
    min_interval: Duration,
) -> Result<()> {
    if min_interval.is_zero() {
        loop {
            render_enqueue_once(&mut runner, config_name, &events, &line_tx).await?;
        }
    }

    let mut tick = tokio::time::interval(min_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        render_enqueue_once(&mut runner, config_name, &events, &line_tx).await?;
    }
}

async fn render_enqueue_once(
    runner: &mut TemplateRunner,
    config_name: &str,
    events: &Arc<AtomicU64>,
    line_tx: &mpsc::Sender<String>,
) -> Result<()> {
    let line = runner
        .next_line()
        .map_err(|e| anyhow::anyhow!("{config_name}: render: {e}"))?;
    line_tx
        .send(line)
        .await
        .map_err(|_| anyhow::anyhow!("{config_name}: sink pipeline closed"))?;
    events.fetch_add(1, Ordering::Relaxed);
    Ok(())
}
