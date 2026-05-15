//! 与 `logspout-daemon` 对接的运行循环：在 **同进程** Tokio 任务中直接消费内存里的 producer 配置。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http::Uri;
use hyper_util::rt::TokioIo;
use logspout_dsl::{TemplateConfig, TemplateRunner};
use logspout_proto::logspout_client::LogspoutClient;
use logspout_proto::HeartbeatRequest;
use tokio::task::JoinHandle;
use tonic::transport::Endpoint;
use tower::service_fn;

use crate::sink::{build_line_sink, LogLineSink};

/// 向守护进程上报心跳所需环境（与原先子进程环境变量一致）。
#[derive(Debug, Clone)]
pub struct ProducerHeartbeatEnv {
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
    let mut client = LogspoutClient::new(channel);
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let total = events.load(Ordering::Relaxed);
        if client
            .heartbeat(HeartbeatRequest {
                id: id.clone(),
                log_events_total: total,
            })
            .await
            .is_err()
        {
            break;
        }
    }
}

pub fn spawn_heartbeat_task(hb: ProducerHeartbeatEnv, events: Arc<AtomicU64>) -> JoinHandle<()> {
    let iv = hb.heartbeat_interval_secs.max(1);
    tokio::spawn(heartbeat_loop(
        hb.control_socket,
        hb.worker_id,
        Duration::from_secs(iv),
        hb.client_connect_uri,
        events,
    ))
}

pub(crate) async fn run_producer_with_config(
    config_name: String,
    cfg: TemplateConfig,
    output_base: PathBuf,
    events: Arc<AtomicU64>,
) -> Result<(), String> {
    if cfg.template.trim().is_empty() {
        return Err(r#"producer config: "template" must be non-empty"#.into());
    }

    let interval_ms = cfg.min_interval_ms;

    let mut sink: Box<dyn LogLineSink> =
        build_line_sink(&cfg, output_base.as_path()).map_err(|e| format!("{config_name}: {e}"))?;

    let mut runner = TemplateRunner::try_new(cfg).map_err(|e| format!("producer config: {e}"))?;

    let sleep = Duration::from_millis(interval_ms.max(1));
    let mut tick = tokio::time::interval(sleep);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let line = runner.next_line().map_err(|e| format!("render: {e}"))?;
        events.fetch_add(1, Ordering::Relaxed);
        sink.emit_line(&line)
            .await
            .map_err(|e| format!("{config_name}: {e}"))?;
    }
}
