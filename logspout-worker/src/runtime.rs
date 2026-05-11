//! 与 `logspout-daemon` 或独立 CLI 对接的运行循环：**同进程** `tokio::spawn` 或单独二进制均可调用。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http::Uri;
use hyper_util::rt::TokioIo;
use logspout_dsl::{parse_template_config, LineSinkType, TemplateConfig, TemplateRunner};
use logspout_proto::logspout_client::LogspoutClient;
use logspout_proto::HeartbeatRequest;
use tonic::transport::Endpoint;
use tower::service_fn;

use crate::sink::{build_line_sink, validate_kafka_config, LogLineSink};

/// 向守护进程上报心跳所需环境（与原先子进程环境变量一致）。
#[derive(Debug, Clone)]
pub struct ProducerHeartbeatEnv {
    pub control_socket: String,
    pub server_id: String,
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

fn spawn_heartbeat(hb: ProducerHeartbeatEnv, events: Arc<AtomicU64>) {
    let iv = hb.heartbeat_interval_secs.max(1);
    tokio::spawn(heartbeat_loop(
        hb.control_socket,
        hb.server_id,
        Duration::from_secs(iv),
        hb.client_connect_uri,
        events,
    ));
}

/// 从 producer YAML 路径读取配置并在 **`output_base` 下解析相对 `output`**，按 `min-interval` 循环写出。
///
/// - **嵌入 daemon**：`output_base` = `[worker].worker_output_dir`（勿依赖 `set_current_dir`，多实例共享进程 cwd）。
/// - **独立二进制**：`output_base` = `std::env::current_dir()` 或期望的工作目录。
///
/// `heartbeat` 为 `Some` 时，并行向本机 daemon 套接字发送心跳（与子进程模式行为一致）。
pub async fn run_producer_at_path(
    config_path: String,
    output_base: PathBuf,
    heartbeat: Option<ProducerHeartbeatEnv>,
) -> Result<(), String> {
    let raw = std::fs::read_to_string(&config_path)
        .map_err(|e| format!("read config: {e}"))?;
    let cfg: TemplateConfig = parse_template_config(Path::new(&config_path), &raw)
        .map_err(|e| format!("parse producer config: {e}"))?;
    if cfg.template.trim().is_empty() {
        return Err(r#"producer config: "template" must be non-empty"#.into());
    }

    if cfg.sink.sink_type == LineSinkType::Kafka {
        if let Some(ref k) = cfg.sink.kafka {
            validate_kafka_config(k).map_err(|e| format!("{config_path}: {e}"))?;
        }
    }

    let interval_ms = cfg.min_interval_ms;

    let mut sink: Box<dyn LogLineSink> = build_line_sink(&cfg, output_base.as_path())
        .map_err(|e| format!("{config_path}: {e}"))?;

    let mut runner = TemplateRunner::try_new(cfg).map_err(|e| format!("producer config: {e}"))?;

    let events = Arc::new(AtomicU64::new(0));
    if let Some(hb) = heartbeat {
        spawn_heartbeat(hb, events.clone());
    }

    let sleep = Duration::from_millis(interval_ms.max(1));
    let mut tick = tokio::time::interval(sleep);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let line = runner
            .next_line()
            .map_err(|e| format!("render: {e}"))?;
        events.fetch_add(1, Ordering::Relaxed);
        sink.emit_line(&line).await.map_err(|e| format!("{config_path}: {e}"))?;
    }
}
