//! 造日志子进程：由 `lsptd` 通过 `current_exe() worker -f CONFIG.json` 拉起（不单独配二进制路径）。

use std::env;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http::Uri;
use hyper_util::rt::TokioIo;
use lspt_proto::lspt_client::LsptClient;
use lspt_proto::HeartbeatRequest;
use tonic::transport::Endpoint;
use tower::service_fn;

fn usage() {
    eprintln!("usage: lsptd worker -f CONFIG.json");
}

fn first_event_from_sample(text: &str) -> String {
    let mut buf = String::new();
    for line in text.lines() {
        if line.is_empty() {
            if !buf.is_empty() {
                return buf.trim_end_matches('\n').to_string();
            }
        } else {
            buf.push_str(line);
            buf.push('\n');
        }
    }
    buf.trim_end_matches('\n').to_string()
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
    let mut client = LsptClient::new(channel);
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

async fn async_main(argv: Vec<String>) {
    let mut it = argv.into_iter().skip(2);
    let mut config_path: Option<String> = None;
    while let Some(a) = it.next() {
        if a == "-f" {
            config_path = it.next();
        } else {
            eprintln!("unknown arg: {a}");
            usage();
            std::process::exit(2);
        }
    }
    let Some(config_path) = config_path else {
        usage();
        std::process::exit(2);
    };

    let raw = fs::read_to_string(&config_path).unwrap_or_else(|e| {
        eprintln!("read config: {e}");
        std::process::exit(1);
    });
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap_or_else(|e| {
        eprintln!("parse json: {e}");
        std::process::exit(1);
    });

    let sample_rel = v["sample-file"].as_str().unwrap_or_else(|| {
        eprintln!("missing string field sample-file");
        std::process::exit(1);
    });

    let interval_ms = v["min-interval"].as_u64().unwrap_or(1000);

    let cfg_dir = std::path::Path::new(&config_path)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let sample_path = cfg_dir.join(sample_rel);
    let sample_txt = fs::read_to_string(&sample_path).unwrap_or_else(|e| {
        eprintln!("read sample-file {}: {e}", sample_path.display());
        std::process::exit(1);
    });

    let evt = first_event_from_sample(&sample_txt);
    if evt.is_empty() {
        eprintln!("empty first event in sample");
        std::process::exit(1);
    }
    let line = Arc::new(evt);
    let events = Arc::new(AtomicU64::new(0));

    if let (Ok(sock), Ok(id), Ok(iv_s), Ok(uri)) = (
        env::var("LSPT_CONTROL_SOCKET"),
        env::var("LSPT_SERVER_ID"),
        env::var("LSPT_HEARTBEAT_INTERVAL_SECS"),
        env::var("LSPT_CLIENT_CONNECT_URI"),
    ) {
        let iv = iv_s.parse::<u64>().unwrap_or(5).max(1);
        tokio::spawn(heartbeat_loop(
            sock,
            id,
            Duration::from_secs(iv),
            uri,
            events.clone(),
        ));
    }

    let sleep = Duration::from_millis(interval_ms.max(1));
    let mut tick = tokio::time::interval(sleep);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        events.fetch_add(1, Ordering::Relaxed);
        println!("{}", line);
    }
}

/// `argv` 须含 `program ... worker -f cfg.json`（与 `std::env::args()` 一致）。
pub fn run(argv: Vec<String>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("worker tokio runtime")
        .block_on(async_main(argv));
}
