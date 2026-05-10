//! producer **仅**支持 **YAML**（路径须 `.yaml` / `.yml`）：`template`（必填）、`fields`、`min-interval`、`output`；不再支持 `sample-file` 旧格式。
//! - 长模板可用 YAML 块标量（`>` / `>-`）折行，避免一行过长。
//! - 由 **lsptd** 拉起的 worker 在 spawn 时 **`current_dir` 已设为** TOML `[log_server].worker_output_dir`（必填）；
//!   producer YAML 的 **`output` 相对该目录（即子进程初始 pwd）**，日志文件 **`append`** 打开。
//! - 手动运行 `lsptd worker` 时继承 shell 的 pwd：**`output` 相对当前工作目录**；省略则写标准输出。
//!
//! 随机抽样等走 [`fake::Fake`]，不直接 `use rand`。

use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
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
    eprintln!("usage: lsptd worker -f CONFIG.yaml");
}

/// 在 `0..len` 上均匀随机下标（`len > 0`）。仅用 `fake`，不直接依赖 `rand`。
#[allow(dead_code)] // Handlebars / `generators` 配置将使用
fn fake_uniform_index(len: usize) -> usize {
    use fake::Fake;
    debug_assert!(len > 0);
    (0..len).fake::<usize>()
}

/// 从切片中均匀随机选一项。
#[allow(dead_code)] // Handlebars / `generators` 配置将使用
fn fake_choose<'a, T>(items: &'a [T]) -> Option<&'a T> {
    if items.is_empty() {
        None
    } else {
        Some(&items[fake_uniform_index(items.len())])
    }
}

#[cfg(test)]
mod fake_pick_tests {
    use super::*;

    #[test]
    fn uniform_index_stays_in_range() {
        for _ in 0..300 {
            let len = 20;
            let i = fake_uniform_index(len);
            assert!(i < len);
        }
    }

    #[test]
    fn choose_none_on_empty() {
        let empty: &[u8] = &[];
        assert!(fake_choose(empty).is_none());
    }

    #[test]
    fn choose_some_from_slice() {
        let v = [1u8, 2, 3];
        for _ in 0..50 {
            assert!(matches!(fake_choose(&v), Some(1) | Some(2) | Some(3)));
        }
    }
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

fn spawn_heartbeat_if_env(events: Arc<AtomicU64>) {
    let (Ok(sock), Ok(id), Ok(iv_s), Ok(uri)) = (
        env::var("LSPT_CONTROL_SOCKET"),
        env::var("LSPT_SERVER_ID"),
        env::var("LSPT_HEARTBEAT_INTERVAL_SECS"),
        env::var("LSPT_CLIENT_CONNECT_URI"),
    ) else {
        return;
    };
    let iv = iv_s.parse::<u64>().unwrap_or(5).max(1);
    tokio::spawn(heartbeat_loop(
        sock,
        id,
        Duration::from_secs(iv),
        uri,
        events,
    ));
}

enum LogSink {
    Stdout,
    File(BufWriter<std::fs::File>),
}

impl LogSink {
    fn emit_line(&mut self, line: &str) {
        match self {
            LogSink::Stdout => println!("{}", line),
            LogSink::File(w) => {
                if let Err(e) = writeln!(w, "{}", line).and_then(|_| w.flush()) {
                    eprintln!("write output: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}

/// `output` 相对 **进程当前工作目录**（pwd）：lsptd 拉起 worker 时已 `current_dir(worker_output_dir)`；手跑则继承 shell。省略 `output` 则 stdout。
fn log_sink(config_path: &str, output_rel: Option<&str>) -> LogSink {
    let cwd = env::current_dir().unwrap_or_else(|e| {
        eprintln!("current_dir: {e} (fall back to directory of config file)");
        Path::new(config_path)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    });
    match output_rel {
        None => LogSink::Stdout,
        Some(r) => {
            let path = cwd.join(r);
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap_or_else(|e| {
                    eprintln!("open output {}: {e}", path.display());
                    std::process::exit(1);
                });
            LogSink::File(BufWriter::new(f))
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
    let cfg: lspt_ext::TemplateConfig =
        lspt_ext::parse_template_config(Path::new(&config_path), &raw).unwrap_or_else(|e| {
            eprintln!("parse producer config: {e}");
            std::process::exit(1);
        });
    if cfg.template.trim().is_empty() {
        eprintln!("producer config: \"template\" must be non-empty");
        std::process::exit(1);
    }

    let interval_ms = cfg.min_interval_ms;
    let output_rel = cfg
        .output
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    let mut sink = log_sink(&config_path, output_rel);

    let mut runner = lspt_ext::TemplateRunner::try_new(cfg).unwrap_or_else(|e| {
        eprintln!("producer config: {e}");
        std::process::exit(1);
    });
    let events = Arc::new(AtomicU64::new(0));
    spawn_heartbeat_if_env(events.clone());

    let sleep = Duration::from_millis(interval_ms.max(1));
    let mut tick = tokio::time::interval(sleep);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let line = runner.next_line().unwrap_or_else(|e| {
            eprintln!("render: {e}");
            std::process::exit(1);
        });
        events.fetch_add(1, Ordering::Relaxed);
        sink.emit_line(&line);
    }
}

/// `argv` 须含 `program ... worker -f cfg.yaml`（与 `std::env::args()` 一致）。
pub fn run(argv: Vec<String>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("worker tokio runtime")
        .block_on(async_main(argv));
}
