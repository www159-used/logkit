//! daemon 启动：pid、日志、UDS/TCP 监听。

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use logen_config::{LogenConfig, LogenError, LogendSection};
use logen_proto::logen_server::LogenServer;
use logen_proto::PingReply;
use logen_worker::EmbeddedWorker;
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

use crate::svc::{LogenSvc, LogenSvcState};

struct PidFileGuard {
    path: std::path::PathBuf,
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// `{tmp_dir}/logend.log`（非阻塞写入）。`RUST_LOG` 优先于 `default_spec`。
fn init_daemon_logging(
    log_path: &Path,
    default_spec: &str,
) -> Result<tracing_appender::non_blocking::WorkerGuard, LogenError> {
    let default_spec = default_spec.trim();
    let default_spec = if default_spec.is_empty() {
        "info"
    } else {
        default_spec
    };
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_spec));

    let parent = log_path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = log_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("logend.log");
    let file_appender = tracing_appender::rolling::never(parent, file_name);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_target(true)
        .compact();

    Registry::default()
        .with(env_filter)
        .with(file_layer)
        .try_init()
        .map_err(|e| LogenError::Cli(format!("tracing-subscriber init: {e}")))?;

    Ok(file_guard)
}

fn unix_process_exists(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if ret == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

pub fn build_worker_runtime(logend: &LogendSection) -> Result<tokio::runtime::Runtime, LogenError> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name("logend-worker-rt");
    if let Some(n) = logend.runtime_threads {
        builder.worker_threads(n.max(1));
    }
    builder
        .build()
        .map_err(|e| LogenError::Cli(format!("build worker runtime: {e}")))
}

pub async fn run(
    cfg: LogenConfig,
    embedded_worker: Arc<dyn EmbeddedWorker>,
) -> Result<(), LogenError> {
    let logend = cfg.logend.clone();
    let worker_output_dir = logend.worker_output_dir.clone();
    let worker_out_path = Path::new(&worker_output_dir);
    fs::create_dir_all(worker_out_path)
        .map_err(|e| LogenError::unix_io(worker_out_path.to_path_buf(), e))?;

    let pid_suffix = logend.pid_record_suffix.clone();
    let max_dec = logend.max_decoding_message_size_bytes as usize;
    let max_enc = logend.max_encoding_message_size_bytes as usize;
    let ping_reply = PingReply {
        pong: logend.ping_reply_text.clone(),
    };

    let tmp_dir = logend.tmp_dir_path();
    fs::create_dir_all(&tmp_dir).map_err(|e| LogenError::unix_io(tmp_dir.clone(), e))?;

    let pid_path_buf = logend.pid_path();
    if pid_path_buf.exists() {
        let raw = fs::read_to_string(&pid_path_buf).unwrap_or_default();
        let trimmed = raw.trim();
        if let Ok(old) = trimmed.parse::<u32>() {
            if unix_process_exists(old) {
                return Err(LogenError::Cli(format!(
                    "logend already running (pid {old}) under {}. Use a different [logend].tmp-dir for another instance, or stop the existing process.",
                    tmp_dir.display()
                )));
            }
        }
        let _ = fs::remove_file(&pid_path_buf);
    }

    let socket_path_buf = logend.socket_path();
    let log_path_buf = logend.log_path();

    let _log_handle = init_daemon_logging(log_path_buf.as_path(), &logend.log_level)?;

    let sock = socket_path_buf.as_path();
    info!(
        "logend starting pid={} tmp-dir={} uds={} worker-output-dir={} log_file={} default_log_spec={} (RUST_LOG overrides if set)",
        std::process::id(),
        tmp_dir.display(),
        sock.display(),
        worker_output_dir,
        log_path_buf.display(),
        logend.log_level.trim(),
    );

    if sock.exists() {
        fs::remove_file(sock).map_err(|e| LogenError::unix_io(sock.to_path_buf(), e))?;
    }

    let uds = UnixListener::bind(sock).map_err(|e| LogenError::unix_io(sock.to_path_buf(), e))?;
    let incoming = UnixListenerStream::new(uds);

    let pid_body = format!("{}{}", std::process::id(), pid_suffix);
    fs::write(pid_path_buf.as_path(), pid_body)
        .map_err(|e| LogenError::write_file(pid_path_buf.to_string_lossy().into_owned(), e))?;
    let _pid_guard = PidFileGuard { path: pid_path_buf };

    info!("listening for gRPC on {}", sock.display());

    let tcp_addr = logend.tcp_listen_addr()?;

    let svc = LogenSvc {
        inner: Arc::new(LogenSvcState {
            ping_reply,
            logend,
            client: cfg.client.clone(),
            embedded_worker,
            workers: Mutex::new(HashMap::new()),
            sessions: crate::session::ControlSessionStore::default(),
        }),
    };

    let make_server = |s: LogenSvc| {
        LogenServer::new(s)
            .max_decoding_message_size(max_dec)
            .max_encoding_message_size(max_enc)
    };

    if let Some(addr) = tcp_addr {
        info!("listening for gRPC on tcp {addr}");
        let uds_server = make_server(svc.clone());
        let tcp_server = make_server(svc);
        tokio::try_join!(
            Server::builder()
                .add_service(uds_server)
                .serve_with_incoming(incoming),
            Server::builder().add_service(tcp_server).serve(addr),
        )
        .map_err(|e| LogenError::Grpc(e.to_string()))?;
    } else {
        Server::builder()
            .add_service(make_server(svc))
            .serve_with_incoming(incoming)
            .await
            .map_err(|e| LogenError::Grpc(e.to_string()))?;
    }

    Ok(())
}
