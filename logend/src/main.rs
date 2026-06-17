use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use logen_proto::logen_server::{Logen, LogenServer};
use logen_proto::{
    CatWorkerReply, CatWorkerRequest, EchoReply, EchoRequest, HeartbeatReply, HeartbeatRequest,
    ListWorkersReply, ListWorkersRequest, PingReply, PingRequest, StartWorkerReply,
    StartWorkerRequest, StatWorkerReply, StatWorkerRequest, StopWorkerReply, StopWorkerRequest,
    WorkerEntry, WorkerStatDetail,
};
use logen_worker::{EmbeddedWorker, SpawnedWorkerTasks, TokioEmbeddedWorker, WorkerHeartbeatEnv};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use logen_config::{load_merged, LogenConfig, LogenError, LogendSection};
use logen_dsl::{format_sink_summary, parse_worker_config};
use tracing::{debug, info, trace, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

#[derive(Parser)]
#[command(
    name = "logend",
    version,
    about = "logend — gRPC control plane (Unix socket); embedded logen-worker drives worker instances",
    disable_help_subcommand = true
)]
struct LogendCli {
    /// 与 logen 共用的 TOML；也可由环境变量 LOGEN_DEFAULTS_FILE 提供
    #[arg(long, value_name = "PATH", env = "LOGEN_DEFAULTS_FILE")]
    defaults_file: Option<PathBuf>,
}

struct PidFileGuard {
    path: std::path::PathBuf,
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct RunningWorker {
    /// `logen start` 传入的展示标签（多为用户本地路径）
    config_label: String,
    /// gRPC 投递的 YAML 全文（内存副本；`cat` 直接返回，不依赖托管文件是否仍存在）
    instance_yaml: String,
    worker_task: tokio::task::JoinHandle<()>,
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
    spawned_at: Instant,
    last_heartbeat: Instant,
    last_reported_log_events: u64,
    /// 上一心跳间隔内的 Δevents/Δt（采样）
    eps_interval: f64,
    /// [`format_sink_summary`]，与嵌套 `sink:` 一致
    sink_summary: String,
    retry_total: u64,
}

struct LogenSvcState {
    ping_reply: Arc<str>,
    logend: LogendSection,
    control_socket_path: String,
    worker_heartbeat_uri: String,
    embedded_worker: Arc<dyn EmbeddedWorker>,
    workers: Mutex<HashMap<String, RunningWorker>>,
}

#[derive(Clone)]
struct LogenSvc {
    inner: Arc<LogenSvcState>,
}

enum IdPick {
    One(String),
    None,
    Many(Vec<String>),
}

/// 优先精确 key；否则按 id `starts_with` 匹配；多个时返回全部（已排序）。
fn pick_worker_id(guard: &HashMap<String, RunningWorker>, key: &str) -> IdPick {
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

fn reap_exited(guard: &mut HashMap<String, RunningWorker>) {
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

#[tonic::async_trait]
impl Logen for LogenSvc {
    async fn ping(
        &self,
        _req: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingReply>, tonic::Status> {
        trace!("rpc Ping");
        Ok(tonic::Response::new(PingReply {
            pong: self.inner.ping_reply.to_string(),
        }))
    }

    async fn echo(
        &self,
        req: tonic::Request<EchoRequest>,
    ) -> Result<tonic::Response<EchoReply>, tonic::Status> {
        let msg = req.into_inner().message;
        debug!("rpc Echo chars={}", msg.len());
        Ok(tonic::Response::new(EchoReply { message: msg }))
    }

    async fn list_workers(
        &self,
        _req: tonic::Request<ListWorkersRequest>,
    ) -> Result<tonic::Response<ListWorkersReply>, tonic::Status> {
        let timeout = Duration::from_secs(self.inner.logend.heartbeat_timeout_secs.max(1));
        let mut guard = self.inner.workers.lock().await;
        reap_exited(&mut guard);

        let workers: Vec<WorkerEntry> = guard
            .iter()
            .map(|(id, r)| {
                let healthy = r.last_heartbeat.elapsed() <= timeout;
                WorkerEntry {
                    id: id.clone(),
                    config_path: r.config_label.clone(),
                    alive: true,
                    healthy,
                    sink_summary: r.sink_summary.clone(),
                }
            })
            .collect();
        debug!("rpc ListWorkers -> {} entries", workers.len());
        Ok(tonic::Response::new(ListWorkersReply { workers }))
    }

    async fn stat_worker(
        &self,
        req: tonic::Request<StatWorkerRequest>,
    ) -> Result<tonic::Response<StatWorkerReply>, tonic::Status> {
        let prefix = req.into_inner().id_prefix;
        let timeout = Duration::from_secs(self.inner.logend.heartbeat_timeout_secs.max(1));
        let hb_timeout = self.inner.logend.heartbeat_timeout_secs;
        let hb_interval = self.inner.logend.heartbeat_interval_secs;

        let mut guard = self.inner.workers.lock().await;
        reap_exited(&mut guard);

        let now = Instant::now();
        let workers: Vec<WorkerStatDetail> = guard
            .iter()
            .filter(|(id, _)| prefix.is_empty() || id.starts_with(&prefix))
            .map(|(id, r)| {
                let healthy = r.last_heartbeat.elapsed() <= timeout;
                let secs_hb = now.duration_since(r.last_heartbeat).as_secs_f64();
                let uptime = now.duration_since(r.spawned_at).as_secs_f64().max(1e-9);
                let events_est = r.last_reported_log_events as f64 + r.eps_interval * secs_hb;
                let eps_rt = events_est / uptime;
                WorkerStatDetail {
                    id: id.clone(),
                    config_path: r.config_label.clone(),
                    alive: true,
                    healthy,
                    eps: eps_rt,
                    log_events_total: r.last_reported_log_events,
                    seconds_since_heartbeat: secs_hb,
                    heartbeat_timeout_secs: hb_timeout,
                    heartbeat_interval_secs: hb_interval,
                    eps_interval: r.eps_interval,
                    log_events_estimated: events_est,
                    sink_summary: r.sink_summary.clone(),
                    retry_total: r.retry_total,
                }
            })
            .collect();

        debug!(
            "rpc StatWorker id_prefix={:?} -> {} entries",
            prefix,
            workers.len()
        );
        Ok(tonic::Response::new(StatWorkerReply { workers }))
    }

    async fn start_worker(
        &self,
        req: tonic::Request<StartWorkerRequest>,
    ) -> Result<tonic::Response<StartWorkerReply>, tonic::Status> {
        let msg = req.into_inner();
        let yaml = msg.instance_yaml;
        if yaml.trim().is_empty() {
            return Err(tonic::Status::invalid_argument(
                "instance_yaml required (non-empty instance .yaml / .yml body)",
            ));
        }
        let worker_cfg = parse_worker_config(Path::new("instance.yaml"), &yaml)
            .map_err(|e| tonic::Status::invalid_argument(format!("实例 YAML: {e}")))?;
        let sink_summary = format_sink_summary(&worker_cfg.sink);

        let label = msg.config_label;
        let config_label = if label.trim().is_empty() {
            "(no label)".to_string()
        } else {
            label
        };

        let id = uuid::Uuid::new_v4().to_string();
        let hb = WorkerHeartbeatEnv {
            control_socket: self.inner.control_socket_path.clone(),
            worker_id: id.clone(),
            heartbeat_interval_secs: self.inner.logend.heartbeat_interval_secs.max(1),
            client_connect_uri: self.inner.worker_heartbeat_uri.clone(),
        };
        let output_base = PathBuf::from(&self.inner.logend.worker_output_dir);
        let SpawnedWorkerTasks {
            worker_task,
            heartbeat_task,
        } = self.inner.embedded_worker.spawn_worker_task(
            id.clone(),
            config_label.clone(),
            worker_cfg,
            output_base,
            Some(hb),
        );

        info!(
            "rpc StartWorker id={} label={:?} sink={}",
            id, config_label, sink_summary
        );

        let now = Instant::now();
        let mut guard = self.inner.workers.lock().await;
        guard.insert(
            id.clone(),
            RunningWorker {
                config_label,
                instance_yaml: yaml,
                worker_task,
                heartbeat_task,
                spawned_at: now,
                last_heartbeat: now,
                last_reported_log_events: 0,
                eps_interval: 0.0,
                sink_summary,
                retry_total: 0,
            },
        );
        Ok(tonic::Response::new(StartWorkerReply {
            id,
            status: "started".into(),
        }))
    }

    async fn stop_worker(
        &self,
        req: tonic::Request<StopWorkerRequest>,
    ) -> Result<tonic::Response<StopWorkerReply>, tonic::Status> {
        let id = req.into_inner().id;
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("id required"));
        }
        let mut guard = self.inner.workers.lock().await;
        reap_exited(&mut guard);
        let id = match pick_worker_id(&guard, &id) {
            IdPick::One(s) => s,
            IdPick::None => {
                return Err(tonic::Status::not_found("no such worker id"));
            }
            IdPick::Many(ids) => {
                debug!(
                    "rpc StopWorker ambiguous prefix {:?} matches {}",
                    id,
                    ids.len()
                );
                return Err(tonic::Status::invalid_argument(format!(
                    "id prefix {id:?} matches multiple workers:\n{}",
                    ids.join("\n")
                )));
            }
        };
        let Some(running) = guard.remove(&id) else {
            return Err(tonic::Status::not_found("no such worker id"));
        };
        info!("rpc StopWorker id={}", id);
        if let Some(task) = running.heartbeat_task {
            task.abort();
        }
        running.worker_task.abort();
        Ok(tonic::Response::new(StopWorkerReply {
            status: "stopped".into(),
        }))
    }

    async fn cat_worker(
        &self,
        req: tonic::Request<CatWorkerRequest>,
    ) -> Result<tonic::Response<CatWorkerReply>, tonic::Status> {
        let id = req.into_inner().id;
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("id required"));
        }
        let mut guard = self.inner.workers.lock().await;
        reap_exited(&mut guard);
        let id = match pick_worker_id(&guard, &id) {
            IdPick::One(s) => s,
            IdPick::None => {
                return Err(tonic::Status::not_found("no such worker id"));
            }
            IdPick::Many(ids) => {
                debug!(
                    "rpc CatWorker ambiguous prefix {:?} matches {}",
                    id,
                    ids.len()
                );
                return Err(tonic::Status::invalid_argument(format!(
                    "id prefix {id:?} matches multiple workers:\n{}",
                    ids.join("\n")
                )));
            }
        };
        let Some(running) = guard.get(&id) else {
            return Err(tonic::Status::not_found("no such worker id"));
        };
        debug!("rpc CatWorker id={}", id);
        let label = running.config_label.clone();
        let yaml = running.instance_yaml.clone();
        drop(guard);
        Ok(tonic::Response::new(CatWorkerReply {
            config_path: label,
            yaml,
        }))
    }

    async fn heartbeat(
        &self,
        req: tonic::Request<HeartbeatRequest>,
    ) -> Result<tonic::Response<HeartbeatReply>, tonic::Status> {
        let msg = req.into_inner();
        let id = msg.id;
        let log_events_total = msg.log_events_total;
        let retry_total = msg.retry_total;
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("id required"));
        }
        trace!(
            "rpc Heartbeat id={} log_events_total={} retry_total={}",
            id,
            log_events_total,
            retry_total
        );
        let mut guard = self.inner.workers.lock().await;
        let Some(running) = guard.get_mut(&id) else {
            warn!("rpc Heartbeat unknown worker id={}", id);
            return Err(tonic::Status::not_found("no such worker id"));
        };
        if running.worker_task.is_finished() {
            warn!("rpc Heartbeat worker task already finished id={}", id);
            if let Some(r) = guard.remove(&id) {
                if let Some(task) = r.heartbeat_task {
                    task.abort();
                }
            }
            return Err(tonic::Status::failed_precondition("worker task has ended"));
        }
        let now = Instant::now();
        let dt = now.duration_since(running.last_heartbeat);
        let secs = dt.as_secs_f64();
        if secs > 1e-9 && log_events_total >= running.last_reported_log_events {
            let de = log_events_total - running.last_reported_log_events;
            running.eps_interval = de as f64 / secs;
        }
        running.last_reported_log_events = log_events_total;
        running.last_heartbeat = now;
        running.retry_total = retry_total;
        Ok(tonic::Response::new(HeartbeatReply {}))
    }
}

#[cfg(unix)]
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

#[cfg(unix)]
fn build_worker_runtime(logend: &LogendSection) -> Result<tokio::runtime::Runtime, LogenError> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name("logend-worker-rt");
    if let Some(n) = logend.runtime_threads {
        builder.worker_threads(n.max(1));
    }
    builder
        .build()
        .map_err(|e| LogenError::Cli(format!("build worker runtime: {e}")))
}

#[cfg(unix)]
async fn run(cfg: LogenConfig, embedded_worker: Arc<dyn EmbeddedWorker>) -> Result<(), LogenError> {
    let logend = cfg.logend.clone();
    let worker_output_dir = logend.worker_output_dir.clone();
    let worker_out_path = Path::new(&worker_output_dir);
    fs::create_dir_all(worker_out_path)
        .map_err(|e| LogenError::unix_io(worker_out_path.to_path_buf(), e))?;

    let pid_suffix = logend.pid_record_suffix.clone();
    let max_dec = logend.max_decoding_message_size_bytes as usize;
    let max_enc = logend.max_encoding_message_size_bytes as usize;
    let ping_reply: Arc<str> = Arc::from(logend.ping_reply_text.clone().into_boxed_str());
    let worker_heartbeat_uri = cfg.worker_heartbeat_uri().to_string();

    let tmp_dir = logend.tmp_dir_path();
    fs::create_dir_all(&tmp_dir).map_err(|e| LogenError::unix_io(tmp_dir.clone(), e))?;

    let pid_path_buf = logend.pid_path();
    if pid_path_buf.exists() {
        let raw = fs::read_to_string(&pid_path_buf).unwrap_or_default();
        let trimmed = raw.trim();
        if let Ok(old) = trimmed.parse::<u32>() {
            if unix_process_exists(old) {
                return Err(LogenError::Cli(format!(
                    "logend already running (pid {old}) under {}. Use a different [logend].tmp_dir for another instance, or stop the existing process.",
                    tmp_dir.display()
                )));
            }
        }
        let _ = fs::remove_file(&pid_path_buf);
    }

    let socket_path_buf = logend.socket_path();
    let control_socket_path = socket_path_buf.to_string_lossy().into_owned();
    let log_path_buf = logend.log_path();

    let _log_handle = init_daemon_logging(log_path_buf.as_path(), &logend.log_level)?;

    let sock = socket_path_buf.as_path();
    info!(
        "logend starting pid={} tmp_dir={} uds={} worker_output_dir={} log_file={} default_log_spec={} (RUST_LOG overrides if set)",
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
            control_socket_path,
            worker_heartbeat_uri,
            embedded_worker,
            workers: Mutex::new(HashMap::new()),
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

#[cfg(unix)]
fn main() {
    let cli = LogendCli::parse();
    let cfg = match load_merged(cli.defaults_file.as_deref()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("logend tokio runtime");
    let worker_runtime = match build_worker_runtime(&cfg.logend) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let embedded_worker: Arc<dyn EmbeddedWorker> = Arc::new(TokioEmbeddedWorker::new(
        worker_runtime.handle().clone(),
        rt.handle().clone(),
    ));
    rt.block_on(async {
        if let Err(e) = run(cfg, embedded_worker).await {
            eprintln!("{e}");
            std::process::exit(1);
        }
    });
}

#[cfg(not(unix))]
fn main() {
    eprintln!("logend requires Unix domain sockets");
}
