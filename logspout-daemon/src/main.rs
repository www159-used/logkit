//! logspout-daemon — gRPC 控制面（Unix 套接字）；造日志由嵌入的 [`logspout_worker::run_producer_at_path`] 任务完成（亦可单独运行 `logspout-worker` 二进制调试）。

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use logspout_proto::logspout_server::{Logspout, LogspoutServer};
use logspout_proto::{
    CatLogServerReply, CatLogServerRequest, EchoReply, EchoRequest, HeartbeatReply,
    HeartbeatRequest, ListServersReply, ListServersRequest, LogServerEntry, PingReply, PingRequest,
    ServerStatDetail, StartLogServerReply, StartLogServerRequest, StatServerReply,
    StatServerRequest, StopLogServerReply, StopLogServerRequest,
};
use logspout_worker::{EmbeddedProducerWorker, ProducerHeartbeatEnv, TokioEmbeddedProducerWorker};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use logspout_config::{load_merged, LogspoutConfig, LogspoutError, WorkerSection};

#[derive(Parser)]
#[command(
    name = "logspout-daemon",
    version,
    about = "logspout-daemon — gRPC 控制面（Unix 套接字）；造日志在进程内由 logspout-worker 库驱动",
    disable_help_subcommand = true
)]
struct LogspoutDaemonCli {
    /// 与 logspout 共用的 TOML；也可由环境变量 LOGSPOUT_DEFAULTS_FILE 提供
    #[arg(long, value_name = "PATH", env = "LOGSPOUT_DEFAULTS_FILE")]
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

struct RunningServer {
    /// `logspout start` 传入的展示标签（多为用户本地路径）
    config_label: String,
    /// gRPC 投递的 YAML 全文（内存副本；`cat` 直接返回，不依赖托管文件是否仍存在）
    producer_yaml: String,
    /// 托管于 worker_output_dir/.logspout/{id}.yaml；[`logspout_worker::run_producer_at_path`] 启动时读入
    config_storage_path: String,
    worker_task: tokio::task::JoinHandle<()>,
    spawned_at: Instant,
    last_heartbeat: Instant,
    last_reported_log_events: u64,
    /// 上一心跳间隔内的 Δevents/Δt（采样）
    eps_interval: f64,
}

struct LogspoutSvcState {
    ping_reply: Arc<str>,
    worker: WorkerSection,
    control_socket_path: String,
    client_connect_uri: String,
    worker_output_dir: String,
    producer_worker: Arc<dyn EmbeddedProducerWorker>,
    servers: Mutex<HashMap<String, RunningServer>>,
}

#[derive(Clone)]
struct LogspoutSvc {
    inner: Arc<LogspoutSvcState>,
}

enum IdPick {
    One(String),
    None,
    Many(Vec<String>),
}

/// 优先精确 key；否则按 id `starts_with` 匹配；多个时返回全部（已排序）。
fn pick_server_id(guard: &HashMap<String, RunningServer>, key: &str) -> IdPick {
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

fn reap_exited(guard: &mut HashMap<String, RunningServer>) {
    let mut dead: Vec<String> = Vec::new();
    for (id, running) in guard.iter() {
        if running.worker_task.is_finished() {
            dead.push(id.clone());
        }
    }
    for id in dead {
        if let Some(r) = guard.remove(&id) {
            let _ = fs::remove_file(&r.config_storage_path);
        }
    }
}

#[tonic::async_trait]
impl Logspout for LogspoutSvc {
    async fn ping(
        &self,
        _req: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingReply>, tonic::Status> {
        Ok(tonic::Response::new(PingReply {
            pong: self.inner.ping_reply.to_string(),
        }))
    }

    async fn echo(
        &self,
        req: tonic::Request<EchoRequest>,
    ) -> Result<tonic::Response<EchoReply>, tonic::Status> {
        Ok(tonic::Response::new(EchoReply {
            message: req.into_inner().message,
        }))
    }

    async fn list_servers(
        &self,
        _req: tonic::Request<ListServersRequest>,
    ) -> Result<tonic::Response<ListServersReply>, tonic::Status> {
        let timeout = Duration::from_secs(self.inner.worker.heartbeat_timeout_secs.max(1));
        let mut guard = self.inner.servers.lock().await;
        reap_exited(&mut guard);

        let servers: Vec<LogServerEntry> = guard
            .iter()
            .map(|(id, r)| {
                let healthy = r.last_heartbeat.elapsed() <= timeout;
                LogServerEntry {
                    id: id.clone(),
                    config_path: r.config_label.clone(),
                    alive: true,
                    healthy,
                }
            })
            .collect();
        Ok(tonic::Response::new(ListServersReply { servers }))
    }

    async fn stat_server(
        &self,
        req: tonic::Request<StatServerRequest>,
    ) -> Result<tonic::Response<StatServerReply>, tonic::Status> {
        let prefix = req.into_inner().id_prefix;
        let timeout = Duration::from_secs(self.inner.worker.heartbeat_timeout_secs.max(1));
        let hb_timeout = self.inner.worker.heartbeat_timeout_secs;
        let hb_interval = self.inner.worker.heartbeat_interval_secs;

        let mut guard = self.inner.servers.lock().await;
        reap_exited(&mut guard);

        let now = Instant::now();
        let servers: Vec<ServerStatDetail> = guard
            .iter()
            .filter(|(id, _)| prefix.is_empty() || id.starts_with(&prefix))
            .map(|(id, r)| {
                let healthy = r.last_heartbeat.elapsed() <= timeout;
                let secs_hb = now.duration_since(r.last_heartbeat).as_secs_f64();
                let uptime = now.duration_since(r.spawned_at).as_secs_f64().max(1e-9);
                let events_est = r.last_reported_log_events as f64 + r.eps_interval * secs_hb;
                let eps_rt = events_est / uptime;
                ServerStatDetail {
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
                }
            })
            .collect();

        Ok(tonic::Response::new(StatServerReply { servers }))
    }

    async fn start_log_server(
        &self,
        req: tonic::Request<StartLogServerRequest>,
    ) -> Result<tonic::Response<StartLogServerReply>, tonic::Status> {
        let msg = req.into_inner();
        let yaml = msg.producer_yaml;
        if yaml.trim().is_empty() {
            return Err(tonic::Status::invalid_argument(
                "producer_yaml required (non-empty producer .yaml / .yml body)",
            ));
        }
        logspout_dsl::parse_template_config(Path::new("producer.yaml"), &yaml)
            .map_err(|e| tonic::Status::invalid_argument(format!("producer YAML: {e}")))?;

        let label = msg.config_label;
        let config_label = if label.trim().is_empty() {
            "(no label)".to_string()
        } else {
            label
        };

        let id = uuid::Uuid::new_v4().to_string();
        let worker_out = PathBuf::from(&self.inner.worker_output_dir);
        let storage_dir = worker_out.join(".logspout");
        tokio::fs::create_dir_all(&storage_dir).await.map_err(|e| {
            tonic::Status::internal(format!("create_dir_all {}: {e}", storage_dir.display()))
        })?;
        let storage_rel = storage_dir.join(format!("{id}.yaml"));
        tokio::fs::write(&storage_rel, yaml.as_bytes())
            .await
            .map_err(|e| {
                tonic::Status::internal(format!("write {}: {e}", storage_rel.display()))
            })?;
        let abs_s = fs::canonicalize(&storage_rel)
            .map_err(|e| {
                tonic::Status::internal(format!("canonicalize {}: {e}", storage_rel.display()))
            })?
            .to_string_lossy()
            .into_owned();

        let hb = ProducerHeartbeatEnv {
            control_socket: self.inner.control_socket_path.clone(),
            server_id: id.clone(),
            heartbeat_interval_secs: self.inner.worker.heartbeat_interval_secs.max(1),
            client_connect_uri: self.inner.client_connect_uri.clone(),
        };
        let output_base = PathBuf::from(&self.inner.worker_output_dir);
        let cfg_path = abs_s.clone();
        let worker_task = self.inner.producer_worker.spawn_producer_task(
            id.clone(),
            cfg_path,
            output_base,
            Some(hb),
        );

        let now = Instant::now();
        let mut guard = self.inner.servers.lock().await;
        guard.insert(
            id.clone(),
            RunningServer {
                config_label,
                producer_yaml: yaml,
                config_storage_path: abs_s,
                worker_task,
                spawned_at: now,
                last_heartbeat: now,
                last_reported_log_events: 0,
                eps_interval: 0.0,
            },
        );
        Ok(tonic::Response::new(StartLogServerReply {
            id,
            status: "started".into(),
        }))
    }

    async fn stop_log_server(
        &self,
        req: tonic::Request<StopLogServerRequest>,
    ) -> Result<tonic::Response<StopLogServerReply>, tonic::Status> {
        let id = req.into_inner().id;
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("id required"));
        }
        let mut guard = self.inner.servers.lock().await;
        reap_exited(&mut guard);
        let id = match pick_server_id(&guard, &id) {
            IdPick::One(s) => s,
            IdPick::None => {
                return Err(tonic::Status::not_found("no such log-server id"));
            }
            IdPick::Many(ids) => {
                return Err(tonic::Status::invalid_argument(format!(
                    "id prefix {id:?} matches multiple servers:\n{}",
                    ids.join("\n")
                )));
            }
        };
        let Some(running) = guard.remove(&id) else {
            return Err(tonic::Status::not_found("no such log-server id"));
        };
        let storage = running.config_storage_path.clone();
        running.worker_task.abort();
        let _ = fs::remove_file(&storage);
        Ok(tonic::Response::new(StopLogServerReply {
            status: "stopped".into(),
        }))
    }

    async fn cat_log_server(
        &self,
        req: tonic::Request<CatLogServerRequest>,
    ) -> Result<tonic::Response<CatLogServerReply>, tonic::Status> {
        let id = req.into_inner().id;
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("id required"));
        }
        let mut guard = self.inner.servers.lock().await;
        reap_exited(&mut guard);
        let id = match pick_server_id(&guard, &id) {
            IdPick::One(s) => s,
            IdPick::None => {
                return Err(tonic::Status::not_found("no such log-server id"));
            }
            IdPick::Many(ids) => {
                return Err(tonic::Status::invalid_argument(format!(
                    "id prefix {id:?} matches multiple servers:\n{}",
                    ids.join("\n")
                )));
            }
        };
        let Some(running) = guard.get(&id) else {
            return Err(tonic::Status::not_found("no such log-server id"));
        };
        let label = running.config_label.clone();
        let yaml = running.producer_yaml.clone();
        drop(guard);
        Ok(tonic::Response::new(CatLogServerReply {
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
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("id required"));
        }
        let mut guard = self.inner.servers.lock().await;
        let Some(running) = guard.get_mut(&id) else {
            return Err(tonic::Status::not_found("no such log-server id"));
        };
        if running.worker_task.is_finished() {
            if let Some(r) = guard.remove(&id) {
                let _ = fs::remove_file(&r.config_storage_path);
            }
            return Err(tonic::Status::failed_precondition(
                "log server producer task has ended",
            ));
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
async fn run(cfg: LogspoutConfig) -> Result<(), LogspoutError> {
    let worker_output_dir = cfg.worker.worker_output_dir.trim().to_string();
    if worker_output_dir.is_empty() {
        return Err(LogspoutError::Cli(
            "[worker].worker_output_dir must be set (non-empty directory; producer YAML \"output\" is relative to it)"
                .into(),
        ));
    }
    let worker_out_path = Path::new(&worker_output_dir);
    fs::create_dir_all(worker_out_path)
        .map_err(|e| LogspoutError::unix_io(worker_out_path.to_path_buf(), e))?;

    let pid_suffix = cfg.daemon.pid_record_suffix.clone();
    let max_dec = cfg.protocol.grpc.max_decoding_message_size_bytes as usize;
    let max_enc = cfg.protocol.grpc.max_encoding_message_size_bytes as usize;
    let ping_reply: Arc<str> =
        Arc::from(cfg.protocol.grpc.ping_reply_text.clone().into_boxed_str());
    let client_connect_uri = cfg.protocol.grpc.client_connect_uri.clone();

    let tmp_dir = cfg.tmp_dir_path();
    fs::create_dir_all(&tmp_dir).map_err(|e| LogspoutError::unix_io(tmp_dir.clone(), e))?;

    let pid_path_buf = cfg.daemon_pid_path();
    if pid_path_buf.exists() {
        let raw = fs::read_to_string(&pid_path_buf).unwrap_or_default();
        let trimmed = raw.trim();
        if let Ok(old) = trimmed.parse::<u32>() {
            if unix_process_exists(old) {
                return Err(LogspoutError::Cli(format!(
                    "logspout-daemon already running (pid {old}) under {}. Use a different [common].tmp_dir for another instance, or stop the existing process.",
                    tmp_dir.display()
                )));
            }
        }
        let _ = fs::remove_file(&pid_path_buf);
    }

    let socket_path_buf = cfg.daemon_socket_path();
    let control_socket_path = socket_path_buf.to_string_lossy().into_owned();
    let log_path_buf = cfg.daemon_log_path();
    let log_path = log_path_buf.to_string_lossy().into_owned();

    let sock = socket_path_buf.as_path();
    if sock.exists() {
        fs::remove_file(sock).map_err(|e| LogspoutError::unix_io(sock.to_path_buf(), e))?;
    }

    let uds =
        UnixListener::bind(sock).map_err(|e| LogspoutError::unix_io(sock.to_path_buf(), e))?;
    let incoming = UnixListenerStream::new(uds);

    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path.as_str())
        .map_err(|e| LogspoutError::write_file(log_path.clone(), e))?;

    let pid_body = format!("{}{}", std::process::id(), pid_suffix);
    fs::write(pid_path_buf.as_path(), pid_body)
        .map_err(|e| LogspoutError::write_file(pid_path_buf.to_string_lossy().into_owned(), e))?;
    let _pid_guard = PidFileGuard { path: pid_path_buf };

    writeln!(log, "logspout-daemon grpc on {}", sock.display())
        .map_err(|e| LogspoutError::write_file(log_path.clone(), e))?;
    log.flush()
        .map_err(|e| LogspoutError::write_file(log_path.clone(), e))?;

    let svc = LogspoutSvc {
        inner: Arc::new(LogspoutSvcState {
            ping_reply,
            worker: cfg.worker,
            control_socket_path,
            client_connect_uri,
            worker_output_dir,
            producer_worker: Arc::new(TokioEmbeddedProducerWorker),
            servers: Mutex::new(HashMap::new()),
        }),
    };
    let grpc = LogspoutServer::new(svc)
        .max_decoding_message_size(max_dec)
        .max_encoding_message_size(max_enc);

    Server::builder()
        .add_service(grpc)
        .serve_with_incoming(incoming)
        .await
        .map_err(|e| LogspoutError::Grpc(e.to_string()))?;

    Ok(())
}

#[cfg(unix)]
fn main() {
    let cli = LogspoutDaemonCli::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("logspout-daemon tokio runtime");
    rt.block_on(async {
        match load_merged(cli.defaults_file.as_deref()) {
            Ok(cfg) => {
                if let Err(e) = run(cfg).await {
                    eprintln!("{e}");
                    std::process::exit(1);
                }
            }
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
    });
}

#[cfg(not(unix))]
fn main() {
    eprintln!("logspout-daemon requires Unix domain sockets");
}
