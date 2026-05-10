//! lsptd — gRPC 控制面（Unix 套接字）；`worker` 子命令由守护进程自举拉起造日志。

mod worker;

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use lspt_proto::lspt_server::{Lspt, LsptServer};
use lspt_proto::{
    CatLogServerReply, CatLogServerRequest, EchoReply, EchoRequest, HeartbeatReply, HeartbeatRequest,
    ListServersReply, ListServersRequest, LogServerEntry, PingReply, PingRequest, ServerStatDetail,
    StartLogServerReply, StartLogServerRequest, StatServerReply, StatServerRequest,
    StopLogServerReply, StopLogServerRequest,
};
use tokio::net::UnixListener;
use tokio::process::Child;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use lspt_config::{load_merged, LogServerSection, LsptConfig, LsptError};

#[derive(Parser)]
#[command(
    name = "lsptd",
    version,
    about = "lsptd — gRPC 控制面（Unix 套接字）；子命令 worker 造日志",
    disable_help_subcommand = true
)]
struct LsptdCli {
    /// 与 lspt 共用的 TOML；也可由环境变量 LSPT_DEFAULTS_FILE 提供
    #[arg(long, value_name = "PATH", env = "LSPT_DEFAULTS_FILE")]
    defaults_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<LsptdCmd>,
}

#[derive(Subcommand)]
enum LsptdCmd {
    /// 造日志 worker（通常由守护进程 spawn；可手跑调试）
    Worker {
        #[arg(short = 'f', value_name = "CONFIG.yaml")]
        config: String,
    },
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
    /// `lspt start` 传入的展示标签（多为用户本地路径）
    config_label: String,
    /// gRPC 投递的 YAML 全文（内存副本；`cat` 直接返回，不依赖托管文件是否仍存在）
    producer_yaml: String,
    /// 托管于 worker_output_dir/.lspt/{id}.yaml，供子进程 `worker -f` **启动时读一次** 后载入内存
    config_storage_path: String,
    child: Child,
    spawned_at: Instant,
    last_heartbeat: Instant,
    last_reported_log_events: u64,
    /// 上一心跳间隔内的 Δevents/Δt（采样）
    eps_interval: f64,
}

struct LsptSvcState {
    ping_reply: Arc<str>,
    log_server: LogServerSection,
    control_socket_path: String,
    client_connect_uri: String,
    worker_output_dir: String,
    servers: Mutex<HashMap<String, RunningServer>>,
}

#[derive(Clone)]
struct LsptSvc {
    inner: Arc<LsptSvcState>,
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
    let mut ids: Vec<String> = guard.keys().filter(|id| id.starts_with(key)).cloned().collect();
    ids.sort();
    match ids.len() {
        0 => IdPick::None,
        1 => IdPick::One(ids[0].clone()),
        _ => IdPick::Many(ids),
    }
}

fn reap_exited(guard: &mut HashMap<String, RunningServer>) {
    let mut dead: Vec<String> = Vec::new();
    for (id, running) in guard.iter_mut() {
        match running.child.try_wait() {
            Ok(Some(_)) => dead.push(id.clone()),
            Ok(None) => {}
            Err(e) => {
                dead.push(id.clone());
                let _ = writeln!(std::io::stderr(), "lsptd try_wait {}: {e}", id);
            }
        }
    }
    for id in dead {
        if let Some(r) = guard.remove(&id) {
            let _ = fs::remove_file(&r.config_storage_path);
        }
    }
}

#[tonic::async_trait]
impl Lspt for LsptSvc {
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
        let timeout = Duration::from_secs(self.inner.log_server.heartbeat_timeout_secs.max(1));
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
        let timeout = Duration::from_secs(self.inner.log_server.heartbeat_timeout_secs.max(1));
        let hb_timeout = self.inner.log_server.heartbeat_timeout_secs;
        let hb_interval = self.inner.log_server.heartbeat_interval_secs;

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
                let events_est =
                    r.last_reported_log_events as f64 + r.eps_interval * secs_hb;
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
        lspt_ext::parse_template_config(Path::new("producer.yaml"), &yaml).map_err(|e| {
            tonic::Status::invalid_argument(format!("producer YAML: {e}"))
        })?;

        let label = msg.config_label;
        let config_label = if label.trim().is_empty() {
            "(no label)".to_string()
        } else {
            label
        };

        let id = uuid::Uuid::new_v4().to_string();
        let worker_out = PathBuf::from(&self.inner.worker_output_dir);
        let storage_dir = worker_out.join(".lspt");
        tokio::fs::create_dir_all(&storage_dir).await.map_err(|e| {
            tonic::Status::internal(format!("create_dir_all {}: {e}", storage_dir.display()))
        })?;
        let storage_rel = storage_dir.join(format!("{id}.yaml"));
        tokio::fs::write(&storage_rel, yaml.as_bytes()).await.map_err(|e| {
            tonic::Status::internal(format!("write {}: {e}", storage_rel.display()))
        })?;
        let abs_s = fs::canonicalize(&storage_rel).map_err(|e| {
            tonic::Status::internal(format!("canonicalize {}: {e}", storage_rel.display()))
        })?
        .to_string_lossy()
        .into_owned();

        let exe = std::env::current_exe()
            .map_err(|e| tonic::Status::internal(format!("current_exe: {e}")))?;
        let hb_iv = self.inner.log_server.heartbeat_interval_secs.max(1).to_string();
        let child = tokio::process::Command::new(exe)
            .current_dir(&self.inner.worker_output_dir)
            .arg("worker")
            .arg("-f")
            .arg(&abs_s)
            .env("LSPT_CONTROL_SOCKET", &self.inner.control_socket_path)
            .env("LSPT_SERVER_ID", &id)
            .env("LSPT_HEARTBEAT_INTERVAL_SECS", &hb_iv)
            .env("LSPT_CLIENT_CONNECT_URI", &self.inner.client_connect_uri)
            .spawn()
            .map_err(|e| tonic::Status::internal(format!("spawn worker: {e}")))?;

        let now = Instant::now();
        let mut guard = self.inner.servers.lock().await;
        guard.insert(
            id.clone(),
            RunningServer {
                config_label,
                producer_yaml: yaml,
                config_storage_path: abs_s,
                child,
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
        let Some(mut running) = guard.remove(&id) else {
            return Err(tonic::Status::not_found("no such log-server id"));
        };
        let storage = running.config_storage_path.clone();
        running
            .child
            .kill()
            .await
            .map_err(|e| tonic::Status::internal(format!("kill log server (worker): {e}")))?;
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
        match running.child.try_wait() {
            Ok(Some(status)) => {
                if let Some(r) = guard.remove(&id) {
                    let _ = fs::remove_file(&r.config_storage_path);
                }
                Err(tonic::Status::failed_precondition(format!(
                    "log server worker exited: {status}"
                )))
            }
            Ok(None) => {
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
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
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
async fn run(cfg: LsptConfig) -> Result<(), LsptError> {
    let worker_output_dir = cfg.log_server.worker_output_dir.trim().to_string();
    if worker_output_dir.is_empty() {
        return Err(LsptError::Cli(
            "[log_server].worker_output_dir must be set (non-empty directory; producer YAML \"output\" is relative to it)"
                .into(),
        ));
    }
    let worker_out_path = Path::new(&worker_output_dir);
    fs::create_dir_all(worker_out_path).map_err(|e| LsptError::unix_io(worker_out_path.to_path_buf(), e))?;

    let pid_suffix = cfg.daemon.pid_record_suffix.clone();
    let max_dec = cfg.protocol.grpc.max_decoding_message_size_bytes as usize;
    let max_enc = cfg.protocol.grpc.max_encoding_message_size_bytes as usize;
    let ping_reply: Arc<str> = Arc::from(cfg.protocol.grpc.ping_reply_text.clone().into_boxed_str());
    let client_connect_uri = cfg.protocol.grpc.client_connect_uri.clone();

    let tmp_dir = cfg.tmp_dir_path();
    fs::create_dir_all(&tmp_dir).map_err(|e| LsptError::unix_io(tmp_dir.clone(), e))?;

    let pid_path_buf = cfg.daemon_pid_path();
    if pid_path_buf.exists() {
        let raw = fs::read_to_string(&pid_path_buf).unwrap_or_default();
        let trimmed = raw.trim();
        if let Ok(old) = trimmed.parse::<u32>() {
            if unix_process_exists(old) {
                return Err(LsptError::Cli(format!(
                    "lsptd already running (pid {old}) under {}. Use a different [common].tmp_dir for another instance, or stop the existing process.",
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
        fs::remove_file(sock).map_err(|e| LsptError::unix_io(sock.to_path_buf(), e))?;
    }

    let uds = UnixListener::bind(sock).map_err(|e| LsptError::unix_io(sock.to_path_buf(), e))?;
    let incoming = UnixListenerStream::new(uds);

    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path.as_str())
        .map_err(|e| LsptError::write_file(log_path.clone(), e))?;

    let pid_body = format!("{}{}", std::process::id(), pid_suffix);
    fs::write(pid_path_buf.as_path(), pid_body)
        .map_err(|e| LsptError::write_file(pid_path_buf.to_string_lossy().into_owned(), e))?;
    let _pid_guard = PidFileGuard {
        path: pid_path_buf,
    };

    writeln!(log, "lsptd grpc on {}", sock.display())
        .map_err(|e| LsptError::write_file(log_path.clone(), e))?;
    log.flush()
        .map_err(|e| LsptError::write_file(log_path.clone(), e))?;

    let svc = LsptSvc {
        inner: Arc::new(LsptSvcState {
            ping_reply,
            log_server: cfg.log_server,
            control_socket_path,
            client_connect_uri,
            worker_output_dir,
            servers: Mutex::new(HashMap::new()),
        }),
    };
    let grpc = LsptServer::new(svc)
        .max_decoding_message_size(max_dec)
        .max_encoding_message_size(max_enc);

    Server::builder()
        .add_service(grpc)
        .serve_with_incoming(incoming)
        .await
        .map_err(|e| LsptError::Grpc(e.to_string()))?;

    Ok(())
}

#[cfg(unix)]
fn main() {
    let cli = LsptdCli::parse();
    if let Some(LsptdCmd::Worker { config }) = cli.command {
        let exe = std::env::args().next().unwrap_or_else(|| "lsptd".to_string());
        worker::run(vec![exe, "worker".into(), "-f".into(), config]);
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("lsptd tokio runtime");
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
    eprintln!("lsptd requires Unix domain sockets");
}
