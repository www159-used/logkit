//! lsptd — gRPC 控制面（Unix 套接字）；`worker` 子命令由守护进程自举拉起造日志。

mod worker;

use std::collections::HashMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lspt_proto::lspt_server::{Lspt, LsptServer};
use lspt_proto::{
    EchoReply, EchoRequest, HeartbeatReply, HeartbeatRequest, ListServersReply,
    ListServersRequest, LogServerEntry, PingReply, PingRequest, ServerStatDetail,
    StartLogServerReply, StartLogServerRequest, StatServerReply, StatServerRequest,
    StopLogServerReply, StopLogServerRequest,
};
use tokio::net::UnixListener;
use tokio::process::Child;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use lspt_config::{load_merged, parse_cli_args, LogServerSection, LsptConfig, LsptError};

struct PidFileGuard {
    path: std::path::PathBuf,
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct RunningServer {
    config_path: String,
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

fn collect_rel_paths_prefixed(cwd: &Path, prefix: &str) -> std::io::Result<Vec<String>> {
    let mut out = Vec::new();
    let mut stack = vec![cwd.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in fs::read_dir(&dir)? {
            let e = e?;
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.is_file() {
                let Ok(rel) = p.strip_prefix(cwd) else {
                    continue;
                };
                let s = rel.to_string_lossy().replace('\\', "/");
                if s.starts_with(prefix) {
                    out.push(s);
                }
            }
        }
    }
    out.sort();
    Ok(out)
}

/// `path` 已是非空。若为现有普通文件则原样返回；否则按 **lsptd 进程 cwd** 下相对路径前缀匹配唯一文件。
fn resolve_config_path_arg(path: &str) -> Result<String, tonic::Status> {
    let p = Path::new(path);
    if p.is_file() {
        return Ok(path.to_string());
    }
    if p.exists() {
        return Err(tonic::Status::invalid_argument(format!(
            "config_path exists but is not a regular file: {path}"
        )));
    }
    let cwd = env::current_dir().map_err(|e| {
        tonic::Status::internal(format!("current_dir for config prefix resolution: {e}"))
    })?;
    let candidates = collect_rel_paths_prefixed(&cwd, path).map_err(|e| {
        tonic::Status::internal(format!("scan cwd for config prefix {path:?}: {e}"))
    })?;
    match candidates.len() {
        0 => Err(tonic::Status::invalid_argument(format!(
            "no file under lsptd cwd matches path prefix {path:?}"
        ))),
        1 => Ok(candidates[0].clone()),
        _ => Err(tonic::Status::invalid_argument(format!(
            "ambiguous config path prefix {path:?} matches:\n{}",
            candidates.join("\n")
        ))),
    }
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
        guard.remove(&id);
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
                    config_path: r.config_path.clone(),
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
                    config_path: r.config_path.clone(),
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
        let path = msg.config_path;
        if path.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "config_path required (producer .yaml / .yml path)",
            ));
        }
        let path = resolve_config_path_arg(&path)?;
        let exe = std::env::current_exe()
            .map_err(|e| tonic::Status::internal(format!("current_exe: {e}")))?;
        let id = uuid::Uuid::new_v4().to_string();
        let hb_iv = self.inner.log_server.heartbeat_interval_secs.max(1).to_string();
        let child = tokio::process::Command::new(exe)
            .arg("worker")
            .arg("-f")
            .arg(&path)
            .env("LSPT_CONTROL_SOCKET", &self.inner.control_socket_path)
            .env("LSPT_SERVER_ID", &id)
            .env("LSPT_HEARTBEAT_INTERVAL_SECS", &hb_iv)
            .env("LSPT_CLIENT_CONNECT_URI", &self.inner.client_connect_uri)
            .env(
                "LSPT_WORKER_OUTPUT_DIR",
                &self.inner.worker_output_dir,
            )
            .spawn()
            .map_err(|e| tonic::Status::internal(format!("spawn worker: {e}")))?;

        let now = Instant::now();
        let mut guard = self.inner.servers.lock().await;
        guard.insert(
            id.clone(),
            RunningServer {
                config_path: path,
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
        running
            .child
            .kill()
            .await
            .map_err(|e| tonic::Status::internal(format!("kill log server (worker): {e}")))?;
        Ok(tonic::Response::new(StopLogServerReply {
            status: "stopped".into(),
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
                guard.remove(&id);
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
async fn run(cfg: LsptConfig) -> Result<(), LsptError> {
    let LsptConfig {
        daemon,
        protocol,
        log_server,
        ..
    } = cfg;

    let worker_output_dir = log_server.worker_output_dir.trim().to_string();
    if worker_output_dir.is_empty() {
        return Err(LsptError::Cli(
            "[log_server].worker_output_dir must be set (non-empty directory; producer YAML \"output\" is relative to it)"
                .into(),
        ));
    }
    let worker_out_path = Path::new(&worker_output_dir);
    fs::create_dir_all(worker_out_path).map_err(|e| LsptError::unix_io(worker_out_path.to_path_buf(), e))?;

    let socket_path = daemon.socket_path.clone();
    let control_socket_path = daemon.socket_path;
    let client_connect_uri = protocol.grpc.client_connect_uri.clone();
    let log_path = daemon.log_file;
    let pid_path = daemon.pid_file;
    let pid_suffix = daemon.pid_record_suffix;

    let max_dec = protocol.grpc.max_decoding_message_size_bytes as usize;
    let max_enc = protocol.grpc.max_encoding_message_size_bytes as usize;
    let ping_reply: Arc<str> = Arc::from(protocol.grpc.ping_reply_text.into_boxed_str());

    let sock = Path::new(&socket_path);
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
    fs::write(pid_path.as_str(), pid_body)
        .map_err(|e| LsptError::write_file(pid_path.clone(), e))?;
    let _pid_guard = PidFileGuard {
        path: Path::new(&pid_path).to_path_buf(),
    };

    writeln!(log, "lsptd grpc on {}", socket_path)
        .map_err(|e| LsptError::write_file(log_path.clone(), e))?;
    log.flush()
        .map_err(|e| LsptError::write_file(log_path.clone(), e))?;

    let svc = LsptSvc {
        inner: Arc::new(LsptSvcState {
            ping_reply,
            log_server,
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
    let argv: Vec<String> = env::args().collect();
    if argv.len() >= 2 && argv[1] == "worker" {
        worker::run(argv);
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("lsptd tokio runtime");
    let args: Vec<String> = env::args().skip(1).collect();
    rt.block_on(async {
        match parse_cli_args(args).and_then(|(path, _)| load_merged(path.as_deref())) {
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
