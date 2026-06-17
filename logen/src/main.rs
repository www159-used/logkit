//! logen — CLI：`ping` / `echo` / `list` / `start` / `stop` / `stat` / `cat`。

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use http::Uri;
use hyper_util::rt::TokioIo;
use logen_proto::logen_client::LogenClient;
use logen_proto::{
    CatWorkerRequest, EchoRequest, ListWorkersRequest, PingRequest, StartWorkerRequest,
    StatWorkerRequest, StopWorkerRequest,
};
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

use logen_config::{
    load_merged, ClientConnect, ClientOverrides, ClientTransport, LogenError,
};
use logen_dsl::{load_worker_config, worker_config_to_yaml};

#[derive(Parser)]
#[command(
    name = "logen",
    version,
    about = "logen — gRPC client for logend (unix or tcp)",
    disable_help_subcommand = true
)]
struct Cli {
    /// 与 logend 共用的 TOML；也可由环境变量 LOGEN_DEFAULTS_FILE 提供
    #[arg(long, value_name = "PATH", env = "LOGEN_DEFAULTS_FILE")]
    defaults_file: Option<PathBuf>,

    /// 覆盖 Unix 套接字路径（与 `-H`/`-P` 互斥时优先于 `[client]` TCP）
    #[arg(short = 'S', long = "sock", value_name = "PATH")]
    socket: Option<PathBuf>,

    /// 远端 logend 主机（TCP；也可在 TOML `[client]` 或环境变量 LOGEN_HOST 配置）
    #[arg(short = 'H', long = "host", value_name = "HOST", env = "LOGEN_HOST")]
    host: Option<String>,

    /// 远端 logend 端口（TCP；也可在 TOML `[client]` 或环境变量 LOGEN_PORT 配置）
    #[arg(short = 'P', long = "port", value_name = "PORT", env = "LOGEN_PORT")]
    port: Option<u16>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Ping,
    Echo {
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        text: Vec<String>,
    },
    #[command(aliases = ["ls"])]
    List,
    Start {
        ///实例 YAML（单文件；见 [`logen_dsl`]）。
        #[arg(required = true, value_name = "CONFIG.yaml")]
        config: String,
    },
    Stop {
        id: String,
    },
    Stat {
        id_prefix: Option<String>,
    },
    /// 打印运行中 worker 实例的 YAML（id 支持唯一前缀，与 stop 相同）
    Cat {
        id: String,
    },
}

#[cfg(unix)]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

#[cfg(unix)]
async fn connect_channel(connect: &ClientConnect) -> Result<Channel, LogenError> {
    match connect.transport {
        ClientTransport::Unix => {
            let sock_path = connect.unix_socket.as_ref().ok_or_else(|| {
                LogenError::Cli("internal error: unix connect without socket path".into())
            })?;
            if !sock_path.exists() {
                return Err(LogenError::Cli(format!(
                    "unix socket \"{}\" does not exist. Start logend first, or pass -S/--sock, \
                     or set [client] host/port for tcp remote.",
                    sock_path.display()
                )));
            }
            let path = sock_path.to_string_lossy().into_owned();
            let endpoint = Endpoint::from_shared(connect.endpoint_uri.clone())
                .map_err(|e| LogenError::Grpc(e.to_string()))?;
            endpoint
                .connect_with_connector(service_fn(move |_: Uri| {
                    let path = path.clone();
                    async move {
                        let s = tokio::net::UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(TokioIo::new(s))
                    }
                }))
                .await
                .map_err(|e| {
                    LogenError::Grpc(format!(
                        "transport error on unix socket {}: {e}",
                        sock_path.display()
                    ))
                })
        }
        ClientTransport::Tcp => {
            Endpoint::from_shared(connect.endpoint_uri.clone())
                .map_err(|e| LogenError::Grpc(e.to_string()))?
                .connect()
                .await
                .map_err(|e| {
                    LogenError::Grpc(format!(
                        "tcp connect to {} failed: {e}. Check logend [logend].listen and firewall.",
                        connect.endpoint_uri
                    ))
                })
        }
    }
}

#[cfg(unix)]
async fn run() -> Result<(), LogenError> {
    let cli = Cli::parse();
    let cfg = load_merged(cli.defaults_file.as_deref())?;

    let connect = cfg.resolve_client_connect(ClientOverrides {
        socket: cli.socket.as_deref(),
        host: cli.host.as_deref(),
        port: cli.port,
    })?;

    let max_dec = cfg.logend.max_decoding_message_size_bytes as usize;
    let max_enc = cfg.logend.max_encoding_message_size_bytes as usize;

    let channel = connect_channel(&connect).await?;

    let mut client = LogenClient::new(channel)
        .max_decoding_message_size(max_dec)
        .max_encoding_message_size(max_enc);

    match cli.command {
        Commands::Ping => {
            let r = client
                .ping(PingRequest {})
                .await
                .map_err(|s| LogenError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().pong);
        }
        Commands::Echo { text } => {
            let r = client
                .echo(EchoRequest {
                    message: text.join(" "),
                })
                .await
                .map_err(|s| LogenError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().message);
        }
        Commands::List => {
            let r = client
                .list_workers(ListWorkersRequest {})
                .await
                .map_err(|s| LogenError::Grpc(s.to_string()))?;
            println!("id\talive\thealthy\tsink");
            for s in r.into_inner().workers {
                println!("{}\t{}\t{}\t{}", s.id, s.alive, s.healthy, s.sink_summary);
            }
        }
        Commands::Stat { id_prefix } => {
            let id_prefix = id_prefix.unwrap_or_default();
            let r = client
                .stat_worker(StatWorkerRequest { id_prefix })
                .await
                .map_err(|s| LogenError::Grpc(s.to_string()))?;
            let list = r.into_inner().workers;
            if list.is_empty() {
                println!("(no matching workers)");
                return Ok(());
            }
            for s in list {
                println!("id:\t\t{}", s.id);
                println!("config_path:\t{}", s.config_path);
                println!("sink:\t\t{}", s.sink_summary);
                println!("alive:\t\t{}", s.alive);
                println!("healthy:\t{}", s.healthy);
                println!(
                    "eps:\t\t{:.3}\t(realtime est.: extrapolated total / uptime)",
                    s.eps
                );
                println!(
                    "eps_interval:\t{:.3}\t(last heartbeat window Δ/Δt)",
                    s.eps_interval
                );
                println!("events_total:\t{}", s.log_events_total);
                println!("events_est:\t{:.1}", s.log_events_estimated);
                println!("retry_total:\t{}", s.retry_total);
                println!("sec_since_hb:\t{:.3}", s.seconds_since_heartbeat);
                println!("hb_timeout_s:\t{}", s.heartbeat_timeout_secs);
                println!("hb_interval_s:\t{}", s.heartbeat_interval_secs);
                println!();
            }
        }
        Commands::Start { config } => {
            let path = PathBuf::from(&config);
            let merged =
                load_worker_config(path.as_path()).map_err(|e| LogenError::Cli(e.to_string()))?;
            let instance_yaml = worker_config_to_yaml(&merged)
                .map_err(|e| LogenError::Cli(format!("serialize instance YAML: {e}")))?;
            let config_label = config;
            let r = client
                .start_worker(StartWorkerRequest {
                    instance_yaml,
                    config_label,
                })
                .await
                .map_err(|s| LogenError::Grpc(s.to_string()))?;
            let inner = r.into_inner();
            println!("{}\t{}", inner.id, inner.status);
        }
        Commands::Stop { id } => {
            if id.is_empty() {
                return Err(LogenError::Cli("stop needs <id>".into()));
            }
            let r = client
                .stop_worker(StopWorkerRequest { id })
                .await
                .map_err(|s| LogenError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().status);
        }
        Commands::Cat { id } => {
            if id.is_empty() {
                return Err(LogenError::Cli("cat needs <id>".into()));
            }
            let r = client
                .cat_worker(CatWorkerRequest { id })
                .await
                .map_err(|s| LogenError::Grpc(s.to_string()))?;
            let inner = r.into_inner();
            print!("{}", inner.yaml);
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("logen requires Unix domain sockets");
}
