//! logen — CLI：`ping` / `echo` / `list` / `start` / `stop` / `stat` / `cat`。

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use http::Uri;
use hyper_util::rt::TokioIo;
use logen_proto::logen_client::LogenClient;
use logen_proto::{
    CatWorkerRequest, EchoRequest, ListWorkersRequest, PingRequest, StartWorkerRequest,
    StatWorkerRequest, StopWorkerRequest,
};
use tonic::transport::Endpoint;
use tower::service_fn;

use logen_config::{load_merged, LogenError};
use logen_dsl::{load_worker_config, worker_config_to_yaml};

#[derive(Parser)]
#[command(
    name = "logen",
    version,
    about = "logen — gRPC client (Unix socket)",
    disable_help_subcommand = true
)]
struct Cli {
    /// 与 logend 共用的 TOML；也可由环境变量 LOGEN_DEFAULTS_FILE 提供
    #[arg(long, value_name = "PATH", env = "LOGEN_DEFAULTS_FILE")]
    defaults_file: Option<PathBuf>,

    /// 覆盖由 [common].tmp_dir 推导的 Unix 套接字路径（默认 {tmp_dir}/logend.sock）
    #[arg(short = 'S', long = "sock", value_name = "PATH")]
    socket: Option<PathBuf>,

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
async fn run() -> Result<(), LogenError> {
    let cli = Cli::parse();
    let cfg = load_merged(cli.defaults_file.as_deref())?;

    let sock_path = if let Some(p) = &cli.socket {
        p.display().to_string()
    } else {
        cfg.client_socket_path()
    };
    let max_dec = cfg.protocol.grpc.max_decoding_message_size_bytes as usize;
    let max_enc = cfg.protocol.grpc.max_encoding_message_size_bytes as usize;

    if !Path::new(&sock_path).exists() {
        return Err(LogenError::Cli(format!(
            "unix socket \"{sock_path}\" does not exist. Start logend first, or pass -S/--sock PATH pointing at the daemon socket, \
             or set [common].tmp_dir via --defaults-file / LOGEN_DEFAULTS_FILE (same as logend)."
        )));
    }

    let path = sock_path.clone();
    let endpoint = Endpoint::from_shared(cfg.protocol.grpc.client_connect_uri.clone())
        .map_err(|e| LogenError::Grpc(e.to_string()))?;
    let channel = endpoint
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
                "transport error on unix socket {sock_path}: {e}. \
                 Try -S/--sock matching logend's socket, or fix [common].tmp_dir in --defaults-file."
            ))
        })?;

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
                println!("sec_since_hb:\t{:.3}", s.seconds_since_heartbeat);
                println!("hb_timeout_s:\t{}", s.heartbeat_timeout_secs);
                println!("hb_interval_s:\t{}", s.heartbeat_interval_secs);
                println!();
            }
        }
        Commands::Start { config } => {
            let path = PathBuf::from(&config);
            let merged = load_worker_config(path.as_path())
                .map_err(|e| LogenError::Cli(e.to_string()))?;
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
