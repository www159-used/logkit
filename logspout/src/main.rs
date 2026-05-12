//! logspout — CLI：`ping` / `echo` / `list` / `start` / `stop` / `stat` / `cat`。

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use http::Uri;
use hyper_util::rt::TokioIo;
use logspout_proto::logspout_client::LogspoutClient;
use logspout_proto::{
    CatWorkerRequest, EchoRequest, ListWorkersRequest, PingRequest, StartWorkerRequest,
    StatWorkerRequest, StopWorkerRequest,
};
use tonic::transport::Endpoint;
use tower::service_fn;

use logspout_config::{load_merged, LogspoutError};
use logspout_dsl::{load_and_merge_producer_paths, template_config_to_yaml};

#[derive(Parser)]
#[command(
    name = "logspout",
    version,
    about = "logspout — gRPC client (Unix socket)",
    disable_help_subcommand = true
)]
struct Cli {
    /// 与 logspout-daemon 共用的 TOML；也可由环境变量 LOGSPOUT_DEFAULTS_FILE 提供
    #[arg(long, value_name = "PATH", env = "LOGSPOUT_DEFAULTS_FILE")]
    defaults_file: Option<PathBuf>,

    /// 覆盖由 [common].tmp_dir 推导的 Unix 套接字路径（默认 {tmp_dir}/logspout-daemon.sock）
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
    List,
    Start {
        /// Producer YAML：可多个路径，CLI 合并为单份配置再传给 daemon（后者覆盖前者）。
        #[arg(required = true, num_args = 1.., value_name = "CONFIG.yaml")]
        config_paths: Vec<String>,
    },
    Stop {
        id: String,
    },
    Stat {
        id_prefix: Option<String>,
    },
    /// 打印运行中实例的 producer YAML（id 支持唯一前缀，与 stop 相同）
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
async fn run() -> Result<(), LogspoutError> {
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
        return Err(LogspoutError::Cli(format!(
            "unix socket \"{sock_path}\" does not exist. Start logspout-daemon first, or pass -S/--sock PATH pointing at the daemon socket, \
             or set [common].tmp_dir via --defaults-file / LOGSPOUT_DEFAULTS_FILE (same as logspout-daemon)."
        )));
    }

    let path = sock_path.clone();
    let endpoint = Endpoint::from_shared(cfg.protocol.grpc.client_connect_uri.clone())
        .map_err(|e| LogspoutError::Grpc(e.to_string()))?;
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
            LogspoutError::Grpc(format!(
                "transport error on unix socket {sock_path}: {e}. \
                 Try -S/--sock matching logspout-daemon's socket, or fix [common].tmp_dir in --defaults-file."
            ))
        })?;

    let mut client = LogspoutClient::new(channel)
        .max_decoding_message_size(max_dec)
        .max_encoding_message_size(max_enc);

    match cli.command {
        Commands::Ping => {
            let r = client
                .ping(PingRequest {})
                .await
                .map_err(|s| LogspoutError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().pong);
        }
        Commands::Echo { text } => {
            let r = client
                .echo(EchoRequest {
                    message: text.join(" "),
                })
                .await
                .map_err(|s| LogspoutError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().message);
        }
        Commands::List => {
            let r = client
                .list_workers(ListWorkersRequest {})
                .await
                .map_err(|s| LogspoutError::Grpc(s.to_string()))?;
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
                .map_err(|s| LogspoutError::Grpc(s.to_string()))?;
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
        Commands::Start { config_paths } => {
            let paths: Vec<PathBuf> = config_paths.iter().map(PathBuf::from).collect();
            let merged = load_and_merge_producer_paths(&paths)
                .map_err(|e| LogspoutError::Cli(e.to_string()))?;
            let producer_yaml = template_config_to_yaml(&merged)
                .map_err(|e| LogspoutError::Cli(format!("serialize merged producer YAML: {e}")))?;
            let config_label = config_paths.join(" + ");
            let r = client
                .start_worker(StartWorkerRequest {
                    producer_yaml,
                    config_label,
                })
                .await
                .map_err(|s| LogspoutError::Grpc(s.to_string()))?;
            let inner = r.into_inner();
            println!("{}\t{}", inner.id, inner.status);
        }
        Commands::Stop { id } => {
            if id.is_empty() {
                return Err(LogspoutError::Cli("stop needs <id>".into()));
            }
            let r = client
                .stop_worker(StopWorkerRequest { id })
                .await
                .map_err(|s| LogspoutError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().status);
        }
        Commands::Cat { id } => {
            if id.is_empty() {
                return Err(LogspoutError::Cli("cat needs <id>".into()));
            }
            let r = client
                .cat_worker(CatWorkerRequest { id })
                .await
                .map_err(|s| LogspoutError::Grpc(s.to_string()))?;
            let inner = r.into_inner();
            print!("{}", inner.yaml);
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("logspout requires Unix domain sockets");
}
