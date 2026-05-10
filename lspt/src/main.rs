//! lspt — CLI：`ping` / `echo` / `list` / `start` / `stop` / `stat`。

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use http::Uri;
use hyper_util::rt::TokioIo;
use lspt_proto::lspt_client::LsptClient;
use lspt_proto::{
    EchoRequest, ListServersRequest, PingRequest, StartLogServerRequest, StatServerRequest,
    StopLogServerRequest,
};
use tonic::transport::Endpoint;
use tower::service_fn;

use lspt_config::{load_merged, LsptConfig, LsptError};

#[derive(Parser)]
#[command(
    name = "lspt",
    version,
    about = "lspt gRPC 客户端（Unix 套接字）",
    disable_help_subcommand = true
)]
struct Cli {
    /// 与 lsptd 共用的 TOML；也可由环境变量 LSPT_DEFAULTS_FILE 提供
    #[arg(long, value_name = "PATH", env = "LSPT_DEFAULTS_FILE")]
    defaults_file: Option<PathBuf>,

    /// 覆盖合并配置中的 gRPC Unix 套接字路径（等价于 [client].socket_path）
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
        config_path: String,
    },
    Stop {
        id: String,
    },
    Stat {
        id_prefix: Option<String>,
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
fn merge_cli_config(cli: &Cli, mut cfg: LsptConfig) -> LsptConfig {
    if let Some(p) = &cli.socket {
        cfg.client.socket_path = p.display().to_string();
    }
    cfg
}

#[cfg(unix)]
async fn run() -> Result<(), LsptError> {
    let cli = Cli::parse();
    let cfg = merge_cli_config(&cli, load_merged(cli.defaults_file.as_deref())?);

    let sock_path = cfg.client.socket_path.clone();
    let max_dec = cfg.protocol.grpc.max_decoding_message_size_bytes as usize;
    let max_enc = cfg.protocol.grpc.max_encoding_message_size_bytes as usize;

    if !Path::new(&sock_path).exists() {
        return Err(LsptError::Cli(format!(
            "unix socket \"{sock_path}\" does not exist. Start lsptd first, or pass -S/--sock PATH pointing at the daemon socket, \
             or set [client].socket_path via --defaults-file / LSPT_DEFAULTS_FILE."
        )));
    }

    let path = sock_path.clone();
    let endpoint = Endpoint::from_shared(cfg.protocol.grpc.client_connect_uri.clone())
        .map_err(|e| LsptError::Grpc(e.to_string()))?;
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
            LsptError::Grpc(format!(
                "transport error on unix socket {sock_path}: {e}. \
                 Try -S/--sock with the same path as lsptd [daemon].socket_path, or fix --defaults-file."
            ))
        })?;

    let mut client = LsptClient::new(channel)
        .max_decoding_message_size(max_dec)
        .max_encoding_message_size(max_enc);

    match cli.command {
        Commands::Ping => {
            let r = client
                .ping(PingRequest {})
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().pong);
        }
        Commands::Echo { text } => {
            let r = client
                .echo(EchoRequest {
                    message: text.join(" "),
                })
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().message);
        }
        Commands::List => {
            let r = client
                .list_servers(ListServersRequest {})
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            for s in r.into_inner().servers {
                println!("{}\t{}\t{}\t{}", s.id, s.config_path, s.alive, s.healthy);
            }
        }
        Commands::Stat { id_prefix } => {
            let id_prefix = id_prefix.unwrap_or_default();
            let r = client
                .stat_server(StatServerRequest { id_prefix })
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            let list = r.into_inner().servers;
            if list.is_empty() {
                println!("(no matching servers)");
                return Ok(());
            }
            for s in list {
                println!("id:\t\t{}", s.id);
                println!("config_path:\t{}", s.config_path);
                println!("alive:\t\t{}", s.alive);
                println!("healthy:\t{}", s.healthy);
                println!("eps:\t\t{:.3}\t(realtime est.: extrapolated total / uptime)", s.eps);
                println!("eps_interval:\t{:.3}\t(last heartbeat window Δ/Δt)", s.eps_interval);
                println!("events_total:\t{}", s.log_events_total);
                println!("events_est:\t{:.1}", s.log_events_estimated);
                println!("sec_since_hb:\t{:.3}", s.seconds_since_heartbeat);
                println!("hb_timeout_s:\t{}", s.heartbeat_timeout_secs);
                println!("hb_interval_s:\t{}", s.heartbeat_interval_secs);
                println!();
            }
        }
        Commands::Start { config_path } => {
            if config_path.is_empty() {
                return Err(LsptError::Cli("start needs producer YAML path (.yaml or .yml)".into()));
            }
            let r = client
                .start_log_server(StartLogServerRequest { config_path })
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            let inner = r.into_inner();
            println!("{}\t{}", inner.id, inner.status);
        }
        Commands::Stop { id } => {
            if id.is_empty() {
                return Err(LsptError::Cli("stop needs <id>".into()));
            }
            let r = client
                .stop_log_server(StopLogServerRequest { id })
                .await
                .map_err(|s| LsptError::Grpc(s.to_string()))?;
            println!("{}", r.into_inner().status);
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("lspt requires Unix domain sockets");
}
