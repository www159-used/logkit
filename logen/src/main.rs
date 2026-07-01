//! logen — CLI：`ping` / `echo` / `list` / `start` / `stop` / `stat` / `cat`。

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use logen_proto::logen_client::LogenClient;
use logen_proto::{
    CatWorkerRequest, EchoRequest, ListWorkersRequest, PingRequest, StartWorkerRequest,
    StatWorkerRequest, StopWorkerRequest,
};

use logen_config::{
    connect_client_channel, load_merged, ClientConnect, ClientOverrides, LogenError,
};
use logen_dsl::read_worker_instance_yaml;

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
    #[arg(short = 'S', long = "socket", alias = "sock", value_name = "PATH")]
    socket: Option<PathBuf>,

    /// 远端 logend 主机（TCP；也可在 TOML `[client]` 或环境变量 LOGEN_HOST 配置）
    #[arg(short = 'H', long = "host", value_name = "HOST", env = "LOGEN_HOST")]
    host: Option<String>,

    /// 远端 logend 端口（省略时默认约定端口 11159；`-H` 时生效）
    #[arg(short = 'P', long = "port", value_name = "PORT", env = "LOGEN_PORT")]
    port: Option<u16>,

    /// 覆盖 `[client].auto-kafka-protocol`；控制 `start` 时是否自动补全 Kafka 传输
    #[arg(long = "auto-kafka-protocol", value_name = "BOOL")]
    auto_kafka_protocol: Option<bool>,

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

    let client_overrides = ClientOverrides {
        socket: cli.socket.as_deref(),
        host: cli.host.as_deref(),
        port: cli.port,
        auto_kafka_protocol: cli.auto_kafka_protocol,
    };
    let connect = cfg.resolve_client_connect(client_overrides)?;

    let max_dec = cfg.logend.max_decoding_message_size_bytes as usize;
    let max_enc = cfg.logend.max_encoding_message_size_bytes as usize;

    let channel = connect_client_channel(&connect).await?;

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
            let instance_yaml = read_worker_instance_yaml(path.as_path())
                .map_err(|e| LogenError::Cli(e.to_string()))?;
            let kafka_broker_host = match &connect {
                ClientConnect::Tcp { host, .. } => Some(host.clone()),
                ClientConnect::Unix { .. } => None,
            };
            let r = client
                .start_worker(StartWorkerRequest {
                    instance_yaml,
                    config_label: config,
                    auto_kafka_protocol: cli.auto_kafka_protocol,
                    kafka_broker_host,
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
