//! logen — 控制脚本提交 CLI。

use std::io::Read;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use logen_proto::logen_client::LogenClient;
use logen_proto::{
    CloseControlSessionRequest, EvalControlSessionRequest, OpenControlSessionRequest,
    RunControlScriptRequest,
};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use logen_config::{
    connect_client_channel, load_merged, ClientConnect, ClientOverrides, LogenError,
};

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
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        /// 控制脚本（`.logen`；在远端 logend 内解释，须显式调用 `start(logen(...))`）。
        #[arg(value_name = "CONFIG.logen|-")]
        config: Option<String>,
        /// 直接执行一段控制脚本。
        #[arg(short = 'e', long, conflicts_with = "config", value_name = "SOURCE")]
        source: Option<String>,
    },
}

/// 读取 `.logen` 控制脚本全文；拒绝 YAML 入口（硬切换，不兼容）。
fn read_control_script(path: &Path) -> Result<String, LogenError> {
    if path == Path::new("-") {
        let mut source = String::new();
        std::io::stdin()
            .read_to_string(&mut source)
            .map_err(|e| LogenError::Cli(format!("stdin: {e}")))?;
        return Ok(source);
    }
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if ext != "logen" {
        return Err(LogenError::Cli(format!(
            "run requires a .logen script (got {}); YAML instance configs are no longer accepted",
            path.display()
        )));
    }
    std::fs::read_to_string(path).map_err(|e| LogenError::Cli(format!("{}: {e}", path.display())))
}

fn run_source(
    config: Option<String>,
    source: Option<String>,
) -> Result<(String, String), LogenError> {
    match (config, source) {
        (Some(path), None) => {
            let source = read_control_script(Path::new(&path))?;
            let label = if path == "-" { "(stdin)".into() } else { path };
            Ok((source, label))
        }
        (None, Some(source)) => Ok((source, "(eval)".into())),
        (None, None) => Err(LogenError::Cli(
            "run requires CONFIG.logen, `-` for stdin, or --eval SOURCE".into(),
        )),
        (Some(_), Some(_)) => unreachable!("clap rejects conflicting run sources"),
    }
}

fn kafka_broker_host(connect: &ClientConnect) -> Option<String> {
    match connect {
        ClientConnect::Tcp { host, .. } => Some(host.clone()),
        ClientConnect::Unix { .. } => None,
    }
}

fn print_control_reply(reply: logen_proto::RunControlScriptReply) {
    print!("{}", reply.output);
    if !reply.worker_id.is_empty() {
        println!("{}\t{}", reply.worker_id, reply.status);
    }
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
        Some(Commands::Run { config, source }) => {
            let (script, config_label) = run_source(config, source)?;
            let kafka_broker_host = kafka_broker_host(&connect);
            let r = client
                .run_control_script(RunControlScriptRequest {
                    script,
                    config_label,
                    auto_kafka_protocol: cli.auto_kafka_protocol,
                    kafka_broker_host,
                })
                .await
                .map_err(|s| LogenError::Grpc(s.to_string()))?;
            print_control_reply(r.into_inner());
        }
        None => {
            let session = client
                .open_control_session(OpenControlSessionRequest {
                    config_label: "interactive".into(),
                    auto_kafka_protocol: cli.auto_kafka_protocol,
                    kafka_broker_host: kafka_broker_host(&connect),
                })
                .await
                .map_err(|s| LogenError::Grpc(s.to_string()))?
                .into_inner()
                .session_id;
            let mut editor = DefaultEditor::new()
                .map_err(|e| LogenError::Cli(format!("interactive editor: {e}")))?;
            loop {
                match editor.readline("logen> ") {
                    Ok(line) => {
                        let source = line.trim();
                        if source.is_empty() {
                            continue;
                        }
                        if matches!(source, ":quit" | ":exit") {
                            break;
                        }
                        if source == ":help" {
                            println!(":help  :quit  :exit  :source FILE.logen");
                            continue;
                        }
                        let source = if let Some(path) = source.strip_prefix(":source ") {
                            read_control_script(Path::new(path.trim()))?
                        } else {
                            source.to_string()
                        };
                        let _ = editor.add_history_entry(&source);
                        match client
                            .eval_control_session(EvalControlSessionRequest {
                                session_id: session.clone(),
                                source,
                            })
                            .await
                        {
                            Ok(reply) => {
                                print_control_reply(reply.into_inner());
                            }
                            Err(err) => eprintln!("{err}"),
                        }
                    }
                    Err(ReadlineError::Interrupted) => continue,
                    Err(ReadlineError::Eof) => break,
                    Err(err) => return Err(LogenError::Cli(format!("interactive editor: {err}"))),
                }
            }
            let _ = client
                .close_control_session(CloseControlSessionRequest {
                    session_id: session,
                })
                .await;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("logen requires Unix domain sockets");
}
