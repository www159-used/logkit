//! 独立二进制：与嵌入 daemon 共用 [`logspout_worker::run_producer_at_path`]。

use std::env;
use std::path::Path;

use clap::Parser;
use logspout_worker::{ProducerHeartbeatEnv, run_producer_at_path};

#[derive(Parser)]
#[command(name = "logspout-worker", disable_help_subcommand = true)]
struct Cli {
    #[arg(short = 'f', value_name = "CONFIG.yaml")]
    config: String,
}

fn output_base_for_cli(config_path: &str) -> std::path::PathBuf {
    env::current_dir().unwrap_or_else(|_| {
        Path::new(config_path)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    })
}

fn heartbeat_from_env() -> Option<ProducerHeartbeatEnv> {
    let (Ok(sock), Ok(id), Ok(iv_s), Ok(uri)) = (
        env::var("LOGSPOUT_CONTROL_SOCKET"),
        env::var("LOGSPOUT_SERVER_ID"),
        env::var("LOGSPOUT_HEARTBEAT_INTERVAL_SECS"),
        env::var("LOGSPOUT_CLIENT_CONNECT_URI"),
    ) else {
        return None;
    };
    let heartbeat_interval_secs = iv_s.parse::<u64>().unwrap_or(5).max(1);
    Some(ProducerHeartbeatEnv {
        control_socket: sock,
        server_id: id,
        heartbeat_interval_secs,
        client_connect_uri: uri,
    })
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let base = output_base_for_cli(&cli.config);
    let hb = heartbeat_from_env();
    if let Err(e) = run_producer_at_path(cli.config, base, hb).await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
