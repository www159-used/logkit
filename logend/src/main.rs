//! logend — gRPC 控制面入口。

mod registry;
mod serve;
mod session;
mod svc;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use logen_config::load_merged;
use logen_worker::{EmbeddedWorker, TokioEmbeddedWorker};

use serve::{build_worker_runtime, run};

#[derive(Parser)]
#[command(
    name = "logend",
    version,
    about = "logend — gRPC control plane (Unix socket, optional TCP); embedded logen-worker drives worker instances",
    disable_help_subcommand = true
)]
struct LogendCli {
    /// 与 logen 共用的 TOML；也可由环境变量 LOGEN_DEFAULTS_FILE 提供
    #[arg(long, value_name = "PATH", env = "LOGEN_DEFAULTS_FILE")]
    defaults_file: Option<PathBuf>,
}

#[cfg(unix)]
fn main() {
    let cli = LogendCli::parse();
    let cfg = match load_merged(cli.defaults_file.as_deref()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("logend tokio runtime");
    let worker_runtime = match build_worker_runtime(&cfg.logend) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let embedded_worker: Arc<dyn EmbeddedWorker> = Arc::new(TokioEmbeddedWorker::new(
        worker_runtime.handle().clone(),
        rt.handle().clone(),
    ));
    rt.block_on(async {
        if let Err(e) = run(cfg, embedded_worker).await {
            eprintln!("{e}");
            std::process::exit(1);
        }
    });
}

#[cfg(not(unix))]
fn main() {
    eprintln!("logend requires Unix domain sockets");
}
