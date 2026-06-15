//! HTTP → HTTPS 反向代理：按端口映射暴露上游（默认 HTTP :15440→HTTPS :54400、:1940→:9400），mTLS 使用 OEM 证书目录。

mod config;
mod proxy;
mod tls;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use config::{apply_insecure_override, load, tcp_port};
use proxy::Mapping;

#[derive(Parser, Debug)]
#[command(name = "jumpserver", about = "HTTP 反向代理到 HTTPS 上游（OEM mTLS）")]
struct Cli {
    /// YAML 配置（须显式指定）；port_maps 覆盖内置映射
    #[arg(short, long, env = "JUMPSERVER_CONFIG")]
    config: Option<PathBuf>,

    /// 调试日志（等同 RUST_LOG=debug）
    #[arg(short, long)]
    verbose: bool,

    /// 强制不校验上游 HTTPS 服务端证书（默认已 insecure；`-k` 可再显式开启）
    #[arg(short = 'k', long, env = "JUMPSERVER_INSECURE")]
    insecure: bool,
}

fn init_logging(verbose: bool) {
    let mut builder = env_logger::Builder::from_default_env();
    builder.format_timestamp_secs();
    if verbose {
        builder.filter_level(log::LevelFilter::Debug);
    } else if std::env::var_os("RUST_LOG").is_none() {
        builder.filter_level(log::LevelFilter::Info);
    }
    builder.init();
}

fn log_startup(cfg: &config::RuntimeConfig) {
    let oem = resolve_oem::oem_name();
    log::info!(
        target: "jumpserver",
        "OEM={oem} upstream_host={} ca={} cert={} key={}",
        cfg.upstream_host,
        cfg.tls.ca.display(),
        cfg.tls.cert.display(),
        cfg.tls.key.display(),
    );
    for (&listen, &upstream) in &cfg.listen_to_upstream {
        log::info!(
            target: "jumpserver",
            "map https://{}:{upstream} -> http://0.0.0.0:{listen}",
            cfg.upstream_host,
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging(cli.verbose);

    let mut cfg = load(cli.config.as_deref()).map_err(|e| anyhow::anyhow!("{e}"))?;
    apply_insecure_override(&mut cfg, cli.insecure);
    if cfg.tls_insecure {
        log::warn!(
            target: "jumpserver",
            "tls insecure: skip upstream server cert/hostname verification (like curl --insecure)"
        );
    }
    log_startup(&cfg);

    let material = tls::load_material(&cfg.tls)?;
    log::debug!(target: "jumpserver", "TLS material loaded");
    let client = tls::build_http_client(&material, cfg.tls_insecure)?;

    let mut handles = Vec::new();
    for (&listen, &upstream_port) in &cfg.listen_to_upstream {
        let mapping = Mapping {
            listen: tcp_port(listen).map_err(|e| anyhow::anyhow!("{e}"))?,
            upstream_port: tcp_port(upstream_port).map_err(|e| anyhow::anyhow!("{e}"))?,
            upstream_host: cfg.upstream_host.clone(),
            client: client.clone(),
        };
        handles.push(tokio::spawn(async move {
            if let Err(e) = mapping.serve().await {
                log::error!(target: "jumpserver", "listen {listen}: {e:#}");
            }
        }));
    }

    tokio::signal::ctrl_c().await?;
    log::info!(target: "jumpserver", "shutting down");
    for h in handles {
        h.abort();
    }

    Ok(())
}
