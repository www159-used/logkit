//! CLI：发现 Kafka 传输配置并写入 `kafka.sink.yaml`（`logen start` 亦会自动合并）。

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser;
use kafka_protocol::{render_kafka_sink_yaml, KafkaProtocolError, KafkaProtocolOptions};

const OUT_FILE: &str = "kafka.sink.yaml";

#[derive(Parser, Debug)]
#[command(
    name = "kafka-protocol",
    about = "从 client.conf 或 server.properties 生成 kafka sink YAML",
    disable_help_subcommand = true
)]
struct Cli {
    /// 显式配置路径（client.conf 或 server.properties）
    #[arg(value_name = "PATH")]
    config_path: Option<PathBuf>,

    #[arg(long = "client-conf", value_name = "PATH", env = "KAFKA_CLIENT_CONF")]
    client_conf: Option<PathBuf>,

    #[arg(
        long = "server-properties",
        value_name = "PATH",
        env = "KAFKA_SERVER_PROPERTIES"
    )]
    server_properties: Option<PathBuf>,

    #[arg(long = "broker-host", value_name = "HOST", env = "LOGEN_HOST")]
    broker_host: Option<String>,

    #[arg(long, value_name = "HOST:PORT,...")]
    brokers: Option<String>,

    #[arg(short = 'o', long, default_value = OUT_FILE)]
    output: PathBuf,
}

fn cli_to_opts(cli: &Cli) -> KafkaProtocolOptions {
    let mut opts = KafkaProtocolOptions {
        client_conf: cli.client_conf.clone(),
        server_properties: cli.server_properties.clone(),
        broker_host: cli.broker_host.clone(),
        brokers: cli.brokers.clone(),
    };
    if opts.client_conf.is_none() && opts.server_properties.is_none() {
        if let Some(path) = &cli.config_path {
            if path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|n| n == "server.properties")
            {
                opts.server_properties = Some(path.clone());
            } else {
                opts.client_conf = Some(path.clone());
            }
        }
    }
    opts
}

fn main() {
    if let Err(e) = run() {
        eprintln!("kafka-protocol: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), KafkaProtocolError> {
    let cli = Cli::parse();
    let opts = cli_to_opts(&cli);
    let yaml = render_kafka_sink_yaml(&opts)?;
    let out = output_path(&cli.output)?;
    fs::write(&out, yaml).map_err(|e| KafkaProtocolError::Io(out.clone(), e))?;
    eprintln!("kafka-protocol: 已写入 {}", out.display());
    Ok(())
}

fn output_path(output: &Path) -> Result<PathBuf, KafkaProtocolError> {
    if output.is_absolute() {
        return Ok(output.to_path_buf());
    }
    Ok(env::current_dir()
        .map_err(|e| KafkaProtocolError::Io(PathBuf::from("."), e))?
        .join(output))
}
