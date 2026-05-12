//! TLS metadata 探测与可选单次写入；配置构造见 [`logspout_worker::kafka_smoke::kafka_config_fixture_jks_dir`]。
//!
//! 仓库根目录示例：`./target/release/logspout-kafka-probe --assets-dir logspout-worker/assets --topic MYTOPIC --produce 'hello'`

use std::path::PathBuf;

use clap::Parser;
use logspout_worker::kafka_smoke::{
    kafka_config_fixture_jks_dir, probe_kafka_ssl_cluster, produce_one_kafka_ssl_line,
    FIXTURE_BOOTSTRAP_BROKER,
};

#[derive(Parser)]
#[command(name = "logspout-kafka-probe")]
struct Cli {
    #[arg(long, default_value = FIXTURE_BOOTSTRAP_BROKER)]
    brokers: String,
    #[arg(long, default_value = "assets")]
    assets_dir: PathBuf,
    #[arg(long, default_value = "logspout-kafka-probe")]
    topic: String,
    #[arg(long)]
    produce: Option<String>,
}

fn main() {
    let cli = Cli::parse();
    let k = kafka_config_fixture_jks_dir(&cli.brokers, &cli.topic, &cli.assets_dir, true);
    match probe_kafka_ssl_cluster(&k) {
        Ok((brokers, topics)) => {
            println!("ok: brokers={brokers} topics={topics}");
        }
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
    if let Some(payload) = cli.produce.as_deref() {
        match produce_one_kafka_ssl_line(&k, payload) {
            Ok(()) => {
                println!(
                    "produce: ok topic={} bytes={}",
                    cli.topic.trim(),
                    payload.len()
                );
            }
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
    }
}
