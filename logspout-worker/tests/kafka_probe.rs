//! Kafka TLS 集成测试：仓库根 `assets/` JKS、metadata 探针、可选 produce。
//!
//! Kafka 连接参数来自 `tests/fixtures/kafka_asset_broker.yaml`（与示例 worker 配置同源）。
//!
//! ```text
//! cargo test -p logspout-worker --test kafka_probe
//! cargo test -p logspout-worker --test kafka_probe print_workspace_tls_assets_dir -- --nocapture
//! ```

mod fixtures;

use std::path::PathBuf;

use logspout_dsl::KafkaConfig;
use logspout_worker::{probe_kafka_ssl_cluster, produce_one_kafka_ssl_line, KafkaLineSink};

use crate::fixtures::kafka_config_from_kafka_asset_broker_yaml;

fn workspace_tls_assets_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("assets")
}

/// 可选环境变量覆盖 YAML：`KAFKA_PROBE_BROKERS`（逗号分隔）、`KAFKA_PROBE_TOPIC`、`KAFKA_PROBE_ASSETS_DIR`（其下须有 `truststore.jks` / `keystore.jks`）。
fn kafka_probe_config_from_yaml_and_env() -> KafkaConfig {
    let mut k = kafka_config_from_kafka_asset_broker_yaml();
    if let Ok(b) = std::env::var("KAFKA_PROBE_BROKERS") {
        let brokers: Vec<String> = b
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        if !brokers.is_empty() {
            k.brokers = Some(brokers);
        }
    }
    if let Ok(t) = std::env::var("KAFKA_PROBE_TOPIC") {
        k.topic = Some(t);
    }
    if let Ok(dir) = std::env::var("KAFKA_PROBE_ASSETS_DIR") {
        let p = PathBuf::from(dir);
        k.ssl_truststore_location = Some(p.join("truststore.jks").to_string_lossy().into_owned());
        k.ssl_keystore_location = Some(p.join("keystore.jks").to_string_lossy().into_owned());
    }
    k
}

#[test]
fn workspace_tls_assets_resolve() {
    let d = workspace_tls_assets_dir();
    assert!(
        d.join("truststore.jks").is_file(),
        "missing {}; 请在仓库根布局下运行",
        d.display()
    );
    assert!(d.join("keystore.jks").is_file(), "missing {}", d.display());
}

#[test]
fn print_workspace_tls_assets_dir() {
    let d = workspace_tls_assets_dir();
    eprintln!("KAFKA_TLS_FIXTURE_ASSETS_DIR={}", d.display());
    assert!(d.is_dir());
}

#[test]
fn kafka_line_sink_try_new_with_jks_fixture() {
    let k = kafka_config_from_kafka_asset_broker_yaml();
    KafkaLineSink::try_new(&k).expect("create Kafka sink with JKS fixture");
}

const PRODUCE_PAYLOAD: &str = "produce one record";

/// 需要可达的 fixture SSL Kafka；可选环境变量覆盖 YAML 中的 bootstrap/topic/JKS 目录。
#[test]
#[ignore = "network: live SSL Kafka; optional KAFKA_PROBE_BROKERS / KAFKA_PROBE_TOPIC / KAFKA_PROBE_ASSETS_DIR / KAFKA_PROBE_PRODUCE"]
fn kafka_probe_metadata_and_optional_produce() {
    let k = kafka_probe_config_from_yaml_and_env();
    let (n_brokers, n_topics) = probe_kafka_ssl_cluster(&k).expect("probe cluster");
    assert!(n_brokers > 0, "expected at least one broker");
    let topic = k.topic.as_deref().unwrap_or("(no topic)");
    produce_one_kafka_ssl_line(&k, PRODUCE_PAYLOAD).expect("produce one line");
    eprintln!("produce ok: topic={topic} topics_meta={n_topics}");
}