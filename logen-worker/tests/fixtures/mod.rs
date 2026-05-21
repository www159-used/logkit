//! 集成测试共享：与本目录 YAML 等静态资源配套的加载逻辑（非库 API）。

use logen_dsl::{worker_config_from_document, KafkaConfig, WorkerConfig};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "tests/fixtures/"]
#[include = "*.yaml"]
struct KafkaProbeFixtures;

/// 读取编译期嵌入的 `kafka_asset_broker.yaml` 中的 `sink.kafka`（与手工跑 worker 的示例配置同源）。
pub fn kafka_config_from_kafka_asset_broker_yaml() -> KafkaConfig {
    let asset = KafkaProbeFixtures::get("kafka_asset_broker.yaml")
        .expect("embedded kafka_asset_broker.yaml missing");
    let raw = std::str::from_utf8(asset.data.as_ref()).expect("kafka_asset_broker.yaml must be UTF-8");
    let mut doc: serde_yaml::Value = serde_yaml::from_str(raw)
        .unwrap_or_else(|e| panic!("parse embedded kafka_asset_broker.yaml: {e}"));
    let cfg: WorkerConfig = worker_config_from_document(&mut doc)
        .unwrap_or_else(|e| panic!("parse embedded kafka_asset_broker.yaml: {e}"));
    cfg.sink
        .kafka_section()
        .expect("kafka_asset_broker.yaml must use sink.type kafka with kafka: section")
        .clone()
}
