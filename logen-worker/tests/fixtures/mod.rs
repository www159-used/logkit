//! 集成测试共享：与本目录 YAML 等静态资源配套的加载逻辑（非库 API）。

use logen_model::KafkaConfig;
use logen_worker::agent_fixtures;

/// 读取编译期嵌入的 `kafka_asset_broker.yaml` 中的 `sink.kafka`（与手工跑 worker 的示例配置同源）。
pub fn kafka_config_from_kafka_asset_broker_yaml() -> KafkaConfig {
    agent_fixtures::kafka_config_from_yaml(include_str!("kafka_asset_broker.yaml"))
}
