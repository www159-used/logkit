//! 编译期嵌入的 agent Kafka YAML fixture；供单测、bench 与集成测试 loader 共用。

use logen_dsl::{worker_config_from_document, KafkaAgentFormat, KafkaConfig, WorkerConfig};

use crate::sink::kafka_agent::{build_runtime_agent_config, RuntimeAgentConfig};
use crate::SinkError;

pub const BENCH_YAML: &str = include_str!("../tests/fixtures/kafka_agent_bench.yaml");
pub const NO_DOMAIN_YAML: &str = include_str!("../tests/fixtures/kafka_agent_no_domain.yaml");

pub fn kafka_config_from_yaml(yaml: &str) -> KafkaConfig {
    let mut doc: serde_yaml::Value = serde_yaml::from_str(yaml)
        .unwrap_or_else(|e| panic!("parse embedded agent kafka yaml: {e}"));
    let cfg: WorkerConfig = worker_config_from_document(&mut doc)
        .unwrap_or_else(|e| panic!("parse embedded agent kafka yaml: {e}"));
    cfg.sink
        .kafka_section()
        .expect("fixture must use sink.type kafka with kafka: section")
        .clone()
}

pub fn agent_runtime_config(
    yaml: &str,
    format: KafkaAgentFormat,
) -> Result<RuntimeAgentConfig, SinkError> {
    let mut k = kafka_config_from_yaml(yaml);
    k.agent
        .as_mut()
        .expect("fixture must include sink.kafka.agent")
        .format = format;
    build_runtime_agent_config(&k)
}
