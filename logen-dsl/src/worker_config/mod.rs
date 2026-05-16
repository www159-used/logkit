//! Worker 模板配置 YAML 中与 Serde 直接对应的形状（不含 `parse_template_config` 或 Handlebars 渲染逻辑）。

mod kafka;
mod sink;
mod template;

pub use kafka::{
    validate_agent_source_id, KafkaAgentConfig, KafkaConfig, KafkaSinkMode,
};
pub use sink::SinkConfig;
pub use template::TemplateConfig;

pub(crate) fn default_min_interval_ms() -> u64 {
    1000
}

pub(crate) fn default_max_size_bytes() -> u64 {
    0
}
