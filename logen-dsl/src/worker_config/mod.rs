//! Worker 实例配置 YAML 中与 Serde 直接对应的形状（不含 `parse_worker_config` 或 Handlebars 渲染逻辑）。

mod body;
pub mod branch;
pub mod field_spec;
pub mod slot;
mod kafka;
mod sink;
mod worker;

pub use body::BodyConfig;
pub use branch::{OneOfBranch, OneOfTemplateBranch};
pub use field_spec::FieldSpec;
pub use slot::TemplateSlot;
pub use kafka::{
    validate_agent_source_id, KafkaAgentConfig, KafkaConfig, KafkaSinkMode,
};
pub use sink::{format_sink_summary, validate_sink, SinkConfig};
pub use worker::WorkerConfig;

use std::time::Duration;

pub(crate) fn default_min_interval() -> Duration {
    Duration::ZERO
}

pub(crate) fn default_threads() -> u32 {
    1
}

pub(crate) fn default_max_size_bytes() -> u64 {
    0
}
