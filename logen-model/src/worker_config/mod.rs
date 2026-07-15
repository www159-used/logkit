//! Worker 实例配置形状（Serde）与字段运行态（[`TemplateRunner`] / [`TemplateSlot`]）。

mod body;
mod branch;
mod field_spec;
mod kafka;
mod sink;
mod slot;
mod worker;

pub use body::BodyConfig;
pub use branch::{OneOfBranch, OneOfTemplateBranch};
pub use field_spec::FieldSpec;
pub use kafka::{
    validate_agent_source_id, KafkaAgentConfig, KafkaAgentFormat, KafkaConfig, KafkaSinkMode,
};
pub use sink::{format_sink_summary, validate_sink, SinkConfig};
pub use slot::{TemplateRunner, TemplateSlot};
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

pub(crate) fn default_one_of_weight() -> u32 {
    1
}

pub(crate) fn default_timestamp_format() -> String {
    "%Y-%m-%d %H:%M:%S".to_string()
}
