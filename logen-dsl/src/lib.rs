mod field_spec;
mod error;
mod facade;
mod human_size;
mod template;
mod worker_config;

pub use field_spec::{FieldSpec, OneOfBranch, OneOfTemplateBranch};
pub use error::{ConfigParseError, Error};
pub use facade::TemplateSlot;
pub use template::{parse_worker_config, worker_config_to_yaml, TemplateRunner};
pub use worker_config::{
    format_sink_summary, validate_agent_source_id, validate_sink, KafkaAgentConfig, KafkaConfig,
    KafkaSinkMode, SinkConfig, WorkerConfig,
};
