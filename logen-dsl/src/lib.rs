mod branch;
mod config_load;
mod config_merge;
mod error;
mod facade;
mod field_spec;
mod human_duration;
mod human_size;
mod template;
mod worker_config;

pub use config_load::{load_worker_config, worker_config_from_document};
pub use error::{ConfigParseError, Error};
pub use facade::TemplateSlot;
pub use branch::{OneOfBranch, OneOfTemplateBranch};
pub use field_spec::FieldSpec;
pub use template::{parse_worker_config, worker_config_to_yaml, TemplateRunner};
pub use worker_config::{
    format_sink_summary, validate_agent_source_id, validate_sink, BodyConfig, KafkaAgentConfig,
    KafkaConfig, KafkaSinkMode, SinkConfig, WorkerConfig,
};
