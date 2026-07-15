//! Worker 实例领域模型：[`WorkerConfig`]、模板渲染、sink 校验。
//!
//! ## 生产入口
//!
//! - [`build_worker_config`]：从已展平的配置文档（`serde_yaml::Value`）构造 [`WorkerConfig`]
//! - [`parse_worker_instance_yaml`]：同上，输入为 YAML 文本（fixture / 遗留路径）
//!
//! [`read_worker_instance_yaml`] 仅用于展开 `include` / `body` 的遗留 YAML 工具链，**不是** `.logen` 启动路径。

mod config_load;
mod config_merge;
mod error;
mod human_duration;
mod human_size;
mod template;
mod worker_config;

pub use config_load::{
    build_worker_config, parse_worker_instance_yaml, read_worker_instance_yaml,
};
#[doc(hidden)]
pub use config_load::{
    load_worker_config, load_worker_config_with_kafka_protocol, worker_config_from_document,
};
pub use error::{ConfigParseError, Error};
#[doc(hidden)]
pub use template::parse_worker_config;
pub use template::{worker_config_to_yaml, TemplateRunner};
pub use worker_config::{
    format_sink_summary, validate_agent_source_id, validate_sink, BodyConfig, FieldSpec,
    KafkaAgentConfig, KafkaAgentFormat, KafkaConfig, KafkaSinkMode, OneOfBranch,
    OneOfTemplateBranch, SinkConfig, TemplateSlot, WorkerConfig,
};
