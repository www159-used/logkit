//! Worker 实例 YAML 模型、`include`/`body` 合并，以及模板渲染。
//!
//! ## 生产解析入口（方案 A）
//!
//! - [`read_worker_instance_yaml`]：CLI（`logen`）展开 `include` / `body`，得到可下发的 YAML 文本
//! - [`parse_worker_instance_yaml`]：daemon（`logend`）唯一一次 typed 解析为 [`WorkerConfig`]
//!
//! 其余 `load_*` / `parse_worker_config` / `worker_config_from_document` 供单测与 fixture，
//! 已标 `#[doc(hidden)]`，**不是**稳定对外 API。

mod config_load;
mod config_merge;
mod error;
mod human_duration;
mod human_size;
mod template;
mod worker_config;

pub use config_load::{parse_worker_instance_yaml, read_worker_instance_yaml};
#[doc(hidden)]
pub use config_load::{
    load_worker_config, load_worker_config_with_kafka_protocol, worker_config_from_document,
};
pub use error::{ConfigParseError, Error};
#[doc(hidden)]
pub use template::parse_worker_config;
pub use template::{worker_config_to_yaml, TemplateRunner};
pub use worker_config::{
    finalize_file_sink_output, format_sink_summary, validate_agent_source_id, validate_sink,
    BodyConfig, FieldSpec, KafkaAgentConfig, KafkaAgentFormat, KafkaConfig, KafkaSinkMode,
    OneOfBranch, OneOfTemplateBranch, SinkConfig, TemplateSlot, WorkerConfig,
};
