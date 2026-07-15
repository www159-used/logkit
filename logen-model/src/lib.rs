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
mod worker_config;

pub use config_load::{
    build_worker_config, finalize_worker_config, parse_worker_instance_yaml,
    read_worker_instance_yaml,
};
#[doc(hidden)]
pub use config_load::{
    load_worker_config, load_worker_config_with_kafka_protocol, parse_worker_config,
    worker_config_from_document,
};
pub use error::{ConfigParseError, Error};
pub use logen_model_macros::body_preset;
pub use worker_config::{
    format_sink_summary, validate_agent_source_id, validate_sink, BodyConfig, FieldSpec,
    KafkaAgentConfig, KafkaAgentFormat, KafkaConfig, KafkaSinkMode, OneOfBranch,
    OneOfTemplateBranch, SinkConfig, TemplateRunner, TemplateSlot, WorkerConfig,
};
