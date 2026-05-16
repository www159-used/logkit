//! 声明式模板造日志：**`template`** + **`fields`**；**`sink:`** 嵌套块（**`type`**：`kafka` | `file` | `stdout`）。**`output`** 仅 **`file`** 需要；**`kafka`** 配 **`sink.kafka:`**（可选 **`mode: common`**（默认）或 **`agent`**、`headers:` 等）。**`sink.max-size`** 仅 **`file`** 支持：整数（字节）或字符串（如 **`64KiB`**、**`10MiB`**，1024 进制）。
//! 一份 worker 模板配置 **`.yaml`** 对应一棵配置树；**`logen start`** 读入后经 [`parse_template_config`](crate::parse_template_config) 校验并序列化为单份 YAML 交给 daemon / worker。
//!
//! **用户可读规范（mdBook）**：仓库内 `guide/` 目录，`cd guide && mdbook build`（或 `mdbook serve`）生成 HTML。

mod field_spec;
mod error;
mod facade;
mod human_size;
mod worker_config;
mod runner;

pub use field_spec::{FieldSpec, OneOfBranch, OneOfTemplateBranch};
pub use error::{ConfigParseError, Error};
pub use facade::TemplateSlot;
pub use worker_config::{
    validate_agent_source_id, KafkaAgentConfig, KafkaConfig, KafkaSinkMode, SinkConfig,
    TemplateConfig,
};
pub use runner::{
    format_sink_summary, parse_template_config, template_config_to_yaml, validate_template_sink,
    TemplateRunner,
};
