//! 声明式模板造日志：**`template`** + **`fields`**；**`sink:`** 嵌套块（**`type`**：`kafka` | `file` | `stdout`）。**`output`** 仅 **`file`** 需要；**`kafka`** 配 **`sink.kafka:`**（仅声明 worker 识别的键；可选 **`headers:`**）。**`sink.max-size`** 可为整数（字节）或字符串（如 **`64KiB`**、**`10MiB`**，1024 进制）。
//! 可将 schema 与 sink 拆成多个 YAML，由 **`logspout start`**（或 [`load_and_merge_producer_paths`]）合并后序列化为**单份 YAML** 交给 daemon / worker。

mod builtins;
mod error;
mod facade;
mod human_size;
mod runner;

pub use builtins::{FieldSpec, OneOfBranch, OneOfTemplateBranch};
pub use error::{ConfigParseError, Error};
pub use facade::TemplateSlot;
pub use runner::{
    format_sink_summary, load_and_merge_producer_paths, merge_producer_layers,
    parse_template_config, template_config_to_yaml, validate_template_sink, KafkaConfig,
    LineSinkType, ProducerConfigLayer, SinkConfig, TemplateConfig, TemplateRunner,
};
