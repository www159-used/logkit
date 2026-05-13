//! 声明式模板造日志：**`template`** + **`fields`**；**`sink:`** 嵌套块（**`type`**：`kafka` | `file` | `stdout`）。**`output`** 仅 **`file`** 需要；**`kafka`** 配 **`sink.kafka:`**（仅声明 worker 识别的键；可选 **`headers:`**）。**`sink.max-size`** 可为整数（字节）或字符串（如 **`64KiB`**、**`10MiB`**，1024 进制）。
//! 一份 producer **`.yaml`** 对应一棵配置树；**`logspout start`** 读入后经 [`parse_template_config`](crate::parse_template_config) 校验并序列化为单份 YAML 交给 daemon / worker。

mod builtins;
mod error;
mod facade;
mod human_size;
mod runner;

pub use builtins::{FieldSpec, OneOfBranch, OneOfTemplateBranch};
pub use error::{ConfigParseError, Error};
pub use facade::TemplateSlot;
pub use runner::{
    format_sink_summary, parse_template_config, template_config_to_yaml, validate_template_sink,
    KafkaConfig, LineSinkType, SinkConfig, TemplateConfig, TemplateRunner,
};
