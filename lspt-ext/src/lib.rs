//! 声明式模板造日志：**`template`** + **`fields`**（每项 `type` 对应 [`TemplateSlot`]；**`type: template`** 子树、**`type: one-of`** 在 **字面量 / 内联 template** 间 lazy 多选），以及可选 **`min-interval`**、**`output`**（由 worker 解析；**仅** `.yaml` / `.yml`）。
//!
//! [`TemplateSlot`]: crate::TemplateSlot

mod builtins;
mod error;
mod facade;
mod runner;

pub use error::{ConfigParseError, Error};
pub use facade::TemplateSlot;
pub use builtins::{FieldSpec, OneOfBranch, OneOfTemplateBranch};
pub use runner::{parse_template_config, TemplateConfig, TemplateRunner};
