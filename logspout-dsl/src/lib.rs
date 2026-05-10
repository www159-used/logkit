//! 声明式模板造日志：**`template`** + **`fields`**（每项 `type` 对应 [`TemplateSlot`]；**`type: template`** / **`type: one-of`** 等），以及可选 **`min-interval`**、**`max-size`（`0` 不限制）**、**`output`**（由 worker 解析；**仅** `.yaml` / `.yml`）。
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
