//! 声明式模板造日志：**`template`** + **`fields`**（每项 `type` 对应实现了 [`TemplateSlot`] 门面的槽位，底层常用 [`fake`]），以及可选 **`min-interval`**、**`output`**（由 worker 解析；**仅** `.yaml` / `.yml`）。
//!
//! [`TemplateSlot`]: crate::TemplateSlot

mod builtins;
mod error;
mod facade;
mod runner;

pub use error::{ConfigParseError, Error};
pub use facade::TemplateSlot;
pub use builtins::FieldSpec;
pub use runner::{parse_template_config, TemplateConfig, TemplateRunner};
