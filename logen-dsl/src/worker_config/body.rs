//! [`BodyConfig`]：YAML 中日志体须写在 `body:` 下；include 合并时 **整包替换**，不与其他 body 深合并。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::field_spec::FieldSpec;

/// 日志体：`template` + `fields` 成对出现，合并时作为原子单元。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BodyConfig {
    /// Handlebars 源字符串；占位符须与 `fields` 键一致。
    pub template: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, FieldSpec>,
}
