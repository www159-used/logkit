//! `one-of` 分支 **声明**（[`OneOfBranch`] / [`OneOfTemplateBranch`]）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::field_spec::FieldSpec;

/// [`FieldSpec::OneOf`] 的单分支：字面量、`{ w, v }`，或含子 `template` 的模板分支。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OneOfBranch {
    Literal(String),
    WeightedLiteral {
        #[serde(default = "super::default_one_of_weight")]
        w: u32,
        v: String,
    },
    Template(OneOfTemplateBranch),
}

/// 含 `template` 的分支；由 [`super::slot::FieldSpec::into_slot`] 预编译 Handlebars + 子字段槽。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OneOfTemplateBranch {
    #[serde(default = "super::default_one_of_weight")]
    pub w: u32,
    pub template: String,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSpec>,
}
