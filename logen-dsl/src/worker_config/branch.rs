//! `one-of` 分支：声明（[`OneOfBranch`]）+ 运行态 [`OneOfSlot`]。

use std::collections::BTreeMap;

use logen_branch::BranchPicker;
use serde::{Deserialize, Serialize};

use crate::Error;

use super::field_spec::FieldSpec;
use super::slot::{make_composite_template_slot, CompositeTemplateSlot, TemplateSlot};

fn default_weight() -> u32 {
    1
}

/// [`FieldSpec::OneOf`] 的单分支：字面量、`{ w, v }`，或预编译模板子树。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OneOfBranch {
    Literal(String),
    WeightedLiteral {
        #[serde(default = "default_weight")]
        w: u32,
        v: String,
    },
    Template(OneOfTemplateBranch),
}

/// 含 `template` 的分支；`into_slot` 时预编译 Handlebars + 子字段槽。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OneOfTemplateBranch {
    #[serde(default = "default_weight")]
    pub w: u32,
    pub template: String,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSpec>,
}

enum OneOfArm {
    Literal(String),
    Nested(Box<CompositeTemplateSlot>),
}

pub(crate) struct OneOfSlot {
    picker: BranchPicker,
    arms: Vec<OneOfArm>,
}

impl OneOfSlot {
    pub(crate) fn from_branches(branches: Vec<OneOfBranch>) -> Result<Self, Error> {
        if branches.is_empty() {
            return Err(Error::EmptyOneOfBranches);
        }
        let mut weights = Vec::with_capacity(branches.len());
        let mut arms = Vec::with_capacity(branches.len());
        for b in branches {
            let (w, arm) = match b {
                OneOfBranch::Literal(s) => (1, OneOfArm::Literal(s)),
                OneOfBranch::WeightedLiteral { w, v } => (w, OneOfArm::Literal(v)),
                OneOfBranch::Template(OneOfTemplateBranch {
                    w,
                    template,
                    fields,
                }) => (
                    w,
                    OneOfArm::Nested(Box::new(make_composite_template_slot(
                        template, fields,
                    )?)),
                ),
            };
            weights.push(w);
            arms.push(arm);
        }
        let picker = BranchPicker::new(&weights)?;
        Ok(Self { picker, arms })
    }
}

impl TemplateSlot for OneOfSlot {
    fn next_value(&mut self) -> String {
        let i = self.picker.choose();
        match &mut self.arms[i] {
            OneOfArm::Literal(s) => s.clone(),
            OneOfArm::Nested(c) => c.next_value(),
        }
    }
}
