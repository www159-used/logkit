//! **门面**（facade）：对外只暴露 [`TemplateSlot`]，由各 [`FieldSpec`](crate::FieldSpec) 通过 [`FieldSpec::into_slot`](crate::FieldSpec::into_slot) 转成 `dyn TemplateSlot`，再由 [`TemplateRunner`](crate::TemplateRunner) 每轮取字符串填模板。
//!
//! [`TemplateSlot`]: crate::TemplateSlot

/// 与 Handlebars 占位符一一对应；每种内置 `type` 对应一种实现，对外只暴露此 trait。
pub trait TemplateSlot: Send {
    fn next_value(&mut self) -> String;
}
