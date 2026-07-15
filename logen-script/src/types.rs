//! 静态类型。

use std::fmt;

/// 脚本代数类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Type {
    Body,
    Sink,
    Config,
    Str,
    Int,
    Float,
    Duration,
    /// `counter()` 等字段生成器。
    Field,
    /// `` `…${c}…` `` 插值模板（编译前）。
    Template,
    Unit,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Type::Body => "Body",
            Type::Sink => "Sink",
            Type::Config => "Config",
            Type::Str => "Str",
            Type::Int => "Int",
            Type::Float => "Float",
            Type::Duration => "Duration",
            Type::Field => "Field",
            Type::Template => "Template",
            Type::Unit => "Unit",
        };
        f.write_str(s)
    }
}

/// 形参：名字 + 类型；`optional` 表示可省略。
#[derive(Debug, Clone)]
pub struct Param {
    pub name: &'static str,
    pub ty: Type,
    pub optional: bool,
}

/// 内置函数签名。
#[derive(Debug, Clone)]
pub struct BuiltinSig {
    pub name: &'static str,
    pub params: &'static [Param],
    pub ret: Type,
    /// 若 true，允许仅位置参数且按 `params` 顺序绑定（用于 `preset_json()`）。
    pub allow_positional: bool,
}
