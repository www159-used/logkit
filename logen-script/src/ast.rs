//! Stmt / Expr AST（未类型标注）。

#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub stmts: Vec<Stmt>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Let { name: String, value: Expr },
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// `name` 或 `a.b.c`
    Path(Vec<String>),
    Str(String),
    /// 有符号整数（`i64`）。
    Int(i64),
    /// 浮点（`f64`）。
    Float(f64),
    /// 已规范化的 humantime 可解析字符串，如 `10ms`。
    Duration(String),
    /// `` `x=${c}` ``
    TemplateLit {
        parts: Vec<TplPart>,
    },
    Call {
        callee: String,
        args: Vec<Arg>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TplPart {
    Text(String),
    /// `${field_expr}`；表达式必须产生 `Field`。
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Arg {
    Positional(Expr),
    Named { name: String, value: Expr },
}
